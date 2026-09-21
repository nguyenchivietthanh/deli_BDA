"""Small local replacement for the BigQuery calls used by the SPX BOT.

The existing BOT modules keep their business/API logic. This adapter stores all
tables in one SQLite database beside the scripts and accepts the subset of the
BigQuery REST interface and Standard SQL used by the BOT.
"""

from __future__ import annotations

import json
import re
import sqlite3
import threading
import time
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path


DATABASE_FILE = Path(__file__).resolve().parent / "bot_deli.sqlite3"
_LOCK = threading.RLock()
# One SQLite connection is reused per (database file, thread). create_service()
# is called many times per cycle; opening a fresh connection and re-running the
# PRAGMA block every time was a measurable cost. _LOCK still serializes writes.
_CONNECTION_LOCAL = threading.local()
LOCK_RETRY_ATTEMPTS = 8
LOCK_RETRY_SECONDS = 1.5

# These indexes match the queue/checkpoint queries used by the split BDA/Admin
# workers. They are created lazily after the corresponding table/schema exists,
# so a brand-new SQLite file can still be bootstrapped normally.
PERFORMANCE_INDEXES = {
    "lt_pending_candidate": [
        ("idx_pending_candidate_latest", ("candidate_rule_version", "candidate_id", "candidate_checked_at")),
        ("idx_pending_candidate_due", ("candidate_rule_version", "precheck_at", "final_check_at", "arrived_time")),
        ("idx_pending_candidate_queue", ("queue_stage", "next_check_at", "final_check_at")),
        ("idx_pending_candidate_order", ("order_number", "source_type", "candidate_id")),
        # Dem don theo TO transit moi vong (2026-09-17). Khong co index nay la
        # quet ca bang hon 1,5 trieu dong.
        ("idx_pending_candidate_to", ("to_number", "trip_number")),
    ],
    "lt_pending_result": [
        ("idx_pending_result_latest", ("result_rule_version", "candidate_id", "checked_at")),
        ("idx_pending_result_status", ("pending_status", "processing_stage", "checked_at")),
        ("idx_pending_result_order", ("order_number", "checked_at")),
    ],
    "lt_pending_status_changed": [
        ("idx_pending_changed_order", ("order_number", "status_changed_at")),
    ],
    "lt_ended_trip": [
        ("idx_ended_sequence", ("trip_id", "sequence_number", "observed_at")),
        ("idx_ended_arrived", ("arrived_time", "trip_id", "sequence_number")),
    ],
    "lt_handover_trip": [
        ("idx_handover_sequence", ("source", "trip_id", "sequence_number", "observed_at")),
        ("idx_handover_arrived", ("source", "arrived_time", "trip_id", "sequence_number")),
    ],
    "lt_unit": [
        ("idx_lt_unit_source_sequence", ("source_type", "trip_id", "sequence_number", "to_number")),
        ("idx_lt_unit_trip", ("trip_number", "sequence_number", "arrived_time")),
    ],
    "lt_to_sorting_detail_state": [
        ("idx_to_detail_checkpoint", ("source", "trip_id", "sequence_number", "to_number", "checked_at")),
    ],
    "bot3_reconcile_arrival_cache": [
        ("idx_reconcile_arrival", ("shipment_id", "station_name", "fetched_at")),
    ],
}

# Tables whose writers all mean "latest row per key wins". insert_rows() turns
# an append + later dedup into a single INSERT .. ON CONFLICT DO UPDATE, so the
# table holds exactly one row per key and queries no longer need a
# ROW_NUMBER() OVER (PARTITION BY key) pass over the whole table.
UPSERT_KEYS = {
    "lt_pending_candidate": "candidate_id",
    # One row per Tracking Detail verdict key. Upserting keeps the table at the
    # size of the live working set instead of growing by ~1,000-2,650 rows every
    # BOT 3 cycle, the way bot3_reconcile_arrival_cache does.
    "bot3_tracking_verdict_cache": "cache_key",
}


class _CountIf:
    def __init__(self):
        self.total = 0

    def step(self, value):
        if value:
            self.total += 1

    def finalize(self):
        return self.total


def _quote(name):
    return '"' + str(name).replace('"', '""') + '"'


def _sqlite_type(bigquery_type):
    value = str(bigquery_type or "STRING").upper()
    if value in {"INTEGER", "INT64", "FLOAT", "FLOAT64", "NUMERIC", "BIGNUMERIC", "BOOLEAN"}:
        return "NUMERIC"
    return "TEXT"


def _json_value(value):
    if value is None or isinstance(value, (str, int, float)):
        return value
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat(sep=" ")
    return json.dumps(value, ensure_ascii=False, default=str)


class LocalSqliteStore:
    def __init__(self, path=DATABASE_FILE):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def connection(self):
        cache = getattr(_CONNECTION_LOCAL, "connections", None)
        if cache is None:
            cache = {}
            _CONNECTION_LOCAL.connections = cache
        key = str(self.path)
        conn = cache.get(key)
        if conn is not None:
            return conn

        conn = sqlite3.connect(self.path, timeout=120)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 120000")
        # WAL allows the BDA collector and Admin worker to read/write the same
        # SQLite file with far fewer lock conflicts. SQLite still has one writer
        # at a time, so writes below are deliberately short and retried.
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        # Check-point more eagerly and hard-cap the WAL. Without this the WAL
        # file has grown into the hundreds of MB on this workstation (a reader
        # such as DB Browser blocking the passive auto-checkpoint), which makes
        # every read slow because SQLite must scan the whole WAL.
        conn.execute("PRAGMA wal_autocheckpoint = 400")
        conn.execute("PRAGMA journal_size_limit = 67108864")
        # Keep sorts/temporary CTE work off disk without reserving a large
        # cache. Chrome is the main memory consumer on this workstation.
        conn.execute("PRAGMA temp_store = MEMORY")
        conn.execute("PRAGMA cache_size = -16384")
        conn.create_aggregate("COUNTIF", 1, _CountIf)
        # `with conn:` commits/rolls back but never closes, so the connection is
        # safe to keep and reuse for the life of this thread.
        cache[key] = conn
        return conn

    @staticmethod
    def _table_columns(conn, table):
        return {
            row["name"]
            for row in conn.execute(f"PRAGMA table_info({_quote(table)})").fetchall()
        }

    def _ensure_table_indexes(self, conn, table):
        specs = PERFORMANCE_INDEXES.get(str(table), ())
        if not specs:
            return
        existing_columns = self._table_columns(conn, table)
        for index_name, columns in specs:
            if not set(columns).issubset(existing_columns):
                continue
            column_sql = ", ".join(_quote(column) for column in columns)
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS {_quote(index_name)} "
                f"ON {_quote(table)} ({column_sql})"
            )

    @staticmethod
    def _is_lock_error(error):
        message = str(error).lower()
        return "database is locked" in message or "database is busy" in message

    def _retry_locked(self, action):
        last_error = None
        for attempt in range(LOCK_RETRY_ATTEMPTS):
            try:
                return action()
            except sqlite3.OperationalError as error:
                last_error = error
                if not self._is_lock_error(error) or attempt + 1 >= LOCK_RETRY_ATTEMPTS:
                    raise
                wait_seconds = LOCK_RETRY_SECONDS * (attempt + 1)
                print(
                    "SQLite database locked; retry "
                    f"{attempt + 1}/{LOCK_RETRY_ATTEMPTS - 1} after {wait_seconds:.1f}s"
                )
                time.sleep(wait_seconds)
        raise last_error

    def table_exists(self, table):
        def action():
            with self.connection() as conn:
                return conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
                ).fetchone() is not None
        return self._retry_locked(action)

    def table_fields(self, table):
        if not self.table_exists(table):
            return []
        def action():
            with self.connection() as conn:
                return conn.execute(f"PRAGMA table_info({_quote(table)})").fetchall()
        rows = self._retry_locked(action)
        return [{"name": row["name"], "type": row["type"] or "TEXT", "mode": "NULLABLE"} for row in rows]

    def ensure_table(self, table, fields):
        fields = fields or []
        def action():
            with _LOCK, self.connection() as conn:
                exists = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
                ).fetchone() is not None
                if not exists:
                    column_sql = [f"{_quote(field['name'])} {_sqlite_type(field.get('type'))}" for field in fields]
                    if not column_sql:
                        column_sql = ["_placeholder TEXT"]
                    conn.execute(f"CREATE TABLE {_quote(table)} ({', '.join(column_sql)})")
                existing = {
                    row["name"]
                    for row in conn.execute(f"PRAGMA table_info({_quote(table)})").fetchall()
                }
                for field in fields:
                    if field["name"] not in existing:
                        conn.execute(
                            f"ALTER TABLE {_quote(table)} ADD COLUMN {_quote(field['name'])} {_sqlite_type(field.get('type'))}"
                        )
                self._ensure_table_indexes(conn, table)
        self._retry_locked(action)

    def _ensure_upsert_index(self, conn, table, key_column):
        index_name = f"ux_{table}_{key_column}"
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='index' AND name=?", (index_name,)
        ).fetchone()
        if exists:
            return
        # A UNIQUE index fails if the append-only table still has duplicates.
        # Collapse to the newest row per key first (highest rowid == last write).
        conn.execute(
            f"""
            DELETE FROM {_quote(table)}
            WHERE rowid NOT IN (
              SELECT MAX(rowid) FROM {_quote(table)} GROUP BY {_quote(key_column)}
            )
            """
        )
        conn.execute(
            f"CREATE UNIQUE INDEX {_quote(index_name)} "
            f"ON {_quote(table)} ({_quote(key_column)})"
        )

    def insert_rows(self, table, rows, fields=None):
        if not rows:
            return 0
        field_names = [field["name"] for field in fields or []]
        discovered = []
        for row in rows:
            for key in row:
                if key not in field_names and key not in discovered:
                    discovered.append(key)
        all_names = field_names + discovered
        self.ensure_table(table, [{"name": name, "type": "STRING"} for name in all_names])
        upsert_key = UPSERT_KEYS.get(str(table))
        def action():
            with _LOCK, self.connection() as conn:
                names = [
                    field["name"]
                    for field in conn.execute(f"PRAGMA table_info({_quote(table)})").fetchall()
                    if field["name"] != "_placeholder"
                ]
                placeholders = ", ".join("?" for _ in names)
                sql = (
                    f"INSERT INTO {_quote(table)} "
                    f"({', '.join(_quote(name) for name in names)}) VALUES ({placeholders})"
                )
                if upsert_key and upsert_key in names:
                    self._ensure_upsert_index(conn, table, upsert_key)
                    updates = ", ".join(
                        f"{_quote(name)} = excluded.{_quote(name)}"
                        for name in names
                        if name != upsert_key
                    )
                    sql += f" ON CONFLICT({_quote(upsert_key)}) DO UPDATE SET {updates}"
                conn.executemany(sql, [[_json_value(row.get(name)) for name in names] for row in rows])
        self._retry_locked(action)
        return len(rows)

    def drop_table(self, table):
        self._retry_locked(
            lambda: self._drop_table_once(table)
        )

    def _drop_table_once(self, table):
        with _LOCK, self.connection() as conn:
            conn.execute(f"DROP TABLE IF EXISTS {_quote(table)}")

    def _translate_sql(self, query):
        # The BOT issues a small number of static query templates over and over.
        # Caching the regex translation keeps every cycle from re-running ~8
        # substitutions on identical strings.
        return _translate_sql_cached(query)

    def query(self, query):
        sql = self._translate_sql(query)
        replace_match = re.match(
            r"CREATE\s+OR\s+REPLACE\s+TABLE\s+(\"[^\"]+\")\s+AS\s+(.+)$",
            sql,
            flags=re.I | re.S,
        )
        def action():
            with _LOCK, self.connection() as conn:
                if replace_match:
                    target = replace_match.group(1)
                    target_name = target.strip('"')
                    select_sql = replace_match.group(2)
                    temp = _quote("__replace_" + target_name)
                    conn.execute(f"DROP TABLE IF EXISTS {temp}")
                    conn.execute(f"CREATE TABLE {temp} AS {select_sql}")
                    conn.execute(f"DROP TABLE IF EXISTS {target}")
                    conn.execute(f"ALTER TABLE {temp} RENAME TO {target}")
                    self._ensure_table_indexes(conn, target_name)
                    return [], []
                cursor = conn.execute(sql)
                if not cursor.description:
                    return [], []
                fields = [column[0] for column in cursor.description]
                rows = [dict(row) for row in cursor.fetchall()]
                return fields, rows
        return self._retry_locked(action)


@lru_cache(maxsize=1024)
def _translate_sql_cached(query):
    sql = query.strip().rstrip(";")
    sql = re.sub(
        r"`[^`]+\.([^`.]+)\.([^`.]+)`",
        lambda match: _quote(match.group(2)),
        sql,
    )
    sql = re.sub(r"`([^`]+)`", lambda match: _quote(match.group(1)), sql)
    sql = re.sub(
        r"DATETIME_SUB\(\s*CURRENT_DATETIME\('\s*Asia/Bangkok\s*'\),\s*INTERVAL\s+(\d+)\s+(DAY|HOUR|MINUTE)\s*\)",
        lambda match: f"datetime('now', '+7 hours', '-{match.group(1)} {match.group(2).lower()}s')",
        sql,
        flags=re.I,
    )
    sql = re.sub(r"CURRENT_DATETIME\('\s*Asia/Bangkok\s*'\)", "datetime('now', '+7 hours')", sql, flags=re.I)
    sql = re.sub(
        r"DATETIME_ADD\(\s*([A-Za-z_][A-Za-z0-9_.]*)\s*,\s*INTERVAL\s+(\d+)\s+HOUR\s*\)",
        lambda match: f"datetime({match.group(1)}, '+{match.group(2)} hours')",
        sql,
        flags=re.I,
    )
    sql = re.sub(
        r"DATETIME_SUB\(([^,()]+),\s*INTERVAL\s+(\d+)\s+(DAY|HOUR|MINUTE)\)",
        lambda match: f"datetime({match.group(1)}, '-{match.group(2)} {match.group(3).lower()}s')",
        sql,
        flags=re.I,
    )
    sql = re.sub(
        r"FORMAT_DATETIME\((['\"]%Y-%m-%d %H:%M:%S['\"]),\s*([^()]+?)\)",
        r"strftime(\1, \2)",
        sql,
        flags=re.I,
    )
    sql = re.sub(r"([A-Za-z_][A-Za-z0-9_]*|\*)\.\*\s+EXCEPT\s*\(\s*rn\s*\)", r"\1.*", sql, flags=re.I)
    sql = re.sub(r"\*\s+EXCEPT\s*\(\s*rn\s*\)", "*", sql, flags=re.I)
    return sql


class _Request:
    def __init__(self, callback):
        self.callback = callback

    def execute(self):
        return self.callback()


class _TablesApi:
    def __init__(self, store):
        self.store = store

    def get(self, projectId=None, datasetId=None, tableId=None):
        def callback():
            if not self.store.table_exists(tableId):
                raise RuntimeError(f"Not found: Table {tableId}")
            return {"schema": {"fields": self.store.table_fields(tableId)}}
        return _Request(callback)

    def insert(self, projectId=None, datasetId=None, body=None):
        def callback():
            reference = (body or {}).get("tableReference", {})
            table = reference.get("tableId")
            fields = ((body or {}).get("schema") or {}).get("fields") or []
            self.store.ensure_table(table, fields)
            return {"tableReference": reference}
        return _Request(callback)

    def patch(self, projectId=None, datasetId=None, tableId=None, body=None):
        return _Request(lambda: self.store.ensure_table(tableId, ((body or {}).get("schema") or {}).get("fields") or []))

    def delete(self, projectId=None, datasetId=None, tableId=None):
        return _Request(lambda: self.store.drop_table(tableId))


class _JobsApi:
    def __init__(self, store):
        self.store = store

    def query(self, projectId=None, body=None):
        def callback():
            fields, rows = self.store.query((body or {}).get("query") or "")
            return {
                "schema": {"fields": [{"name": field} for field in fields]},
                "rows": [{"f": [{"v": row.get(field)} for field in fields]} for row in rows],
            }
        return _Request(callback)

    def insert(self, projectId=None, body=None, media_body=None):
        def callback():
            config = (body or {}).get("configuration") or {}
            load = config.get("load") or {}
            destination = load.get("destinationTable") or {}
            table = destination.get("tableId")
            stream = getattr(media_body, "_fd", None)
            if stream is None and hasattr(media_body, "stream"):
                stream = media_body.stream()
            if stream is None:
                raise RuntimeError("SQLite load job missing local upload stream")
            stream.seek(0)
            payload = stream.read()
            if isinstance(payload, bytes):
                payload = payload.decode("utf-8")
            rows = [json.loads(line) for line in payload.splitlines() if line.strip()]
            self.store.insert_rows(table, rows, load.get("schema", {}).get("fields") or [])
            return {"jobReference": {"projectId": projectId or "local", "jobId": "sqlite-load"}}
        return _Request(callback)

    def get(self, **kwargs):
        return _Request(lambda: {"status": {"state": "DONE"}})


class LocalBigQueryService:
    def __init__(self, database_file=DATABASE_FILE):
        self.store = LocalSqliteStore(database_file)

    def tables(self):
        return _TablesApi(self.store)

    def jobs(self):
        return _JobsApi(self.store)


def create_service(database_file=DATABASE_FILE):
    return LocalBigQueryService(database_file)
