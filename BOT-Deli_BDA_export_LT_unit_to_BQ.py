import argparse
import hashlib
import importlib.util
import json
import os
import tempfile
import traceback
from datetime import datetime, timedelta
from pathlib import Path

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

import sqlite_store


BASE_DIR = Path(__file__).resolve().parent
LT_BOT_PATH = BASE_DIR / "BOT-Deli_BDA_arrived_LT.py"
if not LT_BOT_PATH.exists():
    LT_BOT_PATH = Path(r"C:\Users\spxvn25689\Desktop\BOT DELI\BOT-Deli_BDA_arrived_LT.py")

SERVICE_ACCOUNT_FILE = BASE_DIR / "ops-support.json"
if not SERVICE_ACCOUNT_FILE.exists():
    SERVICE_ACCOUNT_FILE = Path(r"C:\Users\spxvn25689\Desktop\Get_Data_Sorting\ops-support.json")

STATE_FILE = BASE_DIR / "bot_deli_bda_export_lt_unit_bq_state.json"
BIGQUERY_PROJECT_ID = "bot-503107"
BIGQUERY_DATASET_ID = "deli_bda"
BIGQUERY_TABLE_ID = "lt_unit"
BIGQUERY_HANDOVER_TRIP_TABLE_ID = "lt_handover_trip"
BIGQUERY_TO_CANDIDATE_TABLE_ID = "lt_to_sorting_pending_candidate"
BIGQUERY_TO_DETAIL_STATE_TABLE_ID = "lt_to_sorting_detail_state"
SOC_CODE = "BD A Mega SOC"
PAGE_COUNT = 100
RUN_INTERVAL_SECONDS = 60 * 60
HANDOVER_LOOKBACK_DAYS = 7
SOURCE_ENDED = "ENDED"
SOURCE_HANDOVER = "HANDOVER"
HANDOVER_SEQUENCE_COMPLETE_SOURCE = "HANDOVER_SEQUENCE_COMPLETE"
HANDOVER_SEQUENCE_COMPLETE_TO_NUMBER = "__SEQUENCE_COMPLETE__"
PENDING_CANDIDATE_RULE_VERSION = "pending_candidate_v20260809_unified_v2"

TO_PENDING_BOT_PATH = BASE_DIR / "BOT-Deli_BDA_check_to_sorting_pending.py"
if not TO_PENDING_BOT_PATH.exists():
    TO_PENDING_BOT_PATH = Path(r"C:\Users\spxvn25689\Desktop\BOT DELI\BOT-Deli_BDA_check_to_sorting_pending.py")


def load_lt_bot():
    spec = importlib.util.spec_from_file_location("bot_deli_bda_arrived_lt", LT_BOT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


LT_BOT = load_lt_bot()
TO_PENDING_BOT = None


def get_to_pending_bot():
    global TO_PENDING_BOT
    if TO_PENDING_BOT is None:
        spec = importlib.util.spec_from_file_location("bot_deli_bda_to_pending_prefilter", TO_PENDING_BOT_PATH)
        TO_PENDING_BOT = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(TO_PENDING_BOT)
    return TO_PENDING_BOT


def is_login_expired_error(error):
    text = str(error).lower()
    return (
        "login credentials expired" in text
        or "retcode\": 401" in text
        or "retcode': 401" in text
        or "http 401" in text
    )


class FmsSession:
    def __init__(self, soc_code):
        self.soc_code = soc_code
        self.headers = None
        self.refresh_headers()

    def refresh_headers(self):
        print("Refresh FMS cookie/header...")
        self.headers = LT_BOT.get_fms_headers(self.soc_code)

    def request_json(self, method, url, payload=None, label="", fail_soft=True, max_retries=None, quiet=False):
        for refresh_attempt in range(1, 3):
            try:
                return LT_BOT.request_json_with_retry(
                    method,
                    url,
                    headers=self.headers,
                    payload=payload or {},
                    fail_soft=False,
                    label=label,
                    max_retries=max_retries or LT_BOT.REQUEST_MAX_RETRIES,
                    quiet=quiet,
                )
            except Exception as e:
                if is_login_expired_error(e) and refresh_attempt < 2:
                    if not quiet:
                        print(f"{label}: FMS login expired. Lay cookie moi roi retry...")
                    self.refresh_headers()
                    continue
                if fail_soft:
                    if not quiet:
                        print(f"{label}: FMS request loi, bo qua record/page nay: {e}")
                    return None
                raise
        return None

    def fetch_list(
        self,
        url_template,
        payload=None,
        count=PAGE_COUNT,
        label="",
        fail_soft=True,
        return_complete=False,
    ):
        all_rows = []
        page = 1
        total = 0
        total_pages = 0
        browser_role = (self.headers or {}).get("__browser_fetch_role") if isinstance(self.headers, dict) else None

        def result(rows, result_total, complete):
            if return_complete:
                return rows, result_total, complete
            return rows, result_total

        while True:
            url = url_template.format(page=page, count=count)
            data = self.request_json(
                "GET",
                url,
                payload=payload or {},
                label=f"{label} | page {page}" if label else f"page {page}",
                fail_soft=fail_soft,
            )
            if data is None:
                if browser_role:
                    print(f"{label or 'FMS list'} loi o page {page}. Bo ca batch de lan sau retry, tranh mat data partial.")
                    return result([], total, False)
                print(f"{label or 'FMS list'} loi o page {page}. Giu lai {len(all_rows)} records da lay duoc.")
                return result(all_rows, total, False)

            data_node = data.get("data") or {}
            if page == 1:
                total = int(data_node.get("total") or 0)
                total_pages = (total + count - 1) // count if total > 0 else 0
                print(f"{label or 'FMS list'} - total: {total} | count/page: {count} | pages: {total_pages}")
                if total == 0:
                    return result([], 0, True)

            current_page_data = data_node.get("list") or []
            if not current_page_data:
                if browser_role and page < total_pages:
                    print(f"{label or 'FMS list'} page {page} rong bat thuong. Bo ca batch de lan sau retry.")
                    return result([], total, False)
                return result(all_rows, total, page >= total_pages)

            all_rows.extend(current_page_data)
            if page >= total_pages:
                break
            page += 1

        return result(all_rows, total, True)


def load_state():
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def datetime_from_text(value):
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def bq_datetime(value):
    dt = datetime_from_text(value) if isinstance(value, str) else value
    if not dt:
        return None
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def read_lt_rows_from_sheet():
    worksheet = LT_BOT.open_target_sheet()
    rows = worksheet.get_all_values()
    if not rows:
        return []

    headers = rows[0]
    while headers and not str(headers[-1]).strip():
        headers.pop()

    result = []
    for row in rows[1:]:
        row_map = {headers[i]: row[i] if i < len(row) else "" for i in range(len(headers))}
        trip_number = (row_map.get("trip_number") or "").strip()
        if trip_number:
            row_map["trip_number"] = trip_number
            row_map["sequence_number"] = str(row_map.get("sequence_number") or "").strip()
            result.append(row_map)
    return result


def lt_sequence_key(row):
    return f"{row.get('trip_number') or ''}|{row.get('sequence_number') or ''}"


def lt_arrived_sort_key(row):
    """Prioritize the oldest valid arrival; keep missing timestamps at the end."""
    arrived_time = datetime_from_text(row.get("arrived_time"))
    return (arrived_time is None, arrived_time or datetime.max, lt_sequence_key(row))


def first_value(data, keys):
    if not isinstance(data, dict):
        return ""
    for key in keys:
        value = data.get(key)
        if value not in (None, ""):
            return str(value).strip()
    for key in keys:
        value = find_nested_value(data, key)
        if value not in (None, ""):
            return str(value).strip()
    return ""


def find_nested_value(data, key):
    if isinstance(data, dict):
        if key in data:
            return data[key]
        for value in data.values():
            found = find_nested_value(value, key)
            if found not in (None, ""):
                return found
    elif isinstance(data, list):
        for value in data:
            found = find_nested_value(value, key)
            if found not in (None, ""):
                return found
    return ""


def find_nested_list(data, key):
    if isinstance(data, dict):
        value = data.get(key)
        if isinstance(value, list):
            return value
        for child in data.values():
            found = find_nested_list(child, key)
            if found:
                return found
    elif isinstance(data, list):
        for child in data:
            found = find_nested_list(child, key)
            if found:
                return found
    return []


def first_list_value(data, keys):
    if not isinstance(data, dict):
        return []
    for key in keys:
        value = data.get(key)
        if isinstance(value, list):
            return value
    for key in keys:
        value = find_nested_list(data, key)
        if value:
            return value
    return []


def normalize_text(value):
    return str(value or "").strip().lower()


def to_path_contains_bda(value):
    """Whether FMS TO Path explicitly includes the BDA station."""
    return normalize_text(SOC_CODE) in normalize_text(value)


def to_int(value, default=0):
    try:
        return int(float(value or default))
    except (TypeError, ValueError):
        return default


def extract_order_id(row):
    return first_value(
        row,
        [
            "fleet_order_id",
            "spx_tracking_number",
            "tracking_number",
            "shipment_id",
            "order_id",
            "order_number",
            "scan_number",
        ],
    )


def create_bq_service():
    return sqlite_store.create_service()


def lt_unit_schema_fields():
    return [
        {"name": "trip_number", "type": "STRING"},
        {"name": "trip_id", "type": "STRING"},
        {"name": "source_type", "type": "STRING"},
        {"name": "sequence_number", "type": "INTEGER"},
        {"name": "station", "type": "STRING"},
        {"name": "to_station", "type": "STRING"},
        {"name": "arrived_time", "type": "DATETIME"},
        {"name": "item_type", "type": "STRING"},
        {"name": "scan_number", "type": "STRING"},
        {"name": "to_number", "type": "STRING"},
        {"name": "to_path", "type": "STRING"},
        {"name": "sender", "type": "STRING"},
        {"name": "receiver", "type": "STRING"},
        {"name": "to_parcel_quantity", "type": "INTEGER"},
        {"name": "loaded_station_name", "type": "STRING"},
        {"name": "unloaded_station_name", "type": "STRING"},
        {"name": "exported_at", "type": "DATETIME"},
    ]


def handover_trip_schema_fields():
    return [
        {"name": "trip_number", "type": "STRING"},
        {"name": "trip_id", "type": "STRING"},
        {"name": "sequence_number", "type": "INTEGER"},
        {"name": "arrived_time", "type": "DATETIME"},
        {"name": "station", "type": "STRING"},
        {"name": "to_station", "type": "STRING"},
        {"name": "trip_station_json", "type": "STRING"},
        {"name": "raw_json", "type": "STRING"},
        {"name": "source", "type": "STRING"},
        {"name": "observed_at", "type": "DATETIME"},
    ]


def to_candidate_schema_fields():
    return [
        {"name": "candidate_id", "type": "STRING"},
        {"name": "source_type", "type": "STRING"},
        {"name": "candidate_source", "type": "STRING"},
        {"name": "trip_number", "type": "STRING"},
        {"name": "trip_id", "type": "STRING"},
        {"name": "sequence_number", "type": "INTEGER"},
        {"name": "arrived_time", "type": "DATETIME"},
        {"name": "station", "type": "STRING"},
        {"name": "to_station", "type": "STRING"},
        {"name": "loaded_station_name", "type": "STRING"},
        {"name": "unloaded_station_name", "type": "STRING"},
        {"name": "to_number", "type": "STRING"},
        {"name": "to_path", "type": "STRING"},
        {"name": "sender", "type": "STRING"},
        {"name": "receiver", "type": "STRING"},
        {"name": "to_parcel_quantity", "type": "INTEGER"},
        {"name": "order_number", "type": "STRING"},
        {"name": "shipment_id", "type": "STRING"},
        {"name": "remark_received_station", "type": "STRING"},
        {"name": "remark", "type": "STRING"},
        {"name": "scan_time", "type": "STRING"},
        {"name": "receive_status", "type": "STRING"},
        {"name": "journey_type", "type": "STRING"},
        {"name": "candidate_reason", "type": "STRING"},
        {"name": "candidate_status", "type": "STRING"},
        {"name": "candidate_rule_version", "type": "STRING"},
        {"name": "candidate_checked_at", "type": "DATETIME"},
    ]


def to_detail_state_schema_fields():
    return [
        {"name": "trip_id", "type": "STRING"},
        {"name": "trip_number", "type": "STRING"},
        {"name": "sequence_number", "type": "INTEGER"},
        {"name": "to_number", "type": "STRING"},
        {"name": "source", "type": "STRING"},
        {"name": "checked_at", "type": "DATETIME"},
    ]


def is_not_found_error(error):
    if isinstance(error, HttpError) and getattr(error.resp, "status", None) == 404:
        return True
    return "not found" in str(error).lower()


def ensure_table_schema(service, table_id, fields):
    try:
        table = service.tables().get(
            projectId=BIGQUERY_PROJECT_ID,
            datasetId=BIGQUERY_DATASET_ID,
            tableId=table_id,
        ).execute()
        existing_fields = table.get("schema", {}).get("fields", [])
        existing_names = {field["name"] for field in existing_fields}
        missing_fields = [field for field in fields if field["name"] not in existing_names]
        if missing_fields:
            service.tables().patch(
                projectId=BIGQUERY_PROJECT_ID,
                datasetId=BIGQUERY_DATASET_ID,
                tableId=table_id,
                body={"schema": {"fields": existing_fields + missing_fields}},
            ).execute()
            print(f"Updated BigQuery schema {table_id}: add {len(missing_fields)} fields")
        return
    except Exception as e:
        if not is_not_found_error(e):
            raise

    service.tables().insert(
        projectId=BIGQUERY_PROJECT_ID,
        datasetId=BIGQUERY_DATASET_ID,
        body={
            "tableReference": {
                "projectId": BIGQUERY_PROJECT_ID,
                "datasetId": BIGQUERY_DATASET_ID,
                "tableId": table_id,
            },
            "schema": {"fields": fields},
        },
    ).execute()
    print(f"Created BigQuery table {BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{table_id}")


def ensure_tables(service):
    ensure_table_schema(service, BIGQUERY_TABLE_ID, lt_unit_schema_fields())
    ensure_table_schema(service, BIGQUERY_HANDOVER_TRIP_TABLE_ID, handover_trip_schema_fields())
    ensure_table_schema(service, BIGQUERY_TO_CANDIDATE_TABLE_ID, to_candidate_schema_fields())
    ensure_table_schema(service, BIGQUERY_TO_DETAIL_STATE_TABLE_ID, to_detail_state_schema_fields())


def load_exported_trip_numbers(service):
    query = f"""
        SELECT DISTINCT trip_number
        FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID}`
        WHERE trip_number IS NOT NULL
    """
    try:
        response = service.jobs().query(
            projectId=BIGQUERY_PROJECT_ID,
            body={"query": query, "useLegacySql": False},
        ).execute()
    except Exception as e:
        if is_not_found_error(e):
            print(f"BigQuery table {BIGQUERY_TABLE_ID} chua ton tai. Export lai tu dau.")
            return None
        raise
    return {
        row["f"][0].get("v")
        for row in response.get("rows", [])
        if row.get("f") and row["f"][0].get("v")
    }


def load_exported_ended_sequence_keys(service):
    query = f"""
        SELECT DISTINCT trip_number || '|' || CAST(sequence_number AS TEXT) AS sequence_key
        FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID}`
        WHERE source_type = '{SOURCE_ENDED}'
          AND trip_number IS NOT NULL
          AND trip_number != ''
          AND sequence_number IS NOT NULL
    """
    try:
        response = service.jobs().query(
            projectId=BIGQUERY_PROJECT_ID,
            body={"query": query, "useLegacySql": False},
        ).execute()
    except Exception as e:
        if is_not_found_error(e):
            return None
        raise
    return {
        row["f"][0].get("v")
        for row in response.get("rows", [])
        if row.get("f") and row["f"][0].get("v")
    }


def load_exported_handover_sequence_keys(service):
    query = f"""
        SELECT DISTINCT trip_id || '|' || CAST(sequence_number AS TEXT) AS sequence_key
        FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID}`
        WHERE source_type = '{SOURCE_HANDOVER}'
          AND trip_id IS NOT NULL
          AND trip_id != ''
          AND sequence_number IS NOT NULL
    """
    try:
        response = service.jobs().query(
            projectId=BIGQUERY_PROJECT_ID,
            body={"query": query, "useLegacySql": False},
        ).execute()
    except Exception as e:
        if is_not_found_error(e):
            return set()
        raise
    return {
        row["f"][0].get("v")
        for row in response.get("rows", [])
        if row.get("f") and row["f"][0].get("v")
    }


def query_rows(service, query):
    """Return BigQuery-compatible query results as dictionaries.

    The local SQLite adapter intentionally exposes the same jobs().query()
    response shape, so this helper is safe for both runtimes.
    """
    response = service.jobs().query(
        projectId=BIGQUERY_PROJECT_ID,
        body={"query": query, "useLegacySql": False},
    ).execute()
    fields = [field.get("name") for field in response.get("schema", {}).get("fields", [])]
    result = []
    for row in response.get("rows", []):
        values = row.get("f", [])
        result.append({
            field: values[index].get("v") if index < len(values) else None
            for index, field in enumerate(fields)
        })
    return result


def load_completed_to_keys(service, trip_id, sequence_number):
    """Reuse only TO detail checkpoints written after candidate storage."""
    # A sequence/inbound marker or a row in lt_unit proves discovery only.
    # Ended also sees TOs absent from the Handover inbound list.
    rows = query_rows(service, f"""
        SELECT DISTINCT to_number
        FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TO_DETAIL_STATE_TABLE_ID}`
        WHERE trip_id = '{str(trip_id).replace(chr(39), chr(39) * 2)}'
          AND sequence_number = {int(sequence_number)}
          AND source IN ('BDA_TO_DETAIL', 'BDA_HANDOVER_TO_DETAIL', 'ADMIN_HANDOVER_TO_DETAIL')
          AND to_number IS NOT NULL
          AND to_number != ''
    """)
    return {str(row['to_number']) for row in rows}


def merge_handover_trip_stations(previous_json, current_json):
    """Keep all known stops when a newer Handover response is incomplete."""
    try:
        previous = json.loads(previous_json or "[]")
    except Exception:
        previous = []
    try:
        current = json.loads(current_json or "[]")
    except Exception:
        current = []
    if not isinstance(previous, list):
        previous = []
    if not isinstance(current, list):
        current = []

    stations_by_sequence = {}
    for index, station in enumerate(previous, start=1):
        if not isinstance(station, dict):
            continue
        sequence = station_sequence_number(station, index)
        stations_by_sequence[sequence] = station
    for index, station in enumerate(current, start=1):
        if not isinstance(station, dict):
            continue
        sequence = station_sequence_number(station, index)
        stations_by_sequence[sequence] = {
            **stations_by_sequence.get(sequence, {}),
            **station,
        }
    return [stations_by_sequence[key] for key in sorted(stations_by_sequence)]


def insert_bq_rows(service, rows, table_id=BIGQUERY_TABLE_ID, schema_fields=None):
    if not rows:
        return 0

    schema_fields = schema_fields or lt_unit_schema_fields()
    total_inserted = 0
    for start in range(0, len(rows), 5000):
        chunk = rows[start:start + 5000]
        temp_path = write_ndjson_file(chunk)
        try:
            for attempt in range(1, 6):
                try:
                    run_bq_load_job(service, temp_path, table_id, schema_fields)
                    break
                except Exception as e:
                    if attempt >= 5:
                        raise
                    print(f"BigQuery load loi lan {attempt}/5: {e}. Cho 3s roi retry...")
                    LT_BOT.ti.sleep(3)
        finally:
            try:
                Path(temp_path).unlink(missing_ok=True)
            except Exception:
                pass
        total_inserted += len(chunk)

    return total_inserted


def write_ndjson_file(rows):
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        suffix=".ndjson",
        delete=False,
    ) as temp_file:
        for row in rows:
            temp_file.write(json.dumps(row, ensure_ascii=False) + "\n")
        return temp_file.name


def run_bq_load_job(service, file_path, table_id=BIGQUERY_TABLE_ID, schema_fields=None):
    schema_fields = schema_fields or lt_unit_schema_fields()
    job_body = {
        "configuration": {
            "load": {
                "destinationTable": {
                    "projectId": BIGQUERY_PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET_ID,
                    "tableId": table_id,
                },
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "writeDisposition": "WRITE_APPEND",
                "createDisposition": "CREATE_IF_NEEDED",
                "schema": {"fields": schema_fields},
                "ignoreUnknownValues": False,
            }
        }
    }
    media = MediaFileUpload(file_path, mimetype="application/octet-stream", resumable=False)
    job = service.jobs().insert(
        projectId=BIGQUERY_PROJECT_ID,
        body=job_body,
        media_body=media,
    ).execute()
    job_ref = job["jobReference"]

    while True:
        get_kwargs = {
            "projectId": job_ref["projectId"],
            "jobId": job_ref["jobId"],
        }
        if job_ref.get("location"):
            get_kwargs["location"] = job_ref["location"]
        job_status = service.jobs().get(**get_kwargs).execute()
        status = job_status.get("status") or {}
        if status.get("state") == "DONE":
            if status.get("errorResult"):
                raise RuntimeError(status)
            return
        LT_BOT.ti.sleep(2)


def stable_insert_id(row):
    key = "|".join(
        str(row.get(column) or "")
        for column in ["source_type", "trip_number", "sequence_number", "item_type", "scan_number", "to_number"]
    )
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


def fetch_first_matching_trip(
    fms_session,
    label,
    url_template,
    trip_number,
    max_pages=150,
    max_retries=None,
    quiet=False,
):
    page = 1
    total_pages = 1
    while page <= total_pages and page <= max_pages:
        data = fms_session.request_json(
            "GET",
            url_template.format(page=page, count=PAGE_COUNT),
            payload={},
            fail_soft=True,
            label=f"{label} | page {page}",
            max_retries=max_retries,
            quiet=quiet,
        )
        if not data:
            return None

        data_node = data.get("data") or {}
        if page == 1:
            total = int(data_node.get("total") or 0)
            total_pages = max(1, min((total + PAGE_COUNT - 1) // PAGE_COUNT, max_pages))
            if not quiet:
                print(f"{label} - total: {total} | scan pages: {total_pages}")

        for trip in data_node.get("list") or []:
            if str(trip.get("trip_number") or "") == str(trip_number):
                return trip

        page += 1

    return None


def find_trip_for_lt(fms_session, lt_row):
    trip_number = lt_row["trip_number"]
    arrived_time = datetime_from_text(lt_row.get("arrived_time"))
    if not arrived_time:
        return None

    window_from = arrived_time - timedelta(hours=4)
    window_to = arrived_time + timedelta(hours=4)
    start_time = LT_BOT.to_epoch_seconds(window_from)
    end_time = LT_BOT.to_epoch_seconds(window_to)
    update_time = LT_BOT.to_epoch_seconds(datetime.now().replace(hour=23, minute=59, second=59, microsecond=0))
    trip_filters = f"&trip_number={trip_number}&keyword={trip_number}"
    route_templates = [
        (
            f"Find LT {trip_number} - first_station=2490",
            "https://spx.shopee.vn/api/admin/transportation/trip/history/list"
            f"?pageno={{page}}&trip_station_status=90&count={{count}}&arrived_time={start_time},{end_time}"
            f"&first_station=2490&mtime=1731430800,{update_time}{trip_filters}",
            1,
        ),
        (
            f"Find LT {trip_number} - first_station=4110, middle_station=2490",
            "https://spx.shopee.vn/api/admin/transportation/trip/history/list"
            f"?pageno={{page}}&trip_station_status=90&count={{count}}&arrived_time={start_time},{end_time}"
            f"&first_station=4110&middle_station=2490&mtime=1731430800,{update_time}{trip_filters}",
            2,
        ),
    ]

    for label, url_template, loaded_sequence_number in route_templates:
        route_retry = 1 if loaded_sequence_number == 2 else LT_BOT.REQUEST_MAX_RETRIES
        trip = fetch_first_matching_trip(
            fms_session,
            label,
            url_template,
            trip_number,
            max_retries=route_retry,
            quiet=True,
        )
        if trip:
            trip["_loaded_sequence_number"] = loaded_sequence_number
            return trip
    return None


def fetch_loading_list(fms_session, trip_id, trip_number, loaded_sequence_number):
    for label, url_prefix in [
        (
            f"History loading list - {trip_number} - sequence {loaded_sequence_number}",
            "https://spx.shopee.vn/api/admin/transportation/trip/history/loading/list",
        ),
        (
            f"Handover loading list - {trip_number} - sequence {loaded_sequence_number}",
            "https://spx.shopee.vn/api/admin/transportation/trip/loading/list",
        ),
    ]:
        rows, total = fms_session.fetch_list(
            url_prefix
            +
            f"?trip_id={trip_id}&pageno={{page}}&count={{count}}"
            f"&loaded_sequence_number={loaded_sequence_number}&type=outbound",
            {},
            label=label,
        )
        if total > 0 or rows:
            return rows
    return []


def fetch_to_detail(fms_session, to_number):
    url_template = (
        "https://spx.shopee.vn/api/in-station/general_to/detail/search"
        f"?pageno={{page}}&to_number={to_number}&count={{count}}"
    )
    first_url = url_template.format(page=1, count=PAGE_COUNT)
    page_one = fms_session.request_json(
        "GET",
        first_url,
        payload={},
        fail_soft=True,
        label=f"General TO detail - {to_number} | page 1",
    )
    data_node = (page_one or {}).get("data") or {}
    detail_rows, total = fms_session.fetch_list(
        url_template,
        {},
        label=f"General TO detail - {to_number}",
    )

    sample = detail_rows[0] if detail_rows else {}
    sender = first_value(
        data_node,
        ["sender", "sender_name", "sender_station_name", "from_station_name", "loaded_station_name"],
    ) or first_value(
        sample,
        ["sender", "sender_name", "sender_station_name", "from_station_name", "loaded_station_name"],
    )
    receiver = first_value(
        data_node,
        ["receiver", "receiver_name", "receiver_station_name", "to_station_name", "unloaded_station_name"],
    ) or first_value(
        sample,
        ["receiver", "receiver_name", "receiver_station_name", "to_station_name", "unloaded_station_name"],
    )
    to_path = first_value(
        data_node,
        ["to_path", "path", "route", "route_name", "transportation_path"],
    ) or first_value(
        sample,
        ["to_path", "path", "route", "route_name", "transportation_path"],
    )

    # `quantity` is the TO's declared parcel quantity. `total` belongs to the
    # paginated detail list and is retained only as a compatibility fallback.
    order_count = to_int(first_value(data_node, ["quantity"]))
    if order_count <= 0:
        forward_quantity = to_int(first_value(data_node, ["forward_quantity"]))
        return_quantity = to_int(first_value(data_node, ["return_quantity"]))
        order_count = forward_quantity + return_quantity
    if order_count <= 0:
        order_count = to_int(total or len(detail_rows))

    return {
        "sender": sender,
        "receiver": receiver,
        "to_path": to_path,
        "order_count": order_count,
        "detail_rows": detail_rows,
    }


def parse_datetime_value(value):
    if value in (None, "", 0, "0", "-"):
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    try:
        number = float(value)
        if number > 10_000_000_000:
            number = number / 1000
        if number > 1_000_000_000:
            return datetime.fromtimestamp(number)
    except (TypeError, ValueError, OSError):
        pass
    text = str(value or "").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(text).replace(tzinfo=None)
    except ValueError:
        return None


def handover_window():
    now = datetime.now()
    window_from = (now - timedelta(days=HANDOVER_LOOKBACK_DAYS)).replace(hour=0, minute=0, second=0, microsecond=0)
    window_to = now.replace(hour=23, minute=59, second=59, microsecond=0)
    return window_from, window_to


def station_name(station):
    return first_value(
        station,
        ["station_name", "name", "station", "loaded_station_name", "unloaded_station_name", "hub_name"],
    )


def station_sequence_number(station, fallback_index):
    # Internal unload counters may not match the Station No. accepted by the
    # pending/inbound endpoints. The trip list order is the reliable fallback.
    sequence = to_int(first_value(station, ["station_no", "station_number"]), default=fallback_index)
    return sequence if sequence > 0 else fallback_index


def station_reference_time(station):
    return parse_datetime_value(first_value(station, [
        "ata",
        "actual_arrival_time",
        "arrival_time",
        "arrived_time",
        "unloaded_time",
        "unload_time",
        "atd",
        "actual_departure_time",
        "departure_time",
        "departed_time",
        "mtime",
        "update_time",
        "updated_time",
    ]))


def station_arrived_time(station):
    """Return an actual arrival at this station, never a departure timestamp."""
    return parse_datetime_value(first_value(station, [
        "ata",
        "actual_arrival_time",
        "arrival_time",
        "arrived_time",
    ]))


def stations_after_bda(stations):
    result = []
    bda_seen = False
    for station in stations or []:
        name = station_name(station)
        if normalize_text(name) == normalize_text(SOC_CODE):
            bda_seen = True
            continue
        if bda_seen and name:
            result.append(station)
    return result


def latest_arrived_time_after_bda(stations):
    arrived_times = [
        station_arrived_time(station)
        for station in stations_after_bda(stations)
    ]
    arrived_times = [value for value in arrived_times if value]
    return max(arrived_times) if arrived_times else None


def fetch_handover_trip_detail(fms_session, trip_id, trip_number):
    for label, url in [
        (
            f"Handover trip detail - {trip_number}",
            f"https://spx.shopee.vn/api/admin/transportation/trip/detail?trip_id={trip_id}&new_process_switch=false",
        ),
        (
            f"Handover history trip detail - {trip_number}",
            f"https://spx.shopee.vn/api/admin/transportation/trip/history/detail?trip_id={trip_id}&new_process_switch=false",
        ),
    ]:
        data = fms_session.request_json("GET", url, payload={}, label=label, fail_soft=True)
        if (data or {}).get("data"):
            return data
    return None


def refresh_single_destination_handover_detail(fms_session, trip):
    """The handover list can hide the destination ATA for a one-stop LT."""
    stations = trip.get("trip_station") or []
    if len(stations_after_bda(stations)) > 1:
        return trip

    detail_data = fetch_handover_trip_detail(
        fms_session,
        trip.get("trip_id") or trip.get("id"),
        trip.get("trip_number") or "",
    )
    detail_stations = first_list_value(
        (detail_data or {}).get("data") or {},
        ["trip_station", "trip_stations", "stations", "station_list", "trip_station_list"],
    )
    if detail_stations:
        trip["trip_station"] = detail_stations
        trip["arrived_time"] = bq_datetime(latest_arrived_time_after_bda(detail_stations))
    return trip


def fetch_handover_trip_candidates(fms_session, limit=None):
    window_from, window_to = handover_window()
    start_time = LT_BOT.to_epoch_seconds(window_from)
    end_time = LT_BOT.to_epoch_seconds(window_to)
    rows, total = fms_session.fetch_list(
        "https://spx.shopee.vn/api/admin/transportation/trip/list"
        f"?mtime={start_time},{end_time}&pageno={{page}}&count={{count}}&query_type=2",
        {},
        label=f"Handover LT list - {window_from:%Y-%m-%d %H:%M} -> {window_to:%Y-%m-%d %H:%M}",
    )

    candidates = []
    for row in rows:
        trip_id = first_value(row, ["trip_id", "id", "lh_trip_id", "linehaul_trip_id"])
        trip_number = first_value(row, ["trip_number", "lh_trip_number", "linehaul_trip_number"])
        if not trip_id or not trip_number:
            continue
        trip_station = first_list_value(row, ["trip_station", "trip_stations", "stations", "station_list", "trip_station_list"])
        candidate = {
            "id": trip_id,
            "trip_id": trip_id,
            "trip_number": trip_number,
            "arrived_time": bq_datetime(latest_arrived_time_after_bda(trip_station)),
            "station": SOC_CODE,
            "to_station": first_value(row, ["to_station", "to_station_name", "last_station_name", "destination_station_name"]),
            "trip_station": trip_station,
            "_raw": row,
            "_source": "handover",
        }
        candidates.append(refresh_single_destination_handover_detail(fms_session, candidate))
        if limit and len(candidates) >= int(limit):
            break

    print(f"Handover LT candidates from BDA API: {len(candidates)} | total API rows: {total}")
    return candidates


def store_handover_trips(service, trips):
    observed_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for trip in trips:
        trip_id = str(trip.get("trip_id") or trip.get("id") or "")
        if not trip_id:
            continue
        for context in handover_export_contexts(trip):
            # Store every arrived receiving stop independently, just like Ended.
            # Example: BDA -> HN -> BN creates trip_id|2 and trip_id|3.
            rows.append({
                "trip_number": trip.get("trip_number") or "",
                "trip_id": trip_id,
                "sequence_number": context.get("unloaded_sequence_number"),
                "arrived_time": bq_datetime(context.get("arrived_time")),
                "station": trip.get("station") or SOC_CODE,
                "to_station": context.get("to_station") or trip.get("to_station") or "",
                "trip_station_json": json.dumps(trip.get("trip_station") or [], ensure_ascii=False),
                "raw_json": json.dumps(trip.get("_raw") or {}, ensure_ascii=False),
                "source": "handover",
                "observed_at": observed_at,
            })

    # A Handover sequence is observed every cycle. Keep a single current row
    # for each trip_id + receiving sequence instead of one growing snapshot.
    existing_rows = query_rows(service, f"""
        WITH latest AS (
          SELECT
            trip_id,
            sequence_number,
            trip_station_json,
            arrived_time,
            to_station,
            ROW_NUMBER() OVER (
              PARTITION BY trip_id, sequence_number
              ORDER BY observed_at DESC
            ) AS rn
          FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_HANDOVER_TRIP_TABLE_ID}`
          WHERE source = 'handover'
        )
        SELECT trip_id, sequence_number, trip_station_json, arrived_time, to_station
        FROM latest
        WHERE rn = 1
    """)
    existing_by_sequence = {
        f"{row.get('trip_id') or ''}|{row.get('sequence_number') or ''}": row
        for row in existing_rows
        if row.get("trip_id") and row.get("sequence_number") is not None
    }
    existing_fingerprints = {
        sequence_key: (
            row.get("trip_station_json") or "",
            row.get("arrived_time") or "",
            row.get("to_station") or "",
        )
        for sequence_key, row in existing_by_sequence.items()
    }
    changed_rows = [
        row for row in rows
        if existing_fingerprints.get(f"{row['trip_id']}|{row['sequence_number']}") != (
            row["trip_station_json"],
            row["arrived_time"] or "",
            row["to_station"],
        )
    ]
    inserted = insert_bq_rows(
        service,
        changed_rows,
        BIGQUERY_HANDOVER_TRIP_TABLE_ID,
        handover_trip_schema_fields(),
    )

    # Compact historic snapshots created by previous versions. If a receiving
    # station changes, the row just written above is the one retained.
    columns = ", ".join(field["name"] for field in handover_trip_schema_fields())
    service.jobs().query(
        projectId=BIGQUERY_PROJECT_ID,
        body={"query": f"""
            CREATE OR REPLACE TABLE `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_HANDOVER_TRIP_TABLE_ID}` AS
            WITH latest AS (
              SELECT
                {columns},
                ROW_NUMBER() OVER (
                  PARTITION BY trip_id, sequence_number
                  ORDER BY observed_at DESC
                ) AS rn
              FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_HANDOVER_TRIP_TABLE_ID}`
              WHERE source = 'handover'
            )
            SELECT {columns}
            FROM latest
            WHERE rn = 1
        """, "useLegacySql": False},
    ).execute()
    unchanged = len(rows) - len(changed_rows)
    if inserted or unchanged:
        print(f"Stored Handover LT sequences: new/updated={inserted} | unchanged={unchanged}")
    return inserted


def handover_export_context(trip):
    contexts = handover_export_contexts(trip)
    return contexts[0] if contexts else None


def handover_export_contexts(trip):
    stations = trip.get("trip_station") or []
    bda_sequence = None
    contexts = []
    not_arrived = 0
    for index, station in enumerate(stations, start=1):
        name = station_name(station)
        sequence = station_sequence_number(station, index)
        if normalize_text(name) == normalize_text(SOC_CODE):
            bda_sequence = sequence
            continue
        if not bda_sequence:
            continue
        arrived_time = station_arrived_time(station)
        if not arrived_time:
            not_arrived += 1
            continue
        contexts.append({
            "loaded_sequence_number": bda_sequence,
            "unloaded_sequence_number": sequence,
            "to_station": name,
            "arrived_time": arrived_time,
        })
    if not contexts:
        print(
            f"Skip handover {trip.get('trip_number')}: chua co ATA/Arrived "
            f"tai diem sau BDA ({not_arrived} chua arrived)"
        )
    return contexts


def fetch_handover_inbound_list(fms_session, trip_id, trip_number, unloaded_sequence_number):
    url_variants = []
    for param_name in ["actual_unloaded_sequence_number", "unloaded_sequence_number"]:
        suffix = (
            f"?trip_id={trip_id}&pageno={{page}}&count={{count}}"
            f"&{param_name}={unloaded_sequence_number}&type=inbound"
        )
        url_variants.extend([
            (
                f"Handover inbound list - {trip_number} - sequence {unloaded_sequence_number} - {param_name}",
                "https://spx.shopee.vn/api/admin/transportation/trip/loading/list" + suffix,
            ),
            (
                f"History inbound list - {trip_number} - sequence {unloaded_sequence_number} - {param_name}",
                "https://spx.shopee.vn/api/admin/transportation/trip/history/loading/list" + suffix,
            ),
        ])

    for label, url_template in url_variants:
        rows, total = fms_session.fetch_list(url_template, {}, label=label)
        if total > 0 or rows:
            return rows
    return []


def source_to_from_inbound_row(row):
    return first_value(row, ["parcel_number", "to_number", "scan_number", "tracking_number"])


def build_handover_to_sorting_rows(fms_session, trip, context, collect_candidates=False):
    """Build Handover TO rows and, for the BDA worker, prefilter TO orders.

    The Admin worker only needs the TO rows.  BOT 1 can reuse the same TO
    Detail response for short Handover trips and stage the suspicious orders
    immediately, avoiding a second TO Detail pass later.
    """
    trip_id = str(trip.get("trip_id") or trip.get("id") or "")
    trip_number = trip.get("trip_number") or ""
    sequence = context["unloaded_sequence_number"]
    inbound_rows = fetch_handover_inbound_list(fms_session, trip_id, trip_number, sequence)
    bq_rows = []
    to_candidates = []
    to_detail_states = []
    exported_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    seen_to = set()

    for inbound_row in inbound_rows:
        to_number = source_to_from_inbound_row(inbound_row)
        if not to_number.startswith("TO") or to_number in seen_to:
            continue

        sender_field = first_value(inbound_row, ["sender_station_name", "sender_station", "loaded_station_name"])
        if sender_field and normalize_text(sender_field) != normalize_text(SOC_CODE):
            continue

        order_count = to_int(first_value(inbound_row, ["number_of_order", "to_parcel_quantity", "to_quantity"]))
        if order_count == 1:
            continue

        to_detail = fetch_to_detail(fms_session, to_number)
        if normalize_text(to_detail.get("sender")) != normalize_text(SOC_CODE):
            print(f"Skip handover transit TO {to_number}: sender={to_detail.get('sender')}")
            continue
        if to_detail.get("order_count") == 1:
            print(f"Skip handover single-order TO {to_number}")
            continue

        seen_to.add(to_number)
        receiver = to_detail.get("receiver") or context.get("to_station") or first_value(
            inbound_row,
            ["receiver_station_name", "receive_station_name", "unloaded_station_name", "actual_unloaded_station_name"],
        )
        common = {
            "trip_number": trip_number,
            "trip_id": trip_id,
            "source_type": SOURCE_HANDOVER,
            "sequence_number": to_int(sequence),
            "station": SOC_CODE,
            "to_station": context.get("to_station") or receiver,
            "arrived_time": bq_datetime(context.get("arrived_time") or trip.get("arrived_time")),
            "item_type": "TO_SORTING",
            "scan_number": to_number,
            "to_number": to_number,
            "to_path": to_detail.get("to_path") or "",
            "sender": to_detail.get("sender") or sender_field or SOC_CODE,
            "receiver": receiver,
            "to_parcel_quantity": to_detail.get("order_count") or order_count,
            "loaded_station_name": to_detail.get("sender") or sender_field or SOC_CODE,
            "unloaded_station_name": receiver,
            "exported_at": exported_at,
        }
        bq_rows.append(common)
        if collect_candidates:
            to_candidates.extend(build_bda_to_candidates(common, to_number, to_detail))
            to_detail_states.append({
                "trip_id": trip_id,
                "trip_number": trip_number,
                "sequence_number": to_int(sequence),
                "to_number": to_number,
                "source": "BDA_HANDOVER_TO_DETAIL",
                "checked_at": exported_at,
            })

    if collect_candidates:
        return bq_rows, to_candidates, to_detail_states
    return bq_rows


def export_handover_trips_to_lt_unit(
    handover_trips,
    admin_session,
    bq_service,
    collect_candidates=False,
):
    """Expand Handover TOs after their LT list was captured under BDA role."""
    exported_sequence_keys = load_exported_handover_sequence_keys(bq_service)
    total_inserted = 0
    exported_sequences = 0
    failed_sequences = []

    for trip in handover_trips:
        contexts = handover_export_contexts(trip)
        stored_sequence = to_int(trip.get("sequence_number"), default=0)
        if stored_sequence:
            contexts = [
                context for context in contexts
                if context["unloaded_sequence_number"] == stored_sequence
            ]
        if not contexts:
            continue
        trip_id = str(trip.get("trip_id") or trip.get("id") or "")
        for context in contexts:
            sequence = context["unloaded_sequence_number"]
            sequence_key = f"{trip_id}|{sequence}"
            if sequence_key in exported_sequence_keys:
                print(f"Skip handover {trip.get('trip_number')} receiving sequence {sequence}: da export lt_unit")
                continue

            try:
                built_rows = build_handover_to_sorting_rows(
                    admin_session,
                    trip,
                    context,
                    collect_candidates=collect_candidates,
                )
                if collect_candidates:
                    bq_rows, to_candidates, to_detail_states = built_rows
                else:
                    bq_rows = built_rows
                    to_candidates = []
                    to_detail_states = []
                inserted = insert_bq_rows(bq_service, bq_rows)
                candidate_inserted = 0
                if to_candidates:
                    candidate_inserted = insert_bq_rows(
                        bq_service,
                        to_candidates,
                        table_id=BIGQUERY_TO_CANDIDATE_TABLE_ID,
                        schema_fields=to_candidate_schema_fields(),
                    )
                if to_detail_states:
                    insert_bq_rows(
                        bq_service,
                        to_detail_states,
                        table_id=BIGQUERY_TO_DETAIL_STATE_TABLE_ID,
                        schema_fields=to_detail_state_schema_fields(),
                    )
                # A successful empty result is still complete. Without this
                # sequence marker, Handover sequences that contain no BDA TO
                # Sorting rows are selected and scanned again on every cycle.
                insert_bq_rows(
                    bq_service,
                    [{
                        "trip_id": trip_id,
                        "trip_number": trip.get("trip_number") or "",
                        "sequence_number": to_int(sequence),
                        "to_number": HANDOVER_SEQUENCE_COMPLETE_TO_NUMBER,
                        "source": HANDOVER_SEQUENCE_COMPLETE_SOURCE,
                        "checked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }],
                    table_id=BIGQUERY_TO_DETAIL_STATE_TABLE_ID,
                    schema_fields=to_detail_state_schema_fields(),
                )
                total_inserted += inserted
                exported_sequences += 1
                exported_sequence_keys.add(sequence_key)
                print(
                    f"Exported handover {trip.get('trip_number')} receiving sequence {sequence}: "
                    f"{inserted} TO Sorting rows | BDA TO staging={candidate_inserted}"
                )
            except Exception:
                failed_sequences.append(f"{trip.get('trip_number')}:{sequence}")
                print(f"Error when exporting handover {trip.get('trip_number')} receiving sequence {sequence}")
                traceback.print_exc()

    print(
        "Handover export done. "
        f"Inserted rows: {total_inserted}. Exported sequences: {exported_sequences}. Failed: {len(failed_sequences)}"
    )
    return total_inserted


def stable_pending_candidate_id(row):
    key = "|".join(str(row.get(column) or "") for column in [
        "source_type",
        "trip_id",
        "sequence_number",
        "order_number",
        "to_number",
    ])
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


def _deferred_to_expand_mode():
    return str(os.getenv("BOT_DELI_TO_TASK_MODE", "")).strip() in ("1", "true", "yes", "on")


def build_bda_to_candidates(common, to_lookup_number, to_detail):
    """Use the TO Detail response already fetched by BDA; do not call it again in Admin."""
    if _deferred_to_expand_mode():
        # Deferred-expand mode: the lt_unit TO_SORTING row above is the queue;
        # candidate rows are created later by collect_lt_unit_to_candidates.
        return []
    to_bot = get_to_pending_bot()
    source_row = {
        "trip_number": common["trip_number"],
        "trip_id": common["trip_id"],
        "arrived_time": common["arrived_time"],
        "station": common["station"],
        "to_station": common["to_station"],
        "loaded_station_name": common["loaded_station_name"],
        "unloaded_station_name": common["unloaded_station_name"],
        "to_number": to_lookup_number,
        "to_path": to_detail.get("to_path") or "",
        "sender": to_detail.get("sender") or "",
        "to_receiver": to_detail.get("receiver") or "",
    }
    candidates = []
    for row in to_bot.build_rows_for_to(source_row, to_detail):
        path_without_bda = bool(normalize_text(row.get("to_path"))) and not to_path_contains_bda(row.get("to_path"))
        if path_without_bda:
            # This TO is physically on a BDA LT but its own path never names
            # BDA. It is a suspected misroute, so use the Transit rule: an
            # existing Received remark proves it has moved; otherwise queue it.
            received_remark = row.get("remark_received_station") or to_bot.extract_received_station(row.get("remark"))
            if received_remark:
                continue
            source_type = "TO_TRANSIT"
            candidate_source = "BDA_LT_TO_PATH_WITHOUT_BDA"
            candidate_reason = "BDA_LT_TO_PATH_WITHOUT_BDA_NO_RECEIVED_REMARK"
        else:
            if row.get("pending_status") not in ("PENDING", "NEED_TRACKING_RECEIVED_CHECK"):
                continue
            source_type = "TO_SORTING"
            candidate_source = "BDA_TO_DETAIL_PREFILTER"
            candidate_reason = row.get("pending_status") or ""
        candidate = {
            "candidate_id": "",
            "source_type": source_type,
            "candidate_source": candidate_source,
            "trip_number": row.get("trip_number") or "",
            "trip_id": str(row.get("trip_id") or ""),
            "sequence_number": to_int(common.get("sequence_number")) or None,
            "arrived_time": row.get("arrived_time") or None,
            "station": row.get("station") or "",
            "to_station": row.get("to_station") or "",
            "loaded_station_name": row.get("loaded_station_name") or "",
            "unloaded_station_name": row.get("unloaded_station_name") or "",
            "to_number": row.get("to_number") or to_lookup_number,
            "to_path": row.get("to_path") or "",
            "sender": row.get("sender") or "",
            "receiver": row.get("to_receiver") or "",
            "to_parcel_quantity": to_int(to_detail.get("order_count")) or None,
            "order_number": row.get("order_number") or "",
            "shipment_id": row.get("shipment_id") or row.get("order_number") or "",
            "remark_received_station": row.get("remark_received_station") or "",
            "remark": row.get("remark") or "",
            "scan_time": row.get("scan_time") or "",
            "receive_status": row.get("receive_status") or "",
            "journey_type": row.get("journey_type") or "",
            "candidate_reason": candidate_reason,
            "candidate_status": "PENDING_CANDIDATE",
            "candidate_rule_version": PENDING_CANDIDATE_RULE_VERSION,
            "candidate_checked_at": row.get("checked_at") or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        candidate["candidate_id"] = stable_pending_candidate_id(candidate)
        candidates.append(candidate)
    return candidates


def build_bq_rows_for_trip(fms_session, lt_row, trip, source_type=SOURCE_ENDED, request_cache=None,
                          completed_to_keys=None):
    trip_number = lt_row["trip_number"]
    trip_id = trip.get("id") or trip.get("trip_id")
    loaded_sequence_number = trip.get("_loaded_sequence_number", 1)
    target_sequence_number = to_int(lt_row.get("sequence_number"), default=0)
    target_station = lt_row.get("to_station") or ""
    if not trip_id:
        print(f"Skip {trip_number}: khong tim thay trip_id")
        return [], [], []
    if target_sequence_number <= 0:
        print(f"Skip {trip_number}: khong co sequence_number tren Sheet LT")
        return [], [], []

    request_cache = request_cache if request_cache is not None else {}
    loading_cache_key = f"{trip_id}|{loaded_sequence_number}"
    loading_rows = request_cache.get(loading_cache_key)
    if loading_rows is None:
        loading_rows = fetch_loading_list(fms_session, trip_id, trip_number, loaded_sequence_number)
        request_cache[loading_cache_key] = loading_rows
    bq_rows = []
    to_candidates = []
    to_detail_states = []
    exported_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    completed_to_keys = completed_to_keys or set()
    reused_to_keys = set()

    for loading_row in loading_rows:
        scan_number = str(loading_row.get("scan_number") or "").strip()
        to_number = str(loading_row.get("to_number") or "").strip()
        common = {
            "trip_number": trip_number,
            "trip_id": str(trip_id),
            "source_type": source_type,
            "sequence_number": target_sequence_number,
            "station": lt_row.get("station") or "",
            "to_station": lt_row.get("to_station") or "",
            "arrived_time": bq_datetime(lt_row.get("arrived_time")),
            "scan_number": scan_number,
            "to_number": to_number,
            "to_parcel_quantity": to_int(loading_row.get("to_parcel_quantity")),
            "loaded_station_name": loading_row.get("loaded_station_name") or "",
            "unloaded_station_name": loading_row.get("unloaded_station_name") or "",
            "exported_at": exported_at,
        }

        if scan_number.startswith("TO"):
            to_lookup_number = to_number or scan_number
            if to_lookup_number in completed_to_keys:
                reused_to_keys.add(to_lookup_number)
                continue
            to_detail = request_cache.get(f"TO|{to_lookup_number}")
            if to_detail is None:
                to_detail = fetch_to_detail(fms_session, to_lookup_number)
                request_cache[f"TO|{to_lookup_number}"] = to_detail
            path_without_bda = bool(normalize_text(to_detail.get("to_path"))) and not to_path_contains_bda(to_detail.get("to_path"))
            if normalize_text(to_detail.get("sender")) != normalize_text(SOC_CODE) and not path_without_bda:
                print(f"Skip transit TO {to_lookup_number}: sender={to_detail.get('sender')}")
                continue
            if path_without_bda:
                print(f"Treat BDA LT TO Path without BDA as TRANSIT {to_lookup_number}: path={to_detail.get('to_path')}")
            if normalize_text(to_detail.get("receiver")) != normalize_text(target_station):
                continue
            if to_detail.get("order_count") == 1:
                order_id = extract_order_id((to_detail.get("detail_rows") or [{}])[0])
                bq_rows.append(
                    {
                        **common,
                        "item_type": "BULKY",
                        "scan_number": order_id or scan_number,
                        "to_number": to_lookup_number,
                        "to_path": to_detail.get("to_path") or "",
                        "sender": to_detail.get("sender") or "",
                        "receiver": to_detail.get("receiver") or "",
                    }
                )
                print(f"Treat single-order TO as BULKY {to_lookup_number}: order={order_id or scan_number}")
                continue
            bq_rows.append(
                {
                    **common,
                    "item_type": "TO_SORTING",
                    "to_number": to_lookup_number,
                    "to_path": to_detail.get("to_path") or "",
                    "sender": to_detail.get("sender") or "",
                    "receiver": to_detail.get("receiver") or "",
                }
            )
            to_candidates.extend(build_bda_to_candidates(common, to_lookup_number, to_detail))
            to_detail_states.append({
                "trip_id": str(trip_id),
                "trip_number": trip_number,
                "sequence_number": target_sequence_number,
                "to_number": to_lookup_number,
                "source": "BDA_TO_DETAIL",
                "checked_at": exported_at,
            })

    if reused_to_keys:
        print(
            f"Reuse completed TO detail {trip_number} sequence {target_sequence_number}: "
            f"{len(reused_to_keys)} TO(s); expand only remaining TOs"
        )
    return bq_rows, to_candidates, to_detail_states


def run_once(
    limit=None,
    trip_number=None,
    capture_handover=True,
    expand_handover_with_admin=True,
):
    requested_trip_number = trip_number.strip() if trip_number else None
    state = load_state()
    bq_service = create_bq_service()
    ensure_tables(bq_service)
    exported_sequence_keys = load_exported_ended_sequence_keys(bq_service)
    if exported_sequence_keys is None:
        exported_sequence_keys = set()
        state_exported = set()
        state["exported_sequence_keys"] = []
        save_state(state)
    else:
        state_exported = set(state.get("exported_sequence_keys") or [])
        exported_sequence_keys.update(state_exported)

    all_lt_rows = read_lt_rows_from_sheet()
    if requested_trip_number:
        lt_rows = [
            row for row in all_lt_rows
            if row.get("trip_number") == requested_trip_number
        ]
        if not lt_rows:
            print(f"Khong tim thay LT {requested_trip_number} trong Google Sheet.")
            return
    else:
        lt_rows = [
            row for row in all_lt_rows
            if lt_sequence_key(row) not in exported_sequence_keys
        ]
        # `station` is the station that LOADED the goods on this leg, and BOT 1
        # only has work when BDA is the one dispatching. A trip that merely
        # passes through or terminates at BDA loads nothing there
        # ("load_quantity": 0 in the FMS trip response), so there is no order to
        # expand - confirmed by the ops team 2026-09-07 for LT0Q954WSPN31,
        # which runs ... -> BDA -> BD B with BDA on the receiving side only.
        # The code agrees: find_trip_for_lt() has just two route templates and
        # both require station 2490 (BDA), so such a sequence can never be
        # matched. Left in, those rows fail forever, and because the queue is
        # sorted oldest-first they get retried at the head of every cycle at a
        # cost of two FMS requests each (fetch_first_matching_trip does not
        # retry an empty result).
        # Measured 2026-09-07 over the 1355 sequences that arrived before that
        # day: all 1094 which exported had station='BD A Mega SOC' and all 261
        # still stuck did not - a clean split, no overlap. Those x2 requests
        # x ~0.8s pacing were burning ~7 min of a 15 min cycle, and the set
        # grew by roughly 130/day.
        # These are NOT sibling legs of a BDA trip pulled in by arrived_LT.py's
        # has_point_in_window rule - checked, and 0 of the 286 belong to a trip
        # that has any BDA leg on the Sheet. They are standalone single-leg
        # trips into BD *B* Mega SOC and other stations, so dropping them here
        # cannot orphan a leg whose siblings we do expand. Why the discovery
        # step collects them at all is a separate, smaller question.
        # A blank station is still attempted. Nothing is written to
        # exported_sequence_keys, so deleting this block restores the previous
        # behaviour with no data lost.
        bda_rows = [
            row for row in lt_rows
            if not normalize_text(row.get("station"))
            or normalize_text(row.get("station")) == normalize_text(SOC_CODE)
        ]
        if len(bda_rows) != len(lt_rows):
            print(
                f"Bo qua {len(lt_rows) - len(bda_rows)} sequence khong xuat phat "
                f"tu {SOC_CODE}: khong tim duoc trip tren FMS"
            )
            lt_rows = bda_rows
        lt_rows.sort(key=lt_arrived_sort_key)
        if limit:
            lt_rows = lt_rows[:limit]

    fms_session = FmsSession(SOC_CODE)
    total_inserted = 0
    exported_this_run = []
    failed_this_run = []
    not_found_this_run = []
    trip_cache = {}
    request_cache = {}

    print(f"LT ended sequence chua export: {len(lt_rows)}")
    for lt_row in lt_rows:
        trip_number = lt_row["trip_number"]
        sequence_key = lt_sequence_key(lt_row)
        sequence_number = lt_row.get("sequence_number") or ""
        try:
            trip = trip_cache.get(trip_number)
            if trip is None:
                trip = find_trip_for_lt(fms_session, lt_row)
                trip_cache[trip_number] = trip
            if not trip:
                not_found_this_run.append(sequence_key)
                failed_this_run.append(sequence_key)
                continue

            bq_rows, to_candidates, to_detail_states = build_bq_rows_for_trip(
                fms_session,
                lt_row,
                trip,
                request_cache=request_cache,
                completed_to_keys=load_completed_to_keys(
                    bq_service,
                    trip.get("id") or trip.get("trip_id"),
                    to_int(lt_row.get("sequence_number")),
                ),
            )
            inserted = insert_bq_rows(bq_service, bq_rows)
            total_inserted += inserted
            # BOT 1 owns this staging table. At the end of its Ended LT run,
            # BOT 1 promotes these rows into the durable pending queue.
            candidate_inserted = insert_bq_rows(
                bq_service,
                to_candidates,
                table_id=BIGQUERY_TO_CANDIDATE_TABLE_ID,
                schema_fields=to_candidate_schema_fields(),
            )
            state_inserted = insert_bq_rows(
                bq_service,
                to_detail_states,
                table_id=BIGQUERY_TO_DETAIL_STATE_TABLE_ID,
                schema_fields=to_detail_state_schema_fields(),
            )
            exported_this_run.append(sequence_key)
            print(
                f"Exported {trip_number} sequence {sequence_number}: lt_unit={inserted} rows | "
                f"BDA TO staging={candidate_inserted} | TO detail checkpoints={state_inserted}"
            )
        except Exception:
            failed_this_run.append(sequence_key)
            print(f"Error when exporting {trip_number} sequence {sequence_number}")
            traceback.print_exc()

        state["exported_sequence_keys"] = sorted(state_exported.union(exported_this_run))
        state["last_run_at"] = datetime.now().isoformat(timespec="seconds")
        state["last_total_inserted"] = total_inserted
        state["last_failed_trip_numbers"] = failed_this_run[-100:]
        save_state(state)

    if not requested_trip_number and capture_handover:
        try:
            # The Handover list belongs to the BDA worker. A separate Admin
            # worker can expand its inbound TOs later without changing the
            # BDA browser profile or delaying Ended LT collection.
            handover_trips = fetch_handover_trip_candidates(fms_session, limit=limit)
            store_handover_trips(bq_service, handover_trips)
            if expand_handover_with_admin:
                admin_session = FmsSession("Admin")
                handover_inserted = export_handover_trips_to_lt_unit(
                    handover_trips,
                    admin_session,
                    bq_service,
                )
                total_inserted += handover_inserted
            else:
                print(
                    "Stored Handover LT only. "
                    "Admin worker will expand inbound TOs in its own cycle."
                )
        except Exception:
            print("Error when capturing/exporting handover LT units")
            traceback.print_exc()

    print(
        "Done. "
        f"Inserted {total_inserted} BigQuery rows. "
        f"Exported ended LT: {len(exported_this_run)}. Failed ended LT: {len(failed_this_run)}. "
        f"Trip lookup unavailable: {len(not_found_this_run)}"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="Gioi han so LT export trong lan chay nay de test.")
    parser.add_argument("--trip", default=None, help="Export dung 1 ma LT cu the, vi du --trip LT0Q7S4U3S6P1.")
    parser.add_argument("--skip-handover", action="store_true", help="Khong lay danh sach LT Handover.")
    parser.add_argument(
        "--capture-handover-only",
        action="store_true",
        help="Chi luu LT Handover duoi quyen BDA; khong chuyen sang Admin de ra TO.",
    )
    parser.add_argument("--once", action="store_true", help="Chay 1 vong roi dung. Neu dung --limit hoac --trip thi mac dinh cung chi chay 1 vong.")
    parser.add_argument("--interval-minutes", type=int, default=60, help="So phut nghi giua 2 lan chay lien tuc.")
    args = parser.parse_args()

    if args.once or args.limit or args.trip:
        run_once(
            limit=args.limit,
            trip_number=args.trip,
            capture_handover=not args.skip_handover,
            expand_handover_with_admin=not args.capture_handover_only,
        )
        return

    interval_seconds = max(1, args.interval_minutes) * 60
    while True:
        try:
            run_once(
                capture_handover=not args.skip_handover,
                expand_handover_with_admin=not args.capture_handover_only,
            )
        except Exception:
            print("Run export BigQuery failed:")
            traceback.print_exc()
        print(f"Sleep {interval_seconds} seconds before next BigQuery export run.")
        LT_BOT.ti.sleep(interval_seconds)


if __name__ == "__main__":
    main()
