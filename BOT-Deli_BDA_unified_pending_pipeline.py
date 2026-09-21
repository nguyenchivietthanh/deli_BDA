import argparse
import hashlib
import importlib.util
import io
import json
import os
import re
import tempfile
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote

import requests
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

import sqlite_store


BASE_DIR = Path(__file__).resolve().parent
BULKY_BOT_PATH = BASE_DIR / "BOT-Deli_BDA_check_bulky_pending.py"
TO_BOT_PATH = BASE_DIR / "BOT-Deli_BDA_check_to_sorting_pending.py"
EXPORT_BOT_PATH = BASE_DIR / "BOT-Deli_BDA_export_LT_unit_to_BQ.py"
for path_name, fallback in [
    ("BULKY_BOT_PATH", r"C:\Users\spxvn25689\Desktop\BOT DELI\BOT-Deli_BDA_check_bulky_pending.py"),
    ("TO_BOT_PATH", r"C:\Users\spxvn25689\Desktop\BOT DELI\BOT-Deli_BDA_check_to_sorting_pending.py"),
    ("EXPORT_BOT_PATH", r"C:\Users\spxvn25689\Desktop\BOT DELI\BOT-Deli_BDA_export_LT_unit_to_BQ.py"),
]:
    path = globals()[path_name]
    if not path.exists():
        globals()[path_name] = Path(fallback)

SERVICE_ACCOUNT_FILE = BASE_DIR / "ops-support.json"
if not SERVICE_ACCOUNT_FILE.exists():
    SERVICE_ACCOUNT_FILE = Path(r"C:\Users\spxvn25689\Desktop\Get_Data_Sorting\ops-support.json")

BIGQUERY_PROJECT_ID = "bot-503107"
BIGQUERY_DATASET_ID = "deli_bda"
BIGQUERY_LT_UNIT_TABLE_ID = "lt_unit"
BIGQUERY_BULKY_CANDIDATE_TABLE_ID = "lt_bulky_pending_candidate"
BIGQUERY_TO_CANDIDATE_TABLE_ID = "lt_to_sorting_pending_candidate"
BIGQUERY_CANDIDATE_TABLE_ID = "lt_pending_candidate"
BIGQUERY_RESULT_TABLE_ID = "lt_pending_result"
BIGQUERY_STATUS_CHANGED_TABLE_ID = "lt_pending_status_changed"
BIGQUERY_HANDOVER_TRIP_TABLE_ID = "lt_handover_trip"
BIGQUERY_ENDED_TRIP_TABLE_ID = "lt_ended_trip"
BIGQUERY_TO_DETAIL_STATE_TABLE_ID = "lt_to_sorting_detail_state"
RECONCILE_ARRIVAL_CACHE_TABLE_ID = "bot3_reconcile_arrival_cache"
TRACKING_VERDICT_CACHE_TABLE_ID = "bot3_tracking_verdict_cache"
# Tracking Detail is the single most expensive thing BOT 3 does: one FMS GET per
# candidate that BatchSearch reports PENDING, ~1,000-2,650 per cycle, at 1.5-2.2s
# each. Measured 2026-09-09: 72% of a day's calls repeat a lookup already made,
# because the in-memory cache only lives for one cycle. Keeping verdicts in
# SQLite for this long removes those repeats - measured saving at 60 minutes is
# 41% of calls (16,503 of 40,629 on 09/09, 38% on 08/09).
#
# The TTL is exactly how long a newly handled order can keep showing as PENDING,
# so it is deliberately short. The cache key carries current_station AND
# order_status_code from BatchSearch (which is re-read every cycle regardless),
# so any scan that moves the parcel or changes its status invalidates the entry
# immediately - the TTL only covers an order that is provably sitting still.
# That extra key part matters: measured 2026-09-09, 773 of the 1,096 orders
# cleared by Tracking Detail (70%) never changed station, so station alone would
# not have invalidated them.
#
# Set to 0 to disable the persistent cache entirely.
#
# 2026-09-17: 60 -> 240. Muc tiet kiem 41% do ngay 09/09 da BIEN MAT sau khi
# nhip hoi lai don PENDING bi chan boi PENDING_RECHECK_MINUTES: log in
# "tracking verdict reused from cache: 0" suot nhieu ngay. Ly do: mot don PENDING
# chi duoc chon hoi lai khi checked_at <= now - PENDING_RECHECK_MINUTES, tuc muc
# dem cua no luc do da gia HON nhip hoi lai - ma TTL 60 luon ngan hon nhip do
# (120, roi 180). Muc dem chet truoc khi co ai can.
#
# TTL phai LON HON nhip hoi lai, co bien cho do tre hang cho: don den han o phut
# 180 nhung chi duoc gap o vong ke tiep, ma mot vong do duoc ~60 phut. Dat dung
# 180 thi muc dem gia dung ~180-240 phut luc can dung -> van gan nhu khong trung.
# 240 = 180 + mot vong.
#
# Cai gia, noi thang: don duoc xu ly ma KHONG doi tram lan ma trang thai (70% so
# don Tracking Detail go ra, do 09/09) co the nam tren sheet lau hon, xau nhat
# khoang hai nhip hoi lai (~6 tieng) thay vi mot. Don doi trang thai sang
# Delivering/Delivered van roi sheet dung nhip, vi BatchSearch van chay moi lan
# hoi lai va khoa dem vo ngay khi order_status_code doi.
#
# Doi PENDING_RECHECK_MINUTES thi PHAI doi so nay theo: TTL = nhip hoi lai + 60.
TRACKING_VERDICT_CACHE_MINUTES = 240
# Deferred-expand mode (BOT_DELI_TO_TASK_MODE=1): rÃ£ LT KHÃ”NG táº¡o candidate Ä‘Æ¡n
# con ngay. collect_lt_unit_to_candidates chá»‰ rÃ£ TO detail + táº¡o candidate khi
# TO Ä‘Ã£ >= DEFERRED_EXPAND_MIN_ARRIVED_HOURS (gáº§n precheck 24h).
DEFERRED_EXPAND_MIN_ARRIVED_HOURS = 20
SOC_CODE = "BD A Mega SOC"
ADMIN_ROLE = "Admin"
ADMIN_HANDOVER_TO_DETAIL_SOURCE = "ADMIN_HANDOVER_TO_DETAIL"
HANDOVER_SEQUENCE_COMPLETE_SOURCE = "HANDOVER_SEQUENCE_COMPLETE"
HANDOVER_SEQUENCE_COMPLETE_TO_NUMBER = "__SEQUENCE_COMPLETE__"
TRACKING_SEARCH_URL = "https://spx.shopee.vn/api/fleet_order/order/tracking_list/search"
TRACKING_INFO_URL = "https://spx.shopee.vn/api/fleet_order/order/detail/tracking_info"
PARCEL_SWEEPER_SCAN_STATUS_CODE = 580
# Tracking Info proves that a parcel has been physically handled downstream
# before it is safe to remove a TO candidate.  Keep this list narrow: OnHold
# here is the base order-level state (5), not every specialized pickup OnHold.
TRACKING_INFO_PHYSICAL_HANDLING_STATUS_CODES = {
    PARCEL_SWEEPER_SCAN_STATUS_CODE,  # Parcel_Sweeper_Scan
    5,    # OnHold
    49,   # LMHub_Assigning
    50,   # LMHub_Assigned
    210,  # LMHub_Packing
    211,  # LMHub_Packed
    68,   # Return_FMHub_Packing
    69,   # Return_FMHub_Packed
    115,  # Return_FMHub_Assigning
    116,  # Return_FMHub_Assigned
    117,  # Return_SOC_Assigning
    119,  # Return_LMHub_Assigning
    417,  # Hub_Assigning
    456,  # Return_Hub_Assigning
    789,  # SP_HD_Assigning
}
TRACKING_INFO_PHYSICAL_HANDLING_STATUS_NAMES = {
    PARCEL_SWEEPER_SCAN_STATUS_CODE: "Parcel_Sweeper_Scan",
    5: "OnHold",
    49: "LMHub_Assigning",
    50: "LMHub_Assigned",
    210: "LMHub_Packing",
    211: "LMHub_Packed",
    68: "Return_FMHub_Packing",
    69: "Return_FMHub_Packed",
    115: "Return_FMHub_Assigning",
    116: "Return_FMHub_Assigned",
    117: "Return_SOC_Assigning",
    119: "Return_LMHub_Assigning",
    417: "Hub_Assigning",
    456: "Return_Hub_Assigning",
    789: "SP_HD_Assigning",
}
CANDIDATE_RULE_VERSION = "pending_candidate_v20260809_unified_v2"
RESULT_RULE_VERSION = "pending_result_v20260826_mass_returned_to_bda_v8"
SEQUENCE_MARKER_ORDER = "__TRIP_SEQUENCE_PENDING_CHECK__"
# check_bulky_pending.py and check_to_sorting_pending.py already use 500 on
# this same tracking_list/search endpoint in production; the unified pipeline
# was left at a more conservative 250. request_with_split() already falls
# back to a binary split if FMS ever rejects a batch, so raising this is safe
# to try - it roughly halves the number of BatchSearch POST calls needed.
BATCH_SIZE = 500
BQ_LOAD_BATCH_SIZE = 50000
STORED_HANDOVER_LOOKBACK_DAYS = 7
# Pending Inbound is a snapshot of what is still unloaded right now, so asking
# about a trip that arrived days ago tells us nothing and still costs an FMS
# request. 2 days matches PENDING_DEADLINE_AFTER_ARRIVED_HOURS = 48 below: past
# that the order has missed its deadline and there is nothing left to action.
# Ops confirmed 2026-09-08 that bulky pending runs to about 10-20 orders at a
# time, so the older sequences are not a meaningful source of findings either.
# The cost of this bound: after an outage longer than 2 days, whatever aged past
# 48h is dropped silently - no log line marks it. Raise it temporarily to sweep
# up after a long outage.
PENDING_INBOUND_MAX_AGE_DAYS = 2
LT_UNIT_HANDOVER_RETENTION_HOURS = 48
# Handover TO data is prepared as soon as the destination has ATA/Arrived.
# Pending is released only after 24 hours, leaving the team time to reconcile
# before the 36-hour SLA deadline.
PENDING_READY_AFTER_ARRIVED_HOURS = 36
PENDING_DEADLINE_AFTER_ARRIVED_HOURS = 48
PRECHECK_AFTER_ARRIVED_HOURS = 24
FINAL_CHECK_LEAD_HOURS = 4
DEFAULT_BATCH_CANDIDATES_PER_CYCLE = 1000
# Backlog catch-up can request a larger cycle. Requests are still sent to FMS
# in BATCH_SIZE chunks, so this only controls how many queue records a cycle drains.
MAX_BATCH_CANDIDATES_PER_CYCLE = 25000
UNKNOWN_RECHECK_MINUTES = 15
PENDING_RECHECK_MINUTES = 180
# Don PENDING da gia thi kiem lai thua hon (ops chot 2026-09-19).
#
# Do 2026-09-19 tren 3.865 don PENDING dang nam tren sheet va toan bo lich su
# doi trang thai: 98% so lan PENDING -> NOT_PENDING xay ra trong 5 ngay dau
# (1.842 + 11.212 + 5.547 lan), sau 7 ngay chi con 388 lan. Trong khi do 401 don
# qua 7 ngay van bi kiem lai 8 lan/ngay, ton khoang 1 gio may moi ngay cho
# Tracking Info.
#
# Qua moc nay thi nhip kiem lai doi tu PENDING_RECHECK_MINUTES sang
# OLD_PENDING_RECHECK_MINUTES. Don duoi moc giu nguyen nhu cu.
OLD_PENDING_AFTER_DAYS = 5
OLD_PENDING_RECHECK_MINUTES = 720
TRACKING_INFO_MAX_RETRIES = 2
TRACKING_INFO_FAILURE_CIRCUIT_BREAKER = 3
TRACKING_ARRIVED_STATUS_CODES = {879, 880, 881, 882, 883, 884, 885, 886, 887, 888}


def load_module(path, module_name):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


BULKY_BOT = load_module(BULKY_BOT_PATH, "bot_deli_bda_check_bulky_pending")
TO_BOT = load_module(TO_BOT_PATH, "bot_deli_bda_check_to_sorting_pending")
EXPORT_BOT = load_module(EXPORT_BOT_PATH, "bot_deli_bda_export_lt_unit_to_bq")


def now_text():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def to_text(value):
    return str(value or "").strip()


def create_bq_service():
    return sqlite_store.create_service()


def first_value(row, keys):
    return BULKY_BOT.first_value(row, keys)


def normalize_station(value):
    return BULKY_BOT.normalize_station(value)


def normalize_journey_type(value):
    return BULKY_BOT.normalize_journey_type(value)


def normalize_bulk_journey_type(candidate_value, item):
    """Keep TO's transfer_direction mapping separate from Bulky's order_direction."""
    # BatchSearch is authoritative for Bulky.  Do not let a stale candidate
    # value (often inherited as Forward) override order_direction: 0=Return,
    # 1=Forward.
    bulk_type = BULKY_BOT.normalize_bulk_journey_type(first_value(item, [
        "order_direction",
        "order_direction_desc",
        "order_direction_name",
    ]))
    if bulk_type:
        return bulk_type
    candidate_type = normalize_journey_type(candidate_value)
    if candidate_type:
        return candidate_type
    return normalize_journey_type(first_value(item, [
        "journey_type",
        "journey_type_desc",
        "journey_type_name",
        "transfer_direction",
        "transfer_direction_desc",
    ]))


def is_bda_sender(value):
    return normalize_station(value) == normalize_station(SOC_CODE)


def to_path_contains_bda(value):
    """True only when the TO Path explicitly records BD A Mega SOC."""
    return normalize_station(SOC_CODE) in to_text(value).lower()


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
    text = to_text(value)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(text).replace(tzinfo=None)
    except ValueError:
        return None


def chunked(rows, size):
    for start in range(0, len(rows), size):
        yield rows[start:start + size]


def candidate_schema_fields():
    return [
        {"name": "candidate_id", "type": "STRING"},
        {"name": "source_type", "type": "STRING"},
        {"name": "candidate_source", "type": "STRING"},
        {"name": "trip_number", "type": "STRING"},
        {"name": "trip_id", "type": "STRING"},
        {"name": "sequence_number", "type": "INT64"},
        {"name": "arrived_time", "type": "DATETIME"},
        {"name": "precheck_at", "type": "DATETIME"},
        {"name": "final_check_at", "type": "DATETIME"},
        {"name": "eligible_at", "type": "DATETIME"},
        {"name": "deadline_at", "type": "DATETIME"},
        {"name": "station", "type": "STRING"},
        {"name": "to_station", "type": "STRING"},
        {"name": "loaded_station_name", "type": "STRING"},
        {"name": "unloaded_station_name", "type": "STRING"},
        {"name": "to_number", "type": "STRING"},
        {"name": "to_path", "type": "STRING"},
        {"name": "sender", "type": "STRING"},
        {"name": "receiver", "type": "STRING"},
        {"name": "to_parcel_quantity", "type": "INT64"},
        {"name": "order_number", "type": "STRING"},
        {"name": "shipment_id", "type": "STRING"},
        {"name": "remark_received_station", "type": "STRING"},
        {"name": "remark", "type": "STRING"},
        {"name": "scan_time", "type": "STRING"},
        {"name": "receive_status", "type": "STRING"},
        {"name": "journey_type", "type": "STRING"},
        {"name": "candidate_reason", "type": "STRING"},
        {"name": "candidate_status", "type": "STRING"},
        {"name": "queue_stage", "type": "STRING"},
        {"name": "precheck_status", "type": "STRING"},
        {"name": "prechecked_at", "type": "DATETIME"},
        {"name": "next_check_at", "type": "DATETIME"},
        {"name": "check_attempt", "type": "INT64"},
        {"name": "last_error", "type": "STRING"},
        {"name": "candidate_rule_version", "type": "STRING"},
        {"name": "candidate_checked_at", "type": "DATETIME"},
    ]


def result_schema_fields():
    return candidate_schema_fields() + [
        {"name": "order_status_code", "type": "INT64"},
        {"name": "order_status", "type": "STRING"},
        {"name": "status", "type": "STRING"},
        {"name": "current_station", "type": "STRING"},
        {"name": "current_to_number", "type": "STRING"},
        {"name": "destination_station", "type": "STRING"},
        {"name": "return_destination", "type": "STRING"},
        {"name": "destination", "type": "STRING"},
        {"name": "total_on_hold_times", "type": "INT64"},
        {"name": "number_of_return_on_hold", "type": "INT64"},
        {"name": "attempt", "type": "INT64"},
        {"name": "pending_status", "type": "STRING"},
        {"name": "result_reason", "type": "STRING"},
        {"name": "result_rule_version", "type": "STRING"},
        {"name": "processing_stage", "type": "STRING"},
        {"name": "publish_after", "type": "DATETIME"},
        {"name": "published_at", "type": "DATETIME"},
        {"name": "checked_at", "type": "DATETIME"},
    ]


def status_changed_schema_fields():
    """History of orders that were published as Pending, then later resolved."""
    return result_schema_fields() + [
        {"name": "change_event_id", "type": "STRING"},
        {"name": "previous_pending_checked_at", "type": "DATETIME"},
        {"name": "previous_published_at", "type": "DATETIME"},
        {"name": "previous_order_status", "type": "STRING"},
        {"name": "previous_current_station", "type": "STRING"},
        {"name": "status_changed_at", "type": "DATETIME"},
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


def ended_trip_schema_fields():
    return [
        {"name": "trip_number", "type": "STRING"},
        {"name": "trip_id", "type": "STRING"},
        {"name": "sequence_number", "type": "INT64"},
        {"name": "arrived_time", "type": "DATETIME"},
        {"name": "station", "type": "STRING"},
        {"name": "to_station", "type": "STRING"},
        {"name": "source", "type": "STRING"},
        {"name": "observed_at", "type": "DATETIME"},
    ]


def to_detail_state_schema_fields():
    return [
        {"name": "trip_id", "type": "STRING"},
        {"name": "trip_number", "type": "STRING"},
        {"name": "sequence_number", "type": "INT64"},
        {"name": "to_number", "type": "STRING"},
        {"name": "source", "type": "STRING"},
        {"name": "checked_at", "type": "DATETIME"},
    ]


def reconcile_arrival_cache_schema_fields():
    return [
        {"name": "cache_key", "type": "STRING"},
        {"name": "shipment_id", "type": "STRING"},
        {"name": "station_name", "type": "STRING"},
        {"name": "station_arrived_at", "type": "DATETIME"},
        {"name": "fetched_at", "type": "DATETIME"},
        {"name": "last_error", "type": "STRING"},
    ]


def tracking_verdict_cache_schema_fields():
    return [
        {"name": "cache_key", "type": "STRING"},
        {"name": "order_number", "type": "STRING"},
        {"name": "tag_kind", "type": "STRING"},
        {"name": "tag_reason", "type": "STRING"},
        {"name": "decided_at", "type": "DATETIME"},
    ]


def tracking_verdict_key(parts):
    """One string key from the in-memory Tracking Detail cache tuple.

    Deliberately not to_text(): that is `str(value or "").strip()`, so it maps
    False and 0 to the empty string. is_bulky=False would then be
    indistinguishable from a missing field, and order_status_code=0 from no
    status at all - two different lookups sharing one cache entry. Caught by
    the round-trip test before this shipped.
    """
    return "|".join("" if part is None else str(part).strip() for part in parts)


def load_tracking_verdict_cache(service):
    """Verdicts still inside their TTL, as {key: (tag_kind, tag_reason)}.

    Loaded once per cycle. A miss simply means the FMS call is made, so a
    failure here can only cost time, never correctness.
    """
    if TRACKING_VERDICT_CACHE_MINUTES <= 0:
        return {}
    cutoff = (datetime.now() - timedelta(minutes=TRACKING_VERDICT_CACHE_MINUTES)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    rows = query_bq(
        service,
        f"""
        SELECT cache_key, tag_kind, tag_reason
          FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{TRACKING_VERDICT_CACHE_TABLE_ID}`
         WHERE decided_at >= '{cutoff}'
        """,
        fail_soft=True,
    )
    return {
        to_text(row.get("cache_key")): (
            to_text(row.get("tag_kind")),
            to_text(row.get("tag_reason")),
        )
        for row in rows
        if to_text(row.get("cache_key")) and to_text(row.get("tag_kind"))
    }


def store_tracking_verdicts(service, rows):
    if service is None or not rows:
        return
    service.store.insert_rows(
        TRACKING_VERDICT_CACHE_TABLE_ID,
        rows,
        fields=tracking_verdict_cache_schema_fields(),
    )


def prune_tracking_verdict_cache(service):
    """Drop verdicts far past their TTL so the table cannot grow unbounded."""
    if service is None or TRACKING_VERDICT_CACHE_MINUTES <= 0:
        return
    cutoff = (
        datetime.now() - timedelta(minutes=TRACKING_VERDICT_CACHE_MINUTES * 4)
    ).strftime("%Y-%m-%d %H:%M:%S")
    query_bq(
        service,
        f"""
        DELETE FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{TRACKING_VERDICT_CACHE_TABLE_ID}`
         WHERE decided_at < '{cutoff}'
        """,
        fail_soft=True,
    )


def deferred_to_expand_mode():
    """When on: rÃ£ LT KHÃ”NG táº¡o candidate Ä‘Æ¡n con; candidate chá»‰ Ä‘Æ°á»£c táº¡o bá»Ÿi
    collect_lt_unit_to_candidates khi TO Ä‘Ã£ >= DEFERRED_EXPAND_MIN_ARRIVED_HOURS
    (~gáº§n precheck). Báº­t báº±ng BOT_DELI_TO_TASK_MODE=1."""
    return str(os.getenv("BOT_DELI_TO_TASK_MODE", "")).strip() in ("1", "true", "yes", "on")


def ensure_table(service, table_id, fields):
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
    except Exception:
        pass

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


_TABLES_ENSURED = False


def ensure_tables(service, force=False):
    # Schema does not change while the process runs, but ensure_tables() is
    # called from several entry points every cycle and each call does a
    # PRAGMA table_info + diff per table. Run the full check once per process.
    global _TABLES_ENSURED
    if _TABLES_ENSURED and not force:
        return
    candidate_fields = candidate_schema_fields()
    for table_id in [
        BIGQUERY_BULKY_CANDIDATE_TABLE_ID,
        BIGQUERY_TO_CANDIDATE_TABLE_ID,
        BIGQUERY_CANDIDATE_TABLE_ID,
    ]:
        ensure_table(service, table_id, candidate_fields)
    ensure_table(service, BIGQUERY_RESULT_TABLE_ID, result_schema_fields())
    ensure_table(service, BIGQUERY_STATUS_CHANGED_TABLE_ID, status_changed_schema_fields())
    ensure_table(service, BIGQUERY_HANDOVER_TRIP_TABLE_ID, handover_trip_schema_fields())
    ensure_table(service, BIGQUERY_ENDED_TRIP_TABLE_ID, ended_trip_schema_fields())
    ensure_table(service, BIGQUERY_TO_DETAIL_STATE_TABLE_ID, to_detail_state_schema_fields())
    ensure_table(
        service,
        RECONCILE_ARRIVAL_CACHE_TABLE_ID,
        reconcile_arrival_cache_schema_fields(),
    )
    ensure_table(
        service,
        TRACKING_VERDICT_CACHE_TABLE_ID,
        tracking_verdict_cache_schema_fields(),
    )
    _TABLES_ENSURED = True


def query_bq(service, query, fail_soft=False):
    try:
        response = service.jobs().query(
            projectId=BIGQUERY_PROJECT_ID,
            body={"query": query, "useLegacySql": False},
        ).execute()
    except Exception:
        if fail_soft:
            return []
        raise
    fields = [field["name"] for field in response.get("schema", {}).get("fields", [])]
    rows = []
    for row in response.get("rows", []):
        rows.append({fields[i]: cell.get("v") for i, cell in enumerate(row.get("f", []))})
    return rows


def write_ndjson_file(rows):
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=".ndjson", delete=False) as temp_file:
        for row in rows:
            temp_file.write(json.dumps(row, ensure_ascii=False) + "\n")
        return temp_file.name


def safe_unlink(path):
    try:
        Path(path).unlink(missing_ok=True)
    except Exception:
        pass


def load_rows_to_bq(service, rows, table_id):
    if not rows:
        return 0
    total = 0
    for batch in chunked(rows, BQ_LOAD_BATCH_SIZE):
        for attempt in range(1, 6):
            temp_path = write_ndjson_file(batch)
            try:
                media = MediaIoBaseUpload(
                    io.BytesIO(Path(temp_path).read_bytes()),
                    mimetype="application/octet-stream",
                    resumable=False,
                )
                job = service.jobs().insert(
                    projectId=BIGQUERY_PROJECT_ID,
                    body={
                        "configuration": {
                            "load": {
                                "destinationTable": {
                                    "projectId": BIGQUERY_PROJECT_ID,
                                    "datasetId": BIGQUERY_DATASET_ID,
                                    "tableId": table_id,
                                },
                                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                                "writeDisposition": "WRITE_APPEND",
                            }
                        }
                    },
                    media_body=media,
                ).execute()
                job_ref = job["jobReference"]
                while True:
                    get_kwargs = {"projectId": job_ref["projectId"], "jobId": job_ref["jobId"]}
                    if job_ref.get("location"):
                        get_kwargs["location"] = job_ref["location"]
                    status = service.jobs().get(**get_kwargs).execute().get("status") or {}
                    if status.get("state") == "DONE":
                        if status.get("errorResult"):
                            raise RuntimeError(status)
                        break
                    EXPORT_BOT.LT_BOT.ti.sleep(2)
                break
            except Exception as e:
                if attempt >= 5:
                    raise
                print(f"BigQuery load {table_id} loi lan {attempt}/5: {e}. Cho 5s roi retry...")
                EXPORT_BOT.LT_BOT.ti.sleep(5)
            finally:
                safe_unlink(temp_path)
        total += len(batch)
    return total


def stable_candidate_id(row):
    key = "|".join(str(row.get(column) or "") for column in [
        "source_type",
        "trip_id",
        "sequence_number",
        "order_number",
        "to_number",
    ])
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


def finalize_candidate(row):
    arrived_at = parse_datetime_value(row.get("arrived_time"))
    if arrived_at:
        row["precheck_at"] = (arrived_at + timedelta(
            hours=PRECHECK_AFTER_ARRIVED_HOURS
        )).strftime("%Y-%m-%d %H:%M:%S")
        row["final_check_at"] = (arrived_at + timedelta(
            hours=PENDING_READY_AFTER_ARRIVED_HOURS - FINAL_CHECK_LEAD_HOURS
        )).strftime("%Y-%m-%d %H:%M:%S")
        row["eligible_at"] = (arrived_at + timedelta(
            hours=PENDING_READY_AFTER_ARRIVED_HOURS
        )).strftime("%Y-%m-%d %H:%M:%S")
        row["deadline_at"] = (arrived_at + timedelta(
            hours=PENDING_DEADLINE_AFTER_ARRIVED_HOURS
        )).strftime("%Y-%m-%d %H:%M:%S")
    else:
        row.setdefault("precheck_at", None)
        row.setdefault("final_check_at", None)
        row.setdefault("eligible_at", None)
        row.setdefault("deadline_at", None)
    row.setdefault("queue_stage", "NEW")
    row.setdefault("precheck_status", None)
    row.setdefault("prechecked_at", None)
    row.setdefault("next_check_at", row.get("precheck_at"))
    row.setdefault("check_attempt", 0)
    row.setdefault("last_error", "")
    row["candidate_id"] = stable_candidate_id(row)
    return row


def get_lt_unit_trip_candidates(service, limit=None):
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    query = f"""
    SELECT
      trip_number,
      trip_id,
      sequence_number,
      FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', MIN(arrived_time)) AS arrived_time,
      MIN(station) AS station,
      MIN(to_station) AS to_station,
      'lt_unit' AS _source
    FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_LT_UNIT_TABLE_ID}`
    WHERE trip_id IS NOT NULL
      AND trip_id != ''
      AND source_type = 'HANDOVER'
    GROUP BY trip_number, trip_id, sequence_number
    ORDER BY MIN(arrived_time), trip_number, sequence_number
    {limit_clause}
    """
    return query_bq(service, query, fail_soft=True)


def store_ended_trip_rows(service, ended_rows):
    observed_at = now_text()
    rows = []
    for row in ended_rows or []:
        trip_id = to_text(row.get("_trip_id") or row.get("trip_id"))
        trip_number = to_text(row.get("trip_number"))
        sequence = EXPORT_BOT.to_int(row.get("sequence_number"), default=0)
        if not trip_id or not trip_number or sequence <= 0:
            continue
        rows.append({
            "trip_number": trip_number,
            "trip_id": trip_id,
            "sequence_number": sequence,
            "arrived_time": row.get("arrived_time") or None,
            "station": row.get("station") or SOC_CODE,
            "to_station": row.get("to_station") or "",
            "source": "ended",
            "observed_at": observed_at,
        })
    inserted = load_rows_to_bq(service, rows, BIGQUERY_ENDED_TRIP_TABLE_ID)
    if inserted:
        print(f"Stored Ended LT sequence rows: {inserted}")
    return inserted


def handover_trip_to_bq_row(trip, observed_at):
    return {
        "trip_number": trip.get("trip_number") or "",
        "trip_id": str(trip.get("trip_id") or ""),
        "arrived_time": trip.get("arrived_time") or None,
        "station": trip.get("station") or SOC_CODE,
        "to_station": trip.get("to_station") or "",
        "trip_station_json": json.dumps(trip.get("trip_station") or [], ensure_ascii=False),
        "raw_json": json.dumps(trip.get("_raw") or {}, ensure_ascii=False),
        "source": "handover",
        "observed_at": observed_at,
    }


def store_handover_trips(service, trips):
    # Keep the same deduplicated Handover snapshot policy as BOT 1.
    return EXPORT_BOT.store_handover_trips(service, trips)


def parse_json_list(value):
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except Exception:
        return []
    return parsed if isinstance(parsed, list) else []


def pending_inbound_pending_clause(alias):
    """Keep only sequences Pending Inbound has never been asked about.

    This MUST be applied inside the SQL, before LIMIT. The caller
    (collect_lh_pending_candidates) also skips already-marked sequences, but it
    does so in Python *after* the limit has been taken, so the oldest finished
    sequences permanently filled the window and no new trip was ever reached:
    measured 2026-09-08, the 10 rows fetched each cycle were the same 01-04/09
    trips, all 10 already marked, while 5,502 sequences had never been asked.
    Bulky detection had produced zero candidates since 2026-09-06 04:21 as a
    result. Same failure shape as the handover work-split leak fixed 06/09 -
    a bounded queue whose head is occupied by items that can never do work.
    """
    return f"""
      AND {alias}.arrived_time IS NOT NULL
      AND {alias}.arrived_time >= DATETIME_SUB(
        CURRENT_DATETIME('Asia/Bangkok'), INTERVAL {PENDING_INBOUND_MAX_AGE_DAYS} DAY)
      AND NOT EXISTS (
        SELECT 1
        FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_RESULT_TABLE_ID}` marker
        WHERE marker.order_number = '{SEQUENCE_MARKER_ORDER}'
          AND marker.result_rule_version = '{RESULT_RULE_VERSION}'
          AND marker.trip_id = {alias}.trip_id
          AND marker.sequence_number = {alias}.sequence_number
      )
    """


def get_stored_handover_trip_candidates(
    service, limit=None, only_unexpanded=False, only_unchecked_pending_inbound=False
):
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    unexpanded_clause = ""
    if only_unchecked_pending_inbound:
        unexpanded_clause += pending_inbound_pending_clause("latest")
    if only_unexpanded:
        # Bot 2 may use --handover-limit. Exclude sequences already expanded
        # from either Handover or Ended. Otherwise old completed rows keep
        # consuming the limit and newer Handover LTs never get expanded.
        #
        # Old Handover snapshots from before a receiving station has ATA do
        # not contain a sequence or arrived_time. Bot 2 cannot expand those
        # rows yet, so do not make it repeatedly scan and log them as skips.
        # Bot 1 keeps the snapshots for Handover -> Ended reconciliation.
        unexpanded_clause = f"""
      AND latest.sequence_number IS NOT NULL
      AND latest.arrived_time IS NOT NULL
      AND NOT EXISTS (
        SELECT 1
        FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_LT_UNIT_TABLE_ID}` expanded
        WHERE expanded.source_type IN ('HANDOVER', 'ENDED')
          AND expanded.trip_id = latest.trip_id
          AND expanded.sequence_number = latest.sequence_number
      )
      AND NOT EXISTS (
        SELECT 1
        FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TO_DETAIL_STATE_TABLE_ID}` completed
        WHERE completed.trip_id = latest.trip_id
          AND completed.sequence_number = latest.sequence_number
          AND completed.to_number = '{HANDOVER_SEQUENCE_COMPLETE_TO_NUMBER}'
          AND completed.source = '{HANDOVER_SEQUENCE_COMPLETE_SOURCE}'
      )
        """
    query = f"""
    WITH latest AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY trip_id, sequence_number
          ORDER BY observed_at DESC
        ) AS rn
      FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_HANDOVER_TRIP_TABLE_ID}`
      WHERE source = 'handover'
        AND observed_at >= DATETIME_SUB(CURRENT_DATETIME('Asia/Bangkok'), INTERVAL {STORED_HANDOVER_LOOKBACK_DAYS} DAY)
    )
    SELECT
      trip_number,
      trip_id,
      sequence_number,
      FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', arrived_time) AS arrived_time,
      station,
      to_station,
      trip_station_json,
      'handover' AS _source
    FROM latest
    WHERE rn = 1
      {unexpanded_clause}
      AND (
        sequence_number IS NOT NULL
        OR NOT EXISTS (
          SELECT 1
          FROM latest current_sequence
          WHERE current_sequence.trip_id = latest.trip_id
            AND current_sequence.rn = 1
            AND current_sequence.sequence_number IS NOT NULL
        )
      )
    ORDER BY arrived_time, trip_number, sequence_number
    {limit_clause}
    """
    rows = query_bq(service, query, fail_soft=True)
    for row in rows:
        row["trip_station"] = parse_json_list(row.get("trip_station_json"))
    return rows


def get_stored_ended_trip_candidates(
    service, limit=None, only_unchecked_pending_inbound=False
):
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    unchecked_clause = (
        pending_inbound_pending_clause("latest")
        if only_unchecked_pending_inbound
        else ""
    )
    query = f"""
    WITH latest AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY trip_id, sequence_number
          ORDER BY observed_at DESC
        ) AS rn
      FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_ENDED_TRIP_TABLE_ID}`
      WHERE source = 'ended'
    )
    SELECT
      trip_number,
      trip_id,
      sequence_number,
      FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', arrived_time) AS arrived_time,
      station,
      to_station,
      'ended' AS _source
    FROM latest
    WHERE rn = 1
      {unchecked_clause}
    ORDER BY arrived_time, trip_number, sequence_number
    {limit_clause}
    """
    return query_bq(service, query, fail_soft=True)


def get_lt_unit_to_candidates(service, limit=None):
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    if deferred_to_expand_mode():
        # BOT 1 no longer stages Ended-TO candidates, so this scan is the single
        # path: include ENDED rows too, and only expand a TO once it is close to
        # its precheck (arrived + 24h) so unreceived-at-handover orders that get
        # received downstream never become a candidate row.
        source_clause = "u.source_type IN ('HANDOVER', 'ENDED')"
        age_clause = (
            "AND u.arrived_time IS NOT NULL "
            f"AND u.arrived_time <= DATETIME_SUB("
            f"CURRENT_DATETIME('Asia/Bangkok'), INTERVAL {DEFERRED_EXPAND_MIN_ARRIVED_HOURS} HOUR)"
        )
    else:
        source_clause = "u.source_type = 'HANDOVER'"
        age_clause = ""
    query = f"""
    WITH admin_handover_to_detail_state AS (
      SELECT
        trip_id,
        sequence_number,
        to_number,
        ROW_NUMBER() OVER (
          PARTITION BY trip_id, sequence_number, to_number
          ORDER BY checked_at DESC
        ) AS rn
      FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TO_DETAIL_STATE_TABLE_ID}`
      WHERE source = '{ADMIN_HANDOVER_TO_DETAIL_SOURCE}'
    )
    SELECT
      u.trip_number,
      u.trip_id,
      u.sequence_number,
      FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', u.arrived_time) AS arrived_time,
      u.station,
      u.to_station,
      u.loaded_station_name,
      u.unloaded_station_name,
      u.to_number,
      u.to_path,
      u.sender,
      u.receiver AS to_receiver
    FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_LT_UNIT_TABLE_ID}` u
    LEFT JOIN admin_handover_to_detail_state s
      ON u.trip_id = s.trip_id
     AND u.sequence_number = s.sequence_number
     AND u.to_number = s.to_number
     AND s.rn = 1
    WHERE u.item_type = 'TO_SORTING'
      AND u.to_number IS NOT NULL
      AND u.to_number != ''
      AND {source_clause}
      AND LOWER(TRIM(u.sender)) = LOWER('{SOC_CODE}')
      AND s.to_number IS NULL
      {age_clause}
    ORDER BY u.arrived_time, u.trip_number, u.to_number
    {limit_clause}
    """
    return query_bq(service, query, fail_soft=True)


def collect_bda_to_detail_candidates(service):
    if deferred_to_expand_mode():
        # BOT 1 no longer stages BDA TO-detail candidates in this mode; Ended
        # TOs are expanded (late) by collect_lt_unit_to_candidates instead.
        return []
    candidate_columns = ",\n      ".join(
        f"b.{field['name']}" for field in candidate_schema_fields()
    )
    query = f"""
    WITH latest_bda_candidate AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY candidate_id
          ORDER BY candidate_checked_at DESC
        ) AS candidate_latest_rank
      FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TO_CANDIDATE_TABLE_ID}`
      WHERE candidate_source = 'BDA_TO_DETAIL_PREFILTER'
        AND candidate_rule_version = '{CANDIDATE_RULE_VERSION}'
    ),
    latest_unified_candidate AS (
      -- upserted table: one row per candidate_id already
      SELECT candidate_id, candidate_checked_at
      FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_CANDIDATE_TABLE_ID}`
      WHERE candidate_rule_version = '{CANDIDATE_RULE_VERSION}'
    )
    SELECT
      {candidate_columns}
    FROM latest_bda_candidate b
    LEFT JOIN latest_unified_candidate u
      ON b.candidate_id = u.candidate_id
    WHERE b.candidate_latest_rank = 1
      AND (u.candidate_id IS NULL OR u.candidate_checked_at < b.candidate_checked_at)
    ORDER BY b.candidate_checked_at, b.trip_number, b.to_number, b.order_number
    """
    rows = query_bq(service, query, fail_soft=True)
    finalized_rows = [finalize_candidate(dict(row)) for row in rows]
    print(f"BDA TO Detail candidates can merge: {len(finalized_rows)}")
    return finalized_rows


def load_sequence_state_for_trips(service, trip_ids):
    checked_sequences = {}
    clean_trip_ids = [str(trip_id) for trip_id in trip_ids if str(trip_id or "").strip()]
    if not clean_trip_ids:
        return checked_sequences

    for batch in chunked(clean_trip_ids, 500):
        quoted = ", ".join("'" + trip_id.replace("'", "''") + "'" for trip_id in batch)
        marker_query = f"""
        WITH latest AS (
          SELECT
            trip_id,
            sequence_number,
            pending_status,
            ROW_NUMBER() OVER (
              PARTITION BY trip_id, sequence_number
              ORDER BY checked_at DESC
            ) AS rn
          FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_RESULT_TABLE_ID}`
          WHERE trip_id IN ({quoted})
            AND order_number = '{SEQUENCE_MARKER_ORDER}'
            AND result_rule_version = '{RESULT_RULE_VERSION}'
            AND sequence_number IS NOT NULL
        )
        SELECT trip_id, sequence_number
        FROM latest
        WHERE rn = 1
        """
        for row in query_bq(service, marker_query, fail_soft=True):
            trip_id = to_text(row.get("trip_id"))
            sequence = EXPORT_BOT.to_int(row.get("sequence_number"), default=0)
            if trip_id and sequence > 0:
                checked_sequences.setdefault(trip_id, set()).add(sequence)

    return checked_sequences


def build_sequence_marker_result(trip, sequence, station_hint, checked_at):
    base = {
        "candidate_id": hashlib.sha1(f"SEQUENCE|{trip.get('trip_id')}|{sequence}".encode("utf-8")).hexdigest(),
        "source_type": "SEQUENCE",
        "candidate_source": trip.get("_source") or "",
        "trip_number": trip.get("trip_number") or "",
        "trip_id": str(trip.get("trip_id") or ""),
        "sequence_number": sequence,
        "arrived_time": trip.get("arrived_time") or None,
        "station": trip.get("station") or "",
        "to_station": trip.get("to_station") or "",
        "loaded_station_name": "",
        "unloaded_station_name": station_hint or "",
        "to_number": "",
        "to_path": "",
        "sender": "",
        "receiver": station_hint or "",
        "order_number": SEQUENCE_MARKER_ORDER,
        "shipment_id": "",
        "remark_received_station": "",
        "remark": "",
        "scan_time": "",
        "receive_status": "",
        "journey_type": "",
        "candidate_reason": "SEQUENCE_CHECKED",
        "candidate_status": "SEQUENCE_CHECKED",
        "candidate_rule_version": CANDIDATE_RULE_VERSION,
        "candidate_checked_at": checked_at,
    }
    return {
        **base,
        "order_status_code": None,
        "order_status": "",
        "status": "",
        "current_station": "",
        "current_to_number": "",
        "destination_station": "",
        "return_destination": "",
        "destination": "",
        "total_on_hold_times": None,
        "number_of_return_on_hold": None,
        "attempt": None,
        "pending_status": "NOT_PENDING",
        "result_reason": "sequence checked",
        "result_rule_version": RESULT_RULE_VERSION,
        "checked_at": checked_at,
    }


def build_bulky_candidate(trip, source_row, sequence, station_hint, checked_at):
    scan_number = BULKY_BOT.source_scan_number(source_row)
    to_number = BULKY_BOT.source_to_number(source_row)
    sender, receiver, to_path = BULKY_BOT.source_route_info(trip, source_row, station_hint)
    if not scan_number.startswith("SPX"):
        return None
    if not is_bda_sender(sender):
        return None
    return finalize_candidate({
        "candidate_id": "",
        "source_type": "BULKY",
        "candidate_source": "LH_PENDING_SPX",
        "trip_number": trip.get("trip_number") or "",
        "trip_id": str(trip.get("trip_id") or ""),
        "sequence_number": sequence,
        "arrived_time": trip.get("arrived_time") or None,
        "station": trip.get("station") or "",
        "to_station": trip.get("to_station") or "",
        "loaded_station_name": to_text(first_value(source_row, ["loaded_station_name"])) or sender,
        "unloaded_station_name": to_text(first_value(source_row, ["unloaded_station_name", "actual_unloaded_station_name"])) or receiver,
        "to_number": to_number,
        "to_path": to_path,
        "sender": sender,
        "receiver": receiver,
        "order_number": scan_number,
        "shipment_id": scan_number,
        "remark_received_station": "",
        "remark": to_text(first_value(source_row, ["remark", "remarks", "labels_remark"])),
        "scan_time": to_text(first_value(source_row, ["scan_time", "loaded_time", "update_time", "mtime"])),
        "receive_status": to_text(first_value(source_row, ["receive_status", "received_status", "loading_status_name"])),
        "journey_type": "",
        "candidate_reason": "admin pending inbound SPX",
        "candidate_status": "PENDING_CANDIDATE",
        "candidate_rule_version": CANDIDATE_RULE_VERSION,
        "candidate_checked_at": checked_at,
    })


def to_detail_rows_to_candidates(
    candidate,
    to_detail,
    candidate_source,
    include_pending_transit=False,
):
    """Build order candidates from a TO Detail response.

    Normal TO Sorting remains restricted to BDA sender and its established
    pending rules.  A TO that appears in Pending Inbound but belongs to a
    transit sender is handled separately: every child order is a suspect
    unless that order already has a ``Received in [...]`` remark.
    """
    rows = TO_BOT.build_rows_for_to(candidate, to_detail)
    candidates = []
    for row in rows:
        # A TO carried by a BDA LT can be misrouted into BDA even when its own
        # TO Path does not contain BDA (for example BN -> HCM -> HN). Treat it
        # like Pending Inbound transit instead of trusting the sender alone.
        path_without_bda = bool(to_text(row.get("to_path"))) and not to_path_contains_bda(row.get("to_path"))
        is_sorting = is_bda_sender(row.get("sender")) and not path_without_bda
        if is_sorting:
            if row.get("pending_status") not in ("PENDING", "NEED_TRACKING_RECEIVED_CHECK"):
                continue
            source_type = "TO_SORTING"
            source = candidate_source
            candidate_reason = row.get("pending_status") or ""
        else:
            if not include_pending_transit:
                continue
            # Ngung tao tu 2026-09-17 (ops): duong di khong co BDA thi khong phai
            # viec doi soat cua BDA. Nguon nay tao ~500 nghin dong trong 12 ngay
            # ma chua bao gio duoc kiem lan nao.
            if path_without_bda:
                continue
            received_remark = (
                row.get("remark_received_station")
                or TO_BOT.extract_received_station(row.get("remark"))
            )
            if received_remark:
                continue
            source_type = "TO_TRANSIT"
            source = "BDA_LT_TO_PATH_WITHOUT_BDA" if path_without_bda else "LH_PENDING_TO_TRANSIT"
            candidate_reason = (
                "BDA_LT_TO_PATH_WITHOUT_BDA_NO_RECEIVED_REMARK"
                if path_without_bda
                else "TRANSIT_PENDING_INBOUND_NO_RECEIVED_REMARK"
            )

        candidates.append(finalize_candidate({
            "candidate_id": "",
            "source_type": source_type,
            "candidate_source": source,
            "trip_number": row.get("trip_number") or "",
            "trip_id": str(row.get("trip_id") or ""),
            "sequence_number": EXPORT_BOT.to_int(candidate.get("sequence_number"), default=0) or None,
            "arrived_time": row.get("arrived_time") or None,
            "station": row.get("station") or "",
            "to_station": row.get("to_station") or "",
            "loaded_station_name": row.get("loaded_station_name") or "",
            "unloaded_station_name": row.get("unloaded_station_name") or "",
            "to_number": row.get("to_number") or "",
            "to_path": row.get("to_path") or "",
            "sender": row.get("sender") or "",
            "receiver": row.get("to_receiver") or "",
            "to_parcel_quantity": EXPORT_BOT.to_int(to_detail.get("order_count"), default=0) or None,
            "order_number": row.get("order_number") or "",
            "shipment_id": row.get("shipment_id") or row.get("order_number") or "",
            "remark_received_station": row.get("remark_received_station") or "",
            "remark": row.get("remark") or "",
            "scan_time": row.get("scan_time") or "",
            "receive_status": row.get("receive_status") or "",
            "journey_type": row.get("journey_type") or "",
            "candidate_reason": candidate_reason,
            "candidate_status": "PENDING_CANDIDATE",
            "candidate_rule_version": CANDIDATE_RULE_VERSION,
            "candidate_checked_at": row.get("checked_at") or now_text(),
        }))
    return candidates


def station_sequence_number(station, fallback_index):
    # Pending Inbound accepts the visible Station No.  When Trip Detail does
    # not send it, use the station's position instead of an offset internal
    # unload counter.
    sequence = EXPORT_BOT.to_int(first_value(station, [
        "station_no",
        "station_number",
    ]), default=fallback_index)
    return sequence if sequence > 0 else fallback_index


def station_reference_time(station):
    return parse_datetime_value(first_value(station, [
        "atd",
        "ata",
        "actual_departure_time",
        "actual_arrival_time",
        "departure_time",
        "arrival_time",
        "departed_time",
        "arrived_time",
        "unloaded_time",
        "unload_time",
        "mtime",
        "update_time",
        "updated_time",
    ]))


def station_arrived_time(station):
    """Only ATA/Arrived proves that a handover station has been reached."""
    return parse_datetime_value(first_value(station, [
        "ata",
        "actual_arrival_time",
        "arrival_time",
        "arrived_time",
    ]))


def handover_sequence_contexts(trip):
    stations = trip.get("trip_station") or []
    contexts = []
    seen = set()
    bda_seen = False
    for index, station in enumerate(stations, start=1):
        name = BULKY_BOT.station_name(station)
        if normalize_station(name) == normalize_station(SOC_CODE):
            bda_seen = True
            continue
        if not bda_seen:
            continue
        sequence = station_sequence_number(station, index)
        if sequence in seen:
            continue
        seen.add(sequence)
        arrived_time = station_arrived_time(station)
        if not arrived_time:
            continue
        contexts.append({
            "sequence": sequence,
            "station_hint": name,
            "reference_time": arrived_time,
        })
    return contexts


def detail_sequence_contexts(detail_data, target_sequence=None, target_station=None):
    """Return the receiving stop used by Pending Inbound.

    Older Bot1 rows may have saved an internal unload counter instead of the
    visible Station No.  Prefer the stored destination station in that case so
    those rows still get one correct retry without re-ingesting the LT.
    """
    if target_station:
        stations = ((detail_data or {}).get("data") or {}).get("trip_station") or []
        for index, station in enumerate(stations, start=1):
            station_hint = BULKY_BOT.station_name(station)
            if normalize_station(station_hint) != normalize_station(target_station):
                continue
            return [{
                "sequence": station_sequence_number(station, index),
                "station_hint": station_hint,
                "reference_time": None,
            }]

    contexts = []
    for sequence, station_hint in BULKY_BOT.extract_unload_sequences(detail_data):
        if target_sequence and sequence != target_sequence:
            continue
        contexts.append({"sequence": sequence, "station_hint": station_hint, "reference_time": None})
    return contexts


def eligible_sequence_contexts(trip, detail_data):
    if trip.get("_source") != "handover":
        return detail_sequence_contexts(
            detail_data,
            EXPORT_BOT.to_int(trip.get("sequence_number"), default=0) or None,
            trip.get("to_station") or None,
        )

    contexts = handover_sequence_contexts(trip)
    stored_sequence = EXPORT_BOT.to_int(trip.get("sequence_number"), default=0)
    if stored_sequence:
        contexts = [context for context in contexts if context["sequence"] == stored_sequence]
    if not contexts:
        print(
            f"Skip handover {trip.get('trip_number')}: chua co ATA/Arrived "
            "tai station sau BDA"
        )
    return contexts


def collect_lh_pending_candidates(
    service,
    pending_session,
    lt_limit=None,
    force_recheck_sequences=False,
):
    candidates = []
    sequence_markers = []
    # only_unchecked_pending_inbound moves the "already asked" test into SQL,
    # ahead of LIMIT. Without it the window filled with finished sequences and
    # this whole step did nothing - see pending_inbound_pending_clause().
    stored_handover_trips = get_stored_handover_trip_candidates(
        service, limit=lt_limit, only_unchecked_pending_inbound=not force_recheck_sequences
    )
    stored_ended_trips = get_stored_ended_trip_candidates(
        service, limit=lt_limit, only_unchecked_pending_inbound=not force_recheck_sequences
    )
    lt_unit_trips = get_lt_unit_trip_candidates(service, limit=None)
    for row in lt_unit_trips:
        row.setdefault("_source", "lt_unit")

    # Ended is stored per receiving sequence. Keep every sequence instead of
    # collapsing an LT to its first stop. A currently Ended LT wins over an
    # older Handover snapshot of the same trip.
    trips = []
    ended_trip_ids = set()
    seen_sequence_keys = set()
    for row in stored_ended_trips:
        trip_id = to_text(row.get("trip_id"))
        sequence = EXPORT_BOT.to_int(row.get("sequence_number"), default=0)
        key = f"{trip_id}|{sequence}"
        if not trip_id or sequence <= 0 or key in seen_sequence_keys:
            continue
        ended_trip_ids.add(trip_id)
        seen_sequence_keys.add(key)
        trips.append(row)

    handover_sequence_keys = set()
    legacy_handover_trip_ids = set()
    for row in stored_handover_trips:
        trip_id = to_text(row.get("trip_id"))
        sequence = EXPORT_BOT.to_int(row.get("sequence_number"), default=0)
        if sequence <= 0:
            if not trip_id or trip_id in ended_trip_ids or trip_id in legacy_handover_trip_ids:
                continue
            legacy_handover_trip_ids.add(trip_id)
            trips.append(row)
            continue
        key = f"{trip_id}|{sequence}"
        if not trip_id or sequence <= 0 or trip_id in ended_trip_ids or key in handover_sequence_keys:
            continue
        handover_sequence_keys.add(key)
        seen_sequence_keys.add(key)
        trips.append(row)

    for row in lt_unit_trips:
        trip_id = to_text(row.get("trip_id"))
        sequence = EXPORT_BOT.to_int(row.get("sequence_number"), default=0)
        key = f"{trip_id}|{sequence}"
        if (
            not trip_id
            or sequence <= 0
            or trip_id in ended_trip_ids
            or key in handover_sequence_keys
            or key in seen_sequence_keys
        ):
            continue
        seen_sequence_keys.add(key)
        trips.append(row)

    if lt_limit:
        trips = trips[:int(lt_limit)]
    print(
        "Unified LH trip candidates: "
        f"{len(trips)} | ended_stored={len(stored_ended_trips)} | "
        f"handover_stored={len(stored_handover_trips)} | lt_unit_fallback={len(lt_unit_trips)}"
    )
    if not trips:
        return candidates, sequence_markers

    checked_sequences_by_trip = load_sequence_state_for_trips(
        service,
        [trip.get("trip_id") for trip in trips],
    )
    to_detail_cache = {}
    seen_candidate_ids = set()

    for trip in trips:
        checked_at = now_text()
        trip_id = to_text(trip.get("trip_id"))
        trip_number = trip.get("trip_number")
        if trip.get("_source") == "handover":
            detail_data = {"data": {"trip_station": trip.get("trip_station") or []}}
        else:
            detail_data = BULKY_BOT.fetch_trip_detail(pending_session, trip_id, trip_number)
        all_sequence_contexts = eligible_sequence_contexts(trip, detail_data)
        all_sequences = [(context["sequence"], context["station_hint"]) for context in all_sequence_contexts]
        checked_sequences = checked_sequences_by_trip.get(trip_id, set())
        if force_recheck_sequences:
            sequences = all_sequences
            print(f"Force recheck Pending Inbound {trip_number}: {len(sequences)} sequence(s)")
        else:
            # Pending Inbound is an immutable per-LT-sequence snapshot.  The
            # same receiving sequence may first appear in Handover and later
            # move to Ended; its marker is deliberately source-agnostic, so
            # do not call the endpoint a second time.  A newly arrived
            # sequence has no marker and is selected here exactly once.
            sequences = [
                (sequence, station_hint)
                for sequence, station_hint in all_sequences
                if sequence not in checked_sequences
            ]

        trip_candidates = 0
        for sequence, station_hint in sequences:
            pending_rows, _, pending_complete = BULKY_BOT.fetch_pending_loading_list(
                pending_session,
                trip_id,
                trip_number,
                sequence,
            )
            if not pending_complete:
                print(
                    f"Keep {trip_number} sequence {sequence} unmarked: "
                    "Pending Inbound API chua lay xong, se retry o vong sau."
                )
                continue
            sequence_markers.append(build_sequence_marker_result(trip, sequence, station_hint, checked_at))
            for source_row in pending_rows:
                scan_number = BULKY_BOT.source_scan_number(source_row)
                to_number = BULKY_BOT.source_to_number(source_row)
                is_to_pending = scan_number.startswith("TO") or (not scan_number.startswith("SPX") and to_number.startswith("TO"))
                if is_to_pending:
                    if not to_number:
                        continue
                    if to_number not in to_detail_cache:
                        to_detail_cache[to_number] = EXPORT_BOT.fetch_to_detail(pending_session, to_number)
                    to_detail = to_detail_cache.get(to_number) or {}
                    to_candidate = BULKY_BOT.build_to_sorting_candidate_from_pending(
                        trip,
                        source_row,
                        sequence,
                        station_hint,
                    )
                    to_candidate["sequence_number"] = sequence
                    to_candidate["to_path"] = to_detail.get("to_path") or to_candidate.get("to_path") or ""
                    to_candidate["sender"] = to_detail.get("sender") or to_candidate.get("sender") or ""
                    to_candidate["to_receiver"] = to_detail.get("receiver") or to_candidate.get("to_receiver") or ""
                    for candidate in to_detail_rows_to_candidates(
                        to_candidate,
                        to_detail,
                        "LH_PENDING_TO",
                        include_pending_transit=True,
                    ):
                        if candidate["candidate_id"] in seen_candidate_ids:
                            continue
                        seen_candidate_ids.add(candidate["candidate_id"])
                        candidates.append(candidate)
                        trip_candidates += 1
                    continue

                bulky_candidate = build_bulky_candidate(trip, source_row, sequence, station_hint, checked_at)
                if not bulky_candidate or bulky_candidate["candidate_id"] in seen_candidate_ids:
                    continue
                seen_candidate_ids.add(bulky_candidate["candidate_id"])
                candidates.append(bulky_candidate)
                trip_candidates += 1

        print(f"Collected LH candidates {trip_number}: sequences={len(sequences)}/{len(all_sequences)} | candidates={trip_candidates}")
    return candidates, sequence_markers


def to_detail_has_usable_response(to_detail):
    """Only checkpoint a TO detail call when FMS returned usable data.

    `fetch_to_detail(..., fail_soft=True)` returns an empty-shaped dictionary
    after an API failure. Treating that as complete would skip the TO forever.
    """
    if not to_detail:
        return False
    if (to_detail.get("detail_rows") or []) or EXPORT_BOT.to_int(to_detail.get("order_count"), default=0) > 0:
        return True
    return any(to_text(to_detail.get(key)) for key in ("sender", "receiver", "to_path"))


def collect_lt_unit_to_candidates(service, fms_session, to_limit=None):
    candidates = []
    seen_candidate_ids = set()
    detail_states = []
    to_rows = get_lt_unit_to_candidates(service, limit=to_limit)
    print(f"lt_unit TO Sorting candidates can collect: {len(to_rows)}")
    for row in to_rows:
        to_number = row.get("to_number") or ""
        to_detail = EXPORT_BOT.fetch_to_detail(fms_session, to_number)
        if not to_detail_has_usable_response(to_detail):
            print(f"Keep TO {to_number} for retry: FMS TO detail did not return usable data")
            continue
        for candidate in to_detail_rows_to_candidates(row, to_detail, "LT_UNIT_TO_SORTING"):
            if candidate["candidate_id"] in seen_candidate_ids:
                continue
            seen_candidate_ids.add(candidate["candidate_id"])
            candidates.append(candidate)
        detail_states.append({
            "trip_id": to_text(row.get("trip_id")),
            "trip_number": to_text(row.get("trip_number")),
            "sequence_number": EXPORT_BOT.to_int(row.get("sequence_number"), default=0) or None,
            "to_number": to_text(to_number),
            "source": ADMIN_HANDOVER_TO_DETAIL_SOURCE,
            "checked_at": now_text(),
        })
        print(f"Collected TO candidates {to_number}: {len(candidates)} total")
    return candidates, detail_states


def normalize_tracking_items(response):
    data = (response or {}).get("data") or {}
    if isinstance(data, list):
        return data
    for key in ["list", "orders", "items", "result"]:
        value = data.get(key)
        if isinstance(value, list):
            return value
    return []


def search_tracking_batch(fms_session, order_numbers):
    clean_numbers = sorted({to_text(number) for number in order_numbers if to_text(number)})

    def request_one_batch(numbers, max_retries=3):
        payload = {
            "count": BATCH_SIZE,
            "page_no": 1,
            "search_id_list": numbers,
        }
        data = fms_session.request_json(
            "POST",
            TRACKING_SEARCH_URL,
            payload=payload,
            label=f"Batch tracking search {len(numbers)}",
            fail_soft=False,
            max_retries=max_retries,
        )
        return normalize_tracking_items(data)

    def is_rejected_id_list_error(error):
        text = to_text(error).lower()
        return "search_id_list" in text or "api 40000" in text

    def request_with_split(numbers):
        if not numbers:
            return []
        try:
            return request_one_batch(numbers)
        except Exception as error:
            if len(numbers) == 1:
                print(
                    f"Defer BatchSearch shipment {numbers[0]}: {error}. "
                    "Keep candidate for next recheck."
                )
                return []

            midpoint = len(numbers) // 2
            if is_rejected_id_list_error(error):
                print(
                    f"BatchSearch rejected {len(numbers)} IDs, split into "
                    f"{midpoint} + {len(numbers) - midpoint}: {error}"
                )
                return request_with_split(numbers[:midpoint]) + request_with_split(numbers[midpoint:])

            # A transport/service timeout is not evidence that one ID is bad.
            # Defer the complete batch so the worker can continue with later work.
            print(
                f"Defer BatchSearch batch {len(numbers)} after backend timeout/error: {error}. "
                "Keep candidates for next retry."
            )
            return []

    return request_with_split(clean_numbers)


def tracking_item_order_number(item):
    return to_text(first_value(item, ["spx_tracking_number", "tracking_number", "fleet_order_id", "order_number", "shipment_id"]))


def parse_int_field(item, keys):
    value = first_value(item, keys)
    if value in (None, ""):
        return None
    return EXPORT_BOT.to_int(value, default=0)


def status_name(item, status_code):
    return to_text(first_value(item, ["order_status_desc", "order_status_label", "tracking_status", "order_status_name"])) or str(status_code if status_code >= 0 else "")


# When BatchSearch returns one of these order_status values the case is
# settled - the parcel is being delivered / has been delivered / cancelled /
# on hold / on its way back. Not stuck after the BDA handoff.
TERMINAL_NOT_PENDING_STATUS_CODES = {
    2,   # Delivering
    3,   # Cancelled
    4,   # Delivered
    5,   # OnHold
    72,  # Return_FMHub_Returning
    73,  # Return_FMHub_Returned
    74,  # Return_FMHub_Onhold
}


LIQUIDATE_RECEIVED_STATUS_NAMES = {
    611: "Liquidate_Packed",
    616: "Liquidate_Received",
}

# TO transit chi lay tu ngay nay tro di (ops 2026-09-17: don transit cu bo di).
# So sanh chuoi voi arrived_time 'YYYY-MM-DD HH:MM:SS'.
TRANSIT_START_ARRIVED_TIME = "2026-09-18 00:00:00"
TRANSIT_CANDIDATE_SOURCE = "LH_PENDING_TO_TRANSIT"


def is_hard_not_pending_status(status_text, status_code=None):
    if status_code in TERMINAL_NOT_PENDING_STATUS_CODES:
        return True
    text = to_text(status_text).lower()
    if not text:
        return False
    if "lhtransported" in text or "lh_transported" in text or "lhtransporting" in text or "lh_transporting" in text:
        return False
    return any(token in text for token in [
        "delivered",
        "delivering",
        "cancelled",
        "canceled",
        "returning",
        "returned",
        "return_fmhub_onhold",
    ])


def decide_pending_status(candidate, item):
    current_station = normalize_station(first_value(item, ["current_station_name", "current_station", "station_name"]))
    order_status_code = EXPORT_BOT.to_int(item.get("order_status"), default=-1) if item else -1
    order_status = status_name(item, order_status_code) if item else ""

    # A final order status settles the case regardless of when the station
    # received it. Current-station timestamps remain useful for non-final
    # statuses only.
    if item and is_hard_not_pending_status(order_status, order_status_code):
        return "NOT_PENDING", "terminal order status"

    # Hang thanh ly di bo status rieng (Liquidate_*), khong co SOC_Received nen
    # Tracking Info khong bao gio thay "Received" o tram nhan. Do 2026-09-17: 172
    # don BDA -> HCM Mega SOC bi giu PENDING du HCM da nhan. Ops chot: 616/611 dang
    # o tram nhan cua TO = da nhan. 614/615 (con dang cho/da cho di, tram hien tai
    # van la BDA) KHONG thuoc luat nay.
    if item and order_status_code in LIQUIDATE_RECEIVED_STATUS_NAMES:
        receiver = normalize_station(candidate.get("receiver"))
        if receiver and current_station == receiver:
            return "NOT_PENDING", (
                f"{LIQUIDATE_RECEIVED_STATUS_NAMES[order_status_code]} at receiving station"
            )

    # TO transit (X > BDA > Y), ops chot 2026-09-17. Chi BatchSearch, khong
    # Tracking Info. Mot don "con thieu" khi: tram hien tai van la BDA VA tram ke
    # tiep van la Y (receiver cua TO). Day la ket qua TUNG DON - ket luan cuoi cung
    # la theo CA TO, xem _gop_ket_qua_transit_theo_to() trong batchsearch_candidates.
    # Dat truoc kiem tra "received before LT arrived" ben duoi: voi transit, BDA
    # luon nhan TO truoc khi LT toi Y, kiem tra do se giu PENDING sai.
    if candidate.get("source_type") == "TO_TRANSIT":
        if not item:
            return "UNKNOWN", "transit batchsearch missing"
        receiver = normalize_station(candidate.get("receiver") or candidate.get("to_station"))
        next_station = normalize_station(first_value(item, ["next_station_name"]))
        if not receiver:
            return "UNKNOWN", "transit TO receiver missing"
        if current_station == normalize_station(SOC_CODE) and next_station == receiver:
            return "PENDING", "transit at BDA and next station is still the TO receiver"
        return "NOT_PENDING", (
            f"transit moved: current_station={first_value(item, ['current_station_name']) or '-'}, "
            f"next_station={first_value(item, ['next_station_name']) or '-'}"
        )

    arrived_at = parse_datetime_value(candidate.get("arrived_time"))
    current_station_received_at = parse_datetime_value(
        first_value(
            item or {},
            ["current_station_received_time", "current_station_receive_time"],
        )
    )
    if arrived_at and current_station_received_at:
        if current_station_received_at < arrived_at:
            return (
                "PENDING",
                "current station received before LT arrived",
            )

    if candidate.get("source_type") in ("TO_SORTING", "TO_TRANSIT"):
        if candidate.get("candidate_reason") == "PENDING":
            # TO detail can report an empty Remark even after a parcel has
            # moved on from the LT receiving station.  Do not preserve a
            # stale direct suspicion when BatchSearch proves that movement.
            if not item:
                return "UNKNOWN", "to sorting direct pending check missing batchsearch data"
            receive_stations = {
                normalize_station(candidate.get("receiver")),
                normalize_station(candidate.get("remark_received_station")),
                normalize_station(candidate.get("unloaded_station_name")),
                normalize_station(candidate.get("to_station")),
                normalize_station(SOC_CODE),
            }
            receive_stations.discard("")
            if current_station and receive_stations and current_station not in receive_stations:
                return (
                    "NOT_PENDING",
                    "to sorting direct suspicion cleared: current_station moved beyond LT receiving station",
                )
            return "PENDING", "to sorting not received with blank remark"
        if not item:
            return "UNKNOWN", "to sorting tracking check missing batchsearch data"
        if TO_BOT.is_receive_station_hold_status(order_status, order_status_code):
            # A hold-like code alone is not enough: BatchSearch can retain an
            # earlier Received code after the parcel has already moved beyond
            # the station where BDA handed the TO over.
            receive_stations = {
                normalize_station(candidate.get("receiver")),
                normalize_station(candidate.get("remark_received_station")),
                normalize_station(candidate.get("unloaded_station_name")),
                normalize_station(candidate.get("to_station")),
                normalize_station(SOC_CODE),
            }
            receive_stations.discard("")
            if current_station and receive_stations and current_station not in receive_stations:
                return (
                    "NOT_PENDING",
                    "to sorting current_station moved beyond BDA receiving station",
                )
            return "PENDING", "to sorting status remains at received/unloaded hold group"
        return "NOT_PENDING", f"to sorting status moved beyond hold group: {order_status}"

    if not item:
        return "UNKNOWN", "batchsearch missing"

    if candidate.get("source_type") == "BULKY":
        if current_station == normalize_station(SOC_CODE):
            return "PENDING", "bulky current_station is BDA"
        return "NOT_PENDING", "bulky current_station moved from BDA"

    allowed_stations = {
        normalize_station(SOC_CODE),
        normalize_station(candidate.get("remark_received_station")),
        normalize_station(candidate.get("unloaded_station_name")),
        normalize_station(candidate.get("receiver")),
    }
    allowed_stations.discard("")
    if current_station in allowed_stations:
        return "PENDING", "to sorting current_station is BDA or received/unloaded station"
    return "NOT_PENDING", "to sorting current_station moved to another station"


def candidate_processing_stage(candidate, checked_at=None):
    checked_at = checked_at or datetime.now()
    arrived_at = parse_datetime_value(candidate.get("arrived_time"))
    if not arrived_at:
        return "FINAL"

    final_check_at = arrived_at + timedelta(
        hours=PENDING_READY_AFTER_ARRIVED_HOURS - FINAL_CHECK_LEAD_HOURS
    )
    if checked_at < final_check_at:
        return "PRECHECK"
    if checked_at < arrived_at + timedelta(hours=PENDING_READY_AFTER_ARRIVED_HOURS):
        return "FINAL_READY"
    return "FINAL"


def enrich_result(candidate, item, checked_at, processing_stage):
    item = item or {}
    status_code = EXPORT_BOT.to_int(item.get("order_status"), default=-1) if item else -1
    current_station = to_text(first_value(item, ["current_station_name", "current_station", "station_name"]))
    destination_station = BULKY_BOT.resolve_station_value(first_value(item, [
        "station_id",
        "destination_station_id",
        "destination_station_name",
        "destination_station",
        "dest_station_id",
    ]))
    return_destination = BULKY_BOT.resolve_station_value(first_value(item, [
        "return_dest_station_id",
        "return_destination_station_id",
        "return_destination",
        "return_destination_name",
        "return_dest_station_name",
    ]))
    if candidate.get("source_type") in ("TO_SORTING", "TO_TRANSIT"):
        journey_type = normalize_journey_type(candidate.get("journey_type"))
    else:
        journey_type = normalize_bulk_journey_type(candidate.get("journey_type"), item)
    total_on_hold_times = parse_int_field(item, ["total_on_hold_times", "total_onhold_times", "on_hold_times"])
    number_of_return_on_hold = parse_int_field(item, ["number_of_return_on_hold", "return_on_hold_times"])
    total_for_attempt = total_on_hold_times or 0
    return_for_attempt = number_of_return_on_hold or 0
    if journey_type == "Return":
        attempt = return_for_attempt
        destination = return_destination
    else:
        journey_type = journey_type or "Forward"
        attempt = max(0, total_for_attempt - return_for_attempt)
        destination = destination_station

    pending_status, result_reason = decide_pending_status(candidate, item)
    return {
        **candidate,
        "order_status_code": status_code if status_code >= 0 else None,
        "order_status": status_name(item, status_code),
        "status": status_name(item, status_code),
        "current_station": current_station,
        "current_to_number": to_text(first_value(item, ["current_to_number", "to_number"])),
        "journey_type": journey_type,
        "destination_station": destination_station,
        "return_destination": return_destination,
        "destination": destination,
        "total_on_hold_times": total_on_hold_times,
        "number_of_return_on_hold": number_of_return_on_hold,
        "attempt": attempt,
        "pending_status": pending_status,
        "result_reason": result_reason,
        "result_rule_version": RESULT_RULE_VERSION,
        "processing_stage": processing_stage,
        "publish_after": candidate.get("eligible_at") or (
            (parse_datetime_value(candidate.get("arrived_time")) + timedelta(
                hours=PENDING_READY_AFTER_ARRIVED_HOURS
            )).strftime("%Y-%m-%d %H:%M:%S")
            if parse_datetime_value(candidate.get("arrived_time")) else None
        ),
        "checked_at": checked_at,
    }


def enrich_tracking_only_result(candidate, checked_at, processing_stage, pending_status, result_reason):
    """Create a result when Tracking Info resolves a direct TO suspicion.

    Direct TO-detail candidates should not consume BatchSearch merely to prove
    a later Received [Single] or physical downstream scan.  BatchSearch is
    reserved for the unresolved Mass/no-signal cases that still need the
    presentation fields for the pending sheets.
    """
    result = enrich_result(
        candidate,
        {"order_status": -1},
        checked_at,
        processing_stage,
    )
    result["pending_status"] = pending_status
    result["result_reason"] = result_reason
    return result


def latest_candidates_to_batch(service, limit=None, force_recheck_pending=False):
    requested_limit = int(limit) if limit else DEFAULT_BATCH_CANDIDATES_PER_CYCLE
    effective_limit = max(1, min(requested_limit, MAX_BATCH_CANDIDATES_PER_CYCLE))
    if requested_limit != effective_limit:
        print(
            f"BatchSearch queue cap: requested={requested_limit}, "
            f"process_this_cycle={effective_limit}"
        )
    force_pending_clause = "OR r.pending_status = 'PENDING'" if force_recheck_pending else ""
    force_priority_order = """
      CASE
        WHEN r.pending_status = 'PENDING' AND r.order_status_code IN (2, 3, 4) THEN 0
        WHEN r.pending_status = 'PENDING' THEN 1
        ELSE 2
      END,
    """ if force_recheck_pending else ""
    candidate_columns = ",\n      ".join(
        f"c.{field['name']}" for field in candidate_schema_fields()
    )
    query = f"""
    WITH latest_candidate AS (
      -- lt_pending_candidate is upserted (one row per candidate_id), so no
      -- ROW_NUMBER() de-dup pass over the whole table is needed here.
      SELECT *
      FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_CANDIDATE_TABLE_ID}`
      WHERE candidate_rule_version = '{CANDIDATE_RULE_VERSION}'
        AND order_number IS NOT NULL
        AND order_number != ''
        AND order_number != '{SEQUENCE_MARKER_ORDER}'
        AND (
          LOWER(TRIM(sender)) = LOWER('{SOC_CODE}')
          -- TO transit X > BDA > Y lay tu Pending Inbound (ops 2026-09-17), chi
          -- tu TRANSIT_START_ARRIVED_TIME. Nguon BDA_LT_TO_PATH_WITHOUT_BDA
          -- (duong di khong co BDA) khong bao gio duoc chon.
          OR (
            source_type = 'TO_TRANSIT'
            AND candidate_source = '{TRANSIT_CANDIDATE_SOURCE}'
            AND arrived_time >= '{TRANSIT_START_ARRIVED_TIME}'
          )
        )
        AND arrived_time IS NOT NULL
        AND precheck_at IS NOT NULL
        AND precheck_at <= CURRENT_DATETIME('Asia/Bangkok')
    ),
    latest_result AS (
      SELECT
        candidate_id,
        pending_status,
        order_status_code,
        checked_at,
        processing_stage,
        published_at,
        ROW_NUMBER() OVER (
          PARTITION BY candidate_id
          ORDER BY checked_at DESC
        ) AS result_latest_rank
      FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_RESULT_TABLE_ID}`
      WHERE result_rule_version = '{RESULT_RULE_VERSION}'
    )
    SELECT
      {candidate_columns}
    FROM latest_candidate c
    LEFT JOIN latest_result r
      ON c.candidate_id = r.candidate_id
     AND r.result_latest_rank = 1
    WHERE (
        (
          r.candidate_id IS NULL
          AND c.prechecked_at IS NULL
          AND COALESCE(c.queue_stage, 'NEW') = 'NEW'
        )
        OR (
          c.queue_stage = 'PRECHECK_PENDING'
          AND c.final_check_at IS NOT NULL
          AND c.final_check_at <= CURRENT_DATETIME('Asia/Bangkok')
          -- queue_stage is only ever written by the PRECHECK branch, so a
          -- candidate that has passed final_check_at keeps PRECHECK_PENDING
          -- for the rest of its life. Without the check below this clause
          -- re-selects it on EVERY cycle, bypassing PENDING_RECHECK_MINUTES:
          -- measured 2026-09-09, 1,935 already-decided PENDING candidates
          -- were costing a Tracking Detail call each, ~23% of a cycle.
          -- The first FINAL check is what the deadline depends on, so the
          -- `IS NULL` branch keeps it immediate; only re-checks are paced.
          AND (
            r.candidate_id IS NULL
            OR (
              c.arrived_time > DATETIME_SUB(
                CURRENT_DATETIME('Asia/Bangkok'), INTERVAL {OLD_PENDING_AFTER_DAYS} DAY
              )
              AND r.checked_at <= DATETIME_SUB(
                CURRENT_DATETIME('Asia/Bangkok'), INTERVAL {PENDING_RECHECK_MINUTES} MINUTE
              )
            )
            OR (
              c.arrived_time <= DATETIME_SUB(
                CURRENT_DATETIME('Asia/Bangkok'), INTERVAL {OLD_PENDING_AFTER_DAYS} DAY
              )
              AND r.checked_at <= DATETIME_SUB(
                CURRENT_DATETIME('Asia/Bangkok'), INTERVAL {OLD_PENDING_RECHECK_MINUTES} MINUTE
              )
            )
          )
        )
        OR (
          c.queue_stage = 'PRECHECK_UNKNOWN'
          AND c.next_check_at IS NOT NULL
          AND c.next_check_at <= CURRENT_DATETIME('Asia/Bangkok')
        )
        OR (
          r.processing_stage = 'PRECHECK'
          AND c.final_check_at IS NOT NULL
          AND c.final_check_at <= CURRENT_DATETIME('Asia/Bangkok')
        )
        OR (
          r.pending_status = 'UNKNOWN'
          AND r.checked_at <= DATETIME_SUB(
            CURRENT_DATETIME('Asia/Bangkok'), INTERVAL {UNKNOWN_RECHECK_MINUTES} MINUTE
          )
        )
        OR (
          r.pending_status = 'PENDING'
          AND r.processing_stage != 'PRECHECK'
          AND (
            (
              c.arrived_time > DATETIME_SUB(
                CURRENT_DATETIME('Asia/Bangkok'), INTERVAL {OLD_PENDING_AFTER_DAYS} DAY
              )
              AND r.checked_at <= DATETIME_SUB(
                CURRENT_DATETIME('Asia/Bangkok'), INTERVAL {PENDING_RECHECK_MINUTES} MINUTE
              )
            )
            OR (
              c.arrived_time <= DATETIME_SUB(
                CURRENT_DATETIME('Asia/Bangkok'), INTERVAL {OLD_PENDING_AFTER_DAYS} DAY
              )
              AND r.checked_at <= DATETIME_SUB(
                CURRENT_DATETIME('Asia/Bangkok'), INTERVAL {OLD_PENDING_RECHECK_MINUTES} MINUTE
              )
            )
          )
        )
        {force_pending_clause}
      )
    ORDER BY
      CASE
        WHEN c.queue_stage = 'PRECHECK_PENDING'
          AND c.final_check_at IS NOT NULL
          AND c.final_check_at <= CURRENT_DATETIME('Asia/Bangkok') THEN 0
        WHEN c.final_check_at IS NOT NULL
          AND c.final_check_at <= CURRENT_DATETIME('Asia/Bangkok')
          AND (r.candidate_id IS NULL OR r.processing_stage = 'PRECHECK') THEN 0
        -- Orders already published on the Pending sheet that the team is
        -- acting on: recheck these before fresh candidates so a delivered /
        -- resolved order leaves the sheet promptly.
        WHEN r.pending_status = 'PENDING'
          AND r.published_at IS NOT NULL
          AND r.published_at != ''
          AND r.checked_at <= DATETIME_SUB(
            CURRENT_DATETIME('Asia/Bangkok'), INTERVAL {PENDING_RECHECK_MINUTES} MINUTE
          ) THEN 0
        -- UNKNOWN means BatchSearch could not find this order at all - an
        -- ambiguous state with no "BOT Status" update path in pending_work
        -- until it resolves back to a real PENDING/NOT_PENDING. Recheck it
        -- before fresh candidates so it does not sit ambiguous for hours.
        WHEN r.pending_status = 'UNKNOWN' THEN 0
        WHEN r.pending_status = 'PENDING' THEN 1
        ELSE 2
      END,
      {force_priority_order}
      c.deadline_at ASC,
      c.final_check_at ASC,
      c.arrived_time ASC,
      c.candidate_checked_at ASC,
      c.trip_number,
      c.to_number,
      c.order_number
    LIMIT {effective_limit}
    """
    return query_bq(service, query, fail_soft=True)


def iter_tracking_events(events):
    """Yield tracking events, including FMS events nested below a timeline row."""
    for event in events or []:
        if not isinstance(event, dict):
            continue
        yield event
        yield from iter_tracking_events(event.get("children"))
        yield from iter_tracking_events(event.get("event_children"))


def reconcile_cache_key(shipment_id, station_name):
    normalized_station = " ".join(to_text(station_name).casefold().split())
    return f"{to_text(shipment_id)}|{normalized_station}"


def tracking_arrived_at_for_station(tracking_events, station_name, not_before=None):
    """Return the first arrival signal at a station after the LT arrival."""
    station_key = normalize_station(station_name)
    arrived_events = []
    fallback_events = []
    for event in tracking_events:
        if normalize_station(event.get("station_name")) != station_key:
            continue
        event_at = parse_datetime_value(event.get("timestamp"))
        if not event_at:
            continue
        if not_before and event_at < not_before - timedelta(minutes=5):
            continue
        fallback_events.append(event_at)
        status_code = EXPORT_BOT.to_int(event.get("status"), default=-1)
        message = to_text(event.get("message")).lower()
        if status_code in TRACKING_ARRIVED_STATUS_CODES or "arrived" in message:
            arrived_events.append(event_at)
    if arrived_events:
        return min(arrived_events)
    return min(fallback_events) if fallback_events else None


# FMS records two different kinds of event against the same shipment:
#
#   TO level     "[TO2026090520ATA] arrived at [Pleiku SOC] via Linehaul Trip [LT...]"
#   parcel level "Parcel arrived at sorting center" / "Parcel packing into TO [...]"
#
# The TO-level ones are mirrored onto every shipment on the TO's manifest,
# INCLUDING the ones that are missing - they say the trip arrived, not that this
# parcel was in it. Using them as proof of arrival clears every Bulky order.
#
# Caught 2026-09-08 on SPXVN060369754349: at its destination Pleiku SOC there is
# only the TO-level `arrived ... via Linehaul Trip`, timestamped at exactly the
# candidate's arrived_time (both come from the same LT docking). The parcel
# itself only reappears at Kon Tum SOC - it was misrouted, not lost. Counting
# the Pleiku event would have "proved" arrival at a station the parcel never
# reached.
# Ranges, not a fixed set. The forward flow uses 880/882/918/920/928/930, but
# the return (rTO) flow adds 884/887/917/919/927/929 - found 2026-09-08 on
# SPXVN061005077038, whose whole journey is returns. Grouped as arrived 880-889,
# unloading 917-920, unloaded 927-930; no parcel-level status falls in those
# ranges. The message-prefix check below caught the missing codes on its own,
# which is exactly why both checks are kept.
def is_to_level_linehaul_status(status_code):
    # 879-888 is the whole `*_LHArrived` family per stt_mapping.xlsx:
    #   879 FMHub_LHArrived        884 Return_FMHub_LHArrived
    #   880 LMHub_LHArrived        885 Return_LMHub_LHArrived
    #   881 Hub_LHArrived          886 Return_Hub_LHArrived
    #   882 SOC_LHArrived          887 Return_SOC_LHArrived
    #   883 WHS_LHArrived          888 Return_WHS_LHArrived
    # 889 (Airhub_LHTransported) is kept in for safety; it is TO level too.
    #
    # 917-930 is LHUnloading / LHUnloaded. Those carry no mapping entry, but
    # they appear in live tracking as "[TO...] unloading|unloaded at [X] via
    # Linehaul Trip". Ops confirmed 2026-09-08 they must NOT count as handling
    # the parcel - a TO can be unloaded while this particular parcel is absent.
    return 879 <= status_code <= 889 or 917 <= status_code <= 930


# Moi trang thai *Received trong stt_mapping.xlsx. Chi dung cho nhanh Bulky -
# xem received_tag_at_bda_destination(bulky_mode=True).
BULKY_RECEIVED_STATUS_NAMES = {
    1: "LMHub_Received",
    8: "SOC_Received",
    10: "Return_LMHub_Received",
    42: "FMHub_Received",
    58: "Return_SOC_Received",
    67: "Return_FMHub_Received",
    76: "3PL_HubReceived",
    89: "3PL_Received",
    112: "DOP_Received",
    221: "SOC_Manifest_Received",
    342: "Return_SOC_Received_To_Disposal",
    400: "Hub_Received",
    408: "Hub_Manifest_Received",
    440: "Return_Hub_Received",
    616: "Liquidate_Received",
    623: "SOC_Intra_Handover_Received",
    627: "Return_SOC_Intra_Handover_Received",
    731: "Disposal_Received",
    745: "Locker_DOP_Received",
    782: "SP_HD_Received",
    784: "Return_SP_HD_Received",
    802: "WHS_Received",
    810: "Return_WHS_Received",
    828: "WHS_Intra_Handover_Received",
    829: "Return_WHS_Intra_Handover_Received",
    830: "WHS_Manifest_Received",
    843: "SP_Pitstop_Received",
    844: "Return_SP_Pitstop_Received",
    874: "Hub_Intra_Handover_Received",
    877: "Return_Hub_Intra_Handover_Received",
}
BULKY_RECEIVED_STATUS_CODES = frozenset(BULKY_RECEIVED_STATUS_NAMES)

# TO thieu nguyen duoc BDA xu ly lai - xem bda_rehandled_mode. Chi trang thai
# cap SOC. CO Y bo SOC_LHPacking/LHPacked (34/35/61/62): "Parcel [TO] adding
# into LH Task" duoc gan len moi don trong TO, dung loai da dong nham 4.750 don
# ngay 2026-09-14.
BDA_REHANDLED_STATUS_NAMES = {
    8: "SOC_Received",
    58: "Return_SOC_Received",
    221: "SOC_Manifest_Received",
    623: "SOC_Intra_Handover_Received",
    627: "Return_SOC_Intra_Handover_Received",
    9: "SOC_Packing",
    33: "SOC_Packed",
    59: "Return_SOC_Packing",
    60: "Return_SOC_Packed",
    621: "SOC_Intra_Handover_Packing",
    622: "SOC_Intra_Handover_Packed",
    625: "Return_SOC_Intra_Handover_Packing",
    626: "Return_SOC_Intra_Handover_Packed",
    666: "Return_SOC_Cache_Packing",
    670: "Return_SOC_Cache_Packed",
    679: "SOC_Cache_Packing",
    683: "SOC_Cache_Packed",
}
BDA_REHANDLED_STATUS_CODES = frozenset(BDA_REHANDLED_STATUS_NAMES)


_TO_ID_TRONG_NGOAC = re.compile(r"\[\s*(TO\d{6,}[0-9A-Z]*)\s*\]", re.IGNORECASE)


def is_parcel_level_event(event):
    """True when the event is about this parcel, not about the TO carrying it."""
    if is_to_level_linehaul_status(EXPORT_BOT.to_int(event.get("status"), default=-1)):
        return False
    message = to_text(event.get("message")).strip()
    if not message.lower().startswith("parcel"):
        return False
    # Mo dau bang chu "Parcel" VAN co the la su kien cap TO.  FMS viet nhung cau
    # nhu "Parcel [TO202609119Y8BH] adding into LH Task [LT0Q9D4XF3YG1] at BD A
    # Mega SOC" - chu Parcel o dau nhung thu trong ngoac lai la ma TO, tuc ca kien
    # hang chu khong phai don nay.  Chu thich cu o day khang dinh "TO-level
    # messages open with the TO id in brackets; parcel-level ones open with the
    # word Parcel", va dieu do sai: mot cau lam ca hai.
    #
    # Hau qua do duoc 2026-09-14: luat BDA_REHANDLED an nhung cau nay va dong
    # 4.750 don, 2.920 trong so do co order_status khong he thay doi - tuc don van
    # nam yen cho cu.  Rieng ngay 13/09 la 3.812 don.
    if _TO_ID_TRONG_NGOAC.search(message):
        return False
    return True


def cache_reconcile_arrival(
    service,
    shipment_id,
    station_name,
    tracking_events,
    not_before=None,
):
    """Persist the Tracking Info arrival that BOT 3 needs for its deadline."""
    if service is None or not shipment_id or not station_name:
        return
    arrived_at = tracking_arrived_at_for_station(
        tracking_events,
        station_name,
        not_before=not_before,
    )
    service.store.insert_rows(
        RECONCILE_ARRIVAL_CACHE_TABLE_ID,
        [{
            "cache_key": reconcile_cache_key(shipment_id, station_name),
            "shipment_id": to_text(shipment_id),
            "station_name": to_text(station_name),
            "station_arrived_at": arrived_at.strftime("%Y-%m-%d %H:%M:%S") if arrived_at else "",
            "fetched_at": now_text(),
            "last_error": "" if arrived_at else "No matching station arrival in Tracking Info",
        }],
        fields=reconcile_arrival_cache_schema_fields(),
    )


# ---------------------------------------------------------------- chan doan
# TAM THOI, 2026-09-17. Ba don Bulky (vd SPXVN062308795239) bi ket PENDING voi
# NO_HANDLING, trong khi JSON ops copy tu trinh duyet cho ham nay ra
# PARCEL_HANDLED. Cat bo `children` khoi cac nut cap tren thi tai hien DUNG ca hai
# dau ra bot da ghi vao DB luc 09:44 (phan quyet NO_HANDLING + gio toi Hiep Phuoc
# 05:32:21). Nghi FMS tra cho bot mot goi khong kem children. Ghi lai phan hoi
# THO de so voi ban tren trinh duyet.
#
# Chi ghi file JSON, KHONG ghi vao SQLite. Toi da TRACKING_DEBUG_MAX_FILES file,
# de len cai cu nhat -> khong bao gio qua ~2 MB. Khong goi FMS them lan nao.
# Xac nhan xong thi dat False va xoa thu muc.
TRACKING_DEBUG_DUMP = False  # Da tim ra nguyen nhan 2026-09-17 (message rong); tat lai.
TRACKING_DEBUG_DIR = BASE_DIR / "debug_tracking"
TRACKING_DEBUG_MAX_FILES = 50


def _ghi_debug_tracking(shipment_id, destination, lt_arrived_time, mode, tag, reason,
                        response, tracking_list, tracking_events):
    """Luu phan hoi Tracking Info tho. Khong bao gio duoc lam hong phan quyet."""
    if not TRACKING_DEBUG_DUMP:
        return
    try:
        TRACKING_DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        nut_tren = [n for n in (tracking_list or []) if isinstance(n, dict)]
        nut_co_con = sum(1 for n in nut_tren if n.get("children") or n.get("event_children"))
        tong_con = sum(len(n.get("children") or []) for n in nut_tren)
        ban_ghi = {
            "_tom_tat": {
                "shipment_id": shipment_id,
                "mode": mode,
                "verdict": tag,
                "reason": reason,
                "destination": destination,
                "lt_arrived_time": to_text(lt_arrived_time),
                "ghi_luc": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "so_nut_cap_tren": len(nut_tren),
                "so_nut_cap_tren_CO_children": nut_co_con,
                "tong_children_truc_tiep": tong_con,
                "tong_su_kien_sau_de_quy": len(tracking_events or []),
                "response_keys": sorted((response or {}).keys()) if isinstance(response, dict) else str(type(response)),
                "data_keys": sorted(((response or {}).get("data") or {}).keys())
                if isinstance(response, dict) and isinstance(response.get("data"), dict) else None,
            },
            "response": response,
        }
        ten = TRACKING_DEBUG_DIR / (
            f"{to_text(shipment_id)}_{tag}_{datetime.now():%Y%m%d_%H%M%S}.json"
        )
        ten.write_text(json.dumps(ban_ghi, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

        # Xoay vong RIENG theo tung phan quyet. Do 2026-09-17: 50 cho chung bi
        # lap day trong 6 phut boi toan NO_DESTINATION_RECEIVED cua don TO, day mat
        # moi mau NO_HANDLING cua Bulky - dung loai dang can dieu tra. Moi phan
        # quyet giu toi da TRACKING_DEBUG_MAX_FILES file -> tong toi da ~4 MB.
        cac_file = sorted(TRACKING_DEBUG_DIR.glob(f"*_{tag}_*.json"), key=lambda f: f.stat().st_mtime)
        for cu in cac_file[:-TRACKING_DEBUG_MAX_FILES]:
            try:
                cu.unlink()
            except OSError:
                pass
    except Exception as exc:
        print(f"[debug_tracking] khong ghi duoc {shipment_id}: {exc}")


def received_tag_at_bda_destination(
    fms_session,
    shipment_id,
    destination,
    lt_arrived_time=None,
    current_station="",
    cache_service=None,
    bulky_mode=False,
    bda_rehandled_mode=False,
):
    """Return the post-BDA physical/tag signal from Tracking Info.

    A shipment can contain Received events at several stations. Only the event
    at the first station to which BDA sent this TO is relevant for Single/Mass.
    A downstream handling event such as Parcel Sweeper Scan, Assigning,
    LMHub Assigned, or OnHold after LT arrival independently proves that the
    parcel is physically present downstream.

    bulky_mode=True replaces all of the Single/Mass reasoning with one much
    simpler rule, which is what Bulky actually needs (ops, 2026-09-08):

        any parcel-level event, at ANY station, at or after the LT arrived,
        proves the parcel physically exists and is being handled
        -> PARCEL_HANDLED (NOT_PENDING). Otherwise NO_HANDLING (stays PENDING).

    A Bulky parcel is a single parcel, so it needs no Mass/Single consolidation
    logic at all - a scan on it IS the parcel. The station is deliberately not
    restricted, on ops's reasoning: a parcel that is genuinely lost produces no
    operations anywhere, so any operation at all settles it. Verified against
    SPXVN064632481319, sent BDA -> 63-BDG Thuan An 02 Hub: the only event at
    Thuan An is TO-level, and the parcel then reappears being received and
    re-packed **at BDA** - it never left, so it is not lost. An earlier draft
    of this rule required station != BDA and wrongly held that one PENDING.

    This subsumes MASS_RETURNED_TO_BDA: when the destination packs the parcel
    and ships it back, those packing/transporting events are themselves
    parcel-level events after the LT arrived.

    It also avoids a latent bug in the TO path that Bulky would hit constantly.
    The destination `received_events` list below is NOT filtered by
    lt_arrived_at, unlike the two other time-sensitive checks in this function.
    A TO normally passes its destination once in its life so the omission
    rarely shows; a Bulky parcel shuttles back and forth through the same
    stations. Verified on SPXVN061005077038: its candidate is the 2026-09-06
    leg BDA -> 63-BDG Tan Dinh Hub, yet the function returned SINGLE from a
    `Parcel received by pickup hub [Single]` event at that same hub on
    2026-08-30 - seven days earlier, and on the opposite leg. bulky_mode is
    time-filtered throughout, so it cannot make that mistake.

    The TO path's `received_events` list is still not time-filtered; fixing that
    would move TO results, which ops has not asked for.

    bda_rehandled_mode=True adds ONE extra question ahead of the TO reasoning,
    for whole-missing TOs only (ops, 2026-09-10):

        a parcel-level event at BD A Mega SOC, at or after the LT arrived,
        means BDA found the parcel and re-dispatched it -> BDA_REHANDLED
        (NOT_PENDING).

    The case it exists for, ops's own words: BDA scanned the TO as departed on
    06/09 but forgot to load it, the trip reached Cam Xuyen SOC without it, the
    36-hour PENDING call was correct - and on 10/09 BDA found the whole TO,
    re-scanned it and put it on a new trip. Verified on TO202609062A8JQ /
    SPXVN060878861909: `Parcel arrived at sorting center` [Mass] at BDA
    2026-09-10 05:05:17, then packed into the same TO and added to LH Task
    LT0Q9A4X3SKT2, against an arrived_time of 2026-09-07 10:11:01.

    Unlike bulky_mode this is restricted to BDA, on ops's instruction: they want
    BDA's own re-handling to settle it, not a downstream station's.

    It CANNOT be done from BatchSearch's status code alone. A parcel still stuck
    at BDA untouched since 06/09 reports the same code 35 (SOC_LHPacked) as one
    re-dispatched this morning; only the event timestamp separates them.
    """
    shipment_id = to_text(shipment_id)
    destination_key = normalize_station(destination)
    if not shipment_id or not destination_key:
        return "UNKNOWN", "missing shipment or BDA destination"
    lt_arrived_at = parse_datetime_value(lt_arrived_time)

    url = f"{TRACKING_INFO_URL}?shipment_id={quote(shipment_id, safe='')}"
    response = fms_session.request_json(
        "GET",
        url,
        label=f"Tracking info {shipment_id}",
        fail_soft=False,
        max_retries=TRACKING_INFO_MAX_RETRIES,
    )
    tracking_list = ((response or {}).get("data") or {}).get("tracking_list") or []
    tracking_events = list(iter_tracking_events(tracking_list))
    cache_reconcile_arrival(
        cache_service,
        shipment_id,
        destination,
        tracking_events,
        not_before=lt_arrived_at,
    )

    if bda_rehandled_mode and not bulky_mode:
        # Runs before the TO reasoning so it can settle a TO that never left
        # BDA. The TO branch below can never reach this verdict on its own: it
        # clears only when current_station moves BEYOND the receiving stations,
        # and BDA is itself one of them.
        if not lt_arrived_at:
            return "UNKNOWN", "missing LT arrived time, cannot date the events"
        bda_key = normalize_station(SOC_CODE)
        for event in tracking_events:
            if normalize_station(event.get("station_name")) != bda_key:
                continue
            event_at = parse_datetime_value(event.get("timestamp"))
            if not event_at or event_at < lt_arrived_at:
                continue
            # Theo status code, vi Tracking Info tra `message` RONG cho cac lan
            # quet kien nen is_parcel_level_event bo qua chung (2026-09-17: 166
            # don duoc kiem, chi 1 duoc loai). Ops chot 2026-09-17: Received -
            # KE CA tag Mass - hoac Packing/Packed tai BDA sau khi LT toi la BDA
            # da tim thay va xu ly lai. Mass duoc tinh o day vi ca TO thieu, nhan
            # nguyen TO chinh la nhan don nay; chi dung o nhanh nay (da gioi han
            # tram BDA o tren), khong dung cho TO thieu mot phan.
            status_code = EXPORT_BOT.to_int(event.get("status"), default=-1)
            if status_code in BDA_REHANDLED_STATUS_CODES:
                return "BDA_REHANDLED", (
                    f"{BDA_REHANDLED_STATUS_NAMES[status_code]} at "
                    f"{to_text(event.get('station_name'))} on {event_at:%Y-%m-%d %H:%M:%S}, "
                    "after the LT arrived"
                )
            # is_parcel_level_event keeps out the TO-level linehaul rows, which
            # FMS mirrors onto every shipment on the manifest INCLUDING the
            # missing ones. Counting those would clear every whole-missing TO.
            if not is_parcel_level_event(event):
                continue
            return "BDA_REHANDLED", (
                f"{to_text(event.get('message')) or 'event'} at "
                f"{to_text(event.get('station_name'))} on {event_at:%Y-%m-%d %H:%M:%S}, "
                "after the LT arrived"
            )
        # Falls through to the normal TO reasoning: no re-handling at BDA is not
        # by itself a verdict, the TO rules still get their say.

    if bulky_mode:
        # A Bulky order is one physical parcel, so none of the Mass/Single
        # consolidation reasoning below applies to it. Any parcel-level event,
        # at ANY station, at or after the LT arrived, means somebody had this
        # parcel in their hands - so it is not lost. Received [Single] is
        # included by construction: it is simply the earliest such event,
        # normally just ahead of packing/packed.
        #
        # The station is deliberately NOT restricted to "past BDA". A parcel
        # that came back and was re-scanned at BDA is equally accounted for -
        # see SPXVN064632481319 in the docstring.
        #
        # The `at or after lt_arrived_at` guard is the whole point. Without it,
        # a station the parcel visited on an earlier journey clears the current
        # leg - which is exactly how the TO path mis-cleared SPXVN061005077038.
        if not lt_arrived_at:
            return "UNKNOWN", "missing LT arrived time, cannot date the events"
        for event in tracking_events:
            event_station = normalize_station(event.get("station_name"))
            if not event_station:
                continue
            event_at = parse_datetime_value(event.get("timestamp"))
            if not event_at or event_at < lt_arrived_at:
                continue
            # Received o bat ky tram nao sau khi LT toi la du (ops, 2026-09-17):
            # Bulky la mot kien duy nhat, da co Received moi thi chac chan co
            # trang thai moi. Phai xet theo status code vi Tracking Info tra
            # `message` RONG cho cac lan quet kien - is_parcel_level_event doi
            # message bat dau bang "Parcel" nen bo qua chung. Do tren 9 dump
            # Bulky NO_HANDLING: 3 don (vd SPXVN063713037949, SOC_Received
            # [Single] tai BDA 16/09 21:24) bi giu PENDING sai vi ly do nay.
            status_code = EXPORT_BOT.to_int(event.get("status"), default=-1)
            if status_code in BULKY_RECEIVED_STATUS_CODES:
                status_name = BULKY_RECEIVED_STATUS_NAMES.get(status_code, str(status_code))
                return "PARCEL_HANDLED", (
                    f"{status_name} at {to_text(event.get('station_name'))} on "
                    f"{event_at:%Y-%m-%d %H:%M:%S}, after the LT arrived"
                )
            if not is_parcel_level_event(event):
                continue
            return "PARCEL_HANDLED", (
                f"{to_text(event.get('message')) or 'event'} at "
                f"{to_text(event.get('station_name'))} on {event_at:%Y-%m-%d %H:%M:%S}, "
                "after the LT arrived"
            )
        _ghi_debug_tracking(
            shipment_id, destination, lt_arrived_time, "bulky", "NO_HANDLING",
            "no parcel-level event anywhere after the LT arrived",
            response, tracking_list, tracking_events,
        )
        return "NO_HANDLING", "no parcel-level event anywhere after the LT arrived"

    received_events = []
    downstream_single_event = None
    for event in tracking_events:
        status_code = EXPORT_BOT.to_int(event.get("status"), default=-1)
        event_station = normalize_station(event.get("station_name"))
        event_at = parse_datetime_value(event.get("timestamp"))
        if (
            status_code in TRACKING_INFO_PHYSICAL_HANDLING_STATUS_CODES
            and event_station
            and event_station != normalize_station(SOC_CODE)
            and (not lt_arrived_at or (event_at and event_at >= lt_arrived_at))
        ):
            status_name = TRACKING_INFO_PHYSICAL_HANDLING_STATUS_NAMES.get(
                status_code, str(status_code)
            )
            return "PHYSICAL_HANDLING", (
                f"{status_name} at {to_text(event.get('station_name'))} after LT arrived"
            )
        if status_code not in TO_BOT.RECEIVED_STATUS_CODES:
            continue
        raw_tags = event.get("tags") or []
        tags = [to_text(tag).lower() for tag in (raw_tags if isinstance(raw_tags, list) else [raw_tags])]
        if (
            "single" in tags
            and event_station
            and event_station != normalize_station(SOC_CODE)
            and (not lt_arrived_at or (event_at and event_at >= lt_arrived_at))
        ):
            downstream_single_event = event
        if event_station != destination_key:
            continue
        received_events.append(event)

    # A later Received [Single] at a downstream station proves this parcel is
    # no longer an unresolved mass-parcel case for the original LT sequence.
    if downstream_single_event:
        return "SINGLE", (
            "downstream Received has Single tag at "
            f"{to_text(downstream_single_event.get('station_name'))}"
        )

    if not received_events:
        _ghi_debug_tracking(
            shipment_id, destination, lt_arrived_time,
            "to_rehandled" if bda_rehandled_mode else "to", "NO_DESTINATION_RECEIVED",
            "no Received event at BDA destination",
            response, tracking_list, tracking_events,
        )
        return "NO_DESTINATION_RECEIVED", "no Received event at BDA destination"

    # A shipment may be marked Mass at the destination first, then be handled
    # individually and receive a later Single tag at that same destination.
    # Any destination Received [Single] proves this is no longer a mass case.
    for event in received_events:
        raw_tags = event.get("tags") or []
        tags = [to_text(tag).lower() for tag in (raw_tags if isinstance(raw_tags, list) else [raw_tags])]
        if "single" in tags:
            return "SINGLE", "destination Received has Single tag"

    # A TO can be received as Mass at the LT destination, then be packed and
    # transported back to BDA for a new journey. BatchSearch then shows BDA as
    # Current Station, which must not be mistaken for a parcel stuck on the
    # old LT. This extra history check is intentionally restricted to Current
    # Station = BDA, keeping Tracking Info cheap for every other TO order.
    if normalize_station(current_station) == normalize_station(SOC_CODE):
        mass_received_at = None
        for event in received_events:
            event_at = parse_datetime_value(event.get("timestamp"))
            if not event_at:
                continue
            if lt_arrived_at and (not event_at or event_at < lt_arrived_at):
                continue
            if mass_received_at is None or event_at < mass_received_at:
                mass_received_at = event_at

        if mass_received_at:
            processed_tokens = (
                "packing",
                "packed",
                "adding into",
                "added into",
                "transporting",
                "transported",
            )
            for event in tracking_events:
                event_at = parse_datetime_value(event.get("timestamp"))
                if not event_at or event_at <= mass_received_at:
                    continue
                if normalize_station(event.get("station_name")) != destination_key:
                    continue
                message = to_text(event.get("message")).lower()
                if any(token in message for token in processed_tokens):
                    return "MASS_RETURNED_TO_BDA", (
                        "Mass received at destination, then processed/transferred "
                        f"from {to_text(event.get('station_name'))} before returning to BDA"
                    )
    return "MASS", "destination Received has no Single tag"


# Ba moc thoi gian dot quet duoc phep dung, va chi ba. Ten cot di thang vao SQL
# nen phai la danh sach trang, khong nhan chuoi tu ben ngoai.
SWEEP_MARKS = {
    "precheck": "precheck_at",      # arrived + 24h
    "final": "final_check_at",      # arrived + 32h
    "eligible": "eligible_at",      # arrived + 36h, moc len sheet
}


def sweep_candidates_past_eligible(service, limit, mark="eligible"):
    """Candidate da qua mot moc thoi gian ma CHUA duoc check lan nao.

    `mark` chon moc: precheck (+24h), final (+32h), eligible (+36h, mac dinh).
    Quet som hon la dung voi thiet ke: batchsearch_candidates(sweep_precheck_only)
    ep ket qua ve giai doan PRECHECK, ma PRECHECK chinh la viec BOT 3 lam o moc
    24h trong vong binh thuong. Nen quet o moc 'precheck' khong lam gi khac so
    voi duong chinh, chi lam som hon va nhanh hon.

    Danh cho dot quet BatchSearch chay tay - KHONG dung trong vong BOT 3 binh
    thuong. Khac latest_candidates_to_batch o mot cho quan trong: chi lay dong
    queue_stage='NEW'. Quet xong, moi dong hoac bi prune (NOT_PENDING) hoac doi
    sang PRECHECK_PENDING/PRECHECK_UNKNOWN, nen khong dong nao lot lai vao lot
    sau. Neu dung latest_candidates_to_batch thi nhanh
    "r.processing_stage='PRECHECK' AND final_check_at <= now" - von khong co moc
    thoi gian nao - se keo lai toan bo don PENDING vua quet vao dau moi lot, va
    dot quet dam chan tai cho.
    """
    cot = SWEEP_MARKS.get(mark)
    if not cot:
        raise ValueError(
            f"moc quet khong hop le: {mark!r}. Chon mot trong {sorted(SWEEP_MARKS)}"
        )
    return query_bq(
        service,
        f"""
        SELECT {", ".join("c." + field["name"] for field in candidate_schema_fields())}
        FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_CANDIDATE_TABLE_ID}` c
        WHERE c.candidate_rule_version = '{CANDIDATE_RULE_VERSION}'
          AND LOWER(TRIM(c.sender)) = LOWER('{SOC_CODE}')
          AND c.order_number IS NOT NULL
          AND c.order_number != ''
          AND c.order_number != '{SEQUENCE_MARKER_ORDER}'
          AND c.arrived_time IS NOT NULL
          AND c.{cot} IS NOT NULL
          AND c.{cot} <= CURRENT_DATETIME('Asia/Bangkok')
          AND COALESCE(c.queue_stage, 'NEW') = 'NEW'
          AND c.prechecked_at IS NULL
          AND NOT EXISTS (
            SELECT 1
            FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_RESULT_TABLE_ID}` r
            WHERE r.result_rule_version = '{RESULT_RULE_VERSION}'
              AND r.candidate_id = c.candidate_id
          )
        -- KHONG co ORDER BY, va day la co y.
        --
        -- Do 2026-09-14: them ORDER BY vao cau nay lam mot lot mat 564 giay -
        -- ngay ca LIMIT 3 cung mat 383 giay - vi khong chi muc nao phuc vu duoc
        -- ca bo loc lan thu tu, nen SQLite phai sap toan bo 1,34 trieu dong khop
        -- dieu kien truoc khi tra dong dau tien. 24 lot la them gan 4 gio cho
        -- rieng viec sap xep.
        --
        -- Dot quet khong can thu tu: cuoi cung moi dong deu duoc quet. Dong nao
        -- quet roi thi hoac bi prune, hoac doi queue_stage, nen khong con khop
        -- dieu kien o lot sau - SQLite chi can quet den khi du LIMIT dong la
        -- dung. Thu tu uu tien van duoc ton trong o cho no thuc su quan trong:
        -- vong BOT 3 binh thuong, khi cham Tracking Info cho don PENDING sot lai.
        LIMIT {int(limit)}
        """,
        fail_soft=True,
    )


def _khoa_to_transit(row):
    return (to_text(row.get("trip_number")), to_text(row.get("to_number")))


def _bo_to_transit_chua_du_don(service, candidates):
    """Bo cac TO transit ma lat nay khong chua du moi don cua TO trong bang."""
    chon = {}
    for c in candidates:
        if c.get("source_type") == "TO_TRANSIT":
            chon.setdefault(_khoa_to_transit(c), set()).add(to_text(c.get("candidate_id")))
    if not chon:
        return candidates, 0

    tong = {}
    to_numbers = sorted({k[1] for k in chon if k[1]})
    for i in range(0, len(to_numbers), 400):
        phan = to_numbers[i:i + 400]
        danh_sach = ", ".join(f"'{t}'" for t in phan if re.fullmatch(r"[0-9A-Za-z]+", t))
        if not danh_sach:
            continue
        try:
            rows = query_bq(
                service,
                f"""
                SELECT trip_number, to_number, COUNT(DISTINCT candidate_id) AS so_don
                FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_CANDIDATE_TABLE_ID}`
                WHERE source_type = 'TO_TRANSIT'
                  AND candidate_source = '{TRANSIT_CANDIDATE_SOURCE}'
                  AND candidate_rule_version = '{CANDIDATE_RULE_VERSION}'
                  AND order_number IS NOT NULL
                  AND order_number != ''
                  AND to_number IN ({danh_sach})
                GROUP BY trip_number, to_number
                """,
            )
        except Exception as exc:
            # Khong dem duoc thi khong dam ket luan theo TO: bo het transit vong nay.
            # (query_bq fail_soft tra [] -> se lam moi TO trong nhu "du don", nguy hiem.)
            print(f"Khong dem duoc don TO transit ({exc}); bo qua transit vong nay")
            return [c for c in candidates if c.get("source_type") != "TO_TRANSIT"], len(chon)
        for r in rows:
            tong[_khoa_to_transit(r)] = EXPORT_BOT.to_int(r.get("so_don"), default=0)

    thieu = {k for k, ids in chon.items() if len(ids) < tong.get(k, 0)}
    if not thieu:
        return candidates, 0
    return [
        c for c in candidates
        if not (c.get("source_type") == "TO_TRANSIT" and _khoa_to_transit(c) in thieu)
    ], len(thieu)


def _gop_ket_qua_transit_theo_to(cac_don):
    """Ket luan TO transit theo CA TO (ops 2026-09-17).

    cac_don: list (candidate, result, ...) cua MOT TO.
      - Chi can 1 don NOT_PENDING -> ca TO NOT_PENDING.
      - Khong co don nao NOT_PENDING nhung co don UNKNOWN -> ca TO UNKNOWN (giu
        lai, vong sau kiem lai toan bo TO).
      - Moi don PENDING (o BDA va tram ke tiep van la Y) -> ca TO PENDING.
    Sua result tai cho.
    """
    trang_thai = [ket_qua["pending_status"] for _, ket_qua, *_ in cac_don]
    # Luc tao candidate, don nao TO Detail da ghi "Received in [...]" thi bi bo
    # qua - tuc Y da nhan mot phan TO, TO khong the thieu nguyen. Do 2026-09-17:
    # 167/4.675 TO transit co it candidate hon so kien.
    so_kien = max(
        (EXPORT_BOT.to_int(c.get("to_parcel_quantity"), default=0) for c, *_ in cac_don),
        default=0,
    )
    if so_kien > len(cac_don):
        for _, ket_qua, *_ in cac_don:
            ket_qua["pending_status"] = "NOT_PENDING"
            ket_qua["result_reason"] = (
                f"transit TO cleared: only {len(cac_don)}/{so_kien} order(s) unreceived "
                "when collected, the rest already had a Received remark"
            )
        return "NOT_PENDING"
    if "NOT_PENDING" in trang_thai:
        ly_do = next(
            ket_qua["result_reason"] for _, ket_qua, *_ in cac_don
            if ket_qua["pending_status"] == "NOT_PENDING"
        )
        so = trang_thai.count("NOT_PENDING")
        for _, ket_qua, *_ in cac_don:
            if ket_qua["pending_status"] != "NOT_PENDING":
                ket_qua["pending_status"] = "NOT_PENDING"
                ket_qua["result_reason"] = (
                    f"transit TO cleared: {so}/{len(cac_don)} order(s) moved ({ly_do})"
                )
        return "NOT_PENDING"
    if "UNKNOWN" in trang_thai:
        so = trang_thai.count("UNKNOWN")
        for _, ket_qua, *_ in cac_don:
            if ket_qua["pending_status"] != "UNKNOWN":
                ket_qua["pending_status"] = "UNKNOWN"
                ket_qua["result_reason"] = (
                    f"transit TO undecided: {so}/{len(cac_don)} order(s) without BatchSearch data"
                )
        return "UNKNOWN"
    for _, ket_qua, *_ in cac_don:
        ket_qua["result_reason"] = (
            f"transit whole TO missing: all {len(cac_don)} order(s) at BDA, next station is TO receiver"
        )
    return "PENDING"


def batchsearch_candidates(
    service,
    fms_session,
    limit=None,
    force_recheck_pending=False,
    candidates=None,
    sweep_precheck_only=False,
):
    """sweep_precheck_only: ep moi ket qua ve giai doan PRECHECK.

    Dung cho dot quet chay tay. Hai he qua, ca hai deu la co y:
      - chan Tracking Info, vi khoi kiem tra do da co dieu kien
        `processing_stage != "PRECHECK"`. Do 2026-09-14 chi 6,7-10,3% so don can
        toi no, nen bo qua giup lot quet nhanh khoang 6 lan.
      - ket qua PRECHECK KHONG len sheet (export loc
        processing_stage IN ('FINAL_READY','FINAL')), nen khong co nguy co do mot
        dong PENDING chua qua Tracking Info vao mat cac ban.
    Don NOT_PENDING van bi prune_finalized_pending_candidates() xoa han - ham do
    khong nhin processing_stage. Don PENDING o lai voi queue_stage
    PRECHECK_PENDING va duoc BOT 3 uu tien cham Tracking Info o vong sau.
    """
    if candidates is None:
        candidates = latest_candidates_to_batch(
            service,
            limit=limit,
            force_recheck_pending=force_recheck_pending,
        )
    print(f"Unified candidates can batchsearch: {len(candidates)}")
    if not candidates:
        return 0

    # TO transit duoc ket luan theo CA TO, nen moi don cua TO phai nam trong cung
    # mot vong. LIMIT cua bo chon co the cat ngang mot TO o cuoi lat: TO do bo qua
    # vong nay, vong sau no dung dau hang (deadline som hon) va duoc lay tron.
    candidates, transit_bo_qua = _bo_to_transit_chua_du_don(service, candidates)
    if transit_bo_qua:
        print(f"TO transit chua du don trong lat nay, de vong sau: {transit_bo_qua} TO")
    if not candidates:
        return 0

    # A TO counts as whole-missing when every parcel on its manifest is still
    # missing - the same test the Sheet shows as `74/74`.
    #
    # Counting only this cycle's slice does NOT work, and the first production
    # cycle proved it: TO202609062A8JQ is 74/74 on the sheet, but its 74 orders
    # were last checked between 05:55:37 and 06:04:59, so their 120-minute
    # recheck marks are spread over nine minutes and only 15 of them were due
    # when the 07:49 cycle picked its slice. The slice-only test called it
    # partial-missing and skipped the new rule.
    #
    # The count that decides is how many of the TO's orders are PENDING right
    # now. Nothing else.
    #
    # An earlier version also accepted the slice count as a floor, "for a TO
    # whose orders have no result yet". That was wrong and shipped broken for
    # most of 2026-09-10: the slice holds candidates DUE FOR CHECKING, and on a
    # TO's first final pass every one of its orders is a candidate, so the count
    # always equalled the manifest and every TO read as whole-missing. Measured
    # on a live 12,000 slice at 21:02 that day: 279 TOs judged whole-missing, of
    # which **0** had a single order actually PENDING, against 3 genuinely
    # whole-missing TOs in the entire table (vs 805 partial-missing). Ops
    # confirmed the scope again afterwards: the 2026-09-10 rule is for
    # whole-missing TOs only, and partial-missing behaviour must not change.
    #
    # Cost of using PENDING alone: a newly whole-missing TO is recognised on its
    # SECOND pass rather than its first, about 15-20 minutes later. Partial-
    # missing TOs lose nothing, because the "BDA -> X -> back to BDA" case they
    # care about is already handled by MASS_RETURNED_TO_BDA below, and downstream
    # handling by PHYSICAL_HANDLING / SINGLE.
    to_pending_now = {}
    pending_rows = query_bq(
        service,
        f"""
        SELECT to_number, COUNT(DISTINCT order_number) AS missing_now
        FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_RESULT_TABLE_ID}`
        WHERE pending_status = 'PENDING'
          AND result_rule_version = '{RESULT_RULE_VERSION}'
          AND to_number IS NOT NULL
          AND to_number != ''
        GROUP BY to_number
        """,
        fail_soft=True,
    )
    for row in pending_rows or []:
        to_number = to_text(row.get("to_number"))
        if to_number:
            to_pending_now[to_number] = EXPORT_BOT.to_int(row.get("missing_now"), default=0)

    to_manifest_size = {}
    for candidate in candidates:
        to_number = to_text(candidate.get("to_number"))
        if not to_number:
            continue
        quantity = EXPORT_BOT.to_int(candidate.get("to_parcel_quantity"), default=0)
        if quantity > 0:
            to_manifest_size[to_number] = max(to_manifest_size.get(to_number, 0), quantity)

    def is_whole_missing_to(candidate):
        to_number = to_text(candidate.get("to_number"))
        manifest = to_manifest_size.get(to_number, 0)
        if not to_number or manifest <= 0:
            return False
        return to_pending_now.get(to_number, 0) >= manifest

    tracking_info_cache = {}
    # Verdicts kept from earlier cycles; see TRACKING_VERDICT_CACHE_MINUTES.
    tracking_verdict_cache = load_tracking_verdict_cache(service)
    tracking_verdict_rows = []
    tracking_verdict_reused = 0
    result_rows = []
    precheck_candidate_updates = []
    single_filtered = 0
    physical_handling_filtered = 0
    mass_returned_to_bda_filtered = 0
    # Whole-missing TOs cleared because BDA found and re-dispatched them.
    bda_rehandled_checked = 0
    bda_rehandled_filtered = 0
    tracking_info_unknown = 0
    tracking_info_failures = 0
    tracking_info_circuit_open = False
    to_pending_tracking_checked = 0
    # Counted separately so the effect of extending Tracking Detail to Bulky
    # (2026-09-08) can be judged from the log without a query.
    bulky_tracking_checked = 0
    bulky_tracking_filtered = 0
    direct_tracking_checked = 0
    direct_tracking_prechecks = 0
    direct_tracking_filtered = 0
    precheck_processed = 0
    final_processed = 0

    # BatchSearch is the cheap first-pass for every candidate. PRECHECK
    # survivors stay in the candidate queue until final_check_at; Tracking Info
    # is reserved for final-stage TO survivors.
    candidates_for_batchsearch = candidates
    transit_theo_to = {}

    def ghi_ket_qua(candidate, result, processing_stage, checked_at, checked_at_value):
        if processing_stage == "PRECHECK" and result.get("pending_status") in ("PENDING", "UNKNOWN"):
            queue_row = {
                field["name"]: candidate.get(field["name"])
                for field in candidate_schema_fields()
            }
            precheck_status = result.get("pending_status")
            queue_row["queue_stage"] = (
                "PRECHECK_PENDING" if precheck_status == "PENDING"
                else "PRECHECK_UNKNOWN"
            )
            queue_row["precheck_status"] = precheck_status
            queue_row["prechecked_at"] = candidate.get("prechecked_at") or checked_at
            queue_row["next_check_at"] = (
                candidate.get("final_check_at")
                if precheck_status == "PENDING"
                else (checked_at_value + timedelta(minutes=UNKNOWN_RECHECK_MINUTES)).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            )
            queue_row["check_attempt"] = (
                EXPORT_BOT.to_int(candidate.get("check_attempt"), default=0) + 1
            )
            queue_row["last_error"] = (
                result.get("result_reason") or "" if precheck_status == "UNKNOWN" else ""
            )
            queue_row["candidate_checked_at"] = checked_at
            precheck_candidate_updates.append(queue_row)
            return

        # PRECHECK NOT_PENDING is written temporarily so the normal prune
        # step can remove its candidate. FINAL outcomes are durable results.
        result_rows.append(result)

    for batch in chunked(candidates_for_batchsearch, BATCH_SIZE):
        checked_at_value = datetime.now()
        checked_at = checked_at_value.strftime("%Y-%m-%d %H:%M:%S")
        order_numbers = sorted({row["order_number"] for row in batch if row.get("order_number")})
        if not order_numbers:
            continue
        tracking_items = search_tracking_batch(fms_session, order_numbers)
        item_by_order = {
            tracking_item_order_number(item): item
            for item in tracking_items
            if tracking_item_order_number(item)
        }
        for candidate in batch:
            processing_stage = (
                "PRECHECK" if sweep_precheck_only
                else candidate_processing_stage(candidate, checked_at_value)
            )
            if processing_stage == "PRECHECK":
                precheck_processed += 1
            else:
                final_processed += 1
            result = enrich_result(
                candidate,
                item_by_order.get(candidate.get("order_number")),
                checked_at,
                processing_stage,
            )

            # TO transit: chi BatchSearch, khong Tracking Info. Giu lai toi het
            # vong de ket luan theo CA TO, roi moi ghi - xem sau vong lap.
            if candidate.get("source_type") == "TO_TRANSIT":
                transit_theo_to.setdefault(_khoa_to_transit(candidate), []).append(
                    (candidate, result, processing_stage, checked_at, checked_at_value)
                )
                continue

            # Tracking Detail runs only in the final window after BatchSearch
            # identifies a pending TO. PRECHECK stays cheap and reduces the
            # final workload before the 36-hour Sheet release time.
            is_to_sorting = candidate.get("source_type") == "TO_SORTING"
            is_to_transit_at_bda = (
                candidate.get("source_type") == "TO_TRANSIT"
                and normalize_station(result.get("current_station")) == normalize_station(SOC_CODE)
            )
            # Bulky joined this check on 2026-09-08 at the team's request. Until
            # then a Bulky verdict rested on BatchSearch alone with no second
            # pass. Bulky does NOT run the Mass/Single reasoning below - it gets
            # bulky_mode, which asks one much simpler question: was this parcel
            # handled anywhere past BDA after the LT arrived? See the docstring
            # of received_tag_at_bda_destination().
            #
            # Unconditional here, like TO_SORTING rather than TO_TRANSIT: the
            # rule is about the parcel's history, not where it sits right now.
            # Cost is one Tracking Info GET per Bulky PENDING order: +51 against
            # the 1,092 already made for TOs, ~5%.
            is_bulky = candidate.get("source_type") == "BULKY"
            # Ops, 2026-09-10: a whole-missing TO that BDA later found and
            # re-dispatched is not lost. Restricted to whole-missing TOs and to
            # events at BDA itself, both at ops's instruction.
            wants_bda_rehandled = (
                not is_bulky
                and (is_to_sorting or is_to_transit_at_bda)
                and is_whole_missing_to(candidate)
            )
            if (
                (is_to_sorting or is_to_transit_at_bda or is_bulky)
                and result.get("pending_status") == "PENDING"
                and processing_stage != "PRECHECK"
            ):
                destination = (
                    candidate.get("receiver")
                    or candidate.get("unloaded_station_name")
                    or candidate.get("to_station")
                    or ""
                )
                cache_key = (
                    to_text(candidate.get("order_number")),
                    normalize_station(destination),
                    to_text(candidate.get("arrived_time")),
                    normalize_station(result.get("current_station")),
                    # Both this and current_station come from BatchSearch, which
                    # is re-read every cycle. Together they make the key expire
                    # by itself the moment anything happens to the parcel, which
                    # is what lets the verdict survive across cycles at all -
                    # station alone would not, since 70% of Tracking Detail
                    # clearances happen without the parcel changing station.
                    to_text(result.get("order_status_code")),
                    # Bulky asks the same question with bulky_mode=True and gets
                    # a different answer, so it must not share a cache entry
                    # with a TO candidate for the same order.
                    is_bulky,
                    # Same reason: a whole-missing TO asks one extra question
                    # first, so its verdict is not interchangeable with the one
                    # a partial-missing TO gets for the same order.
                    wants_bda_rehandled,
                )
                if tracking_info_circuit_open:
                    tag_kind, tag_reason = "UNKNOWN", "tracking info temporarily paused after repeated FMS errors"
                else:
                    try:
                        if cache_key not in tracking_info_cache:
                            stored_key = tracking_verdict_key(cache_key)
                            stored = tracking_verdict_cache.get(stored_key)
                            if stored is not None:
                                # Same order, same station, same status as when
                                # this was decided: nothing can have happened.
                                tracking_info_cache[cache_key] = stored
                                tracking_verdict_reused += 1
                            else:
                                verdict = received_tag_at_bda_destination(
                                    fms_session,
                                    cache_key[0],
                                    destination,
                                    candidate.get("arrived_time"),
                                    result.get("current_station"),
                                    cache_service=service,
                                    # Bulky takes the simple downstream-handling
                                    # rule instead of the Mass/Single reasoning -
                                    # see the docstring.
                                    bulky_mode=is_bulky,
                                    # Whole-missing TO only: BDA finding the
                                    # parcel and sending it again settles it.
                                    bda_rehandled_mode=wants_bda_rehandled,
                                )
                                tracking_info_cache[cache_key] = verdict
                                # UNKNOWN is never stored: it means the lookup
                                # itself failed, and freezing a failure for the
                                # whole TTL would hide a recoverable error.
                                if verdict[0] != "UNKNOWN":
                                    tracking_verdict_cache[stored_key] = verdict
                                    tracking_verdict_rows.append({
                                        "cache_key": stored_key,
                                        "order_number": cache_key[0],
                                        "tag_kind": verdict[0],
                                        "tag_reason": verdict[1],
                                        "decided_at": checked_at,
                                    })
                        tag_kind, tag_reason = tracking_info_cache[cache_key]
                        tracking_info_failures = 0
                    except Exception as error:
                        tracking_info_failures += 1
                        tag_kind, tag_reason = "UNKNOWN", str(error)
                        if tracking_info_failures >= TRACKING_INFO_FAILURE_CIRCUIT_BREAKER:
                            tracking_info_circuit_open = True
                            print(
                                "Pause Tracking Detail for this cycle after repeated FMS errors. "
                                "Remaining TO candidates will retry next cycle."
                            )

                to_pending_tracking_checked += 1
                if is_bulky:
                    bulky_tracking_checked += 1
                if wants_bda_rehandled:
                    bda_rehandled_checked += 1
                if tag_kind == "BDA_REHANDLED":
                    result["pending_status"] = "NOT_PENDING"
                    result["result_reason"] = f"TO re-handled at {SOC_CODE}: {tag_reason}"
                    bda_rehandled_filtered += 1
                elif tag_kind == "SINGLE":
                    result["pending_status"] = "NOT_PENDING"
                    result["result_reason"] = f"TO Single excluded: {tag_reason}"
                    single_filtered += 1
                elif tag_kind == "PHYSICAL_HANDLING":
                    result["pending_status"] = "NOT_PENDING"
                    result["result_reason"] = f"TO physical downstream handling: {tag_reason}"
                    physical_handling_filtered += 1
                elif tag_kind == "MASS_RETURNED_TO_BDA":
                    result["pending_status"] = "NOT_PENDING"
                    result["result_reason"] = f"TO Mass handled at LT destination then returned to BDA: {tag_reason}"
                    mass_returned_to_bda_filtered += 1
                elif tag_kind == "PARCEL_HANDLED":
                    # Bulky only. NO_HANDLING is the other Bulky verdict and is
                    # deliberately absent here: it falls through and the order
                    # stays PENDING.
                    result["pending_status"] = "NOT_PENDING"
                    result["result_reason"] = f"Bulky parcel handled: {tag_reason}"
                elif tag_kind == "UNKNOWN":
                    tracking_info_unknown += 1
                    result["pending_status"] = "UNKNOWN"
                    result["result_reason"] = f"TO tracking detail deferred: {tag_reason}"
                if is_bulky and result.get("pending_status") == "NOT_PENDING":
                    bulky_tracking_filtered += 1

            ghi_ket_qua(candidate, result, processing_stage, checked_at, checked_at_value)

        # Written per batch rather than at the end, so a cycle that dies partway
        # still leaves the verdicts it paid FMS for.
        if tracking_verdict_rows:
            store_tracking_verdicts(service, tracking_verdict_rows)
            tracking_verdict_rows = []

    # Ket luan TO transit theo CA TO roi moi ghi (ops 2026-09-17).
    transit_ket_luan = {"PENDING": 0, "NOT_PENDING": 0, "UNKNOWN": 0}
    transit_so_don = 0
    for cac_don in transit_theo_to.values():
        transit_ket_luan[_gop_ket_qua_transit_theo_to(cac_don)] += 1
        for candidate, result, processing_stage, checked_at, checked_at_value in cac_don:
            ghi_ket_qua(candidate, result, processing_stage, checked_at, checked_at_value)
            transit_so_don += 1
    if transit_theo_to:
        print(
            f"TO transit (chi BatchSearch): TO={len(transit_theo_to)} don={transit_so_don} | "
            f"thieu nguyen TO={transit_ket_luan['PENDING']} | "
            f"da di (NOT_PENDING)={transit_ket_luan['NOT_PENDING']} | "
            f"chua xac dinh={transit_ket_luan['UNKNOWN']}"
        )

    prune_tracking_verdict_cache(service)
    inserted_results = load_rows_to_bq(service, result_rows, BIGQUERY_RESULT_TABLE_ID)
    updated_candidates = load_rows_to_bq(
        service,
        precheck_candidate_updates,
        BIGQUERY_CANDIDATE_TABLE_ID,
    )
    inserted = inserted_results + updated_candidates
    print(
        "Unified batchsearch done. "
        f"Processed: {inserted} | final results: {inserted_results} | "
        f"precheck queue updated: {updated_candidates} | "
        f"precheck: {precheck_processed} | final: {final_processed} | "
        f"TO direct Tracking Info: precheck={direct_tracking_prechecks} | "
        f"checked={direct_tracking_checked} | filtered={direct_tracking_filtered} | "
        f"TO Pending checked by tracking info: {to_pending_tracking_checked} | "
        f"TO Single excluded: {single_filtered} | "
        f"TO physical handling excluded (Sweeper/Assigning/Assigned/OnHold): {physical_handling_filtered} | "
        f"TO Mass returned to BDA excluded: {mass_returned_to_bda_filtered} | "
        f"whole-missing TO re-handled at BDA: checked={bda_rehandled_checked} "
        f"excluded={bda_rehandled_filtered} | "
        f"Bulky tracking checked: {bulky_tracking_checked} | "
        f"Bulky excluded: {bulky_tracking_filtered} | "
        f"tracking info unknown: {tracking_info_unknown} | "
        f"tracking verdict reused from cache: {tracking_verdict_reused} "
        f"(TTL {TRACKING_VERDICT_CACHE_MINUTES}m)"
    )
    return inserted


def store_candidates(service, candidates, sequence_markers):
    if not candidates and not sequence_markers:
        return 0, 0, 0, 0
    bulky_rows = [row for row in candidates if row.get("source_type") == "BULKY"]
    to_rows = [row for row in candidates if row.get("source_type") == "TO_SORTING"]
    all_inserted = load_rows_to_bq(service, candidates, BIGQUERY_CANDIDATE_TABLE_ID)
    bulky_inserted = load_rows_to_bq(service, bulky_rows, BIGQUERY_BULKY_CANDIDATE_TABLE_ID)
    # TO candidates are staged by BDA before Admin starts. The durable recheck
    # queue is lt_pending_candidate, so do not append them back to staging.
    to_inserted = len(to_rows)
    marker_inserted = load_rows_to_bq(service, sequence_markers, BIGQUERY_RESULT_TABLE_ID)
    return all_inserted, bulky_inserted, to_inserted, marker_inserted


def store_admin_handover_to_detail_states(service, states):
    """Checkpoint TOs only after their candidate batch is durable.

    This protects Bot 2 from re-reading every Handover TO on every cycle. A
    failed TO API call has no state row, so it remains eligible for retry.
    """
    unique_states = {}
    for row in states:
        trip_id = to_text(row.get("trip_id"))
        sequence_number = EXPORT_BOT.to_int(row.get("sequence_number"), default=0)
        to_number = to_text(row.get("to_number"))
        if not trip_id or sequence_number <= 0 or not to_number:
            continue
        unique_states[(trip_id, sequence_number, to_number)] = row
    return load_rows_to_bq(
        service,
        list(unique_states.values()),
        BIGQUERY_TO_DETAIL_STATE_TABLE_ID,
    )


def cleanup_completed_handover_lt_unit(service):
    """Remove old Handover TO checkpoints after a safe 48-hour retention.

    A sequence is removable only when every TO_SORTING row has an Admin TO
    detail checkpoint. The checkpoint is written after candidate storage, so
    every suspicious order is already in the durable `lt_pending_candidate`
    queue (or the TO legitimately produced no candidate).
    """
    ready_query = f"""
    WITH latest_admin_state AS (
      SELECT
        trip_id,
        sequence_number,
        to_number,
        ROW_NUMBER() OVER (
          PARTITION BY trip_id, sequence_number, to_number
          ORDER BY checked_at DESC
        ) AS rn
      FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TO_DETAIL_STATE_TABLE_ID}`
      WHERE source = '{ADMIN_HANDOVER_TO_DETAIL_SOURCE}'
    )
    SELECT
      u.trip_id,
      u.sequence_number,
      COUNT(*) AS to_rows
    FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_LT_UNIT_TABLE_ID}` u
    LEFT JOIN latest_admin_state s
      ON u.trip_id = s.trip_id
     AND u.sequence_number = s.sequence_number
     AND u.to_number = s.to_number
     AND s.rn = 1
    WHERE u.source_type = 'HANDOVER'
      AND u.item_type = 'TO_SORTING'
    GROUP BY u.trip_id, u.sequence_number
    HAVING SUM(CASE WHEN s.to_number IS NULL THEN 1 ELSE 0 END) = 0
       AND MAX(u.arrived_time) <= DATETIME_SUB(
         CURRENT_DATETIME('Asia/Bangkok'),
         INTERVAL {LT_UNIT_HANDOVER_RETENTION_HOURS} HOUR
       )
    """
    completed_sequences = query_bq(service, ready_query, fail_soft=True)
    deleted_rows = 0
    for row in completed_sequences:
        trip_id = to_text(row.get("trip_id")).replace("'", "''")
        sequence_number = EXPORT_BOT.to_int(row.get("sequence_number"), default=0)
        if not trip_id or sequence_number <= 0:
            continue
        delete_query = f"""
        DELETE FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_LT_UNIT_TABLE_ID}`
        WHERE source_type = 'HANDOVER'
          AND trip_id = '{trip_id}'
          AND sequence_number = {sequence_number}
        """
        service.jobs().query(
            projectId=BIGQUERY_PROJECT_ID,
            body={"query": delete_query, "useLegacySql": False},
        ).execute()
        deleted_rows += EXPORT_BOT.to_int(row.get("to_rows"), default=0)

    if completed_sequences:
        print(
            "Cleaned lt_unit Handover checkpoint rows: "
            f"sequences={len(completed_sequences)} | rows={deleted_rows} | "
            f"retention={LT_UNIT_HANDOVER_RETENTION_HOURS}h"
        )
    return deleted_rows


def clear_completed_candidate_staging(service, clear_to_staging=True, clear_bulky_staging=True):
    """Drop temporary candidate staging after a successful Sheet export.

    The durable recheck queue is lt_pending_candidate, so these two source
    staging tables can be rebuilt on the next ingest cycle without losing a
    pending order or its final result.
    """
    table_ids = []
    if clear_to_staging:
        table_ids.append(BIGQUERY_TO_CANDIDATE_TABLE_ID)
    if clear_bulky_staging:
        table_ids.append(BIGQUERY_BULKY_CANDIDATE_TABLE_ID)

    for table_id in table_ids:
        try:
            service.tables().delete(
                projectId=BIGQUERY_PROJECT_ID,
                datasetId=BIGQUERY_DATASET_ID,
                tableId=table_id,
            ).execute()
            print(f"Cleared staging table {table_id} after successful queue promotion")
        except Exception as e:
            if "not found" not in str(e).lower():
                print(f"Could not clear staging {table_id}: {e}")


def capture_published_pending_status_changes(service):
    """Store an audit row when a previously published Pending becomes resolved.

    The normal result table is compacted after each successful Sheet refresh.
    Capture this event first so the operations team can still see orders that
    disappeared from the Pending tab because FMS later received a new status.
    """
    ensure_tables(service)
    query = f"""
    WITH latest_result_ranked AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY candidate_id
          ORDER BY checked_at DESC
        ) AS latest_result_rank
      FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_RESULT_TABLE_ID}`
      WHERE result_rule_version = '{RESULT_RULE_VERSION}'
        AND order_number IS NOT NULL
        AND order_number != ''
        AND order_number != '{SEQUENCE_MARKER_ORDER}'
    ),
    latest_result AS (
      SELECT *
      FROM latest_result_ranked
      WHERE latest_result_rank = 1
        AND pending_status = 'NOT_PENDING'
    ),
    published_pending_ranked AS (
      SELECT
        candidate_id,
        checked_at AS previous_pending_checked_at,
        published_at AS previous_published_at,
        order_status AS previous_order_status,
        current_station AS previous_current_station,
        ROW_NUMBER() OVER (
          PARTITION BY candidate_id
          ORDER BY checked_at DESC
        ) AS published_pending_rank
      FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_RESULT_TABLE_ID}`
      WHERE result_rule_version = '{RESULT_RULE_VERSION}'
        AND pending_status = 'PENDING'
        AND published_at IS NOT NULL
        AND published_at != ''
    )
    SELECT
      r.*,
      p.previous_pending_checked_at,
      p.previous_published_at,
      p.previous_order_status,
      p.previous_current_station
    FROM latest_result r
    INNER JOIN published_pending_ranked p
      ON r.candidate_id = p.candidate_id
    WHERE p.published_pending_rank = 1
      AND r.checked_at > p.previous_pending_checked_at
    """
    changed_rows = query_bq(service, query, fail_soft=True)
    if not changed_rows:
        return 0

    known_rows = query_bq(
        service,
        f"""
        SELECT change_event_id
        FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_STATUS_CHANGED_TABLE_ID}`
        WHERE change_event_id IS NOT NULL AND change_event_id != ''
        """,
        fail_soft=True,
    )
    known_ids = {to_text(row.get("change_event_id")) for row in known_rows}
    rows_to_store = []
    for row in changed_rows:
        event_key = "|".join([
            to_text(row.get("candidate_id")),
            to_text(row.get("previous_pending_checked_at")),
            to_text(row.get("checked_at")),
        ])
        event_id = hashlib.sha1(event_key.encode("utf-8")).hexdigest()
        if event_id in known_ids:
            continue
        row["change_event_id"] = event_id
        row["status_changed_at"] = row.get("checked_at") or now_text()
        rows_to_store.append(row)

    inserted = load_rows_to_bq(service, rows_to_store, BIGQUERY_STATUS_CHANGED_TABLE_ID)
    if inserted:
        print(
            f"Stored {inserted} published Pending -> NOT_PENDING event(s) "
            f"in {BIGQUERY_STATUS_CHANGED_TABLE_ID}"
        )
    return inserted


def _run_sql(service, query):
    service.jobs().query(
        projectId=BIGQUERY_PROJECT_ID,
        body={"query": query, "useLegacySql": False},
    ).execute()


def prune_finalized_pending_candidates(service):
    """Keep only unresolved/recheck candidates after a successful Sheet export.

    lt_pending_candidate is upserted (one row per candidate_id), so this is now
    just two small deletes - wrong rule version, and candidates whose latest
    result is terminal - instead of rewriting the whole table every cycle.
    """
    candidate_table = (
        f"`{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_CANDIDATE_TABLE_ID}`"
    )
    result_table = (
        f"`{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_RESULT_TABLE_ID}`"
    )

    _run_sql(
        service,
        f"DELETE FROM {candidate_table} "
        f"WHERE candidate_rule_version IS NOT '{CANDIDATE_RULE_VERSION}'",
    )
    # Drop candidates whose latest result is a terminal (non-PENDING/UNKNOWN)
    # outcome; those are done and never need another BatchSearch.
    _run_sql(
        service,
        f"""
        DELETE FROM {candidate_table}
        WHERE candidate_id IN (
          SELECT candidate_id FROM (
            SELECT candidate_id, pending_status, ROW_NUMBER() OVER (
              PARTITION BY candidate_id
              ORDER BY checked_at DESC, rowid DESC
            ) AS rn
            FROM {result_table}
            WHERE result_rule_version = '{RESULT_RULE_VERSION}'
          ) WHERE rn = 1 AND pending_status NOT IN ('PENDING', 'UNKNOWN')
        )
        """,
    )
    print(
        f"Pruned {BIGQUERY_CANDIDATE_TABLE_ID}: kept unprocessed, PENDING, and UNKNOWN candidates"
    )


def compact_pending_result_history(service):
    """Keep only durable current results after a successful Sheet export.

    A result is append-only while BatchSearch runs.  Retaining every
    NOT_PENDING attempt makes the local SQLite database grow quickly, even
    though those orders have already been removed from the candidate queue.
    Keep the newest row for each LH sequence marker and the newest result for
    active PENDING/UNKNOWN candidates only.
    """
    result_table = (
        f"`{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_RESULT_TABLE_ID}`"
    )
    _run_sql(
        service,
        f"DELETE FROM {result_table} "
        f"WHERE result_rule_version IS NOT '{RESULT_RULE_VERSION}'",
    )
    _run_sql(
        service,
        f"""
        DELETE FROM {result_table}
        WHERE rowid NOT IN (
          SELECT rowid FROM (
            SELECT rowid, ROW_NUMBER() OVER (
              PARTITION BY candidate_id
              ORDER BY checked_at DESC, rowid DESC
            ) AS rn
            FROM {result_table}
          ) WHERE rn = 1
        )
        """,
    )
    _run_sql(
        service,
        f"""
        DELETE FROM {result_table}
        WHERE order_number IS NOT '{SEQUENCE_MARKER_ORDER}'
          AND pending_status NOT IN ('PENDING', 'UNKNOWN')
        """,
    )
    print(
        f"Compacted {BIGQUERY_RESULT_TABLE_ID}: kept latest sequence markers and PENDING/UNKNOWN results"
    )
    run_sqlite_maintenance(service)


def run_sqlite_maintenance(service):
    """Truncate the WAL and refresh planner stats after the per-cycle rebuilds.

    prune/compact above rewrite whole tables, which bloats the WAL; without an
    explicit checkpoint it only shrinks when SQLite happens to get an exclusive
    moment. Safe to call every cycle - it is cheap when the WAL is already small.
    """
    store = getattr(service, "store", None)
    if store is None:
        return
    try:
        conn = store.connection()
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.execute("PRAGMA optimize")
    except Exception as exc:
        print(f"SQLite maintenance skipped: {exc}")


def run_once(
    lt_limit=None,
    to_limit=None,
    batch_limit=None,
    skip_lh_pending=False,
    skip_lt_unit_to=False,
    skip_batchsearch=False,
    force_recheck_pending=False,
    force_recheck_lh_pending=False,
    include_lt_unit_fallback=True,
    include_bda_prefilter=True,
):
    service = create_bq_service()
    ensure_tables(service)
    if deferred_to_expand_mode():
        # BOT 1 no longer stages Ended-TO candidates in this mode; drop the
        # legacy staging table so its ~150k stale rows stop being scanned.
        clear_completed_candidate_staging(
            service, clear_to_staging=True, clear_bulky_staging=False
        )
    fms_session = EXPORT_BOT.FmsSession(ADMIN_ROLE)
    all_candidates = []
    sequence_markers = []
    admin_handover_to_detail_states = []

    # BOT 1 owns BDA staging in the split-worker model. Legacy/full-pipeline
    # callers can still request this merge explicitly.
    if include_bda_prefilter:
        bda_prefilter_candidates = collect_bda_to_detail_candidates(service)
        all_candidates.extend(bda_prefilter_candidates)
    else:
        bda_prefilter_candidates = []
        print("Skip BDA TO staging merge: BOT 1 promotes its own staging")

    if not skip_lh_pending:
        lh_candidates, markers = collect_lh_pending_candidates(
            service,
            fms_session,
            lt_limit=lt_limit,
            force_recheck_sequences=force_recheck_lh_pending,
        )
        all_candidates.extend(lh_candidates)
        sequence_markers.extend(markers)
    else:
        print("Skip collect LH pending candidates")

    if include_lt_unit_fallback and not skip_lt_unit_to:
        handover_to_candidates, admin_handover_to_detail_states = collect_lt_unit_to_candidates(
            service,
            fms_session,
            to_limit=to_limit,
        )
        all_candidates.extend(handover_to_candidates)
    else:
        print("Skip broad lt_unit TO fallback")

    inserted = store_candidates(service, all_candidates, sequence_markers)
    print(
        "Unified candidates stored. "
        f"all={inserted[0]} | bulky={inserted[1]} | to_sorting={inserted[2]} | sequence_markers={inserted[3]}"
    )

    # Do this only after the durable candidate insert above has succeeded.
    admin_to_checkpoint_inserted = store_admin_handover_to_detail_states(
        service,
        admin_handover_to_detail_states,
    )
    if admin_to_checkpoint_inserted:
        print(
            "Admin Handover TO detail checkpoints stored: "
            f"{admin_to_checkpoint_inserted}"
        )
    cleanup_completed_handover_lt_unit(service)

    if not skip_batchsearch:
        batchsearch_inserted = batchsearch_candidates(
            service,
            fms_session,
            limit=batch_limit,
            force_recheck_pending=force_recheck_pending,
        )
    else:
        print("Skip unified batchsearch")
        batchsearch_inserted = 0

    # The runner clears the BDA staging table only after the Sheet exporter
    # finishes successfully. Until then this return value preserves recovery.
    return {
        "bda_staging_merged": bool(bda_prefilter_candidates),
        "admin_handover_to_checkpoints": admin_to_checkpoint_inserted,
        "batchsearch_inserted": batchsearch_inserted,
        "batchsearch_completed": not skip_batchsearch,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Chay 1 vong roi dung.")
    parser.add_argument("--interval-minutes", type=int, default=60, help="So phut nghi giua 2 lan chay lien tuc.")
    parser.add_argument("--lt-limit", type=int, default=None, help="Gioi han so LT check tu Handover/lt_unit.")
    parser.add_argument("--to-limit", type=int, default=None, help="Gioi han so TO Sorting tu lt_unit.")
    parser.add_argument("--batch-limit", type=int, default=None, help="Gioi han so candidate batchsearch.")
    parser.add_argument("--skip-lh-pending", action="store_true", help="Bo qua collect candidate tu API pending inbound.")
    parser.add_argument("--skip-lt-unit-to", action="store_true", help="Bo qua collect TO Sorting tu lt_unit.")
    parser.add_argument("--skip-batchsearch", action="store_true", help="Chi tao candidate, khong batchsearch.")
    parser.add_argument(
        "--force-recheck-pending",
        action="store_true",
        help="Recheck ngay ket qua PENDING cu, bo qua chu ky 120 phut (chi dung khi can sua du lieu cu).",
    )
    args = parser.parse_args()

    interval_seconds = max(1, args.interval_minutes) * 60
    while True:
        try:
            run_once(
                lt_limit=args.lt_limit,
                to_limit=args.to_limit,
                batch_limit=args.batch_limit,
                skip_lh_pending=args.skip_lh_pending,
                skip_lt_unit_to=args.skip_lt_unit_to,
                skip_batchsearch=args.skip_batchsearch,
                force_recheck_pending=args.force_recheck_pending,
            )
        except Exception:
            print("Unified pending pipeline failed:")
            traceback.print_exc()

        if args.once or args.lt_limit or args.to_limit or args.batch_limit:
            return

        print(f"Sleep {interval_seconds} seconds before next unified pending cycle.")
        EXPORT_BOT.LT_BOT.ti.sleep(interval_seconds)


if __name__ == "__main__":
    main()

