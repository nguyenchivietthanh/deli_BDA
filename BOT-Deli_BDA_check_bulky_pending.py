import argparse
import csv
import io
import importlib.util
import json
import re
import tempfile
import traceback
from datetime import datetime, timedelta
from pathlib import Path

import requests
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload

import sqlite_store


BASE_DIR = Path(__file__).resolve().parent
EXPORT_BOT_PATH = BASE_DIR / "BOT-Deli_BDA_export_LT_unit_to_BQ.py"
if not EXPORT_BOT_PATH.exists():
    EXPORT_BOT_PATH = Path(r"C:\Users\spxvn25689\Desktop\BOT DELI\BOT-Deli_BDA_export_LT_unit_to_BQ.py")
TO_SORTING_PENDING_BOT_PATH = BASE_DIR / "BOT-Deli_BDA_check_to_sorting_pending.py"
if not TO_SORTING_PENDING_BOT_PATH.exists():
    TO_SORTING_PENDING_BOT_PATH = Path(r"C:\Users\spxvn25689\Desktop\BOT DELI\BOT-Deli_BDA_check_to_sorting_pending.py")

SERVICE_ACCOUNT_FILE = BASE_DIR / "ops-support.json"
if not SERVICE_ACCOUNT_FILE.exists():
    SERVICE_ACCOUNT_FILE = Path(r"C:\Users\spxvn25689\Desktop\Get_Data_Sorting\ops-support.json")

STATION_ID_FILE = BASE_DIR / "station_id.csv"
if not STATION_ID_FILE.exists():
    STATION_ID_FILE = Path(r"C:\Users\spxvn25689\Desktop\BOT DELI\station_id.csv")

BIGQUERY_PROJECT_ID = "bot-503107"
BIGQUERY_DATASET_ID = "deli_bda"
BIGQUERY_SOURCE_TABLE_ID = "lt_unit"
BIGQUERY_TARGET_TABLE_ID = "lt_bulky_status"
BIGQUERY_CHECKPOINT_TABLE_ID = "lt_bulky_check_state"
SOC_CODE = "BD A Mega SOC"
ADMIN_ROLE = "Admin"
TRACKING_SEARCH_URL = "https://spx.shopee.vn/api/fleet_order/order/tracking_list/search"
RULE_VERSION = "bulky_admin_pending_v4_bda_current_only"
PAGE_COUNT = 100
BATCH_SIZE = 500
BQ_LOAD_BATCH_SIZE = 50000
BQ_FLUSH_ROWS = 50000
RUN_INTERVAL_SECONDS = 60 * 60
HANDOVER_LOOKBACK_DAYS = 7
TRIP_MARKER_ORDER = "__TRIP_BULKY_PENDING_CHECK__"
SEQUENCE_MARKER_ORDER = "__TRIP_SEQUENCE_BULKY_PENDING_CHECK__"


def load_export_bot():
    spec = importlib.util.spec_from_file_location("bot_deli_bda_export_lt_unit", EXPORT_BOT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


EXPORT_BOT = load_export_bot()


def load_to_sorting_pending_bot():
    spec = importlib.util.spec_from_file_location("bot_deli_bda_check_to_sorting_pending", TO_SORTING_PENDING_BOT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


TO_SORTING_PENDING_BOT = load_to_sorting_pending_bot()


def create_bq_service():
    return sqlite_store.create_service()


def is_not_found_error(error):
    if isinstance(error, HttpError) and getattr(error.resp, "status", None) == 404:
        return True
    return "not found" in str(error).lower()


def bulky_status_schema_fields():
    return [
        {"name": "trip_number", "type": "STRING"},
        {"name": "trip_id", "type": "STRING"},
        {"name": "arrived_time", "type": "DATETIME"},
        {"name": "station", "type": "STRING"},
        {"name": "to_station", "type": "STRING"},
        {"name": "order_number", "type": "STRING"},
        {"name": "to_number", "type": "STRING"},
        {"name": "to_path", "type": "STRING"},
        {"name": "sender", "type": "STRING"},
        {"name": "receiver", "type": "STRING"},
        {"name": "order_status_code", "type": "INT64"},
        {"name": "order_status", "type": "STRING"},
        {"name": "pending_status", "type": "STRING"},
        {"name": "current_station", "type": "STRING"},
        {"name": "current_to_number", "type": "STRING"},
        {"name": "loaded_station_name", "type": "STRING"},
        {"name": "unloaded_station_name", "type": "STRING"},
        {"name": "scan_time", "type": "DATETIME"},
        {"name": "journey_type", "type": "STRING"},
        {"name": "destination_station", "type": "STRING"},
        {"name": "return_destination", "type": "STRING"},
        {"name": "destination", "type": "STRING"},
        {"name": "attempt", "type": "INT64"},
        {"name": "receive_status", "type": "STRING"},
        {"name": "remark", "type": "STRING"},
        {"name": "weight_kg", "type": "FLOAT"},
        {"name": "number_of_order", "type": "INT64"},
        {"name": "unloaded_sequence_number", "type": "INT64"},
        {"name": "actual_unloaded_sequence_number", "type": "INT64"},
        {"name": "rule_version", "type": "STRING"},
        {"name": "checked_at", "type": "DATETIME"},
    ]


def ensure_bulky_status_table(service, table_id):
    try:
        table = service.tables().get(
            projectId=BIGQUERY_PROJECT_ID,
            datasetId=BIGQUERY_DATASET_ID,
            tableId=table_id,
        ).execute()
        add_missing_schema_fields(service, table_id, table)
        return
    except Exception:
        pass

    body = {
        "tableReference": {
            "projectId": BIGQUERY_PROJECT_ID,
            "datasetId": BIGQUERY_DATASET_ID,
            "tableId": table_id,
        },
        "schema": {"fields": bulky_status_schema_fields()},
    }
    service.tables().insert(
        projectId=BIGQUERY_PROJECT_ID,
        datasetId=BIGQUERY_DATASET_ID,
        body=body,
    ).execute()
    print(f"Created BigQuery table {BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{table_id}")


def add_missing_schema_fields(service, table_id, table):
    existing_fields = table.get("schema", {}).get("fields", [])
    existing_names = {field["name"] for field in existing_fields}
    missing_fields = [
        field for field in bulky_status_schema_fields()
        if field["name"] not in existing_names
    ]
    if not missing_fields:
        return
    body = {"schema": {"fields": existing_fields + missing_fields}}
    service.tables().patch(
        projectId=BIGQUERY_PROJECT_ID,
        datasetId=BIGQUERY_DATASET_ID,
        tableId=table_id,
        body=body,
    ).execute()
    print(f"Updated BigQuery schema {BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{table_id}: add {len(missing_fields)} fields")


def query_bq(service, query, fail_soft=False):
    try:
        response = service.jobs().query(
            projectId=BIGQUERY_PROJECT_ID,
            body={"query": query, "useLegacySql": False},
        ).execute()
    except Exception as e:
        if fail_soft and is_not_found_error(e):
            return []
        raise
    fields = [field["name"] for field in response.get("schema", {}).get("fields", [])]
    rows = []
    for row in response.get("rows", []):
        rows.append({
            fields[i]: cell.get("v")
            for i, cell in enumerate(row.get("f", []))
        })
    return rows


def get_bulky_trip_candidates(service, limit=None):
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    query = f"""
    WITH latest_trip_check AS (
      SELECT
        trip_id,
        ARRAY_AGG(checked_at ORDER BY checked_at DESC LIMIT 1)[OFFSET(0)] AS last_checked_at
      FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_CHECKPOINT_TABLE_ID}`
      WHERE order_number = '{TRIP_MARKER_ORDER}'
        AND rule_version = '{RULE_VERSION}'
      GROUP BY trip_id
    ),
    latest_order_state AS (
      SELECT
        trip_id,
        order_number,
        to_number,
        pending_status,
        ROW_NUMBER() OVER (
          PARTITION BY trip_id, COALESCE(NULLIF(order_number, ''), to_number)
          ORDER BY checked_at DESC
        ) AS rn
      FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_CHECKPOINT_TABLE_ID}`
      WHERE order_number != '{TRIP_MARKER_ORDER}'
        AND order_number != '{SEQUENCE_MARKER_ORDER}'
        AND rule_version = '{RULE_VERSION}'
    ),
    pending_trip AS (
      SELECT DISTINCT trip_id
      FROM latest_order_state
      WHERE rn = 1
        AND pending_status = 'PENDING'
    )
    SELECT
      u.trip_number,
      u.trip_id,
      FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', MIN(u.arrived_time)) AS arrived_time,
      ANY_VALUE(u.station) AS station,
      ANY_VALUE(u.to_station) AS to_station
    FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_SOURCE_TABLE_ID}` u
    LEFT JOIN latest_trip_check c
      ON u.trip_id = c.trip_id
    LEFT JOIN pending_trip p
      ON u.trip_id = p.trip_id
    WHERE u.trip_id IS NOT NULL
      AND u.trip_id != ''
      AND (c.last_checked_at IS NULL OR p.trip_id IS NOT NULL)
    GROUP BY u.trip_number, u.trip_id
    ORDER BY MIN(u.arrived_time), u.trip_number
    {limit_clause}
    """
    return query_bq(service, query)


def handover_window():
    now = datetime.now()
    window_from = (now - timedelta(days=HANDOVER_LOOKBACK_DAYS)).replace(hour=0, minute=0, second=0, microsecond=0)
    window_to = now.replace(hour=23, minute=59, second=59, microsecond=0)
    return window_from, window_to


def format_candidate_time(row):
    value = first_value(row, [
        "arrived_time",
        "ata",
        "atd",
        "mtime",
        "update_time",
        "updated_time",
        "ctime",
    ])
    return EXPORT_BOT.LT_BOT.format_datetime(value) if value not in (None, "") else None


def fetch_handover_trip_candidates(fms_session, limit=None):
    window_from, window_to = handover_window()
    start_time = EXPORT_BOT.LT_BOT.to_epoch_seconds(window_from)
    end_time = EXPORT_BOT.LT_BOT.to_epoch_seconds(window_to)
    rows, total = fms_session.fetch_list(
        "https://spx.shopee.vn/api/admin/transportation/trip/list"
        f"?mtime={start_time},{end_time}&pageno={{page}}&count={{count}}&query_type=2",
        {},
        label=f"Handover LT list - {window_from:%Y-%m-%d %H:%M} -> {window_to:%Y-%m-%d %H:%M}",
    )

    candidates = []
    for row in rows:
        row_text = json.dumps(row, ensure_ascii=False)
        if not has_bda_in_path(row_text):
            continue
        trip_id = to_text(first_value(row, ["trip_id", "id", "lh_trip_id", "linehaul_trip_id"]))
        trip_number = to_text(first_value(row, ["trip_number", "lh_trip_number", "linehaul_trip_number"]))
        if not trip_id or not trip_number:
            continue
        candidates.append({
            "trip_number": trip_number,
            "trip_id": trip_id,
            "arrived_time": format_candidate_time(row),
            "station": SOC_CODE,
            "to_station": to_text(first_value(row, ["to_station", "to_station_name", "last_station_name", "destination_station_name"])),
            "trip_station": first_value(row, ["trip_station", "trip_stations", "stations", "station_list", "trip_station_list"]) or [],
            "_raw": row,
            "_source": "handover",
        })
        if limit and len(candidates) >= int(limit):
            break

    print(f"Handover LT candidates from API: {len(candidates)} | total API rows: {total}")
    return candidates


def merge_trip_candidates(primary_rows, secondary_rows, limit=None):
    merged = []
    seen = set()
    for row in list(primary_rows or []) + list(secondary_rows or []):
        trip_id = to_text(row.get("trip_id"))
        trip_number = to_text(row.get("trip_number"))
        key = trip_id or trip_number
        if not key or key in seen:
            continue
        seen.add(key)
        merged.append(row)
        if limit and len(merged) >= int(limit):
            break
    return merged


def previous_pending_rows_for_trips(service, trip_ids):
    result = {}
    clean_trip_ids = [str(trip_id) for trip_id in trip_ids if str(trip_id or "").strip()]
    if not clean_trip_ids:
        return result

    for batch in chunked(clean_trip_ids, 500):
        quoted = ", ".join("'" + trip_id.replace("'", "''") + "'" for trip_id in batch)
        query = f"""
        WITH latest AS (
          SELECT
            *,
            ROW_NUMBER() OVER (
              PARTITION BY trip_id, COALESCE(NULLIF(order_number, ''), to_number)
              ORDER BY checked_at DESC
            ) AS rn
          FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_CHECKPOINT_TABLE_ID}`
          WHERE trip_id IN ({quoted})
            AND order_number != '{TRIP_MARKER_ORDER}'
            AND order_number != '{SEQUENCE_MARKER_ORDER}'
            AND rule_version = '{RULE_VERSION}'
        )
        SELECT * EXCEPT(rn)
        FROM latest
        WHERE rn = 1
          AND pending_status = 'PENDING'
        """
        for row in query_bq(service, query, fail_soft=True):
            result.setdefault(str(row.get("trip_id") or ""), {})[bulky_row_key(row)] = row
    return result


def load_sequence_state_for_trips(service, trip_ids):
    checked_sequences = {}
    pending_sequences = {}
    clean_trip_ids = [str(trip_id) for trip_id in trip_ids if str(trip_id or "").strip()]
    if not clean_trip_ids:
        return checked_sequences, pending_sequences

    for batch in chunked(clean_trip_ids, 500):
        quoted = ", ".join("'" + trip_id.replace("'", "''") + "'" for trip_id in batch)
        marker_query = f"""
        WITH latest AS (
          SELECT
            trip_id,
            unloaded_sequence_number,
            pending_status,
            ROW_NUMBER() OVER (
              PARTITION BY trip_id, unloaded_sequence_number
              ORDER BY checked_at DESC
            ) AS rn
          FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_CHECKPOINT_TABLE_ID}`
          WHERE trip_id IN ({quoted})
            AND order_number = '{SEQUENCE_MARKER_ORDER}'
            AND rule_version = '{RULE_VERSION}'
            AND unloaded_sequence_number IS NOT NULL
        )
        SELECT trip_id, unloaded_sequence_number
        FROM latest
        WHERE rn = 1
        """
        for row in query_bq(service, marker_query, fail_soft=True):
            trip_id = str(row.get("trip_id") or "")
            sequence = EXPORT_BOT.to_int(row.get("unloaded_sequence_number"), default=0)
            if trip_id and sequence > 0:
                checked_sequences.setdefault(trip_id, set()).add(sequence)

        pending_query = f"""
        WITH latest AS (
          SELECT
            trip_id,
            unloaded_sequence_number,
            pending_status,
            ROW_NUMBER() OVER (
              PARTITION BY trip_id, COALESCE(NULLIF(order_number, ''), to_number)
              ORDER BY checked_at DESC
            ) AS rn
          FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_CHECKPOINT_TABLE_ID}`
          WHERE trip_id IN ({quoted})
            AND order_number != '{TRIP_MARKER_ORDER}'
            AND order_number != '{SEQUENCE_MARKER_ORDER}'
            AND rule_version = '{RULE_VERSION}'
            AND unloaded_sequence_number IS NOT NULL
        )
        SELECT trip_id, unloaded_sequence_number
        FROM latest
        WHERE rn = 1
          AND pending_status = 'PENDING'
        """
        for row in query_bq(service, pending_query, fail_soft=True):
            trip_id = str(row.get("trip_id") or "")
            sequence = EXPORT_BOT.to_int(row.get("unloaded_sequence_number"), default=0)
            if trip_id and sequence > 0:
                pending_sequences.setdefault(trip_id, set()).add(sequence)

    return checked_sequences, pending_sequences


def chunked(rows, size):
    for start in range(0, len(rows), size):
        yield rows[start:start + size]


def first_value(row, keys):
    if not isinstance(row, dict):
        return ""
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    for key in keys:
        value = EXPORT_BOT.find_nested_value(row, key)
        if value not in (None, ""):
            return value
    return ""


def to_text(value):
    return str(value or "").strip()


def to_float(value, default=None):
    try:
        return float(str(value).replace("kg", "").strip())
    except (TypeError, ValueError):
        return default


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
    payload = {
        "count": BATCH_SIZE,
        "page_no": 1,
        "search_id_list": order_numbers,
    }
    data = fms_session.request_json(
        "POST",
        TRACKING_SEARCH_URL,
        payload=payload,
        label=f"Batch tracking search {len(order_numbers)}",
        fail_soft=False,
    )
    return normalize_tracking_items(data)


def load_station_name_by_id():
    if not STATION_ID_FILE.exists():
        return {}
    with STATION_ID_FILE.open("r", encoding="utf-8-sig", newline="") as station_file:
        reader = csv.DictReader(station_file)
        station_map = {}
        for row in reader:
            station_id = to_text(row.get("id") or row.get("station_id"))
            station_name = to_text(
                row.get("station_name")
                or row.get("name")
                or row.get("station")
                or row.get("station_name_en")
            )
            if station_id and station_name:
                station_map[station_id] = station_name
        return station_map


STATION_NAME_BY_ID = load_station_name_by_id()


def station_id_key(value):
    text = to_text(value)
    if not text:
        return ""
    try:
        return str(int(float(text)))
    except ValueError:
        return text


def resolve_station_value(value):
    text = to_text(value)
    if not text:
        return ""
    return STATION_NAME_BY_ID.get(station_id_key(text), text)


def normalize_journey_type(value):
    text = to_text(value).lower()
    if text in ("2", "return") or "return" in text:
        return "Return"
    if text in ("1", "forward") or "forward" in text:
        return "Forward"
    return to_text(value)


def normalize_bulk_journey_type(value):
    """BatchSearch uses a different direction code for bulky: 0=Return, 1=Forward."""
    # `to_text()` intentionally converts falsy values to an empty string for
    # optional FMS fields.  Here `0` is meaningful: Bulk BatchSearch uses it
    # for Return, so preserve it before normalizing.
    text = "" if value is None else str(value).strip().lower()
    if text in ("0", "return") or "return" in text:
        return "Return"
    if text in ("1", "forward") or "forward" in text:
        return "Forward"
    return ""


def normalize_station(value):
    return re.sub(r"\s+", " ", to_text(value)).lower()


def tracking_item_order_number(item):
    return to_text(first_value(
        item,
        ["spx_tracking_number", "tracking_number", "fleet_order_id", "order_number", "shipment_id"],
    ))


def tracking_status_name(item, status_code):
    return to_text(first_value(
        item,
        ["order_status_desc", "order_status_label", "tracking_status", "order_status_name"],
    )) or str(status_code if status_code >= 0 else "")


def tracking_int(item, keys):
    return EXPORT_BOT.to_int(first_value(item, keys), default=0)


def enrich_rows_with_tracking(fms_session, rows):
    order_rows = [
        row for row in rows
        if row.get("order_number", "").startswith("SPX")
    ]
    if not order_rows:
        return

    for batch in chunked(order_rows, BATCH_SIZE):
        order_numbers = [row["order_number"] for row in batch]
        tracking_items = search_tracking_batch(fms_session, order_numbers)
        item_by_order = {
            tracking_item_order_number(item): item
            for item in tracking_items
            if tracking_item_order_number(item)
        }
        for row in batch:
            item = item_by_order.get(row["order_number"], {})
            if not item:
                continue

            status_code = EXPORT_BOT.to_int(item.get("order_status"), default=-1)
            current_station = to_text(first_value(item, ["current_station_name", "current_station", "station_name"]))
            raw_destination_station = first_value(item, [
                "station_id",
                "destination_station_id",
                "destination_station_name",
                "destination_station",
                "dest_station_id",
            ])
            raw_return_destination = first_value(item, [
                "return_dest_station_id",
                "return_destination_station_id",
                "return_destination",
                "return_destination_name",
                "return_dest_station_name",
            ])
            destination_station = resolve_station_value(raw_destination_station)
            return_destination = resolve_station_value(raw_return_destination)
            journey_type = normalize_bulk_journey_type(first_value(item, [
                "order_direction",
                "order_direction_desc",
                "order_direction_name",
            ])) or normalize_journey_type(first_value(item, [
                "journey_type",
                "journey_type_desc",
                "journey_type_name",
                "transfer_direction",
                "transfer_direction_desc",
            ]))
            total_on_hold_times = tracking_int(item, ["total_on_hold_times", "total_onhold_times", "on_hold_times"])
            number_of_return_on_hold = tracking_int(item, ["number_of_return_on_hold", "return_on_hold_times"])

            if journey_type == "Return":
                attempt = number_of_return_on_hold
                destination = return_destination
            else:
                journey_type = journey_type or "Forward"
                attempt = max(0, total_on_hold_times - number_of_return_on_hold)
                destination = destination_station

            row["order_status_code"] = status_code if status_code >= 0 else None
            row["order_status"] = tracking_status_name(item, status_code)
            row["current_station"] = current_station
            row["current_to_number"] = to_text(first_value(item, ["current_to_number", "to_number"]))
            row["journey_type"] = journey_type
            row["destination_station"] = destination_station
            row["return_destination"] = return_destination
            row["destination"] = destination
            row["attempt"] = attempt


def fetch_trip_detail(fms_session, trip_id, trip_number):
    for label, url in [
        (
            f"History trip detail - {trip_number}",
            f"https://spx.shopee.vn/api/admin/transportation/trip/history/detail?trip_id={trip_id}&new_process_switch=false",
        ),
        (
            f"Handover trip detail - {trip_number}",
            f"https://spx.shopee.vn/api/admin/transportation/trip/detail?trip_id={trip_id}&new_process_switch=false",
        ),
    ]:
        data = fms_session.request_json(
            "GET",
            url,
            payload={},
            fail_soft=True,
            label=label,
        )
        if data and data.get("data"):
            return data
    return None


def station_name(station):
    return to_text(first_value(
        station,
        ["station_name", "name", "station", "loaded_station_name", "unloaded_station_name", "hub_name"],
    ))


def extract_unload_sequences(detail_data):
    data = (detail_data or {}).get("data") or {}
    trip_stations = data.get("trip_station") or data.get("trip_stations") or []
    sequences = []
    seen = set()
    for index, station in enumerate(trip_stations, start=1):
        name = station_name(station)
        if index == 1 and name == SOC_CODE:
            continue
        # The pending endpoint expects the Station No. from the trip UI. If
        # the field is absent, station list position is safer than the internal
        # unload sequence counter, which can be offset.
        sequence = EXPORT_BOT.to_int(first_value(station, [
            "station_no",
            "station_number",
        ]), default=index)
        if sequence <= 0 or sequence in seen:
            continue
        seen.add(sequence)
        sequences.append((sequence, name))
    if not sequences:
        sequences.append((2, ""))
    return sequences


def fetch_pending_loading_list(fms_session, trip_id, trip_number, unloaded_sequence_number):
    url_suffix = (
        f"?trip_id={trip_id}&pageno={{page}}&count={{count}}"
        f"&unloaded_sequence_number={unloaded_sequence_number}"
        f"&actual_unloaded_sequence_number=0&type=pending"
    )
    request_failed = False
    for label, url_prefix in [
        (
            f"Pending inbound history list - {trip_number} - sequence {unloaded_sequence_number}",
            "https://spx.shopee.vn/api/admin/transportation/trip/history/loading/list",
        ),
        (
            f"Pending inbound handover list - {trip_number} - sequence {unloaded_sequence_number}",
            "https://spx.shopee.vn/api/admin/transportation/trip/loading/list",
        ),
    ]:
        rows, total, complete = fms_session.fetch_list(
            url_prefix + url_suffix,
            {},
            label=label,
            return_complete=True,
        )
        if not complete:
            request_failed = True
            continue
        if total > 0 or rows:
            return rows, total, True
    return [], 0, not request_failed


def parse_scan_time(value):
    if value in (None, "", 0, "0", "-"):
        return None
    try:
        number = int(float(value))
        if number > 100000000000:
            number = number // 1000
        return datetime.fromtimestamp(number).strftime("%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError, OSError):
        pass
    text = to_text(value)
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    return None


def build_to_path(sender, receiver):
    if sender and receiver:
        return f"{sender}>{receiver}"
    return sender or receiver or ""


def source_route_info(trip, source_row, station_hint):
    sender = to_text(first_value(source_row, ["sender_station_name", "sender_station", "loaded_station_name"])) or trip.get("station") or SOC_CODE
    receiver = (
        to_text(first_value(source_row, ["receiver_station_name", "receive_station_name", "unloaded_station_name", "actual_unloaded_station_name"]))
        or station_hint
        or trip.get("to_station")
        or ""
    )
    to_path = to_text(first_value(source_row, ["to_path", "path", "route", "route_name", "transportation_path"])) or build_to_path(sender, receiver)
    return sender, receiver, to_path


def has_bda_in_path(*values):
    for value in values:
        if "bd a mega soc" in to_text(value).lower():
            return True
    return False


def source_scan_number(source_row):
    return to_text(first_value(source_row, ["scan_number", "spx_tracking_number", "tracking_number", "to_number", "parcel_number"]))


def source_to_number(source_row):
    scan_number = source_scan_number(source_row)
    to_number = to_text(first_value(source_row, ["to_number", "parcel_number"]))
    if to_number.startswith("TO"):
        return to_number
    if scan_number.startswith("TO"):
        return scan_number
    return to_number


def build_pending_row(trip, source_row, unloaded_sequence_number, station_hint, checked_at):
    to_number = source_to_number(source_row)
    scan_number = source_scan_number(source_row)
    order_number = scan_number if scan_number.startswith("SPX") else ""
    sender, receiver, to_path = source_route_info(trip, source_row, station_hint)
    receive_status = to_text(first_value(source_row, ["receive_status", "received_status", "loading_status_name"]))
    remark = to_text(first_value(source_row, ["remark", "remarks", "labels_remark"]))

    return {
        "trip_number": trip.get("trip_number") or "",
        "trip_id": str(trip.get("trip_id") or ""),
        "arrived_time": trip.get("arrived_time") or None,
        "station": trip.get("station") or "",
        "to_station": trip.get("to_station") or "",
        "order_number": order_number,
        "to_number": to_number,
        "to_path": to_path,
        "sender": sender,
        "receiver": receiver,
        "order_status_code": None,
        "order_status": "PENDING_INBOUND",
        "pending_status": "PENDING",
        "current_station": "",
        "current_to_number": to_number,
        "loaded_station_name": to_text(first_value(source_row, ["loaded_station_name"])) or sender,
        "unloaded_station_name": to_text(first_value(source_row, ["unloaded_station_name", "actual_unloaded_station_name"])) or receiver,
        "scan_time": parse_scan_time(first_value(source_row, ["scan_time", "loaded_time", "update_time", "mtime"])),
        "journey_type": "",
        "destination_station": "",
        "return_destination": "",
        "destination": "",
        "attempt": None,
        "receive_status": receive_status,
        "remark": remark,
        "weight_kg": to_float(first_value(source_row, ["weight", "weight_kg", "to_weight"])),
        "number_of_order": EXPORT_BOT.to_int(first_value(source_row, ["number_of_order", "to_parcel_quantity", "loose_order_quantity", "to_quantity"]), default=0),
        "unloaded_sequence_number": unloaded_sequence_number,
        "actual_unloaded_sequence_number": EXPORT_BOT.to_int(first_value(source_row, ["actual_unloaded_sequence_number"]), default=0),
        "rule_version": RULE_VERSION,
        "checked_at": checked_at,
    }


def build_to_sorting_candidate_from_pending(trip, source_row, unloaded_sequence_number, station_hint):
    to_number = source_to_number(source_row)
    sender, receiver, to_path = source_route_info(trip, source_row, station_hint)
    return {
        "trip_number": trip.get("trip_number") or "",
        "trip_id": str(trip.get("trip_id") or ""),
        "arrived_time": trip.get("arrived_time") or None,
        "station": trip.get("station") or "",
        "to_station": trip.get("to_station") or "",
        "loaded_station_name": to_text(first_value(source_row, ["loaded_station_name"])) or sender,
        "unloaded_station_name": to_text(first_value(source_row, ["unloaded_station_name", "actual_unloaded_station_name"])) or receiver,
        "to_number": to_number,
        "to_path": to_path,
        "sender": sender,
        "to_receiver": receiver,
        "_pending_unloaded_sequence_number": unloaded_sequence_number,
    }


def bulky_row_key(row):
    trip_id = to_text(row.get("trip_id"))
    order_number = to_text(row.get("order_number"))
    to_number = to_text(row.get("to_number"))
    return f"{trip_id}|{order_number or to_number}"


def build_trip_marker_row(trip, checked_at):
    return {
        "trip_number": trip.get("trip_number") or "",
        "trip_id": str(trip.get("trip_id") or ""),
        "arrived_time": trip.get("arrived_time") or None,
        "station": trip.get("station") or "",
        "to_station": trip.get("to_station") or "",
        "order_number": TRIP_MARKER_ORDER,
        "to_number": "",
        "to_path": "",
        "sender": "",
        "receiver": "",
        "order_status_code": None,
        "order_status": "",
        "pending_status": "NOT_PENDING",
        "current_station": "",
        "current_to_number": "",
        "loaded_station_name": "",
        "unloaded_station_name": "",
        "scan_time": None,
        "journey_type": "",
        "destination_station": "",
        "return_destination": "",
        "destination": "",
        "attempt": None,
        "receive_status": "",
        "remark": "",
        "weight_kg": None,
        "number_of_order": 0,
        "unloaded_sequence_number": None,
        "actual_unloaded_sequence_number": None,
        "rule_version": RULE_VERSION,
        "checked_at": checked_at,
    }


def build_sequence_marker_row(trip, sequence, station_hint, checked_at):
    return {
        "trip_number": trip.get("trip_number") or "",
        "trip_id": str(trip.get("trip_id") or ""),
        "arrived_time": trip.get("arrived_time") or None,
        "station": trip.get("station") or "",
        "to_station": trip.get("to_station") or "",
        "order_number": SEQUENCE_MARKER_ORDER,
        "to_number": "",
        "to_path": "",
        "sender": "",
        "receiver": station_hint or "",
        "order_status_code": None,
        "order_status": "",
        "pending_status": "NOT_PENDING",
        "current_station": "",
        "current_to_number": "",
        "loaded_station_name": "",
        "unloaded_station_name": station_hint or "",
        "scan_time": None,
        "journey_type": "",
        "destination_station": "",
        "return_destination": "",
        "destination": "",
        "attempt": None,
        "receive_status": "",
        "remark": "",
        "weight_kg": None,
        "number_of_order": 0,
        "unloaded_sequence_number": sequence,
        "actual_unloaded_sequence_number": None,
        "rule_version": RULE_VERSION,
        "checked_at": checked_at,
    }


def build_resolved_checkpoint_row(previous_row, checked_at):
    row = {field["name"]: previous_row.get(field["name"]) for field in bulky_status_schema_fields()}
    row["pending_status"] = "NOT_PENDING"
    row["checked_at"] = checked_at
    row["rule_version"] = RULE_VERSION
    return row


def is_bulky_pending_after_tracking(row):
    current_station = normalize_station(row.get("current_station"))
    if not current_station:
        return False
    return current_station == normalize_station(SOC_CODE)


def write_ndjson_file(rows):
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", suffix=".ndjson", delete=False) as temp_file:
        for row in rows:
            temp_file.write(json.dumps(row, ensure_ascii=False) + "\n")
        return temp_file.name


def safe_unlink(path):
    for attempt in range(1, 6):
        try:
            Path(path).unlink(missing_ok=True)
            return
        except PermissionError:
            if attempt >= 5:
                print(f"Khong xoa duoc file tam {path}, bo qua.")
                return
            EXPORT_BOT.LT_BOT.ti.sleep(1)


def load_rows_to_bq(service, rows, table_id):
    if not rows:
        return 0
    total = 0
    for batch in chunked(rows, BQ_LOAD_BATCH_SIZE):
        for attempt in range(1, 6):
            temp_path = write_ndjson_file(batch)
            try:
                body = {
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
                }
                media = MediaIoBaseUpload(
                    io.BytesIO(Path(temp_path).read_bytes()),
                    mimetype="application/octet-stream",
                    resumable=False,
                )
                job = service.jobs().insert(projectId=BIGQUERY_PROJECT_ID, body=body, media_body=media).execute()
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


def run_once(limit=None):
    bq_service = create_bq_service()
    ensure_bulky_status_table(bq_service, BIGQUERY_TARGET_TABLE_ID)
    ensure_bulky_status_table(bq_service, BIGQUERY_CHECKPOINT_TABLE_ID)
    TO_SORTING_PENDING_BOT.ensure_to_sorting_table(
        bq_service,
        TO_SORTING_PENDING_BOT.BIGQUERY_TARGET_TABLE_ID,
    )
    TO_SORTING_PENDING_BOT.ensure_to_sorting_table(
        bq_service,
        TO_SORTING_PENDING_BOT.BIGQUERY_CHECKPOINT_TABLE_ID,
    )
    fms_session = EXPORT_BOT.FmsSession(ADMIN_ROLE)
    handover_trips = fetch_handover_trip_candidates(fms_session, limit=limit)
    remaining_limit = None
    if limit:
        remaining_limit = max(0, int(limit) - len(handover_trips))
    lt_unit_trips = get_bulky_trip_candidates(
        bq_service,
        limit=remaining_limit if remaining_limit else (None if limit else None),
    ) if (not limit or remaining_limit > 0) else []
    for row in lt_unit_trips:
        row.setdefault("_source", "lt_unit")
    trips = merge_trip_candidates(handover_trips, lt_unit_trips, limit=limit)
    print(
        f"Bulky LT candidates can check: {len(trips)} "
        f"| handover={len(handover_trips)} | lt_unit={len(lt_unit_trips)}"
    )
    if not trips:
        return

    previous_pending_by_trip = previous_pending_rows_for_trips(
        bq_service,
        [trip.get("trip_id") for trip in trips],
    )
    checked_sequences_by_trip, pending_sequences_by_trip = load_sequence_state_for_trips(
        bq_service,
        [trip.get("trip_id") for trip in trips],
    )
    total_trips = 0
    total_bulky_pending = 0
    total_to_pending = 0
    pending_buffer = []
    checkpoint_buffer = []
    to_pending_buffer = []
    to_checkpoint_buffer = []
    to_detail_cache = {}

    def flush_buffers(force=False):
        nonlocal pending_buffer, checkpoint_buffer, to_pending_buffer, to_checkpoint_buffer
        if (
            not force
            and len(checkpoint_buffer) < BQ_FLUSH_ROWS
            and len(to_checkpoint_buffer) < BQ_FLUSH_ROWS
        ):
            return 0, 0, 0, 0
        pending_inserted = load_rows_to_bq(bq_service, pending_buffer, BIGQUERY_TARGET_TABLE_ID)
        checkpoint_inserted = load_rows_to_bq(bq_service, checkpoint_buffer, BIGQUERY_CHECKPOINT_TABLE_ID)
        to_pending_inserted = TO_SORTING_PENDING_BOT.load_rows_to_bq(
            bq_service,
            to_pending_buffer,
            TO_SORTING_PENDING_BOT.BIGQUERY_TARGET_TABLE_ID,
        )
        to_checkpoint_inserted = TO_SORTING_PENDING_BOT.load_rows_to_bq(
            bq_service,
            to_checkpoint_buffer,
            TO_SORTING_PENDING_BOT.BIGQUERY_CHECKPOINT_TABLE_ID,
        )
        pending_buffer = []
        checkpoint_buffer = []
        to_pending_buffer = []
        to_checkpoint_buffer = []
        return pending_inserted, checkpoint_inserted, to_pending_inserted, to_checkpoint_inserted

    for trip in trips:
        checked_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        trip_id = trip.get("trip_id")
        trip_number = trip.get("trip_number")
        detail_data = fetch_trip_detail(fms_session, trip_id, trip_number)
        all_sequences = extract_unload_sequences(detail_data)
        checked_sequences = checked_sequences_by_trip.get(str(trip_id), set())
        pending_sequences = pending_sequences_by_trip.get(str(trip_id), set())
        sequences = [
            (sequence, station_hint)
            for sequence, station_hint in all_sequences
            if sequence not in checked_sequences or sequence in pending_sequences
        ]
        seen_keys = set()
        seen_to_numbers = set()
        trip_pending_rows = []
        trip_to_pending_rows = []
        sequence_marker_rows = []

        for sequence, station_hint in sequences:
            pending_rows, total, pending_complete = fetch_pending_loading_list(
                fms_session,
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
            sequence_marker_rows.append(build_sequence_marker_row(trip, sequence, station_hint, checked_at))
            if total == 0:
                continue
            for source_row in pending_rows:
                scan_number = source_scan_number(source_row)
                to_number = source_to_number(source_row)
                sender, receiver, to_path = source_route_info(trip, source_row, station_hint)
                is_to_pending = scan_number.startswith("TO") or (not scan_number.startswith("SPX") and to_number.startswith("TO"))

                if is_to_pending:
                    if not to_number or to_number in seen_to_numbers:
                        continue
                    if to_number not in to_detail_cache:
                        to_detail_cache[to_number] = EXPORT_BOT.fetch_to_detail(fms_session, to_number)
                    to_detail = to_detail_cache.get(to_number) or {}
                    if not has_bda_in_path(
                        to_path,
                        sender,
                        receiver,
                        to_detail.get("to_path"),
                        to_detail.get("sender"),
                        to_detail.get("receiver"),
                    ):
                        print(f"Skip pending TO {to_number}: TO path khong co BDA")
                        continue
                    seen_to_numbers.add(to_number)
                    candidate = build_to_sorting_candidate_from_pending(
                        trip,
                        source_row,
                        sequence,
                        station_hint,
                    )
                    candidate["to_path"] = to_detail.get("to_path") or candidate.get("to_path") or ""
                    candidate["sender"] = to_detail.get("sender") or candidate.get("sender") or ""
                    candidate["to_receiver"] = to_detail.get("receiver") or candidate.get("to_receiver") or ""
                    status_rows = TO_SORTING_PENDING_BOT.build_rows_for_to(candidate, to_detail)
                    if not status_rows:
                        status_rows = [TO_SORTING_PENDING_BOT.build_unknown_checkpoint_row(candidate)]

                    tracking_rows = [
                        row for row in status_rows
                        if row["pending_status"] in ("PENDING", "NEED_TRACKING_RECEIVED_CHECK")
                    ]
                    TO_SORTING_PENDING_BOT.enrich_rows_with_tracking(fms_session, tracking_rows)
                    for row in tracking_rows:
                        if row["pending_status"] == "NEED_TRACKING_RECEIVED_CHECK":
                            row["pending_status"] = (
                                "PENDING"
                                if TO_SORTING_PENDING_BOT.should_keep_received_mass_pending(row)
                                else "NOT_PENDING"
                            )
                    to_pending_rows = [row for row in status_rows if row["pending_status"] == "PENDING"]
                    to_checkpoint_buffer.extend(status_rows)
                    to_pending_buffer.extend(to_pending_rows)
                    trip_to_pending_rows.extend(to_pending_rows)
                    continue

                if not scan_number.startswith("SPX"):
                    continue
                if not has_bda_in_path(to_path, sender, receiver):
                    print(f"Skip pending bulky {scan_number}: TO path khong co BDA")
                    continue
                row = build_pending_row(trip, source_row, sequence, station_hint, checked_at)
                key = bulky_row_key(row)
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                trip_pending_rows.append(row)

        enrich_rows_with_tracking(fms_session, trip_pending_rows)
        moved_to_other_station_rows = [
            build_resolved_checkpoint_row(row, checked_at)
            for row in trip_pending_rows
            if not is_bulky_pending_after_tracking(row)
        ]
        trip_pending_rows = [
            row for row in trip_pending_rows
            if is_bulky_pending_after_tracking(row)
        ]
        previous_for_trip = previous_pending_by_trip.get(str(trip_id), {})
        resolved_rows = [
            build_resolved_checkpoint_row(row, checked_at)
            for key, row in previous_for_trip.items()
            if key not in seen_keys
        ]
        resolved_rows.extend(moved_to_other_station_rows)

        pending_buffer.extend(trip_pending_rows)
        checkpoint_buffer.extend(trip_pending_rows)
        checkpoint_buffer.extend(resolved_rows)
        checkpoint_buffer.extend(sequence_marker_rows)
        if trip.get("_source") != "handover":
            checkpoint_buffer.append(build_trip_marker_row(trip, checked_at))
        pending_inserted, checkpoint_inserted, to_pending_inserted, to_checkpoint_inserted = flush_buffers(force=False)

        total_trips += 1
        total_bulky_pending += len(trip_pending_rows)
        total_to_pending += len(trip_to_pending_rows)
        print(
            f"Checked bulky LT {trip_number}: sequences={len(sequences)}/{len(all_sequences)} "
            f"| pending buffered: {len(trip_pending_rows)} "
            f"| pending TO expanded: {len(trip_to_pending_rows)} "
            f"| moved/cleared: {len(moved_to_other_station_rows)} | resolved: {len(resolved_rows)} "
            f"| pending flushed: {pending_inserted} | checkpoint flushed: {checkpoint_inserted} "
            f"| TO pending flushed: {to_pending_inserted} | TO checkpoint flushed: {to_checkpoint_inserted}"
        )

    pending_inserted, checkpoint_inserted, to_pending_inserted, to_checkpoint_inserted = flush_buffers(force=True)
    print(
        f"Final flush Bulky: pending inserted={pending_inserted} | checkpoint inserted={checkpoint_inserted} "
        f"| TO pending inserted={to_pending_inserted} | TO checkpoint inserted={to_checkpoint_inserted}"
    )
    print(
        f"Done. Checked {total_trips} LT for bulky pending. "
        f"Bulky pending rows: {total_bulky_pending}. TO pending rows expanded: {total_to_pending}"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="Gioi han so LT check bulky pending trong lan nay.")
    parser.add_argument("--once", action="store_true", help="Chay 1 vong roi dung. Co --limit thi mac dinh cung chi chay 1 vong.")
    parser.add_argument("--interval-minutes", type=int, default=60, help="So phut nghi giua 2 lan chay lien tuc.")
    args = parser.parse_args()

    if args.once or args.limit:
        run_once(limit=args.limit)
        return

    interval_seconds = max(1, args.interval_minutes) * 60
    while True:
        try:
            run_once()
        except Exception:
            print("Run bulky pending check failed:")
            traceback.print_exc()
        print(f"Sleep {interval_seconds} seconds before next bulky pending check.")
        EXPORT_BOT.LT_BOT.ti.sleep(interval_seconds)


if __name__ == "__main__":
    main()
