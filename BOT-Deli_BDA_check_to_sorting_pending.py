import argparse
import csv
import io
import importlib.util
import json
import re
import tempfile
import traceback
from datetime import datetime
from pathlib import Path

import requests
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

import sqlite_store


BASE_DIR = Path(__file__).resolve().parent
EXPORT_BOT_PATH = BASE_DIR / "BOT-Deli_BDA_export_LT_unit_to_BQ.py"
if not EXPORT_BOT_PATH.exists():
    EXPORT_BOT_PATH = Path(r"C:\Users\spxvn25689\Desktop\BOT DELI\BOT-Deli_BDA_export_LT_unit_to_BQ.py")

SERVICE_ACCOUNT_FILE = BASE_DIR / "ops-support.json"
if not SERVICE_ACCOUNT_FILE.exists():
    SERVICE_ACCOUNT_FILE = Path(r"C:\Users\spxvn25689\Desktop\Get_Data_Sorting\ops-support.json")

STATION_ID_FILE = BASE_DIR / "station_id.csv"
if not STATION_ID_FILE.exists():
    STATION_ID_FILE = Path(r"C:\Users\spxvn25689\Desktop\BOT DELI\station_id.csv")

BIGQUERY_PROJECT_ID = "bot-503107"
BIGQUERY_DATASET_ID = "deli_bda"
BIGQUERY_SOURCE_TABLE_ID = "lt_unit"
BIGQUERY_TARGET_TABLE_ID = "lt_to_sorting_pending"
BIGQUERY_CHECKPOINT_TABLE_ID = "lt_to_sorting_check_state"
SOC_CODE = "Admin"
TRACKING_SEARCH_URL = "https://spx.shopee.vn/api/fleet_order/order/tracking_list/search"
RULE_VERSION = "to_sorting_v20260807_current_bda_or_remark_v12"
BATCH_SIZE = 500
BQ_LOAD_BATCH_SIZE = 50000
BQ_FLUSH_ROWS = 50000
RUN_INTERVAL_SECONDS = 60 * 60
PENDING_STATUS_CODES = {629, 879, 880, 881, 882, 883, 884, 885, 886, 887, 888, 581, 570}
PENDING_STATUS_NAMES = {
    629: "ASM_Rejected",
    879: "FMHub_LHArrived",
    880: "LMHub_LHArrived",
    881: "Hub_LHArrived",
    882: "SOC_LHArrived",
    883: "WHS_LHArrived",
    884: "Return_FMHub_LHArrived",
    885: "Return_LMHub_LHArrived",
    886: "Return_Hub_LHArrived",
    887: "Return_SOC_LHArrived",
    888: "Return_WHS_LHArrived",
    581: "Missing",
    570: "Pending Intercept",
}
ORDER_STATUS_NAMES = {
    1: "LMHub_Received",
    8: "SOC_Received",
    10: "Return_LMHub_Received",
    42: "FMHub_Received",
    **PENDING_STATUS_NAMES,
}
RECEIVED_STATUS_CODES = {
    # Standard and return Received statuses from stt_mapping.xlsx.  The
    # Tracking Info API uses these codes to attach the Single/Mass tag.
    1,    # LMHub_Received
    8,    # SOC_Received
    10,   # Return_LMHub_Received
    42,   # FMHub_Received
    58,   # Return_SOC_Received
    67,   # Return_FMHub_Received
    76,   # 3PL_HubReceived
    89,   # 3PL_Received
    112,  # DOP_Received
    400,  # Hub_Received
    440,  # Return_Hub_Received
    745,  # Locker_DOP_Received
    782,  # SP_HD_Received
    784,  # Return_SP_HD_Received
    802,  # WHS_Received
    810,  # Return_WHS_Received
}
RECEIVED_RE = re.compile(r"received\s+in\s*\[([^\]]+)\]", re.IGNORECASE)
STATION_NAME_BY_ID = None


def load_export_bot():
    spec = importlib.util.spec_from_file_location("bot_deli_bda_export_lt_unit", EXPORT_BOT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


EXPORT_BOT = load_export_bot()


def load_station_name_by_id():
    global STATION_NAME_BY_ID
    if STATION_NAME_BY_ID is not None:
        return STATION_NAME_BY_ID

    STATION_NAME_BY_ID = {}
    if not STATION_ID_FILE.exists():
        print(f"Khong tim thay file station mapping: {STATION_ID_FILE}")
        return STATION_NAME_BY_ID

    with STATION_ID_FILE.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            station_id = str(row.get("id") or row.get("station_id") or "").strip()
            station_name = str(row.get("station_name") or row.get("name") or "").strip()
            if station_id and station_name:
                STATION_NAME_BY_ID[station_id] = station_name
    print(f"Loaded station mapping: {len(STATION_NAME_BY_ID)} stations")
    return STATION_NAME_BY_ID


def create_bq_service():
    return sqlite_store.create_service()


def to_sorting_schema_fields():
    return [
        {"name": "trip_number", "type": "STRING"},
        {"name": "lt_id", "type": "STRING"},
        {"name": "trip_id", "type": "STRING"},
        {"name": "arrived_time", "type": "DATETIME"},
        {"name": "station", "type": "STRING"},
        {"name": "to_station", "type": "STRING"},
        {"name": "loaded_station_name", "type": "STRING"},
        {"name": "unloaded_station_name", "type": "STRING"},
        {"name": "to_number", "type": "STRING"},
        {"name": "to_path", "type": "STRING"},
        {"name": "sender", "type": "STRING"},
        {"name": "to_receiver", "type": "STRING"},
        {"name": "order_number", "type": "STRING"},
        {"name": "shipment_id", "type": "STRING"},
        {"name": "to_id", "type": "STRING"},
        {"name": "type", "type": "STRING"},
        {"name": "remark_received_station", "type": "STRING"},
        {"name": "remark", "type": "STRING"},
        {"name": "scan_time", "type": "STRING"},
        {"name": "receive_status", "type": "STRING"},
        {"name": "journey_type", "type": "STRING"},
        {"name": "current_station", "type": "STRING"},
        {"name": "order_status_code", "type": "INT64"},
        {"name": "order_status", "type": "STRING"},
        {"name": "status", "type": "STRING"},
        {"name": "destination_station", "type": "STRING"},
        {"name": "return_destination", "type": "STRING"},
        {"name": "destination", "type": "STRING"},
        {"name": "total_on_hold_times", "type": "INT64"},
        {"name": "number_of_return_on_hold", "type": "INT64"},
        {"name": "attempt", "type": "INT64"},
        {"name": "pending_status", "type": "STRING"},
        {"name": "rule_version", "type": "STRING"},
        {"name": "checked_at", "type": "DATETIME"},
    ]


def ensure_to_sorting_table(service, table_id):
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
        "schema": {"fields": to_sorting_schema_fields()},
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
        field for field in to_sorting_schema_fields()
        if field["name"] not in existing_names
    ]
    if not missing_fields:
        return
    body = {
        "schema": {
            "fields": existing_fields + missing_fields,
        }
    }
    service.tables().patch(
        projectId=BIGQUERY_PROJECT_ID,
        datasetId=BIGQUERY_DATASET_ID,
        tableId=table_id,
        body=body,
    ).execute()
    print(f"Updated BigQuery schema {BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{table_id}: add {len(missing_fields)} fields")


def query_bq(service, query):
    response = service.jobs().query(
        projectId=BIGQUERY_PROJECT_ID,
        body={"query": query, "useLegacySql": False},
    ).execute()
    fields = [field["name"] for field in response.get("schema", {}).get("fields", [])]
    rows = []
    for row in response.get("rows", []):
        rows.append({
            fields[i]: cell.get("v")
            for i, cell in enumerate(row.get("f", []))
        })
    return rows


def get_to_sorting_candidates(service, limit=None):
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    query = f"""
    WITH latest_rows AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY to_number, order_number
          ORDER BY checked_at DESC
        ) AS rn
      FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_CHECKPOINT_TABLE_ID}`
      WHERE rule_version = '{RULE_VERSION}'
    ),
    latest_to AS (
      SELECT
        to_number,
        COUNT(*) AS checked_current_rule_rows,
        COUNTIF(pending_status = 'UNKNOWN') AS unknown_rows,
        COUNTIF(pending_status = 'PENDING') AS pending_rows
      FROM latest_rows
      WHERE rn = 1
        AND order_number IS NOT NULL
        AND order_number != ''
      GROUP BY to_number
    )
    SELECT
      u.trip_number,
      u.trip_id,
      FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', u.arrived_time) AS arrived_time,
      u.station,
      u.to_station,
      u.loaded_station_name,
      u.unloaded_station_name,
      u.to_number,
      u.to_path,
      u.sender,
      u.receiver AS to_receiver
    FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_SOURCE_TABLE_ID}` u
    LEFT JOIN latest_to l
      ON u.to_number = l.to_number
    WHERE u.item_type = 'TO_SORTING'
      AND u.to_number IS NOT NULL
      AND u.to_number != ''
      AND (l.to_number IS NULL OR l.checked_current_rule_rows = 0 OR l.unknown_rows > 0 OR l.pending_rows > 0)
    ORDER BY u.arrived_time, u.trip_number, u.to_number
    {limit_clause}
    """
    return query_bq(service, query)


def chunked(rows, size):
    for start in range(0, len(rows), size):
        yield rows[start:start + size]


def first_value(row, keys):
    return EXPORT_BOT.first_value(row, keys)


def normalize_station(value):
    return re.sub(r"\s+", " ", str(value or "").strip()).lower()


def extract_received_station(remark):
    match = RECEIVED_RE.search(str(remark or ""))
    if not match:
        return ""
    return re.sub(r"\s+", " ", match.group(1)).strip()


def extract_order_id(row):
    return EXPORT_BOT.extract_order_id(row)


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


def tracking_item_order_number(item):
    return str(first_value(
        item,
        ["spx_tracking_number", "tracking_number", "fleet_order_id", "order_number", "shipment_id"],
    ) or "").strip()


def tracking_status_name(item, status_code):
    return (
        first_value(item, ["order_status_desc", "order_status_label", "tracking_status", "order_status_name"])
        or ORDER_STATUS_NAMES.get(status_code)
        or str(status_code if status_code >= 0 else "")
    )


def normalize_journey_type(value):
    text = str(value or "").strip()
    if text == "1":
        return "Forward"
    if text == "2":
        return "Return"
    lowered = text.lower()
    if "return" in lowered:
        return "Return"
    if "forward" in lowered:
        return "Forward"
    return text


def normalize_receive_status(value):
    return str(value or "").strip().lower()


def is_received_status(value):
    return normalize_receive_status(value) == "received"


def is_remark_received_at_receiver(remark_received_station, to_receiver):
    return (
        bool(normalize_station(remark_received_station))
        and normalize_station(remark_received_station) == normalize_station(to_receiver)
    )


def is_blank_remark(remark):
    return not str(remark or "").strip()


def is_th2_pending_remark(remark):
    return is_blank_remark(remark)


def classify_to_sorting_pending(receive_status, remark, remark_received_station, is_to_single):
    if not is_received_status(receive_status):
        if is_blank_remark(remark):
            return "PENDING"
        return "NEED_TRACKING_RECEIVED_CHECK"

    if is_to_single:
        return "NOT_PENDING"

    if not is_blank_remark(remark):
        return "NEED_TRACKING_RECEIVED_CHECK"
    return "NOT_PENDING"


def parse_int_field(item, keys):
    value = first_value(item, keys)
    if value in (None, ""):
        return None
    return EXPORT_BOT.to_int(value, default=0)


def station_id_key(value):
    text = str(value or "").strip()
    if not text:
        return ""
    if re.fullmatch(r"\d+(\.0+)?", text):
        return str(int(float(text)))
    return text


def resolve_station_value(value):
    text = str(value or "").strip()
    if not text:
        return ""
    station_map = load_station_name_by_id()
    return station_map.get(station_id_key(text), text)


def is_receive_station_hold_status(status_text, status_code):
    if status_code in RECEIVED_STATUS_CODES:
        return True
    if status_code in PENDING_STATUS_CODES:
        return True
    text = str(status_text or "").strip().lower()
    if not text:
        return False
    if "pendingreceive" in text or "pending_receive" in text:
        return False
    return bool(re.search(
        r"(^|[_\s-])(lharrived|lhunloading|lhunloaded|received|asm_rejected|exception|intercept|missing)$",
        text,
    ))


def should_keep_received_mass_pending(row):
    remark_station = row.get("remark_received_station") or ""
    current_station = row.get("current_station") or ""
    if not current_station:
        return False
    if normalize_station(current_station) == normalize_station("BD A Mega SOC"):
        return True
    if not remark_station:
        return False
    if normalize_station(remark_station) != normalize_station(current_station):
        return False
    return True


def enrich_rows_with_tracking(fms_session, rows):
    if not rows:
        return


    for row_batch in chunked(rows, BATCH_SIZE):
        order_numbers = [row["order_number"] for row in row_batch]
        tracking_items = search_tracking_batch(fms_session, order_numbers)
        item_by_order = {
            tracking_item_order_number(item): item
            for item in tracking_items
            if tracking_item_order_number(item)
        }

        for row in row_batch:
            item = item_by_order.get(row["order_number"])
            if not item:
                continue
            status_code = EXPORT_BOT.to_int(item.get("order_status"), default=-1)
            current_station = first_value(item, ["current_station_name", "current_station", "station_name"])
            raw_destination_station = first_value(item, [
                "station_id",
                "destination_station_id",
                "destination_station_name",
                "destination_station",
                "destination_id",
                "destination_station_desc",
                "destination_station_label",
                "destination_hub_id",
                "destination_hub_name",
                "destination_hub",
                "dest_station_id",
                "dest_station_name",
                "dest_station",
                "dest_station_desc",
                "dst_station_id",
                "dst_station_name",
                "dst_station",
                "end_station_id",
                "end_station_name",
                "final_station_id",
                "final_station_name",
                "receiver_station_id",
                "receiver_station_name",
                "receiver_station",
                "recipient_station_id",
                "recipient_station_name",
                "recipient_station",
            ])
            raw_return_destination = first_value(item, [
                "return_destination",
                "return_destination_id",
                "return_destination_name",
                "return_destination_station_id",
                "return_destination_station",
                "return_dest_station_id",
                "return_dest_station_name",
                "return_dest_station",
                "return_dest_station_desc",
                "return_dest_hub_id",
                "return_dest_hub_name",
                "return_dest_hub",
                "return_destination_station_name",
                "return_station_id",
                "return_station_name",
                "return_station",
            ])
            destination_station = resolve_station_value(raw_destination_station)
            return_destination = resolve_station_value(raw_return_destination)
            journey_type = normalize_journey_type(row.get("journey_type"))
            total_on_hold_times = parse_int_field(item, [
                "total_on_hold_times",
                "total_on_hold_time",
                "on_hold_times",
                "on_hold_count",
            ])
            number_of_return_on_hold = parse_int_field(item, [
                "number_of_return_on_hold",
                "return_on_hold_times",
                "return_on_hold_count",
                "return_onhold_times",
            ])
            total_for_attempt = total_on_hold_times or 0
            return_for_attempt = number_of_return_on_hold or 0
            if journey_type == "Return":
                attempt = return_for_attempt
                destination = return_destination
            else:
                journey_type = journey_type or "Forward"
                attempt = max(0, total_for_attempt - return_for_attempt)
                destination = destination_station

            row["current_station"] = current_station
            row["order_status_code"] = status_code if status_code >= 0 else None
            row["order_status"] = tracking_status_name(item, status_code)
            row["status"] = row["order_status"]
            row["destination_station"] = destination_station
            row["return_destination"] = return_destination
            row["destination"] = destination
            row["journey_type"] = journey_type
            row["total_on_hold_times"] = total_on_hold_times
            row["number_of_return_on_hold"] = number_of_return_on_hold
            row["attempt"] = attempt


def build_rows_for_to(candidate, to_detail):
    checked_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    to_receiver = to_detail.get("receiver") or candidate.get("to_receiver") or ""
    to_path = to_detail.get("to_path") or candidate.get("to_path") or ""
    sender = to_detail.get("sender") or candidate.get("sender") or ""
    rows = []
    detail_rows = to_detail.get("detail_rows") or []
    scan_times = {
        str(first_value(row, ["scan_time", "ctime", "mtime", "receive_time"]) or "").strip()
        for row in detail_rows
        if str(first_value(row, ["scan_time", "ctime", "mtime", "receive_time"]) or "").strip()
    }
    is_to_single = len(scan_times) > 1

    for detail_row in detail_rows:
        order_number = extract_order_id(detail_row)
        if not order_number:
            continue

        remark = first_value(detail_row, ["remark", "remarks", "receive_remark", "operation_remark"])
        remark_received_station = extract_received_station(remark)
        receive_status = first_value(
            detail_row,
            ["receive_status", "receive_status_desc", "receive_status_name", "status", "status_desc"],
        )
        journey_type = normalize_journey_type(
            first_value(detail_row, ["transfer_direction", "journey_type", "journey_type_desc", "journey_type_name"])
        )
        pending_status = classify_to_sorting_pending(
            receive_status,
            remark,
            remark_received_station,
            is_to_single,
        )

        rows.append({
            "trip_number": candidate.get("trip_number") or "",
            "lt_id": candidate.get("trip_number") or "",
            "trip_id": str(candidate.get("trip_id") or ""),
            "arrived_time": candidate.get("arrived_time") or None,
            "station": candidate.get("station") or "",
            "to_station": candidate.get("to_station") or "",
            "loaded_station_name": candidate.get("loaded_station_name") or "",
            "unloaded_station_name": candidate.get("unloaded_station_name") or "",
            "to_number": candidate.get("to_number") or "",
            "to_path": to_path,
            "sender": sender,
            "to_receiver": to_receiver,
            "order_number": order_number,
            "shipment_id": order_number,
            "to_id": candidate.get("to_number") or "",
            "type": "Sorting",
            "remark_received_station": remark_received_station,
            "remark": remark,
            "scan_time": first_value(detail_row, ["scan_time", "ctime", "mtime", "receive_time"]),
            "receive_status": receive_status,
            "journey_type": journey_type,
            "current_station": "",
            "order_status_code": None,
            "order_status": "",
            "status": "",
            "destination_station": "",
            "return_destination": "",
            "destination": "",
            "total_on_hold_times": None,
            "number_of_return_on_hold": None,
            "attempt": None,
            "pending_status": pending_status,
            "rule_version": RULE_VERSION,
            "checked_at": checked_at,
        })
    return rows


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


def build_unknown_checkpoint_row(candidate):
    checked_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return {
        "trip_number": candidate.get("trip_number") or "",
        "lt_id": candidate.get("trip_number") or "",
        "trip_id": str(candidate.get("trip_id") or ""),
        "arrived_time": candidate.get("arrived_time") or None,
        "station": candidate.get("station") or "",
        "to_station": candidate.get("to_station") or "",
        "loaded_station_name": candidate.get("loaded_station_name") or "",
        "unloaded_station_name": candidate.get("unloaded_station_name") or "",
        "to_number": candidate.get("to_number") or "",
        "to_path": candidate.get("to_path") or "",
        "sender": candidate.get("sender") or "",
        "to_receiver": candidate.get("to_receiver") or "",
        "order_number": "",
        "shipment_id": "",
        "to_id": candidate.get("to_number") or "",
        "type": "Sorting",
        "remark_received_station": "",
        "remark": "",
        "scan_time": "",
        "receive_status": "",
        "journey_type": "",
        "current_station": "",
        "order_status_code": None,
        "order_status": "",
        "status": "",
        "destination_station": "",
        "return_destination": "",
        "destination": "",
        "total_on_hold_times": None,
        "number_of_return_on_hold": None,
        "attempt": None,
        "pending_status": "UNKNOWN",
        "rule_version": RULE_VERSION,
        "checked_at": checked_at,
    }


def run_once(limit=None, to_number=None):
    bq_service = create_bq_service()
    ensure_to_sorting_table(bq_service, BIGQUERY_TARGET_TABLE_ID)
    ensure_to_sorting_table(bq_service, BIGQUERY_CHECKPOINT_TABLE_ID)
    if to_number:
        safe_to_number = to_number.replace("\\", "\\\\").replace("'", "\\'")
        query = f"""
        SELECT
          trip_number,
          trip_id,
          FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', arrived_time) AS arrived_time,
          station,
          to_station,
          loaded_station_name,
          unloaded_station_name,
          to_number,
          to_path,
          sender,
          receiver AS to_receiver
        FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_SOURCE_TABLE_ID}`
        WHERE item_type = 'TO_SORTING'
          AND to_number = '{safe_to_number}'
        LIMIT 1
        """
        candidates = query_bq(bq_service, query)
    else:
        candidates = get_to_sorting_candidates(bq_service, limit=limit)
    print(f"TO Sorting candidates can check: {len(candidates)}")
    if not candidates:
        return

    fms_session = EXPORT_BOT.FmsSession(SOC_CODE)
    total_to = 0
    total_orders = 0
    total_pending = 0
    checkpoint_buffer = []
    pending_buffer = []

    def flush_buffers(force=False):
        nonlocal checkpoint_buffer, pending_buffer
        if not force and len(checkpoint_buffer) < BQ_FLUSH_ROWS:
            return 0, 0
        checkpoint_inserted = load_rows_to_bq(bq_service, checkpoint_buffer, BIGQUERY_CHECKPOINT_TABLE_ID)
        pending_inserted = load_rows_to_bq(bq_service, pending_buffer, BIGQUERY_TARGET_TABLE_ID)
        checkpoint_buffer = []
        pending_buffer = []
        return checkpoint_inserted, pending_inserted

    for candidate in candidates:
        to_lookup_number = candidate.get("to_number") or ""
        to_detail = EXPORT_BOT.fetch_to_detail(fms_session, to_lookup_number)
        status_rows = build_rows_for_to(candidate, to_detail)
        if not status_rows:
            status_rows = [build_unknown_checkpoint_row(candidate)]

        tracking_rows = [
            row for row in status_rows
            if row["pending_status"] in ("PENDING", "NEED_TRACKING_RECEIVED_CHECK")
        ]
        enrich_rows_with_tracking(fms_session, tracking_rows)
        for row in tracking_rows:
            if row["pending_status"] == "NEED_TRACKING_RECEIVED_CHECK":
                row["pending_status"] = (
                    "PENDING"
                    if should_keep_received_mass_pending(row)
                    else "NOT_PENDING"
                )

        pending_rows = [row for row in status_rows if row["pending_status"] == "PENDING"]
        checkpoint_buffer.extend(status_rows)
        pending_buffer.extend(pending_rows)
        _, pending_inserted = flush_buffers(force=False)

        total_to += 1
        total_orders += len([row for row in status_rows if row["pending_status"] != "UNKNOWN"])
        total_pending += len(pending_rows)
        print(
            f"Checked TO {to_lookup_number}: orders={len(status_rows)} "
            f"| pending buffered: {len(pending_rows)} | pending flushed: {pending_inserted}"
        )

    checkpoint_inserted, pending_inserted = flush_buffers(force=True)
    print(f"Final flush TO Sorting: checkpoint inserted={checkpoint_inserted} | pending inserted={pending_inserted}")
    print(f"Done. Checked {total_to} TO Sorting | orders: {total_orders} | pending: {total_pending}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="Gioi han so TO Sorting check trong lan nay.")
    parser.add_argument("--to", default="", help="Check rieng 1 ma TO Sorting.")
    parser.add_argument("--once", action="store_true", help="Chay 1 vong roi dung. Co --limit/--to thi mac dinh cung chi chay 1 vong.")
    parser.add_argument("--interval-minutes", type=int, default=60, help="So phut nghi giua 2 lan chay lien tuc.")
    args = parser.parse_args()

    if args.once or args.limit or args.to:
        run_once(limit=args.limit, to_number=args.to.strip())
        return

    interval_seconds = max(1, args.interval_minutes) * 60
    while True:
        try:
            run_once()
        except Exception:
            print("Run TO Sorting pending check failed:")
            traceback.print_exc()
        print(f"Sleep {interval_seconds} seconds before next TO Sorting pending check.")
        EXPORT_BOT.LT_BOT.ti.sleep(interval_seconds)


if __name__ == "__main__":
    main()
