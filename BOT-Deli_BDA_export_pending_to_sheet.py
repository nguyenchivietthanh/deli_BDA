import argparse
import importlib.util
import os
import time
import traceback
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote

import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from openpyxl import load_workbook

import sqlite_store
import browser_fetch


BASE_DIR = Path(__file__).resolve().parent
STATUS_MAPPING_FILE = BASE_DIR / "stt_mapping.xlsx"
ARRIVED_LT_BOT_PATH = BASE_DIR / "BOT-Deli_BDA_arrived_LT.py"
if not ARRIVED_LT_BOT_PATH.exists():
    ARRIVED_LT_BOT_PATH = Path(r"C:\Users\spxvn25689\Desktop\BOT DELI\BOT-Deli_BDA_arrived_LT.py")

SERVICE_ACCOUNT_FILE = BASE_DIR / "ops-support.json"
if not SERVICE_ACCOUNT_FILE.exists():
    SERVICE_ACCOUNT_FILE = Path(r"C:\Users\spxvn25689\Desktop\Get_Data_Sorting\ops-support.json")

BIGQUERY_PROJECT_ID = "bot-503107"
BIGQUERY_DATASET_ID = "deli_bda"
TARGET_SHEET_ID = "1BqmVWiHdCoE0uFM_54EXu0xah4aDivgEPErc_V14enU"
RUN_INTERVAL_SECONDS = 60 * 60
# Must match the result version written by the unified pending pipeline.
PENDING_RESULT_RULE_VERSION = "pending_result_v20260826_mass_returned_to_bda_v8"
PENDING_RESULT_TABLE_ID = "lt_pending_result"
PENDING_STATUS_CHANGED_TABLE_ID = "lt_pending_status_changed"
COGS_CACHE_TABLE_ID = "bot3_cogs_cache"
SEQUENCE_MARKER_ORDER = "__TRIP_SEQUENCE_PENDING_CHECK__"
PENDING_READY_AFTER_ARRIVED_HOURS = 36

# Shift releases remain useful as guaranteed refresh points. Orders only appear
# after 24 hours from actual arrival, leaving time before the 36-hour SLA deadline.
SHEET_RELEASE_TIMES = ("08:00:00", "12:00:00", "14:00:00")

PENDING_COLUMNS = [
    ("shipment_id", "Shipment ID"),
    ("lt_id", "LT ID"),
    ("to_id", "TO ID"),
    ("to_path", "TO Path"),
    ("sender", "Sender"),
    ("receiver", "Receiver"),
    ("type", "Type"),
    ("arrived_time", "Arrived time"),
    ("status", "Status"),
    ("current_station", "Current station"),
    ("attempt", "Attempt"),
    ("journey_type", "Journey type"),
    ("destination", "Destination"),
    ("cogs", "COGS"),
]
PENDING_HEADERS = [label for _, label in PENDING_COLUMNS]

STATUS_CHANGED_COLUMNS = [
    ("shipment_id", "Shipment ID"),
    ("lt_id", "LT ID"),
    ("to_id", "TO ID"),
    ("to_path", "TO Path"),
    ("sender", "Sender"),
    ("receiver", "Receiver"),
    ("type", "Type"),
    ("arrived_time", "Arrived time"),
    ("previous_status", "Previous status"),
    ("status", "New status"),
    ("previous_current_station", "Previous current station"),
    ("current_station", "Current station"),
    ("previous_published_at", "Pending exported at"),
    ("status_changed_at", "Status changed at"),
    ("result_reason", "Reason"),
]
STATUS_CHANGED_HEADERS = [label for _, label in STATUS_CHANGED_COLUMNS]


_STATUS_MAPPING_CACHE = {"mtime": None, "mapping": {}}


def load_status_mapping():
    """Read FMS numeric status labels for the human-facing Google Sheets only."""
    if not STATUS_MAPPING_FILE.exists():
        print(f"Status mapping file not found: {STATUS_MAPPING_FILE}. Keep numeric Status values.")
        return {}

    # This file changes rarely but load_status_mapping() is called several times
    # per BOT 3 cycle. Re-read only when the file on disk actually changed.
    try:
        current_mtime = STATUS_MAPPING_FILE.stat().st_mtime
    except OSError:
        current_mtime = None
    if current_mtime is not None and _STATUS_MAPPING_CACHE["mtime"] == current_mtime:
        return _STATUS_MAPPING_CACHE["mapping"]

    try:
        workbook = load_workbook(STATUS_MAPPING_FILE, read_only=True, data_only=True)
        mapping = {}
        for code, label, *_ in workbook.active.iter_rows(min_row=2, values_only=True):
            if code is None or label is None:
                continue
            try:
                key = str(int(code))
            except (TypeError, ValueError):
                key = str(code).strip()
            mapping[key] = str(label).strip()
        workbook.close()
        print(f"Loaded {len(mapping)} status labels from {STATUS_MAPPING_FILE.name}")
        _STATUS_MAPPING_CACHE["mtime"] = current_mtime
        _STATUS_MAPPING_CACHE["mapping"] = mapping
        return mapping
    except Exception as exc:
        print(f"Cannot read {STATUS_MAPPING_FILE.name}: {exc}. Keep numeric Status values.")
        return {}


def apply_display_statuses(rows, mapping, fields=("status",)):
    """Translate numeric codes without changing values stored in SQLite."""
    for row in rows:
        for field in fields:
            value = row.get(field)
            if value is None:
                row[field] = ""
                continue
            text = str(value).strip()
            row[field] = mapping.get(text, text)
    return rows


def load_arrived_lt_bot():
    spec = importlib.util.spec_from_file_location("bot_deli_bda_arrived_lt", ARRIVED_LT_BOT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ARRIVED_LT_BOT = load_arrived_lt_bot()


def create_bq_service():
    return sqlite_store.create_service()


def ensure_result_release_schema(service):
    """Allow the Sheet-only command to run safely after a BOT upgrade."""
    try:
        service.tables().patch(
            projectId=BIGQUERY_PROJECT_ID,
            datasetId=BIGQUERY_DATASET_ID,
            tableId=PENDING_RESULT_TABLE_ID,
            body={
                "schema": {
                    "fields": [
                        {"name": "processing_stage", "type": "STRING"},
                        {"name": "publish_after", "type": "DATETIME"},
                        {"name": "published_at", "type": "DATETIME"},
                    ]
                }
            },
        ).execute()
    except Exception:
        pass

    # Shared cache retained under its original table name so COGS values
    # collected by the previous BOT 3 version remain reusable.
    try:
        service.tables().insert(
            projectId=BIGQUERY_PROJECT_ID,
            datasetId=BIGQUERY_DATASET_ID,
            body={
                "tableReference": {"tableId": COGS_CACHE_TABLE_ID},
                "schema": {
                    "fields": [
                        {"name": "shipment_id", "type": "STRING"},
                        {"name": "cogs", "type": "NUMERIC"},
                        {"name": "fetched_at", "type": "DATETIME"},
                        {"name": "last_error", "type": "STRING"},
                    ]
                },
            },
        ).execute()
    except Exception:
        pass
    try:
        service.tables().patch(
            projectId=BIGQUERY_PROJECT_ID,
            datasetId=BIGQUERY_DATASET_ID,
            tableId=PENDING_STATUS_CHANGED_TABLE_ID,
            body={
                "schema": {
                    "fields": [
                        {"name": "change_event_id", "type": "STRING"},
                        {"name": "shipment_id", "type": "STRING"},
                        {"name": "order_number", "type": "STRING"},
                        {"name": "trip_number", "type": "STRING"},
                        {"name": "to_number", "type": "STRING"},
                        {"name": "to_path", "type": "STRING"},
                        {"name": "sender", "type": "STRING"},
                        {"name": "receiver", "type": "STRING"},
                        {"name": "source_type", "type": "STRING"},
                        {"name": "arrived_time", "type": "DATETIME"},
                        {"name": "order_status", "type": "STRING"},
                        {"name": "status", "type": "STRING"},
                        {"name": "current_station", "type": "STRING"},
                        {"name": "previous_order_status", "type": "STRING"},
                        {"name": "previous_current_station", "type": "STRING"},
                        {"name": "previous_published_at", "type": "DATETIME"},
                        {"name": "status_changed_at", "type": "DATETIME"},
                        {"name": "result_reason", "type": "STRING"},
                    ]
                }
            },
        ).execute()
    except Exception:
        pass


_SPREADSHEET_CACHE = {}


OPEN_SPREADSHEET_RETRY_SECONDS = (5, 15, 40)


def open_spreadsheet():
    # BOT 3 opens the same spreadsheet from the export step and again from the
    # work-sync step. Reuse one authorized handle; the service-account token
    # refreshes itself on expiry.
    cached = _SPREADSHEET_CACHE.get("spreadsheet")
    if cached is not None:
        return cached
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    # Lam lai khi mang chop tat. Lay token o oauth2.googleapis.com la cu goi
    # mang DAU TIEN cua buoc xuat sheet, va no khong he duoc thu lai: mot cu bat
    # tay TLS hong la ca vong bi bo, du toan bo phan nang truoc do da xong.
    # Quan sat 2026-09-11 tren may BD B: SSLEOFError "UNEXPECTED_EOF_WHILE_READING"
    # tu /token, sau khi pipeline da chay 505s va cham xong 12.000 don.
    # BOT 1 va BOT 2 khong dinh vi chung khong goi Google, chi doc SQLite va FMS.
    lan_cuoi = None
    for lan in range(len(OPEN_SPREADSHEET_RETRY_SECONDS) + 1):
        try:
            creds = Credentials.from_service_account_file(
                str(SERVICE_ACCOUNT_FILE), scopes=scopes
            )
            client = gspread.authorize(creds)
            spreadsheet = client.open_by_key(TARGET_SHEET_ID)
            _SPREADSHEET_CACHE["spreadsheet"] = spreadsheet
            return spreadsheet
        except Exception as exc:
            # Chi thu lai loi mang/TLS. Sai khoa, sai quyen, sai ID sheet thi
            # phai nem ra ngay - thu lai chi lam cham viec phat hien.
            if not _la_loi_mang(exc):
                raise
            lan_cuoi = exc
            if lan >= len(OPEN_SPREADSHEET_RETRY_SECONDS):
                break
            cho = OPEN_SPREADSHEET_RETRY_SECONDS[lan]
            print(
                f"Google auth loi mang ({type(exc).__name__}); doi {cho}s roi thu lai "
                f"({lan + 1}/{len(OPEN_SPREADSHEET_RETRY_SECONDS)})..."
            )
            time.sleep(cho)
    raise lan_cuoi


def _la_loi_mang(exc):
    """True neu day la truc trac duong truyen, khong phai sai cau hinh."""
    ten = type(exc).__name__
    if ten in {"TransportError", "SSLError", "SSLEOFError", "ConnectionError",
               "MaxRetryError", "ReadTimeout", "ConnectTimeout", "Timeout"}:
        return True
    van_ban = str(exc).lower()
    return any(dau in van_ban for dau in (
        "ssl", "connection", "timed out", "timeout", "max retries",
        "temporarily unavailable", "eof occurred",
    ))


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


def load_cogs_cache(service):
    rows = query_bq(
        service,
        f"""
        WITH ranked AS (
          SELECT
            shipment_id,
            cogs,
            fetched_at,
            last_error,
            ROW_NUMBER() OVER (
              PARTITION BY shipment_id
              ORDER BY fetched_at DESC
            ) AS rn
          FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{COGS_CACHE_TABLE_ID}`
        )
        SELECT shipment_id, cogs, fetched_at, last_error
        FROM ranked
        WHERE rn = 1
        """,
    )
    return {str(row.get("shipment_id") or "").strip(): row for row in rows}


def fetch_cogs(shipment_id):
    url = (
        "https://spx.shopee.vn/api/fleet_order/order/detail/show_sensitive_data"
        f"?shipment_id={quote(shipment_id)}&data_field=cogs"
    )
    response = browser_fetch.request_json(
        "Admin",
        "GET",
        url,
        label=f"COGS {shipment_id}",
        max_retries=3,
    )
    value = ((response.get("data") or {}).get("data_detail"))
    if value is None or value == "":
        raise RuntimeError("COGS API returned empty data_detail")
    return value


COGS_ERROR_RETRY_MINUTES = 180

# "Your visits have reached the maximum" (API code 200301004) is a per-account
# daily/session cap on viewing sensitive data (COGS), not a transient throttle
# - browser_fetch's cooldown logic does not recognize this text, so every
# remaining shipment in the batch was retrying 3x with delays and failing
# identically. Once seen, stop calling COGS entirely for a cooldown instead of
# grinding through the rest of the batch one futile retry at a time.
COGS_QUOTA_COOLDOWN_MINUTES = 60
_cogs_quota_exhausted_until = None


def _is_cogs_quota_error(exc):
    text = str(exc).lower()
    return "reached the maximum" in text or "200301004" in text


def _cogs_lookup_recently_failed(record):
    """True when the last attempt for this order errored within the retry window.

    Without this, every cycle re-spends the per-cycle COGS budget on the same
    permanently-failing shipment IDs and never reaches the new ones.
    """
    if not record:
        return False
    if str(record.get("cogs") or "").strip():
        return False
    if not str(record.get("last_error") or "").strip():
        return False
    try:
        last_attempt = datetime.strptime(str(record.get("fetched_at")), "%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return False
    return last_attempt >= datetime.now() - timedelta(minutes=COGS_ERROR_RETRY_MINUTES)


def fill_missing_cogs(service, pending_rows, cache, limit):
    global _cogs_quota_exhausted_until
    if _cogs_quota_exhausted_until and datetime.now() < _cogs_quota_exhausted_until:
        remaining = (_cogs_quota_exhausted_until - datetime.now()).total_seconds() / 60
        print(
            f"COGS: bo qua - tai khoan Admin da cham gioi han xem sensitive data hom nay; "
            f"tam dung {remaining:.0f} phut nua."
        )
        return 0

    shipment_ids = []
    skipped_recent_error = 0
    for row in pending_rows:
        shipment_id = str(row.get("shipment_id") or "").strip()
        if not shipment_id:
            continue
        record = cache.get(shipment_id)
        if str((record or {}).get("cogs") or "").strip():
            continue
        if _cogs_lookup_recently_failed(record):
            skipped_recent_error += 1
            continue
        shipment_ids.append(shipment_id)
    shipment_ids = list(dict.fromkeys(shipment_ids))
    if limit is not None:
        shipment_ids = shipment_ids[:max(0, int(limit))]
    if skipped_recent_error:
        print(
            f"COGS: skip {skipped_recent_error} order(s) that failed in the last "
            f"{COGS_ERROR_RETRY_MINUTES} min"
        )
    if not shipment_ids:
        print("COGS: no pending order needs lookup")
        return 0

    print(f"COGS: fetch {len(shipment_ids)} pending order(s) with the Admin FMS session")
    stored_rows = []
    for index, shipment_id in enumerate(shipment_ids, start=1):
        fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            value = fetch_cogs(shipment_id)
            record = {
                "shipment_id": shipment_id,
                "cogs": value,
                "fetched_at": fetched_at,
                "last_error": "",
            }
            print(f"COGS {index}/{len(shipment_ids)}: {shipment_id} = {value}")
            _cogs_quota_exhausted_until = None
        except Exception as exc:
            record = {
                "shipment_id": shipment_id,
                "cogs": "",
                "fetched_at": fetched_at,
                "last_error": str(exc)[:500],
            }
            print(f"COGS {index}/{len(shipment_ids)} failed: {shipment_id}: {exc}")
            if _is_cogs_quota_error(exc):
                _cogs_quota_exhausted_until = datetime.now() + timedelta(minutes=COGS_QUOTA_COOLDOWN_MINUTES)
                cache[shipment_id] = record
                stored_rows.append(record)
                print(
                    f"COGS: tai khoan Admin da cham gioi han xem sensitive data (\"reached the "
                    f"maximum\"). DUNG lay COGS {COGS_QUOTA_COOLDOWN_MINUTES} phut thay vi retry "
                    f"tung don con lai trong dot nay ({len(shipment_ids) - index} don bo qua)."
                )
                break
        cache[shipment_id] = record
        stored_rows.append(record)
        # browser_fetch._pace() already spaces this endpoint by ~0.6s, so an
        # extra fixed sleep here just lengthened every cycle for no benefit.

    if stored_rows:
        service.store.insert_rows(
            COGS_CACHE_TABLE_ID,
            stored_rows,
            fields=[
                {"name": "shipment_id", "type": "STRING"},
                {"name": "cogs", "type": "NUMERIC"},
                {"name": "fetched_at", "type": "DATETIME"},
                {"name": "last_error", "type": "STRING"},
            ],
        )
    return len(stored_rows)


def attach_cogs(pending_rows, cache):
    for row in pending_rows:
        shipment_id = str(row.get("shipment_id") or "").strip()
        row["cogs"] = (cache.get(shipment_id) or {}).get("cogs") or ""
    return pending_rows


def pending_query(source_type=None):
    source_filter = ""
    if source_type == "Bulky":
        source_filter = "AND source_type = 'BULKY'"
    elif source_type == "Sorting":
        source_filter = "AND source_type IN ('TO_SORTING', 'TO_TRANSIT')"

    return f"""
    WITH latest_result AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY candidate_id
          ORDER BY checked_at DESC
        ) AS latest_result_rank
      FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{PENDING_RESULT_TABLE_ID}`
      WHERE result_rule_version = '{PENDING_RESULT_RULE_VERSION}'
        AND order_number IS NOT NULL
        AND order_number != ''
        AND order_number != '{SEQUENCE_MARKER_ORDER}'
    )
    SELECT
      candidate_id AS _candidate_id,
      source_type AS _source_type,
      trip_id AS _trip_id,
      sequence_number AS _sequence_number,
      COALESCE(NULLIF(shipment_id, ''), order_number, to_number) AS shipment_id,
      trip_number AS lt_id,
      to_number AS to_id,
      to_path,
      sender,
      receiver,
      to_parcel_quantity AS _to_parcel_quantity,
      to_station AS _to_station,
      unloaded_station_name AS _unloaded_station_name,
      CASE
        WHEN source_type = 'BULKY' THEN 'Bulky'
        WHEN source_type = 'TO_TRANSIT' THEN 'Transit'
        ELSE 'Sorting'
      END AS type,
      FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', arrived_time) AS arrived_time,
      COALESCE(NULLIF(status, ''), order_status) AS status,
      current_station,
      attempt,
      journey_type,
      destination,
      FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', checked_at) AS checked_at
    FROM latest_result
    WHERE latest_result_rank = 1
      AND pending_status = 'PENDING'
      -- Terminal states are decided in the pending pipeline before export.
      AND (processing_stage IS NULL OR processing_stage IN ('FINAL_READY', 'FINAL'))
      AND (
        source_type = 'TO_TRANSIT'
        OR LOWER(TRIM(sender)) = LOWER('BD A Mega SOC')
      )
      AND arrived_time IS NOT NULL
      AND DATETIME_ADD(arrived_time, INTERVAL {PENDING_READY_AFTER_ARRIVED_HOURS} HOUR)
        <= CURRENT_DATETIME('Asia/Bangkok')
      {source_filter}
    ORDER BY arrived_time ASC, `type`, to_id, shipment_id
    """


# So ngay gan nhat duoc hien tren tab pending_status_changed (ops chot 2026-09-16).
#
# Khong xoa dong nao: tab nay bi resize + ghi de tron ven moi vong, khong ai sua
# tay, nen chi can loc bot o cau truy van la tab tu ngan lai. Bang
# lt_pending_status_changed VAN giu day du - no la nhat ky, va la thu duy nhat
# con lai de truy lai lich su khi co su co.
STATUS_CHANGED_SHEET_DAYS = 2

# Tab nay chi ghi MOI NGAY MOT LAN (ops chot 2026-09-19).
#
# Ly do: mot lenh ghi Sheet ton ~170 giay bat ke so dong, do duoc 176 giay cho
# 6.251 dong. Noi dung lai gan nhu khong doi giua hai vong cach nhau 15-40 phut,
# va khong ai truc tiep lam viec tren tab nay - no la nhat ky de tra cuu. Bang
# lt_pending_status_changed van duoc ghi day du moi vong, nen khong mat du lieu,
# chi cham hien len sheet.
STATUS_CHANGED_WRITE_EVERY_HOURS = 24
_STATUS_CHANGED_MARKER = BASE_DIR / ".pending_status_changed_last_write"


def _lan_ghi_status_changed_gan_nhat():
    try:
        return datetime.fromtimestamp(_STATUS_CHANGED_MARKER.stat().st_mtime)
    except OSError:
        return None


def _con_bao_lau_toi_luot_status_changed():
    lan_cuoi = _lan_ghi_status_changed_gan_nhat()
    if lan_cuoi is None:
        return 0.0
    da_qua = (datetime.now() - lan_cuoi).total_seconds() / 3600
    return max(0.0, STATUS_CHANGED_WRITE_EVERY_HOURS - da_qua)


def _den_han_ghi_status_changed():
    if str(os.getenv("BOT_DELI_FORCE_STATUS_CHANGED_SHEET", "")).strip() in ("1", "true", "yes", "on"):
        return True
    return _con_bao_lau_toi_luot_status_changed() <= 0


def _ghi_moc_status_changed():
    try:
        _STATUS_CHANGED_MARKER.write_text(
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"), encoding="utf-8"
        )
    except OSError as exc:
        print(f"Khong ghi duoc moc thoi gian tab pending_status_changed: {exc}")


def status_changed_query():
    return f"""
    SELECT
      COALESCE(NULLIF(shipment_id, ''), order_number, to_number) AS shipment_id,
      trip_number AS lt_id,
      to_number AS to_id,
      to_path,
      sender,
      receiver,
      CASE
        WHEN source_type = 'BULKY' THEN 'Bulky'
        WHEN source_type = 'TO_TRANSIT' THEN 'Transit'
        ELSE 'Sorting'
      END AS type,
      FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', arrived_time) AS arrived_time,
      COALESCE(NULLIF(previous_order_status, ''), 'PENDING') AS previous_status,
      COALESCE(NULLIF(status, ''), order_status, 'NOT_PENDING') AS status,
      previous_current_station,
      current_station,
      FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', previous_published_at) AS previous_published_at,
      FORMAT_DATETIME('%Y-%m-%d %H:%M:%S', status_changed_at) AS status_changed_at,
      result_reason
    FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{PENDING_STATUS_CHANGED_TABLE_ID}`
    WHERE status_changed_at IS NOT NULL
      AND status_changed_at >= DATETIME_SUB(
        CURRENT_DATETIME('Asia/Bangkok'), INTERVAL {STATUS_CHANGED_SHEET_DAYS} DAY
      )
    ORDER BY status_changed_at DESC, shipment_id
    """


def get_or_create_worksheet(spreadsheet, title, rows=1000, cols=30):
    try:
        return spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)


def resize_worksheet(worksheet, row_count, col_count):
    """True neu luoi da dung kich thuoc. Nguoi goi phai xu ly khi False."""
    dong = max(2, row_count)
    cot = max(1, col_count)
    # Doi kich thuoc luoi la lenh CAU TRUC, dat hon nhieu so voi ghi gia tri, va
    # truoc day no chay moi vong ke ca khi so dong khong doi. Log 2026-09-16:
    # ca BON lan goi resize trong mot vong deu an HTTP 503 ngay lan dau, mot lan
    # an hai lan lien - tuc no vua dat vua la cho de hong nhat. So dong cac tab
    # nay gan nhu dung yen giua hai vong (4.145 -> 4.191), nen bo qua khi kich
    # thuoc da dung la xoa han bon lenh do khoi moi vong.
    if worksheet.row_count == dong and worksheet.col_count == cot:
        return True
    try:
        _thu_lai_sheets(
            f"Doi kich thuoc tab {worksheet.title}",
            lambda: worksheet.resize(rows=dong, cols=cot),
        )
        return True
    except Exception:
        return False


SHEET_WRITE_RETRY_SECONDS = (5, 15, 40, 90)


def _thu_lai_sheets(mo_ta, ham):
    """Chay ham, thu lai khi Google tra loi tam thoi. Ham phai LAP LAI DUOC."""
    for lan in range(len(SHEET_WRITE_RETRY_SECONDS) + 1):
        try:
            return ham()
        except gspread.exceptions.APIError as exc:
            ma = getattr(getattr(exc, "response", None), "status_code", None)
            if ma not in {429, 500, 502, 503, 504} or lan >= len(SHEET_WRITE_RETRY_SECONDS):
                raise
            cho = SHEET_WRITE_RETRY_SECONDS[lan]
            print(
                f"{mo_ta}: HTTP {ma}; cho {cho}s roi thu lai "
                f"({lan + 1}/{len(SHEET_WRITE_RETRY_SECONDS)})..."
            )
            time.sleep(cho)


def write_rows_to_worksheet(spreadsheet, title, rows, columns=PENDING_COLUMNS):
    headers = [label for _, label in columns]
    worksheet = get_or_create_worksheet(spreadsheet, title, rows=max(1000, len(rows) + 1), cols=len(headers))
    values = [headers]
    values.extend([[row.get(key, "") for key, _ in columns] for row in rows])

    # KHONG clear() truoc roi update() sau nua. Hai lenh do la hai request rieng;
    # ngay 2026-09-14 clear() cua tab to_sorting_pending tra HTTP 500, va neu no
    # da kip chay thi tab nam TRONG RONG cho ops suot den vong sau. resize ve dung
    # so dong roi ghi de ca vung A1 da xoa moi dong cu, nen khong can clear:
    # ngan hon thi resize cat bot, dai hon thi update ghi de het.
    so_dong_cu = worksheet.row_count or 0
    da_resize = resize_worksheet(worksheet, len(values), len(headers))
    _thu_lai_sheets(
        f"Ghi tab {title}",
        lambda: worksheet.update(
            values=values, range_name="A1", value_input_option="USER_ENTERED"
        ),
    )
    # Resize hong ma dot nay it dong hon dot truoc thi phan duoi con du lieu cu.
    # Xoa rieng phan do - chi chay khi resize that bai, va chi cham vung NAM DUOI
    # du lieu vua ghi, nen khong bao gio lam trong tab.
    if not da_resize and so_dong_cu > len(values):
        try:
            _thu_lai_sheets(
                f"Xoa duoi tab {title}",
                # Dang "101:500" la xoa tron ca dong 101 den 500.
                lambda: worksheet.batch_clear([f"{len(values) + 1}:{so_dong_cu}"]),
            )
        except Exception:
            print(f"Canh bao: tab {title} co the con dong cu ben duoi dong {len(values)}.")
    print(f"Exported {len(rows)} rows to sheet tab: {title}")


def mark_pending_rows_as_published(service, rows):
    """Mark only rows that were successfully written to the Pending Sheet."""
    candidate_ids = sorted({str(row.get("_candidate_id") or "").strip() for row in rows if row.get("_candidate_id")})
    if not candidate_ids:
        return 0

    published_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for start in range(0, len(candidate_ids), 500):
        batch = candidate_ids[start:start + 500]
        quoted = ", ".join("'" + value.replace("'", "''") + "'" for value in batch)
        service.jobs().query(
            projectId=BIGQUERY_PROJECT_ID,
            body={
                "query": f"""
                UPDATE `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{PENDING_RESULT_TABLE_ID}`
                SET published_at = '{published_at}'
                WHERE candidate_id IN ({quoted})
                  AND pending_status = 'PENDING'
                  AND result_rule_version = '{PENDING_RESULT_RULE_VERSION}'
                  AND (published_at IS NULL OR published_at = '')
                """,
                "useLegacySql": False,
            },
        ).execute()
    print(f"Marked {len(candidate_ids)} Pending row(s) as exported to Sheet")
    return len(candidate_ids)


def seconds_until_next_shift_release(now=None):
    now = now or datetime.now()
    release_times = [
        datetime.combine(now.date(), datetime.strptime(value, "%H:%M:%S").time())
        for value in SHEET_RELEASE_TIMES
    ]
    for release_at in release_times:
        if release_at > now:
            return (release_at - now).total_seconds(), release_at

    next_release_at = datetime.combine(
        now.date() + timedelta(days=1),
        datetime.strptime(SHEET_RELEASE_TIMES[0], "%H:%M:%S").time(),
    )
    return (next_release_at - now).total_seconds(), next_release_at


@contextmanager
def _do(ten):
    """In thoi gian tung buoc con cua buoc xuat Sheet.

    Buoc publish_pending_outputs gop sau viec khac nhau vao mot con so duy nhat,
    nen khi no cham (do 2026-09-16: 1.329s roi 1.854s) khong co cach nao biet
    buoc nao gay ra. In rieng tung buoc de lan sau khoi phai doan.
    """
    t0 = time.time()
    try:
        yield
    finally:
        print(f"  [xuat sheet] {ten}: {time.time() - t0:.1f}s")


def run_once(fetch_missing_cogs=False, cogs_limit=250):
    bq_service = create_bq_service()
    ensure_result_release_schema(bq_service)
    spreadsheet = open_spreadsheet()
    status_mapping = load_status_mapping()

    with _do("truy van don Pending"):
        all_rows = query_bq(bq_service, pending_query())
    with _do("nap bang dem COGS"):
        cogs_cache = load_cogs_cache(bq_service)
    if fetch_missing_cogs:
        with _do("lay COGS con thieu tu FMS"):
            fill_missing_cogs(bq_service, all_rows, cogs_cache, cogs_limit)
    attach_cogs(all_rows, cogs_cache)
    all_rows = apply_display_statuses(all_rows, status_mapping)
    bulky_rows = [row for row in all_rows if row.get("type") == "Bulky"]
    to_sorting_rows = [row for row in all_rows if row.get("type") == "Sorting"]

    with _do(f"ghi tab pending_all ({len(all_rows):,} dong)"):
        write_rows_to_worksheet(spreadsheet, "pending_all", all_rows)
    # bulky_pending va to_sorting_pending BO tu 2026-09-16 (ops dong y): ca hai
    # chi la pending_all loc theo cot Type, khong co du lieu rieng.
    #
    # Do duoc cung ngay: chi phi mot lenh ghi Sheet la ~170 giay BAT KE so dong -
    # tab bulky 222 dong mat 182s trong khi pending_all 3.843 dong mat 118s. Nen
    # bo hai lenh ghi la bo thang ~390 giay moi vong, khong phai bo theo ty le.
    with _do(f"dong dau published_at ({len(all_rows):,} don)"):
        mark_pending_rows_as_published(bq_service, all_rows)

    changed_rows = []
    if _den_han_ghi_status_changed():
        with _do("truy van lich su doi trang thai"):
            changed_rows = apply_display_statuses(
                query_bq(bq_service, status_changed_query()),
                status_mapping,
                fields=("previous_status", "status"),
            )
        with _do(f"ghi tab pending_status_changed ({len(changed_rows):,} dong)"):
            write_rows_to_worksheet(
                spreadsheet,
                "pending_status_changed",
                changed_rows,
                columns=STATUS_CHANGED_COLUMNS,
            )
        _ghi_moc_status_changed()
    else:
        con = _con_bao_lau_toi_luot_status_changed()
        print(f"Bo qua tab pending_status_changed vong nay (con {con:.1f}h nua toi luot ghi)")
    # bulky/to_sorting van dem de theo doi co cau, du khong con tab rieng.
    print(
        "Done export pending to sheet. "
        f"all={len(all_rows)} (bulky={len(bulky_rows)}, to_sorting={len(to_sorting_rows)}) | "
        f"status_changed={len(changed_rows)}"
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Chay 1 vong roi dung.")
    parser.add_argument("--interval-minutes", type=int, default=60, help="So phut giua 2 lan export sheet.")
    parser.add_argument("--skip-cogs", action="store_true", help="Khong goi API COGS truoc khi xuat Sheet.")
    parser.add_argument("--cogs-limit", type=int, default=250, help="Toi da Pending can goi COGS moi lan xuat.")
    parser.add_argument(
        "--at-shift-release",
        action="store_true",
        help="Chi export dung cac moc 08:00, 12:00, 14:00 (truoc ca 09:00, 13:00, 15:00 mot gio).",
    )
    args = parser.parse_args()

    if args.at_shift_release and not args.once:
        print(
            "Sheet export schedule: 08:00, 12:00, 14:00 "
            "(ca bat dau luc 09:00, 13:00, 15:00)."
        )
        try:
            print("Run initial Sheet export for overdue/ready pending orders.")
            run_once(fetch_missing_cogs=not args.skip_cogs, cogs_limit=args.cogs_limit)
        except Exception:
            print("Initial export pending to sheet failed:")
            traceback.print_exc()
        while True:
            try:
                sleep_seconds, release_at = seconds_until_next_shift_release()
                print(f"Wait until sheet release: {release_at:%Y-%m-%d %H:%M:%S}")
                ARRIVED_LT_BOT.ti.sleep(max(0, sleep_seconds))
                run_once(fetch_missing_cogs=not args.skip_cogs, cogs_limit=args.cogs_limit)
            except Exception:
                print("Export pending to sheet failed:")
                traceback.print_exc()
        return

    interval_seconds = max(1, args.interval_minutes) * 60
    while True:
        try:
            run_once(fetch_missing_cogs=not args.skip_cogs, cogs_limit=args.cogs_limit)
        except Exception:
            print("Export pending to sheet failed:")
            traceback.print_exc()

        if args.once:
            return

        print(f"Sleep {interval_seconds} seconds before next pending sheet export.")
        ARRIVED_LT_BOT.ti.sleep(interval_seconds)


if __name__ == "__main__":
    main()
