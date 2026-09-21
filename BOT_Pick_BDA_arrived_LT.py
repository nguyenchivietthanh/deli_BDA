"""BOT_Pick_BDA - Buoc 1: lay danh sach chuyen LT ma BDA la diem NHAN hang.

Chi dung 1 tai khoan FMS: BDA (SOC_CODE). Khac voi BOT chieu gui (can
first_station=2490), chieu nhan chi can loc middle_station=2490: theo xac
nhan nghiep vu, tham so nay da bao gom ca truong hop BDA la tram cuoi cua
chuyen, nen khong can truy van rieng cho truong hop "BDA la tram cuoi".

Voi moi chuyen LT co trang thai station = 90 (da toi) va co BDA trong lo
trinh, doc trip/history/detail de xac dinh dung vi tri (sequence_number)
va thoi diem BDA nhan hang (ATA), roi ghi 1 dong moi vao bang lt_in_trip
voi parsed_status = NOT_PARSED.

Buoc 2 (BOT_Pick_BDA_export_LT_unit.py) se doc cac dong NOT_PARSED cua bang
nay, goi API "TO trong 1 chuyen LT" (type=inbound) voi unloaded_sequence
lay tu day, roi danh dau lai PARSED.

Moi (trip_id, sequence_number) chi co DUNG 1 dong trong bang: Buoc 1 INSERT
dong moi voi parsed_status=NOT_PARSED khi lan dau phat hien; cac buoc sau
(2, 3...) UPDATE truc tiep dong do khi xu ly xong, khong insert dong moi.
Nho vay bang xem truc tiep bang DB Browser luon gon, khong bi nhan doi.
"""

import argparse
import json
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

import browser_fetch
import sqlite_store


BASE_DIR = Path(__file__).resolve().parent
STATE_FILE = BASE_DIR / "bot_pick_bda_arrived_state.json"

SOC_CODE = "BD A Mega SOC"
BDA_STATION_ID = "2490"

BIGQUERY_PROJECT_ID = "bot-503107"
BIGQUERY_DATASET_ID = "deli_bda"
TRIP_TABLE_ID = "lt_in_trip"

PAGE_COUNT = 100
REQUEST_MAX_RETRIES = 5
RUN_INTERVAL_SECONDS = 60 * 60
MTIME_WINDOW_OVERLAP_HOURS = 2
FIRST_RUN_LOOKBACK_HOURS = 24


# --------------------------------------------------------------- SQLite/BQ

def create_bq_service():
    return sqlite_store.create_service()


def trip_table_schema_fields():
    return [
        {"name": "trip_number", "type": "STRING"},
        {"name": "trip_id", "type": "STRING"},
        {"name": "sequence_number", "type": "INTEGER"},
        {"name": "station", "type": "STRING"},
        {"name": "to_station", "type": "STRING"},
        {"name": "arrived_time", "type": "DATETIME"},
        {"name": "is_last_station", "type": "INTEGER"},
        {"name": "parsed_status", "type": "STRING"},
        {"name": "parsed_at", "type": "DATETIME"},
        {"name": "observed_at", "type": "DATETIME"},
    ]


def ensure_tables(service):
    service.store.ensure_table(TRIP_TABLE_ID, trip_table_schema_fields())
    # Tu tao index bang SQL truc tiep, khong dung PERFORMANCE_INDEXES trong
    # sqlite_store.py - tranh dong cham file dung chung cho ca cac BOT khac.
    service.store.query(
        f'CREATE INDEX IF NOT EXISTS idx_lt_in_trip_trip_id ON "{TRIP_TABLE_ID}" (trip_id)'
    )
    service.store.query(
        f'CREATE INDEX IF NOT EXISTS idx_lt_in_trip_parsed '
        f'ON "{TRIP_TABLE_ID}" (parsed_status, trip_id, sequence_number)'
    )


def query_rows(service, sql):
    _fields, rows = service.store.query(sql)
    return rows


def insert_rows(service, rows, table_id=TRIP_TABLE_ID, schema_fields=None):
    if not rows:
        return 0
    service.store.insert_rows(table_id, rows, schema_fields or trip_table_schema_fields())
    return len(rows)


def _sql_quote(value):
    return str(value).replace("'", "''")


def update_trip_parsed_status(service, trip_id, sequence_number, parsed_status, parsed_at):
    """UPDATE tai cho, khong insert dong moi - giu lt_in_trip 1 dong/chuyen."""
    service.store.query(
        f"""
        UPDATE `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{TRIP_TABLE_ID}`
        SET parsed_status = '{_sql_quote(parsed_status)}', parsed_at = '{_sql_quote(parsed_at)}'
        WHERE trip_id = '{_sql_quote(trip_id)}' AND sequence_number = {int(sequence_number)}
        """
    )


# -------------------------------------------------------------------- util

def now_text():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def to_text(value):
    return str(value or "").strip()


def to_int(value, default=0):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def first_value(data, keys):
    if not isinstance(data, dict):
        return None
    for key in keys:
        value = data.get(key)
        if value not in (None, ""):
            return value
    return None


def timestamp_to_datetime(value):
    if value in (None, "", 0, "0"):
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    try:
        number = float(value)
        if number > 10_000_000_000:
            number = number / 1000
        if number > 1_000_000_000:
            return datetime.fromtimestamp(number, tz=timezone.utc).astimezone(
                timezone(timedelta(hours=7))
            ).replace(tzinfo=None)
    except (TypeError, ValueError, OSError):
        pass
    text = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(text).replace(tzinfo=None)
    except ValueError:
        return None


def format_datetime(value):
    dt = value if isinstance(value, datetime) else timestamp_to_datetime(value)
    return dt.strftime("%Y-%m-%d %H:%M:%S") if dt else None


def to_epoch_seconds(dt):
    return int(dt.timestamp())


# ------------------------------------------------------------------- state

def load_state():
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_state_datetime(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def compute_mtime_window(state, run_at):
    """Con tro tang dan, chong lan giua 2 lan chay - giong BOT chieu gui."""
    last_to = parse_state_datetime(state.get("last_mtime_to"))
    window_to = run_at
    if last_to:
        window_from = last_to - timedelta(hours=MTIME_WINDOW_OVERLAP_HOURS)
        print(
            f"Mtime cursor: tiep tuc tu {last_to.isoformat(timespec='seconds')} "
            f"(overlap {MTIME_WINDOW_OVERLAP_HOURS}h)"
        )
    else:
        window_from = window_to - timedelta(hours=FIRST_RUN_LOOKBACK_HOURS)
        print(f"Chua co mtime cursor. Lay {FIRST_RUN_LOOKBACK_HOURS}h gan nhat.")
    if window_to <= window_from:
        window_from = window_to - timedelta(hours=MTIME_WINDOW_OVERLAP_HOURS)
    return window_from, window_to


# --------------------------------------------------------------------- FMS

def request_json(url, label=""):
    return browser_fetch.request_json(
        SOC_CODE, "GET", url, payload={}, label=label, max_retries=REQUEST_MAX_RETRIES
    )


def fetch_list(url_template, count=PAGE_COUNT, label=""):
    all_rows = []
    page = 1
    total = 0
    while True:
        url = url_template.format(page=page, count=count)
        data = request_json(url, label=f"{label} | page {page}" if label else f"page {page}")
        data_node = (data or {}).get("data") or {}
        if page == 1:
            total = to_int(data_node.get("total"), default=0)
            print(f"{label} - total: {total} | count/page: {count}")
            if total == 0:
                return []
        rows = data_node.get("list") or []
        if not rows:
            break
        all_rows.extend(rows)
        if page * count >= total:
            break
        page += 1
    return all_rows


def fetch_bda_receiving_trips(window_from, window_to):
    start = to_epoch_seconds(window_from)
    end = to_epoch_seconds(window_to)
    url_template = (
        "https://spx.shopee.vn/api/admin/transportation/trip/history/list"
        f"?trip_station_status=90&pageno={{page}}&count={{count}}"
        f"&mtime={start},{end}&middle_station={BDA_STATION_ID}"
    )
    return fetch_list(url_template, label=f"LT nhan BDA (mtime {start}-{end})")


def fetch_trip_detail(trip_id, trip_number):
    url = (
        "https://spx.shopee.vn/api/admin/transportation/trip/history/detail"
        f"?trip_id={trip_id}&new_process_switch=false"
    )
    return request_json(url, label=f"Trip detail - {trip_number}")


# ------------------------------------------------------------ trip parsing

def station_name(station):
    return to_text(first_value(station, [
        "station_name", "name", "station", "unloaded_station_name", "loaded_station_name", "hub_name",
    ]))


def station_sequence_number(station, fallback_index):
    value = first_value(station, ["station_no", "station_number"])
    seq = to_int(value, default=0)
    return seq if seq > 0 else fallback_index


def station_arrived_time(station):
    return timestamp_to_datetime(first_value(station, [
        "ata", "arrived_time", "arrival_time", "actual_arrival_time",
    ]))


def find_bda_stop(detail_data, trip):
    """Tim dung tram BDA trong lo trinh, du BDA o giua hay la tram cuoi."""
    data = (detail_data or {}).get("data") or {}
    stations = data.get("trip_station") or data.get("trip_stations") or []
    for index, station in enumerate(stations, start=1):
        if station_name(station).casefold() != SOC_CODE.casefold():
            continue
        arrived_time = station_arrived_time(station)
        if not arrived_time:
            return None
        prior_station = station_name(stations[index - 2]) if index >= 2 else ""
        if not prior_station:
            prior_station = to_text(first_value(trip, ["first_station_name", "loaded_station_name"]))
        return {
            "sequence_number": station_sequence_number(station, index),
            "station": prior_station,
            "arrived_time": arrived_time,
            "is_last_station": 1 if index == len(stations) else 0,
        }
    return None


def load_known_trip_ids(service):
    rows = query_rows(
        service,
        f"SELECT DISTINCT trip_id FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{TRIP_TABLE_ID}`",
    )
    return {to_text(row.get("trip_id")) for row in rows if to_text(row.get("trip_id"))}


def process_trip(trip, observed_at):
    trip_id = to_text(trip.get("id") or trip.get("trip_id"))
    trip_number = to_text(trip.get("trip_number"))
    if not trip_id or not trip_number:
        return None
    detail_data = fetch_trip_detail(trip_id, trip_number)
    stop = find_bda_stop(detail_data, trip)
    if not stop:
        print(f"Bo qua {trip_number}: BDA chua co ATA trong trip detail")
        return None
    return {
        "trip_number": trip_number,
        "trip_id": trip_id,
        "sequence_number": stop["sequence_number"],
        "station": stop["station"],
        "to_station": SOC_CODE,
        "arrived_time": format_datetime(stop["arrived_time"]),
        "is_last_station": stop["is_last_station"],
        "parsed_status": "NOT_PARSED",
        "parsed_at": None,
        "observed_at": observed_at,
    }


# --------------------------------------------------------------------- run

def run_once(window_from=None, window_to=None, limit=None):
    run_started_at = datetime.now()
    service = create_bq_service()
    ensure_tables(service)
    state = load_state()

    if window_from and window_to:
        window_from = parse_state_datetime(window_from) or datetime.strptime(window_from, "%Y-%m-%d %H:%M:%S")
        window_to = parse_state_datetime(window_to) or datetime.strptime(window_to, "%Y-%m-%d %H:%M:%S")
    else:
        window_from, window_to = compute_mtime_window(state, run_started_at)

    print(f"Mtime window: {window_from} -> {window_to}")
    trips = fetch_bda_receiving_trips(window_from, window_to)
    print(f"Tong so chuyen LT tra ve: {len(trips)}")

    known_trip_ids = load_known_trip_ids(service)
    observed_at = now_text()
    rows = []
    processed = 0
    for trip in trips:
        trip_id = to_text(trip.get("id") or trip.get("trip_id"))
        if trip_id and trip_id in known_trip_ids:
            continue
        if limit and processed >= int(limit):
            print(f"Dat gioi han --limit {limit}, dung xu ly them.")
            break
        try:
            row = process_trip(trip, observed_at)
            if row:
                rows.append(row)
                if trip_id:
                    known_trip_ids.add(trip_id)
                processed += 1
        except Exception:
            print(f"Loi khi xu ly chuyen {trip.get('trip_number')}:")
            traceback.print_exc()

    inserted = insert_rows(service, rows)
    print(
        f"Da ghi {TRIP_TABLE_ID}: {inserted} dong moi "
        f"(tra ve {len(trips)} chuyen, {processed} chuyen moi duoc xu ly)."
    )

    state.update({
        "last_run_started_at": run_started_at.isoformat(timespec="seconds"),
        "last_run_finished_at": datetime.now().isoformat(timespec="seconds"),
        "last_mtime_from": window_from.isoformat(timespec="seconds"),
        "last_mtime_to": window_to.isoformat(timespec="seconds"),
        "last_inserted": inserted,
    })
    save_state(state)
    return inserted


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Chay 1 vong roi dung.")
    parser.add_argument("--limit", type=int, default=None, help="Gioi han so chuyen LT moi xu ly trong 1 vong.")
    parser.add_argument("--from", dest="window_from", default=None, help="Vi du: 2026-09-01 00:00:00")
    parser.add_argument("--to", dest="window_to", default=None, help="Vi du: 2026-09-02 00:00:00")
    parser.add_argument("--interval-minutes", type=int, default=RUN_INTERVAL_SECONDS // 60)
    args = parser.parse_args()

    if args.once or args.window_from or args.window_to or args.limit:
        run_once(window_from=args.window_from, window_to=args.window_to, limit=args.limit)
        return

    interval_seconds = max(1, args.interval_minutes) * 60
    while True:
        try:
            run_once()
        except Exception:
            print("BOT_Pick_BDA_arrived_LT that bai:")
            traceback.print_exc()
        print(f"Nghi {interval_seconds}s truoc chu ky tiep theo.")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()
