import json
import math
import os
import requests
import sys
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials
from selenium import webdriver
from selenium.webdriver.common.by import By
import time as ti

try:
    import browser_fetch
except Exception:
    browser_fetch = None


BASE_DIR = Path(__file__).resolve().parent
SOURCE_BOT_REFERENCE_PATH = Path(r"C:\Users\spxvn25689\Desktop\BOT DELI\BOT-Deli_BDA.py")
SERVICE_ACCOUNT_FILE = BASE_DIR / "ops-support.json"
if not SERVICE_ACCOUNT_FILE.exists():
    SERVICE_ACCOUNT_FILE = Path(r"C:\Users\spxvn25689\Desktop\Get_Data_Sorting\ops-support.json")
TARGET_SHEET_ID = "1BqmVWiHdCoE0uFM_54EXu0xah4aDivgEPErc_V14enU"
# Resolve the LT tab by NAME, not by position. It used to be None, which fell
# through to get_worksheet(0) - the first tab in whatever order the team last
# left them in. Reordering the tabs in the browser would then have pointed this
# bot at pending_work: step [1/3] would append LT rows into the team's live
# working sheet, and step [3/3] would find no `trip_number` column, skip every
# row and report "LT ended sequence chua export: 0" - stopping silently with no
# error. Set 2026-09-08 after the team moved tabs around.
TARGET_WORKSHEET_NAME = "Trang tính1"
STATE_FILE = BASE_DIR / "bot_deli_bda_arrived_state.json"

SOC_CODE = "BD A Mega SOC"
RUN_INTERVAL_SECONDS = 60 * 60
REPROCESS_LOOKBACK_DAYS = 1
ARRIVED_WINDOW_OVERLAP_HOURS = 2

# First run follows the example: run on 01/08 -> get 00:00-08:00 of 30/07.
FIRST_RUN_DAYS_BACK = 2
FIRST_RUN_END_HOUR = 8

# After the first run, the upper bound is calculated from:
# arrived_time + 36h < current COT + 2h + expected BOT delay.
ARRIVED_TO_COT_HOURS = 36
COT_BUFFER_HOURS = 2
BOT_EXPECTED_DELAY_HOURS = 2

TARGET_HEADERS = [
    "id",
    "trip_number",
    "sequence_number",
    "station",
    "to_station",
    "departed_time",
    "sta",
    "arrived_time",
    "unseal_time",
    "unseal_operator",
    "unloaded_time",
    "unloaded_operator",
    "total_to",
    "total_order",
    "to_packed",
    "order_bulky",
    "order_packed",
]


PAGE_COUNT = 100
REQUEST_RETRY_SLEEP_SECONDS = 2
REQUEST_MAX_RETRIES = 5
SHEET_APPEND_TRIP_BATCH_SIZE = 50
SHEET_APPEND_MAX_RETRIES = 6


def request_json_with_retry(method, url, headers=None, payload=None, label="", max_retries=REQUEST_MAX_RETRIES, retry_sleep=REQUEST_RETRY_SLEEP_SECONDS, fail_soft=False, quiet=False):
    last_error = None
    browser_role = (headers or {}).get("__browser_fetch_role") if isinstance(headers, dict) else None
    for attempt in range(1, max_retries + 1):
        try:
            if browser_fetch and browser_role:
                data = browser_fetch.request_json(
                    browser_role,
                    method,
                    url,
                    payload=payload or {},
                    label="" if quiet else label,
                    max_retries=max_retries,
                )
            else:
                response = requests.request(method, url, headers=headers, data=payload or {}, timeout=45)
                if response.status_code != 200:
                    raise RuntimeError(f"HTTP {response.status_code}: {response.text[:300]}")

                data = response.json()
            if isinstance(data, dict):
                api_code = data.get("code")
                retcode = data.get("retcode")
                error_code = data.get("error_code")
                if api_code not in (None, 0, "0"):
                    raise RuntimeError(f"API code loi: {api_code} | msg={data.get('msg') or data.get('message')}")
                if retcode not in (None, 0, "0"):
                    raise RuntimeError(f"API retcode loi: {retcode} | msg={data.get('msg') or data.get('message')}")
                if error_code not in (None, 0, "0"):
                    raise RuntimeError(f"API error_code loi: {error_code} | msg={data.get('msg') or data.get('message')}")

            return data
        except Exception as e:
            last_error = e
            tag = f"[{label}] " if label else ""
            # browser_fetch already confirmed (via its own login circuit
            # breaker) that this FMS session needs a manual re-login. Looping
            # here would only repeat the same instant failure max_retries
            # times for nothing - across a large backlog that is what turned
            # one dead login into many hours of wasted BOT time.
            if browser_fetch and isinstance(e, browser_fetch.BrowserSessionUnavailableError):
                if not quiet:
                    print(f"{tag}{e}")
                if fail_soft:
                    return None
                raise
            if attempt >= max_retries:
                if not quiet:
                    print(f"{tag}Request loi sau {max_retries} lan: {e}")
                if fail_soft:
                    return None
                raise
            if not quiet:
                print(f"{tag}Request loi lan {attempt}/{max_retries}: {e}. Cho {retry_sleep}s roi retry...")
            ti.sleep(retry_sleep)
    raise last_error


def fetch_fms_list(url_template, headers, payload=None, count=PAGE_COUNT, label="", fail_soft=True, max_retries=REQUEST_MAX_RETRIES):
    all_rows = []
    page = 1
    total = 0
    total_pages = 0
    browser_role = (headers or {}).get("__browser_fetch_role") if isinstance(headers, dict) else None

    while True:
        url = url_template.format(page=page, count=count)
        data = request_json_with_retry(
            "GET",
            url,
            headers=headers,
            payload=payload or {},
            label=f"{label} | page {page}" if label else f"page {page}",
            fail_soft=fail_soft,
            max_retries=max_retries,
        )
        if data is None:
            if browser_role:
                print(f"{label or 'FMS list'} loi o page {page}. Bo ca batch de lan sau retry, tranh mat data partial.")
                return [], total
            print(f"{label or 'FMS list'} loi o page {page}. Giu lai {len(all_rows)} records da lay duoc.")
            break

        data_node = data.get("data") or {}
        if page == 1:
            total = int(data_node.get("total") or 0)
            total_pages = math.ceil(total / count) if total > 0 else 0
            print(f"{label or 'FMS list'} - total: {total} | count/page: {count} | pages: {total_pages}")
            if total == 0:
                break

        current_page_data = data_node.get("list") or []
        if not current_page_data:
            if browser_role and page < total_pages:
                print(f"{label or 'FMS list'} page {page} rong bat thuong. Bo ca batch de lan sau retry.")
                return [], total
            break

        all_rows.extend(current_page_data)
        if page >= total_pages:
            break
        page += 1

    return all_rows, total


def local_now():
    return datetime.now().replace(minute=0, second=0, microsecond=0)


def to_epoch_seconds(dt):
    return int(dt.timestamp())


def parse_state_datetime(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def load_state():
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_state(state):
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def compute_arrived_window(state, run_at):
    last_window_to = parse_state_datetime(state.get("last_arrived_time_to"))
    window_to = run_at

    if last_window_to:
        window_from = last_window_to - timedelta(hours=ARRIVED_WINDOW_OVERLAP_HOURS)
        print(
            "Continuous arrived cursor: "
            f"last_to={format_datetime(last_window_to)} | "
            f"overlap={ARRIVED_WINDOW_OVERLAP_HOURS}h"
        )
    else:
        window_from = window_to - timedelta(hours=ARRIVED_WINDOW_OVERLAP_HOURS)
        print(
            "No arrived cursor yet. "
            f"Start with the latest {ARRIVED_WINDOW_OVERLAP_HOURS}h only."
        )

    if window_to <= window_from:
        window_from = window_to - timedelta(hours=ARRIVED_WINDOW_OVERLAP_HOURS)

    return window_from, window_to


def timestamp_to_datetime(value):
    if value in (None, "", 0, "0"):
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    try:
        value = float(value)
        if value > 10_000_000_000:
            value = value / 1000
        return datetime.fromtimestamp(value, tz=timezone.utc).astimezone(
            timezone(timedelta(hours=7))
        ).replace(tzinfo=None)
    except Exception:
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
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    parsed = timestamp_to_datetime(value)
    if parsed:
        return parsed.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def first_value(data, keys):
    if not isinstance(data, dict):
        return None
    for key in keys:
        if key in data and data[key] not in (None, ""):
            return data[key]
    return None


def first_time(data, keys):
    return timestamp_to_datetime(first_value(data, keys))


def station_name(station):
    return first_value(
        station,
        [
            "station_name",
            "name",
            "station",
            "loaded_station_name",
            "unloaded_station_name",
            "hub_name",
        ],
    )


def station_sequence_number(station, fallback_index):
    # The Pending Inbound URL expects the visible LH Trip Station No.  Some
    # Trip Detail payloads expose only a different internal unload counter, so
    # use the list position when explicit Station No. is absent.
    value = first_value(station, ["station_no", "station_number"])
    try:
        station_no = int(value)
        return station_no if station_no > 0 else fallback_index
    except (TypeError, ValueError):
        return fallback_index


def station_arrived_or_departed_time(station):
    return first_time(station, [
        "ata",
        "arrived_time",
        "arrival_time",
        "actual_arrival_time",
    ]) or first_time(station, [
        "atd",
        "departed_time",
        "actual_departure_time",
        "departure_time",
    ])


def build_trip_arrival_points(detail_data, fallback_row):
    data = (detail_data or {}).get("data") or {}
    trip_stations = data.get("trip_station") or data.get("trip_stations") or []
    origin_station = trip_stations[0] if trip_stations else {}

    departed_time = first_time(
        origin_station,
        ["atd", "departed_time", "actual_departure_time", "departure_time"],
    ) or first_time(fallback_row, ["atd", "departed_time", "departure_time"])

    points = []
    bda_seen = False
    for index, destination_station in enumerate(trip_stations, start=1):
        name = station_name(destination_station)
        if str(name or "").strip().casefold() == SOC_CODE.casefold():
            bda_seen = True
            continue
        if not bda_seen or not name:
            continue

        arrived_time = station_arrived_or_departed_time(destination_station)
        if not arrived_time:
            continue

        points.append({
            "sequence_number": station_sequence_number(destination_station, index),
            "station": station_name(origin_station) or first_value(fallback_row, ["loaded_station_name", "first_station_name"]) or SOC_CODE,
            "to_station": name,
            "departed_time": departed_time,
            "sta": first_time(destination_station, ["sta", "standard_arrival_time", "planned_arrival_time"]),
            "arrived_time": arrived_time,
            "unseal_time": first_time(destination_station, ["unseal_time", "unsealed_time"]),
            "unseal_operator": first_value(destination_station, ["unseal_operator", "unsealed_operator", "unseal_operator_name"]),
            "unloaded_time": first_time(destination_station, ["unloaded_time", "unload_time"]),
            "unloaded_operator": first_value(destination_station, ["unloaded_operator", "unload_operator", "unloaded_operator_name"]),
        })

    return points


def get_fms_headers(soc_code):
    if browser_fetch:
        print(f"Open/reuse FMS browser session: {soc_code}")
        browser_fetch.ensure_role(soc_code)
        return {"__browser_fetch_role": soc_code}

    user_data_dir = Path(os.getenv("LOCALAPPDATA")) / "Google" / "Chrome" / "User Data 9"
    options = webdriver.ChromeOptions()
    options.add_argument("--user-data-dir=" + str(user_data_dir))
    options.add_argument("--profile-directory=Default")
    driver = webdriver.Chrome(options=options)
    driver.get("https://spx.shopee.vn")

    cookies_final = ""
    for _ in range(12):
        ti.sleep(5)
        try:
            text_var = driver.find_element(By.XPATH, "/html/body/div[2]/div[1]/div/div[2]/div[1]/div").text
        except Exception:
            text_var = "Pass"

        if "Login station is changed by someone else. System would be switched to" in text_var:
            driver.find_element(By.XPATH, "/html/body/div[2]/div[1]/div/div[3]/div[2]/button").click()
            ti.sleep(3)

        try:
            current_station = driver.find_element(
                By.XPATH, '//*[@id="fms-container"]/div[1]/div[1]/span[1]/span[1]/div/span/span'
            ).text
            if current_station != soc_code:
                driver.find_element(
                    By.XPATH, '//*[@id="fms-container"]/div[1]/div[1]/span[1]/span[1]/div/span/span'
                ).click()
                ti.sleep(3)
                list_soc = driver.find_element(By.XPATH, "/html/body/span/div/div[2]/div/ul").text
                list_items = list_soc.split("\n")
                number_list = list_items.index(soc_code)
                driver.find_element(By.XPATH, f"/html/body/span/div/div[2]/div/ul/li[{number_list + 1}]").click()

            cookies = driver.get_cookies()
            cookie_list = [f"{cookie['name']}={cookie['value']}" for cookie in cookies]
            cookies_final = "; ".join(sorted(cookie_list))
            break
        except Exception:
            continue

    driver.quit()
    if not cookies_final:
        raise RuntimeError("Khong lay duoc cookie FMS. Hay kiem tra Chrome da login SPX va station dung.")

    return {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/json;charset=UTF-8",
        "cookie": cookies_final,
        "origin": "https://spx.shopee.vn",
        "referer": "https://spx.shopee.vn/",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    }


def open_target_sheet():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(str(SERVICE_ACCOUNT_FILE), scopes=scopes)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(TARGET_SHEET_ID)
    if TARGET_WORKSHEET_NAME:
        try:
            return spreadsheet.worksheet(TARGET_WORKSHEET_NAME)
        except gspread.WorksheetNotFound:
            # Renamed rather than reordered. Falling back to position keeps the
            # bot running, but say so loudly - silently writing LT rows into
            # whatever tab happens to be first is the failure this guards.
            fallback = spreadsheet.get_worksheet(0)
            print(
                f"[arrived_LT] CANH BAO: khong thay tab {TARGET_WORKSHEET_NAME!r}. "
                f"Dang dung tab dau tien {fallback.title!r} thay the - "
                "kiem tra lai ten tab truoc khi tin ket qua."
            )
            return fallback
    return spreadsheet.get_worksheet(0)


def sheet_context(worksheet):
    rows = worksheet.get_all_values()
    headers = rows[0] if rows else TARGET_HEADERS
    if not rows:
        worksheet.update("A1", [TARGET_HEADERS])
        headers = TARGET_HEADERS
    while headers and not str(headers[-1]).strip():
        headers.pop()

    missing_headers = [header for header in TARGET_HEADERS if header not in headers]
    if missing_headers:
        headers = headers + missing_headers
        worksheet.update("A1", [headers])
        print(f"Added Sheet LT headers: {', '.join(missing_headers)}")

    next_id = 1
    existing_trip_sequence_keys = set()
    for row in rows[1:]:
        row_map = {headers[i]: row[i] if i < len(row) else "" for i in range(len(headers))}
        try:
            next_id = max(next_id, int(row_map.get("id") or 0) + 1)
        except ValueError:
            pass
        trip_number = row_map.get("trip_number", "").strip()
        if trip_number:
            sequence_number = str(row_map.get("sequence_number") or "").strip()
            existing_trip_sequence_keys.add(f"{trip_number}|{sequence_number}")
    return headers, next_id, existing_trip_sequence_keys


def append_rows(worksheet, headers, rows):
    if not rows:
        return 0
    values = [[row.get(header, "") for header in headers] for row in rows]
    for attempt in range(1, SHEET_APPEND_MAX_RETRIES + 1):
        try:
            worksheet.append_rows(values, value_input_option="USER_ENTERED")
            break
        except Exception as e:
            if attempt >= SHEET_APPEND_MAX_RETRIES:
                raise
            text = str(e).lower()
            wait_seconds = 20 * attempt if "429" in text or "quota" in text else 5 * attempt
            print(f"Google Sheet append loi lan {attempt}/{SHEET_APPEND_MAX_RETRIES}: {e}. Cho {wait_seconds}s roi retry...")
            ti.sleep(wait_seconds)
    return len(values)


def flush_sheet_buffer(worksheet, headers, row_buffer, trip_count):
    if not row_buffer:
        return 0
    pushed = append_rows(worksheet, headers, row_buffer)
    print(f"Pushed batch {pushed} rows from {trip_count} LT")
    row_buffer.clear()
    return pushed


def fetch_ended_trips(headers, window_from, window_to):
    start_time = to_epoch_seconds(window_from)
    end_time = to_epoch_seconds(window_to)
    mtime_start = to_epoch_seconds(window_from - timedelta(hours=ARRIVED_WINDOW_OVERLAP_HOURS))
    update_time = to_epoch_seconds(datetime.now().replace(hour=23, minute=59, second=59, microsecond=0))
    payload = {}
    route_templates = [
        (
            "Ended LHTrip - first_station=2490",
            "https://spx.shopee.vn/api/admin/transportation/trip/history/list"
            f"?pageno={{page}}&trip_station_status=90&count={{count}}&arrived_time={start_time},{end_time}"
            f"&first_station=2490&mtime={mtime_start},{update_time}",
            1,
        ),
        (
            "Ended LHTrip - first_station=4110, middle_station=2490",
            "https://spx.shopee.vn/api/admin/transportation/trip/history/list"
            f"?pageno={{page}}&trip_station_status=90&count={{count}}&arrived_time={start_time},{end_time}"
            f"&first_station=4110&middle_station=2490&mtime={mtime_start},{update_time}",
            2,
        ),
    ]

    all_rows = []
    for label, url_template, loaded_sequence_number in route_templates:
        route_retry = 1 if loaded_sequence_number == 2 else REQUEST_MAX_RETRIES
        trips, total = fetch_fms_list(
            url_template,
            headers,
            payload,
            label=label,
            fail_soft=True,
            max_retries=route_retry,
        )
        if loaded_sequence_number == 2 and not trips and not total:
            print(f"{label}: route phu FMS dang loi/khong co data, bo qua nhanh de BOT chay tiep.")
        for trip in trips:
            trip["_loaded_sequence_number"] = loaded_sequence_number
        print(f"{label}: {total} trips")
        all_rows.extend(trips)
    return all_rows


def process_trip(headers, trip, next_id, existing_trip_sequence_keys, window_from, window_to, max_rows=None):
    trip_id = trip.get("id")
    trip_number = trip.get("trip_number")
    if not trip_id or not trip_number:
        return [], next_id

    detail_data = request_json_with_retry(
        "GET",
        f"https://spx.shopee.vn/api/admin/transportation/trip/history/detail?trip_id={trip_id}&new_process_switch=false",
        headers=headers,
        payload={},
        fail_soft=True,
        label=f"History trip detail - {trip_number}",
    )
    time_points = build_trip_arrival_points(detail_data, trip)
    if not time_points:
        print(f"Skip {trip_number}: khong co ATA/ATD tai diem sau BDA")
        return [], next_id

    # A trip can have an early receiving station and a later station in the
    # current list window. Keep the already-arrived earlier stops too. They
    # may have Pending Inbound that must be reconciled even when BOT started
    # after that intermediate arrival time.
    has_point_in_window = any(
        window_from <= point.get("arrived_time") <= window_to
        for point in time_points
        if point.get("arrived_time")
    )

    rows = []
    for point in time_points:
        sequence_number = point.get("sequence_number")
        arrived_time = point.get("arrived_time")
        sequence_key = f"{trip_number}|{sequence_number}"
        if sequence_key in existing_trip_sequence_keys:
            print(f"Skip duplicate LT {trip_number} sequence {sequence_number}")
            continue
        if arrived_time > window_to:
            print(
                f"Skip {trip_number} sequence {sequence_number}: "
                f"arrived_time {format_datetime(arrived_time)} ngoai window"
            )
            continue
        if arrived_time < window_from and not has_point_in_window:
            print(
                f"Skip {trip_number} sequence {sequence_number}: "
                f"arrived_time {format_datetime(arrived_time)} ngoai window"
            )
            continue
        if arrived_time < window_from:
            print(
                f"Include prior arrived LT {trip_number} sequence {sequence_number}: "
                f"arrived_time {format_datetime(arrived_time)} (same trip has a point in window)"
            )
        if max_rows is not None and len(rows) >= max_rows:
            break

        rows.append({
            # Internal metadata for BOT 1/2. append_rows only sends the sheet
            # headers, therefore this never creates an extra Sheet column.
            "_trip_id": str(trip_id),
            "id": next_id,
            "trip_number": trip_number,
            "sequence_number": sequence_number,
            "station": point.get("station") or "",
            "to_station": point.get("to_station") or "",
            "departed_time": format_datetime(point.get("departed_time")),
            "sta": format_datetime(point.get("sta")),
            "arrived_time": format_datetime(arrived_time),
            "unseal_time": format_datetime(point.get("unseal_time")),
            "unseal_operator": point.get("unseal_operator") or "",
            "unloaded_time": format_datetime(point.get("unloaded_time")),
            "unloaded_operator": point.get("unloaded_operator") or "",
            "total_to": "",
            "total_order": "",
            "to_packed": "",
            "order_bulky": "",
            "order_packed": "",
        })
        next_id += 1
        existing_trip_sequence_keys.add(sequence_key)
    return rows, next_id


def run_once(limit=None, window_from=None, window_to=None):
    run_started_at = datetime.now()
    state = load_state()
    if window_from and window_to:
        window_from = parse_state_datetime(window_from)
        window_to = parse_state_datetime(window_to)
        if not window_from or not window_to:
            raise ValueError("lt-from/lt-to khong dung format. Vi du: 2026-08-10 00:00:00")
    else:
        window_from, window_to = compute_arrived_window(state, local_now())

    print(f"Arrived window: {format_datetime(window_from)} -> {format_datetime(window_to)}")
    worksheet = open_target_sheet()
    sheet_headers, next_id, existing_trip_sequence_keys = sheet_context(worksheet)
    headers = get_fms_headers(SOC_CODE)
    trips = fetch_ended_trips(headers, window_from, window_to)

    total_pushed = 0
    captured_rows = []
    row_buffer = []
    trip_buffer_count = 0
    for trip in trips:
        if limit and total_pushed + len(row_buffer) >= int(limit):
            print(f"Reached LT limit {limit}, stop processing more LT in this run.")
            break
        try:
            remaining_rows = None
            if limit:
                remaining_rows = max(0, int(limit) - total_pushed - len(row_buffer))
                if remaining_rows == 0:
                    break
            rows, next_id = process_trip(
                headers,
                trip,
                next_id,
                existing_trip_sequence_keys,
                window_from,
                window_to,
                max_rows=remaining_rows,
            )
            if rows:
                captured_rows.extend(rows)
                row_buffer.extend(rows)
                trip_buffer_count += 1
                print(f"Buffered LT {trip.get('trip_number')} | batch LT: {trip_buffer_count}/{SHEET_APPEND_TRIP_BATCH_SIZE}")
            if trip_buffer_count >= SHEET_APPEND_TRIP_BATCH_SIZE:
                total_pushed += flush_sheet_buffer(worksheet, sheet_headers, row_buffer, trip_buffer_count)
                trip_buffer_count = 0
        except Exception:
            print(f"Error when processing LT {trip.get('trip_number')}")
            traceback.print_exc()

    total_pushed += flush_sheet_buffer(worksheet, sheet_headers, row_buffer, trip_buffer_count)

    state.update(
        {
            "last_run_started_at": run_started_at.isoformat(timespec="seconds"),
            "last_run_finished_at": datetime.now().isoformat(timespec="seconds"),
            "last_arrived_time_from": window_from.isoformat(timespec="seconds"),
            "last_arrived_time_to": window_to.isoformat(timespec="seconds"),
            "last_total_pushed": total_pushed,
        }
    )
    save_state(state)
    print(f"Done. Pushed {total_pushed} rows.")
    return captured_rows


def main():
    limit = None
    window_from = None
    window_to = None
    if "--limit" in sys.argv:
        try:
            limit = int(sys.argv[sys.argv.index("--limit") + 1])
        except Exception:
            limit = None
    if "--lt-from" in sys.argv:
        try:
            window_from = sys.argv[sys.argv.index("--lt-from") + 1]
        except Exception:
            window_from = None
    if "--lt-to" in sys.argv:
        try:
            window_to = sys.argv[sys.argv.index("--lt-to") + 1]
        except Exception:
            window_to = None

    if "--once" in sys.argv or limit or (window_from and window_to):
        run_once(limit=limit, window_from=window_from, window_to=window_to)
        return

    while True:
        try:
            run_once()
        except Exception:
            print("Run failed:")
            traceback.print_exc()
        print(f"Sleep {RUN_INTERVAL_SECONDS} seconds before next run.")
        ti.sleep(RUN_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
