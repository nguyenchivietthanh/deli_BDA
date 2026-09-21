"""Reset BOT DELI SQLite data and BOT-owned Google Sheet outputs.

Use only after every BOT process and DB Browser have been closed.  The script
keeps SQLite table schemas, the Google Sheet `Valid` tab, and pending_work's
header row, so the next BOT run can start cleanly without reconfiguration.
"""

import argparse
import sqlite3
from pathlib import Path

import gspread
from google.oauth2.service_account import Credentials

import sqlite_store


BASE_DIR = Path(__file__).resolve().parent
SERVICE_ACCOUNT_FILE = BASE_DIR / "ops-support.json"
TARGET_SHEET_ID = "1BqmVWiHdCoE0uFM_54EXu0xah4aDivgEPErc_V14enU"
# Must match TARGET_WORKSHEET_NAME in BOT-Deli_BDA_arrived_LT.py. Duplicated
# rather than imported so this script stays standalone (importing that module
# pulls in browser_fetch and a Chrome session). If they ever drift, this script
# aborts without deleting anything, which is the safe direction.
LT_SHEET_TAB_NAME = "Trang tính1"
STATE_FILES = (
    "bot_deli_bda_arrived_state.json",
    "bot_deli_bda_export_lt_unit_bq_state.json",
)
BOT_OUTPUT_TABS = (
    "pending_all",
    "bulky_pending",
    "to_sorting_pending",
    "pending_status_changed",
)


def clear_sqlite():
    database_file = Path(sqlite_store.DATABASE_FILE)
    connection = sqlite3.connect(database_file, timeout=120)
    try:
        tables = [
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
            )
        ]
        connection.execute("BEGIN IMMEDIATE")
        for table in tables:
            quoted = '"' + table.replace('"', '""') + '"'
            connection.execute(f"DELETE FROM {quoted}")
        connection.commit()
        connection.execute("VACUUM")
        print(f"SQLite reset: cleared {len(tables)} table(s) in {database_file.name}")
    finally:
        connection.close()


def open_spreadsheet():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_file(str(SERVICE_ACCOUNT_FILE), scopes=scopes)
    return gspread.authorize(credentials).open_by_key(TARGET_SHEET_ID)


def clear_sheet_outputs():
    spreadsheet = open_spreadsheet()

    # Resolve the LT tab by name, never by position. This used to be
    # get_worksheet(0), which clears whatever tab the team last dragged to the
    # front - with pending_work there that is a full wipe of the live working
    # sheet, manual columns included. Abort instead of guessing: this function
    # only deletes, so failing loudly is always the safer outcome.
    lt_tab_name = LT_SHEET_TAB_NAME
    try:
        lt_sheet = spreadsheet.worksheet(lt_tab_name)
    except gspread.WorksheetNotFound:
        raise SystemExit(
            f"Reset da DUNG: khong tim thay tab LT ten {lt_tab_name!r}. "
            "Kiem tra ten tab (hoac sua TARGET_WORKSHEET_NAME trong "
            "BOT-Deli_BDA_arrived_LT.py) roi chay lai. Khong xoa gi ca."
        )
    lt_sheet.clear()
    print(f"Google Sheet reset: cleared LT tab '{lt_sheet.title}'")

    for title in BOT_OUTPUT_TABS:
        try:
            worksheet = spreadsheet.worksheet(title)
        except gspread.WorksheetNotFound:
            continue
        worksheet.clear()
        print(f"Google Sheet reset: cleared tab '{title}'")

    # Keep the pending_work headers because the user may add operational columns.
    try:
        work_sheet = spreadsheet.worksheet("pending_work")
        work_sheet.batch_clear(["A2:ZZ"])
        print("Google Sheet reset: cleared pending_work data and kept its header row")
    except gspread.WorksheetNotFound:
        pass


def clear_states():
    for filename in STATE_FILES:
        path = BASE_DIR / filename
        path.unlink(missing_ok=True)
        print(f"State reset: removed {filename}")


def main():
    parser = argparse.ArgumentParser(description="Reset all BOT DELI SQLite and Sheet output data")
    parser.add_argument(
        "--yes-reset-all",
        action="store_true",
        help="Required confirmation because this clears all BOT data and output tabs.",
    )
    parser.add_argument(
        "--keep-sheets",
        action="store_true",
        help="Only reset SQLite/state files; do not clear Google Sheet output tabs.",
    )
    args = parser.parse_args()
    if not args.yes_reset_all:
        parser.error("Add --yes-reset-all to confirm the destructive reset.")

    clear_sqlite()
    clear_states()
    if not args.keep_sheets:
        clear_sheet_outputs()
    print("Reset complete. Valid is preserved. Start the first cycle with an explicit --lt-from time.")


if __name__ == "__main__":
    main()
