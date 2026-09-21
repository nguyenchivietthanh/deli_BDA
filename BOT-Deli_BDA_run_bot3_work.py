"""BOT 3 - independent final-check and operational work queue.

BOT 3 reads BOT 2's final PENDING results, enriches the work queue with the
missing-quantity and reconciliation-hub context, then optionally assigns work.
BOT 3 drains due candidates through BatchSearch/Tracking Info, publishes the
final pending snapshot, enriches pending_work and optionally assigns work.
"""

import argparse
import copy
import importlib.util
import msvcrt
import os
import re
import sqlite3
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo

import gspread
from gspread.exceptions import APIError

# BOT 3 owns a separate Admin Chrome profile. This must be configured before
# browser_fetch or any pending-pipeline module is imported.
BOT3_ADMIN_PROFILE = Path(
    os.getenv(
        "BOT_DELI_CHROME_USER_DATA_DIR_BOT3",
        str(Path(os.getenv("LOCALAPPDATA", "")) / "BOT_DELI_FMS_BROWSER_BOT3_ADMIN" / "User Data"),
    )
)
os.environ["BOT_DELI_CHROME_USER_DATA_DIR_ADMIN"] = str(BOT3_ADMIN_PROFILE)
os.environ["BOT_DELI_CHROME_PROFILE_ADMIN"] = os.getenv(
    "BOT_DELI_CHROME_PROFILE_BOT3",
    "Default",
)

import browser_fetch
import sqlite_store


# Windows terminals may default to cp1252 even though staff names and status
# labels are Vietnamese. Keep logging from aborting after Sheet writes.
for stream in (sys.stdout, sys.stderr):
    if hasattr(stream, "reconfigure"):
        stream.reconfigure(encoding="utf-8", errors="replace")


BASE_DIR = Path(__file__).resolve().parent
WORKSHEET_TITLE = "pending_work"
VALID_WORKSHEET_TITLE = "Valid"
VALID_STAFF_COLUMN = "list_chia_don"
COGS_TABLE_ID = "bot3_cogs_cache"
RECONCILE_CACHE_TABLE_ID = "bot3_reconcile_arrival_cache"
STATUS_CHANGED_TABLE_ID = "lt_pending_status_changed"
# So ngay mot dong duoc phep NAM TREN SHEET truoc khi bi xoa (ops chot 2026-09-14).
# Dem tu luc dong len sheet, khong phai tu luc xe ve - xem WORK_RETENTION_EXTRA_HOURS.
WORK_RETENTION_DAYS = 7
WORK_DATA_START_ROW = 3
ADMIN_ROLE = "Admin"
COGS_URL = "https://spx.shopee.vn/api/fleet_order/order/detail/show_sensitive_data"
TRACKING_INFO_URL = "https://spx.shopee.vn/api/fleet_order/order/detail/tracking_info"
BDA_STATION_NAME = "BD A Mega SOC"
TRACKING_ARRIVED_STATUS_CODES = {879, 880, 881, 882, 883, 884, 885, 886, 887, 888}
TRACKING_CACHE_RETRY_MINUTES = 60
SHEETS_WRITE_RETRY_SECONDS = (65, 75, 90, 120, 180)
BOT3_LOCK_FILE = BASE_DIR / ".bot3_work.lock"

# Bot-owned and team-owned columns may be interleaved in the operational layout.
# Every write below resolves columns by header name, so team data stays intact.
BOT_COLUMNS = [
    ("shipment_id", "Shipment ID"),
    ("lt_id", "LT ID"),
    ("to_id", "TO ID"),
    ("type", "Type"),
    ("status", "Status"),
    ("current_station", "Current station"),
    ("cogs", "COGS"),
    ("bot_status", "BOT Status"),
    ("missing_to_ratio", "Số đơn thiếu / đơn trong TO"),
    ("reconcile_hub", "Hub/SOC cần đối soát"),
    ("deadline_bao_thieu", "deadline_bao_thieu"),
    ("to_path", "TO Path"),
    ("sender", "Sender"),
    ("receiver", "Receiver"),
    ("arrived_time", "Arrived time"),
    ("destination", "Destination"),
]
MANUAL_COLUMNS = [
    ("deadline_cctv", "deadline_cctv"),
    ("deadline_nhan_xet", "deadline_nhan_xet"),
    ("pic_cctv", "PIC_cctv"),
    ("status_bao_thieu", "status_bao_thieu"),
    ("status_cctv_deli", "status_cctv_deli"),
    ("link_cctv", "Link_cctv"),
    ("status_nhan_xet", "status_nhan_xet"),
    ("date_cctv", "date_cctv"),
    ("date_nhan_xet", "date_nhan_xet"),
    ("date_chia_don", "date_chia_don"),
]
WORK_HEADERS = [
    "Shipment ID",
    "LT ID",
    "TO ID",
    "Type",
    "Status",
    "Current station",
    "COGS",
    "BOT Status",
    "Số đơn thiếu / đơn trong TO",
    "Hub/SOC cần đối soát",
    "deadline_bao_thieu",
    "deadline_cctv",
    "deadline_nhan_xet",
    "PIC_cctv",
    "status_bao_thieu",
    "status_cctv_deli",
    "Link_cctv",
    "status_nhan_xet",
    "date_cctv",
    "date_nhan_xet",
    "date_chia_don",
    "TO Path",
    "Sender",
    "Receiver",
    "Arrived time",
    "Destination",
]
BOT_HEADER_COUNT = len(BOT_COLUMNS)
CCTV_PROVIDED_STATUS = "01. Đã cung cấp cam"
CCTV_NOT_PROVIDED_STATUS = "02. Không cung cấp cam"
# Added to the sheet dropdown by the team on 2026-09-07. Unlike 01/02 these are
# investigation outcomes the bot has no way to derive, so once a team member
# picks one the bot must never overwrite or clear the cell - see
# is_team_owned_cctv_status(). The dropdown writes them with no space after the
# number; the spaced spelling is accepted too so that editing the validation
# list later does not silently unprotect these rows.
CCTV_OFF_ROUTE_STATUS = "03.Lạc tuyến"
CCTV_SEARCHING_STATUS = "04.Tìm hàng"
LEGACY_ASSIGNED_STATUS = "04. Chia đơn"
RECONCILED_BOT_STATUS = "RECONCILED"

# So vong lien tiep mot don phai vang mat khoi hang pending truoc khi BOT 3 dam
# dong so dong do tren sheet. 1 = dong ngay nhu truoc; 2 = phai xac nhan mot lan.
CLOSE_CONFIRM_CYCLES = 2
# shipment_id -> so vong lien tiep da vang mat. Giu trong bo nho tien trinh, khong
# ghi xuong dia: restart chi lam cham viec dong so dung mot vong, khong sai ket
# qua, va doi lai khong phai them bang moi vao co so du lieu.
_vang_khoi_hang_pending = {}
# Cot nam trong khoi BOT nhung BOT 3 KHONG duoc ghi de, vi mot cong cu khac so
# huu chung.
#
# 2026-09-15: ops chuyen COGS sang extension "OneBI Fetch Bridge" (Data Suite).
# Extension dien thang vao cot COGS cua pending_work; neu BOT 3 van dong bo cot
# nay thi moi vong no lay gia tri rong trong bot3_cogs_cache ghi de len con so
# vua dien - dung hien tuong ops bao.
#
# 2026-09-11 da thu dat {"COGS"} mot tieng roi bo, vi luc do duong Data Suite
# chua chay. Nguyen nhan tim ra ngay 2026-09-15: payload cu nhet danh sach don
# vao o loc "Shipment ID" dang MULTI_SELECT/UPLOAD - do la widget phia client,
# khong phai bo loc phia may chu, nen may chu dung truy van tren MOCK_TABLE.
# Extension chi dung o EQUAL/INPUT, moi lan MOT ma don.
#
# PHAI doi CUNG LUC voi --skip-cogs/--cogs-limit trong RUN_BOT3_FINAL_WORK.ps1.
# Bat cot o day ma van de BOT 3 goi FMS thi ton request cho mot gia tri khong
# bao gio len duoc sheet; tat FMS ma khong bat cot o day thi COGS bi xoa moi vong.
TOOL_OWNED_HEADERS = {"COGS"}
AUTO_NO_CCTV_TYPES = {"sorting", "to_sorting", "transit", "to_transit"}


def load_module(path, module_name):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


PENDING_RUNNER = load_module(
    BASE_DIR / "BOT-Deli_BDA_run_pending_pipeline.py",
    "bot3_pending_runner",
)
UNIFIED_PENDING = PENDING_RUNNER.UNIFIED_PENDING_BOT


def migrate_legacy_candidate_queue():
    """Classify candidate rows created before the staged queue was added."""
    service = UNIFIED_PENDING.create_bq_service()
    UNIFIED_PENDING.ensure_tables(service)
    store = getattr(service, "store", None)
    if store is None:
        print("BOT 3 queue migration skipped: non-SQLite backend")
        return 0

    with sqlite3.connect(store.path, timeout=5) as conn:
        conn.execute("PRAGMA busy_timeout = 5000")
        blank = conn.execute(
            """
            SELECT COUNT(*)
            FROM lt_pending_candidate
            WHERE queue_stage IS NULL OR TRIM(queue_stage) = ''
            """
        ).fetchone()[0]
        if not blank:
            return 0

        print(f"BOT 3 queue migration: classify {blank} legacy candidate row(s)...")
        conn.execute("DROP TABLE IF EXISTS temp.bot3_latest_result")
        conn.execute(
            """
            CREATE TEMP TABLE bot3_latest_result AS
            SELECT candidate_id, pending_status, processing_stage, checked_at
            FROM (
                SELECT
                    candidate_id,
                    pending_status,
                    processing_stage,
                    checked_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY candidate_id
                        ORDER BY checked_at DESC
                    ) AS rn
                FROM lt_pending_result
            )
            WHERE rn = 1
            """
        )
        conn.execute(
            "CREATE INDEX temp.idx_bot3_latest_result_candidate "
            "ON bot3_latest_result(candidate_id)"
        )
        conn.execute(
            """
            UPDATE lt_pending_candidate AS c
            SET queue_stage = COALESCE(
                    (
                        SELECT CASE
                            WHEN r.processing_stage = 'PRECHECK'
                                 AND r.pending_status = 'PENDING' THEN 'PRECHECK_PENDING'
                            WHEN r.processing_stage = 'PRECHECK'
                                 AND r.pending_status = 'UNKNOWN' THEN 'PRECHECK_UNKNOWN'
                            WHEN r.pending_status = 'PENDING' THEN 'FINAL_PENDING'
                            WHEN r.pending_status = 'UNKNOWN' THEN 'FINAL_UNKNOWN'
                            ELSE 'FINALIZED'
                        END
                        FROM bot3_latest_result r
                        WHERE r.candidate_id = c.candidate_id
                    ),
                    'NEW'
                ),
                precheck_status = COALESCE(
                    precheck_status,
                    (
                        SELECT r.pending_status
                        FROM bot3_latest_result r
                        WHERE r.candidate_id = c.candidate_id
                          AND r.processing_stage = 'PRECHECK'
                    )
                ),
                prechecked_at = COALESCE(
                    prechecked_at,
                    (
                        SELECT r.checked_at
                        FROM bot3_latest_result r
                        WHERE r.candidate_id = c.candidate_id
                          AND r.processing_stage = 'PRECHECK'
                    )
                )
            WHERE queue_stage IS NULL OR TRIM(queue_stage) = ''
            """
        )
        conn.execute("DROP TABLE temp.bot3_latest_result")
    print(f"BOT 3 queue migration done: {blank} row(s) classified")
    return blank


def process_due_pending_slice(batch_limit=5000):
    """Process one bounded due slice so Sheets is refreshed every cycle."""
    service = UNIFIED_PENDING.create_bq_service()
    UNIFIED_PENDING.ensure_tables(service)
    if not UNIFIED_PENDING.latest_candidates_to_batch(service, limit=1):
        print("BOT 3 final-check queue: no due candidate")
        return 0
    print(f"[BOT 3 final check] bounded slice; limit={batch_limit or 'default'}")
    result = PENDING_RUNNER.run_cycle(
        skip_bulky=True,
        skip_to=True,
        batch_limit=batch_limit,
        skip_sheet=True,
        include_lt_unit_fallback=False,
        include_bda_prefilter=False,
        clear_bda_staging=False,
        return_result=True,
    )
    processed = int(result["unified_result"].get("batchsearch_inserted") or 0)
    print(f"BOT 3 final-check slice: processed={processed}")
    return processed


def publish_pending_outputs(fetch_cogs=True, cogs_limit=250):
    service = UNIFIED_PENDING.create_bq_service()
    with _phase("  publish: ghi nhat ky doi trang thai"):
        UNIFIED_PENDING.capture_published_pending_status_changes(service)
    with _phase("  publish: xuat cac tab Sheet"):
        PENDING_RUNNER.EXPORT_PENDING_SHEET_BOT.run_once(
            fetch_missing_cogs=fetch_cogs,
            cogs_limit=cogs_limit,
        )
    with _phase("  publish: don bang tam bulky"):
        UNIFIED_PENDING.clear_completed_candidate_staging(
            UNIFIED_PENDING.create_bq_service(),
            clear_to_staging=False,
            clear_bulky_staging=True,
        )
    # Hai buoc duoi chay SQL tren lt_pending_candidate (1,77 trieu dong) moi vong.
    with _phase("  publish: prune candidate"):
        UNIFIED_PENDING.prune_finalized_pending_candidates(
            UNIFIED_PENDING.create_bq_service()
        )
    with _phase("  publish: compact result"):
        UNIFIED_PENDING.compact_pending_result_history(
            UNIFIED_PENDING.create_bq_service()
        )


def normalize_text(value):
    return " ".join(str(value or "").strip().casefold().split())


def sheets_batch_update_with_retry(worksheet, updates):
    """Retry transient Sheets quota/service errors after the minute resets."""
    for attempt in range(len(SHEETS_WRITE_RETRY_SECONDS) + 1):
        try:
            # gspread prefixes the worksheet title directly onto each range.
            # A failed request therefore mutates its input; retrying that same
            # object would produce ranges such as Sheet!Sheet!A3.
            retry_updates = copy.deepcopy(updates)
            return worksheet.batch_update(
                retry_updates,
                value_input_option="USER_ENTERED",
            )
        except APIError as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            if status_code not in {429, 500, 502, 503, 504} or attempt >= len(SHEETS_WRITE_RETRY_SECONDS):
                raise
            delay = SHEETS_WRITE_RETRY_SECONDS[attempt]
            print(
                f"Google Sheets HTTP {status_code}; wait {delay}s then retry "
                f"({attempt + 1}/{len(SHEETS_WRITE_RETRY_SECONDS)})..."
            )
            time.sleep(delay)


# Doc ca tab la mot HTTP GET lon: pending_work da hon 32.000 dong x ~30 cot. Google
# tra 503 cho chinh cai GET do nhieu lan trong ngay 2026-09-14, va vi khong co lop
# thu lai nao cho phan DOC, ca vong bi huy NGAY SAU khi da ton ~1.700s BatchSearch.
# Nghi ngan thoi: 503 o day la nghen nhat thoi, khong phai het quota phut nhu khi GHI.
SHEETS_READ_RETRY_SECONDS = (5, 15, 40, 90)


def _thu_lai_doc_sheet(ham, nhan):
    """Chay mot lenh DOC sheet, thu lai khi Google tra loi tam thoi.

    Ham phai lap lai duoc - chi dung cho lenh doc, khong dung cho lenh ghi/xoa.
    """
    for attempt in range(len(SHEETS_READ_RETRY_SECONDS) + 1):
        try:
            return ham()
        except APIError as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            if (
                status_code not in {429, 500, 502, 503, 504}
                or attempt >= len(SHEETS_READ_RETRY_SECONDS)
            ):
                raise
            delay = SHEETS_READ_RETRY_SECONDS[attempt]
            print(
                f"Doc {nhan} loi HTTP {status_code}; cho {delay}s roi thu lai "
                f"({attempt + 1}/{len(SHEETS_READ_RETRY_SECONDS)})..."
            )
            time.sleep(delay)


def sheets_get_all_values_with_retry(worksheet, nhan="pending_work"):
    """worksheet.get_all_values() co thu lai khi Google tra loi tam thoi."""
    for attempt in range(len(SHEETS_READ_RETRY_SECONDS) + 1):
        try:
            return worksheet.get_all_values()
        except APIError as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            if (
                status_code not in {429, 500, 502, 503, 504}
                or attempt >= len(SHEETS_READ_RETRY_SECONDS)
            ):
                raise
            delay = SHEETS_READ_RETRY_SECONDS[attempt]
            print(
                f"Doc {nhan} loi HTTP {status_code}; cho {delay}s roi thu lai "
                f"({attempt + 1}/{len(SHEETS_READ_RETRY_SECONDS)})..."
            )
            time.sleep(delay)


def is_cctv_provided(value):
    return normalize_text(value) == normalize_text(CCTV_PROVIDED_STATUS)


def is_cctv_not_provided(value):
    return normalize_text(value) == normalize_text(CCTV_NOT_PROVIDED_STATUS)


def is_legacy_assigned_status(value):
    return normalize_text(value) == normalize_text(LEGACY_ASSIGNED_STATUS)


def _cctv_status_spellings(*values):
    """Accept each status both as written in the dropdown and with a space
    after the leading number, since only whitespace and case are normalised."""
    spellings = set()
    for value in values:
        spellings.add(normalize_text(value))
        spellings.add(normalize_text(value.replace(".", ". ", 1)))
    return spellings


TEAM_OWNED_CCTV_STATUSES = _cctv_status_spellings(
    CCTV_OFF_ROUTE_STATUS,
    CCTV_SEARCHING_STATUS,
)


def is_team_owned_cctv_status(value):
    """True for a status only a person can decide, which the bot must leave
    alone. 01/02 are deliberately NOT in here: the bot still derives those for
    Sorting/Transit rows exactly as before."""
    return normalize_text(value) in TEAM_OWNED_CCTV_STATUSES


def is_team_handled_cctv_status(value):
    """True when a person has actually worked the order, whatever the outcome.

    Used only to pick RECONCILED over RESOLVED once an order leaves the active
    pending queue (2026-09-08, user's rule): 01 means the CCTV was cut, 03 and
    04 mean the team investigated and recorded what they found. All three are a
    human decision, so the row closes as RECONCILED.

    Deliberately excluded:
      - 02 and blank - nobody has touched the order yet, so it closes RESOLVED.
      - LEGACY_ASSIGNED_STATUS ("04. Chia đơn") - the bot wrote that as an
        assignment marker, not a person, and the bot is clearing it. Counting it
        would mark rows RECONCILED that nobody ever looked at.
    """
    return is_cctv_provided(value) or is_team_owned_cctv_status(value)


def has_team_review(value):
    """True when a person has picked any status_nhan_xet option.

    Ops rule 2026-09-11, added alongside is_team_handled_cctv_status(): a review
    note is a second, independent sign that a person worked the order, so an
    order carrying one closes RECONCILED even when status_cctv_deli is blank or
    02. Every option in the dropdown - 01 Cam hợp lệ through 05 Không nhận xét
    cam - is the person's own conclusion, so only the presence of a value
    matters, not which one.

    No value list is checked on purpose. The real cell values do not match the
    Valid tab: measured 2026-09-11, the sheet holds "2.Cam không hợp lệ" and
    "3. Hub/SOC khác không cung cấp cam" while Valid lists "02. Cam không hợp
    lệ" and "03. SOC_ASM_CBS". Any list here would silently miss most rows, the
    same trap the onEdit script works around by matching the leading digit.
    """
    return bool(normalize_text(value))


def is_auto_no_cctv_type(value):
    return normalize_text(value) in AUTO_NO_CCTV_TYPES


# Reconciliation hubs whose no-cam orders open their assignment window a day
# earlier (36h..60h instead of 60h..84h). Ops rule, 2026-09-09: at these SOCs
# the shortage settles fast, so holding the order until 60h burns most of the
# time the team could have used. Matched against the `Hub/SOC cần đối soát`
# column - the first station BDA sent the TO to, not the current station.
#
# Measured when this shipped: 292 of 7,042 sheet rows (4.1%) sit at one of
# these hubs, 223 of them partial-missing, i.e. the only rows this window
# applies to at all. `BD A Mega SOC` is listed on ops's instruction but can
# never match: reconcile_hub is by construction the hub *after* BDA.
EARLY_WINDOW_RECONCILE_HUBS = frozenset(
    normalize_text(name) for name in (
        "BN A Mega SOC",
        "BN B Mega SOC",
        "Hung Yen SOC",
        "DN Mega SOC",
        "SW SOC",
        "BD B Mega SOC",
        "BD A Mega SOC",
        "HCM Mega SOC",
    )
)
EARLY_ASSIGNMENT_WINDOW_HOURS = (36, 60)
DEFAULT_ASSIGNMENT_WINDOW_HOURS = (60, 84)

# The team works 09:00-18:00 but the bot used to assign around the clock, so
# most orders landed on an empty office and cleared themselves before anyone
# read them. Measured 2026-09-09: 74% of all assignments (1,470/1,987) happened
# outside 09:00-18:00, and of the assigned-but-never-touched orders that could
# be traced, 56.5% had left the queue before the next 09:00. Sheet-wide, 61.7%
# of assigned orders were never worked by anyone.
#
# Ops chose 08:00-15:00 so the last three hours of the shift carry no new work.
# The gate cannot make an order miss its assignment window: both windows are
# exactly 24h wide, and a 24h window always contains at least one 08:00.
# Simulated over 6,616 live no-cam rows: 0 missed, delay 9.1h average / 17.0h
# worst case, and at least 7.0h of window still left at the moment of
# assignment. 92% of assignments land in the 08:00 pass; 09:00-14:00 is a
# trickle, not a second wave.
ASSIGN_SHIFT_OPEN_HOUR = 7

# Lich chia don theo dot. Hai cua so gom don, phu kin 24 gio khong ho:
#
#   Cua so A: published_at trong [12:00 hom qua, 08:00 hom nay)
#             -> chia het ke tu dot 08:00.
#   Cua so B: published_at trong [08:00, 12:00) hom nay
#             -> gom lai, tha lam BA dot deu nhau luc 13:00, 14:00, 15:00.
#
# Moc phan loai la published_at, tuc luc BOT chot don la thieu va dua len sheet,
# KHONG phai arrived_time (ops chon 2026-09-11). Mot don ve luc 02h sang chi
# thanh pending sau 36 gio, neu tinh theo arrived_time thi no se roi nham ngay.
#
# Trong moi dot lay DON CU NHAT TRUOC, de don gan deadline cat cam duoc chia som.
#
# NGAY TANG CA: chi can sua HAI dong duoi day.  Vi du lam den 18h:
#     ASSIGN_WINDOW_B_CLOSE_HOUR = 17
#     ASSIGN_WAVE_HOURS = (13, 14, 15, 16, 17, 18)
# Gio dong ca tu suy ra, khong phai sua.  Nho tra lai (13, 14, 15) va 12 khi het
# ngay tang ca, neu khong thi hom sau don van bi giu den chieu.
ASSIGN_WINDOW_B_OPEN_HOUR = 7
ASSIGN_WINDOW_B_CLOSE_HOUR = 12
ASSIGN_WAVE_HOURS = (13,14,15,16)

# Suy ra tu dot cuoi, KHONG dat tay.  in_assign_shift kiem tra
# "gio < ASSIGN_SHIFT_CLOSE_HOUR", nen dat bang dung gio cua dot cuoi se lam dot
# do khong bao gio chay - toi da dat 15 voi dot cuoi 15h va dot ay bi mat trang.
ASSIGN_SHIFT_CLOSE_HOUR = max(ASSIGN_WAVE_HOURS) + 1


def _kiem_lich_chia():
    """Canh bao cau hinh lich vo ly, in mot lan luc nap module."""
    loi = []
    if list(ASSIGN_WAVE_HOURS) != sorted(set(ASSIGN_WAVE_HOURS)):
        loi.append("ASSIGN_WAVE_HOURS phai tang dan va khong trung nhau")
    if ASSIGN_WAVE_HOURS and min(ASSIGN_WAVE_HOURS) < ASSIGN_WINDOW_B_CLOSE_HOUR:
        loi.append(
            f"dot dau {min(ASSIGN_WAVE_HOURS)}h som hon luc dong cua so B "
            f"{ASSIGN_WINDOW_B_CLOSE_HOUR}h, nen lo con phinh them trong khi dang "
            f"tha - cac dot dau se nho hon cac dot sau"
        )
    if ASSIGN_WINDOW_B_CLOSE_HOUR <= ASSIGN_WINDOW_B_OPEN_HOUR:
        loi.append("cua so B phai dong SAU khi mo")
    for dong in loi:
        print(f"BOT 3 canh bao lich chia: {dong}")


_kiem_lich_chia()
# Safety net on the gate: an order this close to the end of its assignment
# window is assigned whatever the clock says, so a long BOT 3 outage cannot
# silently drop it. It should never fire in normal running - the measured worst
# case still has 7.0h of window left.
ASSIGN_OFF_SHIFT_URGENT_HOURS = 6


def collection_window(phat_sinh_luc, now):
    """'A', 'B', hay None cho mot don, theo luc no tro thanh viec pending.

    `phat_sinh_luc` la eligible_at = arrived_time + 36h. Xem
    load_pending_eligible_times() ve ly do khong dung published_at.

    None nghia la don chua toi luot: no phat sinh sau 12:00 hom nay, nen thuoc
    cua so A cua ngay mai.
    """
    if not phat_sinh_luc:
        return None
    hom_nay = now.replace(hour=0, minute=0, second=0, microsecond=0)
    b_mo = hom_nay.replace(hour=ASSIGN_WINDOW_B_OPEN_HOUR)
    b_dong = hom_nay.replace(hour=ASSIGN_WINDOW_B_CLOSE_HOUR)
    a_mo = b_dong - timedelta(days=1)          # 12:00 hom qua
    if a_mo <= phat_sinh_luc < b_mo:
        return "A"
    if b_mo <= phat_sinh_luc < b_dong:
        return "B"
    # Cu hon 12:00 hom qua: da qua luot cua no, van cho di cung cua so A thay vi
    # bo lai mai mai - mot don bi bo sot vi BOT nghi mot ngay khong duoc phep
    # ket thuc la khong bao gio duoc chia.
    if phat_sinh_luc < a_mo:
        return "A"
    return None


def waves_released(now):
    """So dot 13h/14h/15h da toi tai thoi diem now. 0 nghia la chua tha dot nao."""
    return sum(1 for gio in ASSIGN_WAVE_HOURS if now.hour >= gio)


def wave_quota(tong, now):
    """So don cua cua so B duoc phep da chia tinh den bay gio.

    Chia deu cho ba dot, phan du don len dot som. Vi du 90 don -> 30/60/90;
    100 don -> 34/67/100.
    """
    da_toi = waves_released(now)
    if da_toi <= 0:
        return 0
    if da_toi >= len(ASSIGN_WAVE_HOURS):
        return tong
    return -(-tong * da_toi // len(ASSIGN_WAVE_HOURS))   # lam tron len


def assignment_window_hours(reconcile_hub):
    """Return (open, close) hours after arrived_time for a no-cam order."""
    if normalize_text(reconcile_hub) in EARLY_WINDOW_RECONCILE_HUBS:
        return EARLY_ASSIGNMENT_WINDOW_HOURS
    return DEFAULT_ASSIGNMENT_WINDOW_HOURS


def is_full_missing_ratio(value):
    parts = str(value or "").strip().split("/", 1)
    return (
        len(parts) == 2
        and parts[0].strip().isdigit()
        and parts[1].strip().isdigit()
        and int(parts[1].strip()) > 0
        and int(parts[0].strip()) >= int(parts[1].strip())
    )


def is_full_missing_to(value, work_type):
    return normalize_text(work_type) != "bulky" and is_full_missing_ratio(value)


def assignment_group_key(row):
    """One PIC owns a whole LT, every TO inside it included.

    Was (LT ID, TO ID) until 2026-09-07, which kept each TO whole but split
    47% of LTs across several people - one 9-TO trip was spread over all five.
    Grouping by LT subsumes the TO guarantee, since a TO belongs to exactly one
    LT, and it measured *better* balanced, not worse: replaying the real
    least-loaded rule over the 959 assigned orders gave a max-min spread of 34
    (18%) by LT versus 48 (25%) by (LT, TO). Groups do get much bigger though -
    2.5 orders on average becomes 10.2, and the largest goes from 59 to 139.

    Falls back to TO then Shipment so rows with no LT ID stay separated instead
    of collapsing into one giant group.
    """
    return (
        str(row.get("LT ID") or "").strip()
        or str(row.get("TO ID") or "").strip()
        or str(row.get("Shipment ID") or "").strip()
    )


def active_bot_status(existing_row=None):
    # An order that is still in the active pending result remains PENDING even
    # after CCTV is provided. RECONCILED is assigned only after it leaves the
    # active queue.
    return "PENDING"


def load_module(path, module_name):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


SHEET_EXPORT = load_module(
    BASE_DIR / "BOT-Deli_BDA_export_pending_to_sheet.py",
    "bot3_pending_sheet_export",
)


def query_rows(service, query):
    return SHEET_EXPORT.query_bq(service, query)


def open_spreadsheet():
    return SHEET_EXPORT.open_spreadsheet()


def ensure_cogs_table(service):
    service.tables().insert(
        projectId=SHEET_EXPORT.BIGQUERY_PROJECT_ID,
        datasetId=SHEET_EXPORT.BIGQUERY_DATASET_ID,
        body={
            "tableReference": {"tableId": COGS_TABLE_ID},
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


def load_cogs_cache(service):
    rows = query_rows(
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
          FROM `{SHEET_EXPORT.BIGQUERY_PROJECT_ID}.{SHEET_EXPORT.BIGQUERY_DATASET_ID}.{COGS_TABLE_ID}`
        )
        SELECT shipment_id, cogs, fetched_at, last_error
        FROM ranked
        WHERE rn = 1
        """,
    )
    return {str(row.get("shipment_id") or "").strip(): row for row in rows}


def ensure_reconcile_cache_table(service):
    service.tables().insert(
        projectId=SHEET_EXPORT.BIGQUERY_PROJECT_ID,
        datasetId=SHEET_EXPORT.BIGQUERY_DATASET_ID,
        body={
            "tableReference": {"tableId": RECONCILE_CACHE_TABLE_ID},
            "schema": {
                "fields": [
                    {"name": "cache_key", "type": "STRING"},
                    {"name": "shipment_id", "type": "STRING"},
                    {"name": "station_name", "type": "STRING"},
                    {"name": "station_arrived_at", "type": "DATETIME"},
                    {"name": "fetched_at", "type": "DATETIME"},
                    {"name": "last_error", "type": "STRING"},
                ]
            },
        },
    ).execute()


def load_reconcile_cache(service):
    rows = query_rows(
        service,
        f"""
        WITH ranked AS (
          SELECT
            cache_key,
            shipment_id,
            station_name,
            station_arrived_at,
            fetched_at,
            last_error,
            ROW_NUMBER() OVER (
              PARTITION BY cache_key
              ORDER BY fetched_at DESC
            ) AS rn
          FROM `{SHEET_EXPORT.BIGQUERY_PROJECT_ID}.{SHEET_EXPORT.BIGQUERY_DATASET_ID}.{RECONCILE_CACHE_TABLE_ID}`
        )
        SELECT cache_key, shipment_id, station_name, station_arrived_at, fetched_at, last_error
        FROM ranked
        WHERE rn = 1
        """,
    )
    return {str(row.get("cache_key") or ""): row for row in rows if row.get("cache_key")}


def sql_literal(value):
    return "'" + str(value or "").replace("'", "''") + "'"


def load_to_quantities(service, active_rows):
    """Load each TO's original parcel count from result, candidate, or LT data."""
    to_ids = sorted({str(row.get("to_id") or "").strip() for row in active_rows.values() if row.get("to_id")})
    # Pending Inbound TOs may never be written to lt_unit. Their TO Detail
    # quantity travels with the durable candidate/result instead.
    quantities = {}
    for row in active_rows.values():
        to_number = str(row.get("to_id") or "").strip()
        try:
            quantity = max(0, int(float(row.get("_to_parcel_quantity") or 0)))
        except (TypeError, ValueError):
            quantity = 0
        if to_number and quantity:
            quantities[to_number] = max(quantities.get(to_number, 0), quantity)
    for start in range(0, len(to_ids), 500):
        batch = [to_number for to_number in to_ids[start:start + 500] if not quantities.get(to_number)]
        if not batch:
            continue
        # A Pending Inbound TO can bypass lt_unit. The latest durable
        # candidate still carries data.quantity from its TO Detail response.
        try:
            candidate_rows = query_rows(
                service,
                f"""
                SELECT
                  to_number,
                  MAX(COALESCE(to_parcel_quantity, 0)) AS to_parcel_quantity
                FROM `{SHEET_EXPORT.BIGQUERY_PROJECT_ID}.{SHEET_EXPORT.BIGQUERY_DATASET_ID}.lt_pending_candidate`
                WHERE to_number IN ({", ".join(sql_literal(value) for value in batch)})
                GROUP BY to_number
                """,
            )
        except Exception as exc:
            print(f"pending_work: cannot load candidate TO parcel quantities: {exc}")
            candidate_rows = []
        for row in candidate_rows:
            to_number = str(row.get("to_number") or "").strip()
            try:
                quantities[to_number] = max(
                    quantities.get(to_number, 0),
                    max(0, int(float(row.get("to_parcel_quantity") or 0))),
                )
            except (TypeError, ValueError):
                pass

        batch = [to_number for to_number in batch if not quantities.get(to_number)]
        if not batch:
            continue
        try:
            rows = query_rows(
                service,
                f"""
                SELECT
                  to_number,
                  MAX(COALESCE(to_parcel_quantity, 0)) AS to_parcel_quantity
                FROM `{SHEET_EXPORT.BIGQUERY_PROJECT_ID}.{SHEET_EXPORT.BIGQUERY_DATASET_ID}.lt_unit`
                WHERE to_number IN ({", ".join(sql_literal(value) for value in batch)})
                GROUP BY to_number
                """,
            )
        except Exception as exc:
            print(f"pending_work: cannot load TO parcel quantities: {exc}")
            continue
        for row in rows:
            to_number = str(row.get("to_number") or "").strip()
            try:
                quantities[to_number] = max(
                    quantities.get(to_number, 0),
                    max(0, int(float(row.get("to_parcel_quantity") or 0))),
                )
            except (TypeError, ValueError):
                quantities[to_number] = 0
    return quantities


def split_to_path(value):
    return [part.strip() for part in re.split(r"\s*(?:>|->|→)\s*", str(value or "")) if part.strip()]


def first_hub_after_bda(row):
    stations = split_to_path(row.get("to_path"))
    bda_name = normalize_text(BDA_STATION_NAME)
    for index, station in enumerate(stations):
        if normalize_text(station) == bda_name and index + 1 < len(stations):
            return stations[index + 1]
    return str(row.get("_to_station") or row.get("receiver") or "").strip()


def timestamp_to_local_datetime(value):
    try:
        timestamp = int(float(value))
    except (TypeError, ValueError):
        return None
    if timestamp <= 0:
        return None
    return datetime.fromtimestamp(timestamp, timezone.utc).astimezone(ZoneInfo("Asia/Bangkok")).replace(tzinfo=None)


def iter_tracking_events(nodes):
    stack = list(nodes or [])
    while stack:
        event = stack.pop(0)
        if not isinstance(event, dict):
            continue
        yield event
        for key in ("children", "event_children"):
            children = event.get(key) or []
            if isinstance(children, list):
                stack.extend(children)


def tracking_arrived_at(response, station_name, not_before=None):
    target_station = normalize_text(station_name)
    arrived_events = []
    fallback_events = []
    tracking_list = ((response.get("data") or {}).get("tracking_list") or [])
    for event in iter_tracking_events(tracking_list):
        if normalize_text(event.get("station_name")) != target_station:
            continue
        event_time = timestamp_to_local_datetime(event.get("timestamp"))
        if event_time is None:
            continue
        if not_before and event_time < not_before - timedelta(minutes=5):
            continue
        fallback_events.append(event_time)
        try:
            status_code = int(event.get("status"))
        except (TypeError, ValueError):
            status_code = None
        message = normalize_text(event.get("message"))
        if status_code in TRACKING_ARRIVED_STATUS_CODES or "arrived" in message:
            arrived_events.append(event_time)
    if arrived_events:
        return min(arrived_events)
    return min(fallback_events) if fallback_events else None


def fetch_tracking_arrived_at(shipment_id, station_name, not_before):
    url = f"{TRACKING_INFO_URL}?shipment_id={quote(shipment_id)}"
    response = browser_fetch.request_json(
        ADMIN_ROLE,
        "GET",
        url,
        label=f"Reconcile arrival {shipment_id}",
        max_retries=3,
    )
    return tracking_arrived_at(response, station_name, not_before=not_before)


def cache_reconcile_result(service, row):
    service.store.insert_rows(
        RECONCILE_CACHE_TABLE_ID,
        [row],
        fields=[
            {"name": "cache_key", "type": "STRING"},
            {"name": "shipment_id", "type": "STRING"},
            {"name": "station_name", "type": "STRING"},
            {"name": "station_arrived_at", "type": "DATETIME"},
            {"name": "fetched_at", "type": "DATETIME"},
            {"name": "last_error", "type": "STRING"},
        ],
    )


def get_reconcile_arrival(service, cache, shipment_id, station_name, original_arrived_time):
    cache_key = f"{shipment_id}|{normalize_text(station_name)}"
    cached = cache.get(cache_key)
    if cached:
        cached_arrival = parse_deadline(cached.get("station_arrived_at"))
        if cached_arrival:
            return cached_arrival
        fetched_at = parse_deadline(cached.get("fetched_at"))
        if fetched_at and fetched_at >= datetime.now() - timedelta(minutes=TRACKING_CACHE_RETRY_MINUTES):
            return None
    return None


def enrich_reconciliation_context(service, active_rows):
    """Add missing ratio, reconciliation hub, and the correct 36-hour deadline."""
    if not active_rows:
        return

    quantities = load_to_quantities(service, active_rows)
    cache = load_reconcile_cache(service)
    groups = {}
    for shipment_id, row in active_rows.items():
        source_type = normalize_text(row.get("_source_type"))
        if source_type == "bulky" or normalize_text(row.get("type")) == "bulky":
            key = ("BULKY", shipment_id)
        else:
            key = (
                str(row.get("_trip_id") or row.get("lt_id") or ""),
                str(row.get("_sequence_number") or ""),
                str(row.get("to_id") or ""),
            )
        groups.setdefault(key, []).append((shipment_id, row))

    tracking_cache_lookups = 0
    tracking_cache_hits = 0
    for _key, group_rows in groups.items():
        shipment_ids = sorted({shipment_id for shipment_id, _row in group_rows})
        representative = group_rows[0][1]
        source_type = normalize_text(representative.get("_source_type"))
        is_bulky = source_type == "bulky" or normalize_text(representative.get("type")) == "bulky"
        missing_count = len(shipment_ids)
        total_count = 1 if is_bulky else quantities.get(str(representative.get("to_id") or "").strip(), 0)
        ratio = f"{missing_count}/{total_count}" if total_count else f"{missing_count}/?"
        first_hub = first_hub_after_bda(representative)
        is_all_missing = total_count > 0 and missing_count >= total_count
        reconcile_hub = first_hub if is_bulky or is_all_missing else str(representative.get("receiver") or "").strip()
        reconcile_hub = reconcile_hub or first_hub

        arrived_at = parse_deadline(representative.get("arrived_time"))
        if (
            arrived_at
            and not is_bulky
            and reconcile_hub
            and normalize_text(reconcile_hub) != normalize_text(first_hub)
        ):
            tracking_cache_lookups += 1
            tracking_arrival = get_reconcile_arrival(
                service,
                cache,
                shipment_ids[0],
                reconcile_hub,
                arrived_at,
            )
            if tracking_arrival:
                tracking_cache_hits += 1
                arrived_at = tracking_arrival
            else:
                # A later reconciliation hub needs its own real arrival event.
                # Do not substitute the LT arrival at the first receiving hub.
                arrived_at = None

        deadline = (arrived_at + timedelta(hours=36)).strftime("%Y-%m-%d %H:%M:%S") if arrived_at else ""
        for _shipment_id, row in group_rows:
            row["missing_to_ratio"] = ratio
            row["reconcile_hub"] = reconcile_hub
            row["deadline_bao_thieu"] = deadline

    print(
        "pending_work enrichment: "
        f"groups={len(groups)} | TO quantity found={sum(1 for value in quantities.values() if value)} | "
        f"later-hub cache={tracking_cache_hits}/{tracking_cache_lookups} "
        "(missing arrival leaves deadline blank)"
    )


def load_active_pending_rows(service):
    mapping = SHEET_EXPORT.load_status_mapping()
    rows = SHEET_EXPORT.apply_display_statuses(
        query_rows(service, SHEET_EXPORT.pending_query()),
        mapping,
    )

    # One shipment is a single operation case in pending_work. When it appears
    # through two candidate paths, retain the newest checked result.
    by_shipment = {}
    for row in rows:
        shipment_id = str(row.get("shipment_id") or "").strip()
        if not shipment_id:
            continue
        previous = by_shipment.get(shipment_id)
        if previous is None or str(row.get("checked_at") or "") >= str(previous.get("checked_at") or ""):
            by_shipment[shipment_id] = row
    return by_shipment


def load_resolved_shipments(service):
    try:
        rows = query_rows(
            service,
            f"""
            WITH ranked AS (
              SELECT
                COALESCE(NULLIF(shipment_id, ''), order_number, to_number) AS shipment_id,
                status_changed_at,
                ROW_NUMBER() OVER (
                  PARTITION BY COALESCE(NULLIF(shipment_id, ''), order_number, to_number)
                  ORDER BY status_changed_at DESC
                ) AS rn
              FROM `{SHEET_EXPORT.BIGQUERY_PROJECT_ID}.{SHEET_EXPORT.BIGQUERY_DATASET_ID}.{STATUS_CHANGED_TABLE_ID}`
            )
            SELECT shipment_id, status_changed_at
            FROM ranked
            WHERE rn = 1
            """,
        )
    except Exception:
        return set()
    return {str(row.get("shipment_id") or "").strip() for row in rows if row.get("shipment_id")}


def load_pending_published_times(service):
    """Return the last successful pending Sheet publish time for each order."""
    rows = query_rows(
        service,
        f"""
        WITH ranked AS (
          SELECT
            COALESCE(NULLIF(shipment_id, ''), order_number, to_number) AS shipment_id,
            published_at,
            ROW_NUMBER() OVER (
              PARTITION BY COALESCE(NULLIF(shipment_id, ''), order_number, to_number)
              ORDER BY published_at DESC
            ) AS rn
          FROM `{SHEET_EXPORT.BIGQUERY_PROJECT_ID}.{SHEET_EXPORT.BIGQUERY_DATASET_ID}.{SHEET_EXPORT.PENDING_RESULT_TABLE_ID}`
          WHERE published_at IS NOT NULL
            AND published_at != ''
        )
        SELECT shipment_id, published_at
        FROM ranked
        WHERE rn = 1
        """,
    )
    output = {}
    for row in rows:
        shipment_id = str(row.get("shipment_id") or "").strip()
        timestamp = parse_deadline(row.get("published_at"))
        if shipment_id and timestamp:
            output[shipment_id] = timestamp
    return output


def load_pending_eligible_times(service):
    """shipment_id -> eligible_at, moc mot don tro thanh viec pending that su.

    Lich chia theo dot phai bam vao mot moc DUNG YEN. published_at khong dung yen:
    compact_pending_result_history() giu mot dong ket qua cho moi don va lam mat
    published_at, roi lan xuat ke tiep dong dau lai. Do duoc 2026-09-12: ca 2.819
    don PENDING co published_at deu roi dung vao 13h, 14h, 15h cung ngay - ba lan
    xuat gan nhat - va 830 don khac trong hoan toan. Ket qua la moi don deu bi xep
    "cong bo sau 12:00, cho ngay mai", nen ca ngay chi chia duoc 79 don trong khi
    2.217 don PENDING khong co ai nhan.

    eligible_at = arrived_time + 36h, tinh mot lan va khong bao gio doi, va dung
    la thoi diem don tro thanh ca pending phai dua len sheet. Cung do ngay hom do:
    0 dong thieu eligible_at, gia tri trai deu tu 06/09 den 12/09.
    """
    rows = query_rows(
        service,
        f"""
        WITH ranked AS (
          SELECT
            COALESCE(NULLIF(shipment_id, ''), order_number, to_number) AS shipment_id,
            eligible_at,
            ROW_NUMBER() OVER (
              PARTITION BY COALESCE(NULLIF(shipment_id, ''), order_number, to_number)
              ORDER BY eligible_at ASC
            ) AS rn
          FROM `{SHEET_EXPORT.BIGQUERY_PROJECT_ID}.{SHEET_EXPORT.BIGQUERY_DATASET_ID}.{SHEET_EXPORT.PENDING_RESULT_TABLE_ID}`
          WHERE eligible_at IS NOT NULL
            AND eligible_at != ''
        )
        SELECT shipment_id, eligible_at
        FROM ranked
        WHERE rn = 1
        """,
    )
    output = {}
    for row in rows:
        shipment_id = str(row.get("shipment_id") or "").strip()
        timestamp = parse_deadline(row.get("eligible_at"))
        if shipment_id and timestamp:
            output[shipment_id] = timestamp
    return output


def fetch_cogs(shipment_id):
    url = f"{COGS_URL}?shipment_id={quote(shipment_id)}&data_field=cogs"
    response = browser_fetch.request_json(
        ADMIN_ROLE,
        "GET",
        url,
        label=f"COGS {shipment_id}",
        max_retries=3,
    )
    value = ((response.get("data") or {}).get("data_detail"))
    if value is None or value == "":
        raise RuntimeError("COGS API returned empty data_detail")
    return value


def fill_missing_cogs(service, active_rows, cache, limit):
    missing = [shipment_id for shipment_id in active_rows if not str((cache.get(shipment_id) or {}).get("cogs") or "").strip()]
    if limit is not None:
        missing = missing[:max(0, int(limit))]
    if not missing:
        print("COGS: no active Pending orders need lookup")
        return 0

    print(f"COGS: fetch {len(missing)} active Pending order(s) with Admin FMS session")
    rows_to_store = []
    for index, shipment_id in enumerate(missing, start=1):
        fetched_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            cogs = fetch_cogs(shipment_id)
            cache[shipment_id] = {"shipment_id": shipment_id, "cogs": cogs, "fetched_at": fetched_at, "last_error": ""}
            rows_to_store.append(cache[shipment_id])
            print(f"COGS {index}/{len(missing)}: {shipment_id} = {cogs}")
        except Exception as exc:
            cache[shipment_id] = {"shipment_id": shipment_id, "cogs": "", "fetched_at": fetched_at, "last_error": str(exc)[:500]}
            rows_to_store.append(cache[shipment_id])
            print(f"COGS {index}/{len(missing)} failed: {shipment_id}: {exc}")
        time.sleep(0.15)

    if rows_to_store:
        service.store.insert_rows(
            COGS_TABLE_ID,
            rows_to_store,
            fields=[
                {"name": "shipment_id", "type": "STRING"},
                {"name": "cogs", "type": "NUMERIC"},
                {"name": "fetched_at", "type": "DATETIME"},
                {"name": "last_error", "type": "STRING"},
            ],
        )
    return len(rows_to_store)


def get_or_create_worksheet(spreadsheet):
    try:
        return spreadsheet.worksheet(WORKSHEET_TITLE)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=WORKSHEET_TITLE, rows=1000, cols=len(WORK_HEADERS))


def ensure_headers(worksheet):
    # Doc mot dong nhung van la mot lenh GET: Google tra 503 cho chinh no ngay
    # 2026-09-16, va vi day la buoc dau moi vong nen no giet ca vong truoc khi
    # lam duoc gi. Dung chung lop thu lai voi cac lenh doc khac.
    headers = [
        str(value).strip()
        for value in _thu_lai_doc_sheet(
            lambda: worksheet.row_values(1), f"dong tieu de {worksheet.title}"
        )
    ]
    if not headers:
        if worksheet.col_count < len(WORK_HEADERS):
            worksheet.resize(cols=len(WORK_HEADERS))
        worksheet.update(values=[WORK_HEADERS], range_name="A1", value_input_option="USER_ENTERED")
        return WORK_HEADERS

    missing = [header for header in WORK_HEADERS if header not in headers]
    if missing:
        start = len(headers) + 1
        end = start + len(missing) - 1
        if worksheet.col_count < end:
            worksheet.resize(cols=end)
        worksheet.update(
            values=[missing],
            range_name=f"{gspread.utils.rowcol_to_a1(1, start)}:{gspread.utils.rowcol_to_a1(1, end)}",
            value_input_option="USER_ENTERED",
        )
        headers.extend(missing)
        print(f"pending_work: added {len(missing)} missing column(s)")

    # Move whole columns instead of rewriting cells. This preserves row-2
    # formulas, formatting, validation, and every staff-maintained value.
    simulated = list(headers)
    requests = []
    for target_index, header in enumerate(WORK_HEADERS):
        current_index = simulated.index(header)
        if current_index == target_index:
            continue
        requests.append({
            "moveDimension": {
                "source": {
                    "sheetId": worksheet.id,
                    "dimension": "COLUMNS",
                    "startIndex": current_index,
                    "endIndex": current_index + 1,
                },
                "destinationIndex": target_index,
            }
        })
        simulated.insert(target_index, simulated.pop(current_index))
    if requests:
        worksheet.spreadsheet.batch_update({"requests": requests})
        headers = [
            str(value).strip()
            for value in _thu_lai_doc_sheet(
                lambda: worksheet.row_values(1), "dong tieu de sau khi sap cot"
            )
        ]
        print(f"pending_work: reordered {len(WORK_HEADERS)} operational column(s)")
    return headers


def clear_pending_work_rows(worksheet):
    """Clear BOT data while preserving headers and the formula row."""
    worksheet.batch_clear([f"A{WORK_DATA_START_ROW}:ZZ"])
    print("pending_work reset: cleared data from row 3; kept rows 1-2")


def read_existing_rows(worksheet, headers, values=None):
    values = sheets_get_all_values_with_retry(worksheet) if values is None else values
    records = {}
    for row_number, values_row in enumerate(
        values[WORK_DATA_START_ROW - 1:],
        start=WORK_DATA_START_ROW,
    ):
        # Keep the left-most value when an old Sheet layout contains duplicate
        # headers. A trailing blank duplicate must not overwrite BOT-owned data.
        row = {}
        for index, header in enumerate(headers):
            if header and header not in row:
                row[header] = values_row[index] if index < len(values_row) else ""
        shipment_id = str(row.get("Shipment ID") or "").strip().upper()
        if shipment_id and shipment_id not in records:
            records[shipment_id] = (row_number, row)
    return records


def deduplicate_pending_work_rows(worksheet, headers, values=None):
    """Merge duplicate Shipment IDs without losing staff-maintained fields."""
    values = sheets_get_all_values_with_retry(worksheet) if values is None else values
    if len(values) < WORK_DATA_START_ROW or "Shipment ID" not in headers:
        return 0

    shipment_column = headers.index("Shipment ID")
    bot_headers = {label for _, label in BOT_COLUMNS}
    groups = {}
    for row_number, values_row in enumerate(
        values[WORK_DATA_START_ROW - 1:],
        start=WORK_DATA_START_ROW,
    ):
        shipment_id = (
            str(values_row[shipment_column]).strip()
            if shipment_column < len(values_row)
            else ""
        ).upper()
        if shipment_id:
            groups.setdefault(shipment_id, []).append((row_number, values_row))

    duplicate_groups = [rows for rows in groups.values() if len(rows) > 1]
    if not duplicate_groups:
        return 0

    merge_updates = []
    rows_to_delete = []
    for rows in duplicate_groups:
        def row_score(item):
            row_number, values_row = item
            populated_manual = sum(
                1
                for index, header in enumerate(headers)
                if header not in bot_headers
                and index < len(values_row)
                and str(values_row[index]).strip()
            )
            populated_total = sum(1 for value in values_row if str(value).strip())
            # Prefer the row containing the most team work. On a tie, retain
            # the oldest row so its position in the work queue stays stable.
            return populated_manual, populated_total, -row_number

        survivor_number, survivor_values = max(rows, key=row_score)
        merged = list(survivor_values) + [""] * max(0, len(headers) - len(survivor_values))

        # Fill only blank cells on the survivor. Conflicting nonblank staff
        # values remain on the row with the strongest manual-data score.
        for row_number, values_row in sorted(rows, key=row_score, reverse=True):
            if row_number == survivor_number:
                continue
            for index in range(min(len(headers), len(values_row))):
                if not str(merged[index]).strip() and str(values_row[index]).strip():
                    merged[index] = values_row[index]
            rows_to_delete.append(row_number)

        merge_updates.append({
            "range": (
                f"A{survivor_number}:"
                f"{gspread.utils.rowcol_to_a1(survivor_number, len(headers))}"
            ),
            "values": [merged[:len(headers)]],
        })

    for start in range(0, len(merge_updates), 300):
        sheets_batch_update_with_retry(worksheet, merge_updates[start:start + 300])
    deleted = delete_rows(worksheet, rows_to_delete)
    print(
        "pending_work dedupe: "
        f"groups={len(duplicate_groups)} | duplicate rows deleted={deleted}"
    )
    return deleted


def work_row_from_pending(row, cogs_row, bot_status="PENDING"):
    return {
        "Shipment ID": row.get("shipment_id") or "",
        "LT ID": row.get("lt_id") or "",
        "TO ID": row.get("to_id") or "",
        "TO Path": row.get("to_path") or "",
        "Sender": row.get("sender") or "",
        "Receiver": row.get("receiver") or "",
        "Type": row.get("type") or "",
        "Arrived time": row.get("arrived_time") or "",
        "Số đơn thiếu / đơn trong TO": row.get("missing_to_ratio") or "",
        "Hub/SOC cần đối soát": row.get("reconcile_hub") or "",
        "deadline_bao_thieu": row.get("deadline_bao_thieu") or "",
        "Status": row.get("status") or "",
        "Current station": row.get("current_station") or "",
        "Destination": row.get("destination") or "",
        "COGS": (cogs_row or {}).get("cogs") or "",
        "BOT Status": bot_status,
    }


def sheet_cell_text(value):
    """Normalise a cell for comparison. Sheet reads always come back as text."""
    return "" if value is None else str(value).strip()


def sheet_cell_matches(new_value, old_value):
    """True when the Sheet already holds this value, ignoring date rendering.

    The bot writes `deadline_bao_thieu` as ISO ("2026-09-08 15:17:38"), Google
    recognises it as a date and renders it back in the sheet locale
    ("08/09/2026 15:17:38"). A plain string compare therefore reports every
    single row as changed: measured 1,109 of 1,112 rows differing on that one
    column and nothing else. Comparing the parsed instants instead keeps the
    diff honest and leaves the team's formatting alone.
    """
    new_text = sheet_cell_text(new_value)
    old_text = sheet_cell_text(old_value)
    if new_text == old_text:
        return True
    if not new_text or not old_text:
        return False
    new_at = parse_deadline(new_text)
    return bool(new_at) and new_at == parse_deadline(old_text)


def changed_cell_updates(row_number, headers, output, existing_row, bot_headers):
    """Ranges for the BOT cells whose value actually differs from the Sheet.

    Rewriting every BOT cell of every active row was costing 1,142 rows x 16
    columns = 18,272 ranges per cycle and regularly tripping Google's per-minute
    write quota (HTTP 429, then 65s/75s waits). It is 16 ranges per row rather
    than 1 because the BOT columns are NOT contiguous on this sheet any more:
    the team inserted their own columns 12-21 (deadline_cctv .. date_chia_don),
    splitting the BOT block into columns 1-11 and 22-26.

    Almost every cell is rewritten with the value it already holds, so compare
    first and emit nothing when a row is unchanged. Consecutive changed columns
    are merged into one range, which also restores the cheap whole-block write
    whenever a brand new row does change everything at once.
    """
    changed = []
    for header in bot_headers:
        if header not in headers:
            continue
        if header in TOOL_OWNED_HEADERS:
            continue
        new_value = output.get(header, "")
        if not sheet_cell_matches(new_value, existing_row.get(header)):
            changed.append((headers.index(header) + 1, sheet_cell_text(new_value)))
    if not changed:
        return []

    changed.sort()
    updates = []
    run_start, run_values = changed[0][0], [changed[0][1]]
    prev_col = changed[0][0]
    for col, value in changed[1:]:
        if col == prev_col + 1:
            run_values.append(value)
        else:
            updates.append({
                "range": f"{gspread.utils.rowcol_to_a1(row_number, run_start)}:"
                         f"{gspread.utils.rowcol_to_a1(row_number, prev_col)}",
                "values": [run_values],
            })
            run_start, run_values = col, [value]
        prev_col = col
    updates.append({
        "range": f"{gspread.utils.rowcol_to_a1(row_number, run_start)}:"
                 f"{gspread.utils.rowcol_to_a1(row_number, prev_col)}",
        "values": [run_values],
    })
    return updates


def sync_pending_work(worksheet, headers, active_rows, cogs_cache, values=None):
    values = sheets_get_all_values_with_retry(worksheet) if values is None else values
    existing = read_existing_rows(worksheet, headers, values=values)
    row_count = len(values)
    bot_headers = [label for _, label in BOT_COLUMNS]
    bot_columns_are_contiguous = all(
        headers[index] == header
        for index, header in enumerate(bot_headers)
    )
    appended_rows = []
    updates = []
    rows_unchanged = 0
    cctv_status_column = headers.index("status_cctv_deli") + 1

    for shipment_id, pending_row in active_rows.items():
        if shipment_id in existing:
            row_number, existing_row = existing[shipment_id]
            output = work_row_from_pending(
                pending_row,
                cogs_cache.get(shipment_id),
                bot_status=active_bot_status(existing_row),
            )
            row_updates = changed_cell_updates(
                row_number, headers, output, existing_row, bot_headers
            )
            if row_updates:
                updates.extend(row_updates)
            else:
                rows_unchanged += 1
            current_cctv_status = existing_row.get("status_cctv_deli")
            full_missing_to = is_full_missing_to(
                pending_row.get("missing_to_ratio"),
                pending_row.get("type"),
            )
            # A full-missing TO (bulky included) reaches the Sheet with this
            # cell blank on purpose: the team pulls the CCTV footage and records
            # the outcome themselves. So the only value the bot may erase here
            # is the retired '04. Chia đơn' marker. It used to erase
            # '02. Không cung cấp cam' too, which threw away a real conclusion
            # the team had just reached. Ops instruction 2026-09-08: for a row
            # that arrives blank, never erase whatever the team puts in it.
            if full_missing_to and is_legacy_assigned_status(current_cctv_status):
                updates.append({
                    "range": gspread.utils.rowcol_to_a1(row_number, cctv_status_column),
                    "values": [[""]],
                })
            elif (
                is_auto_no_cctv_type(pending_row.get("type"))
                and str(existing_row.get("PIC_cctv") or "").strip()
                # Same rule from the other side: do not pre-fill '02' on a
                # full-missing row either, or the cell is no longer blank when
                # the team gets to it.
                and not full_missing_to
                and not is_legacy_assigned_status(current_cctv_status)
                and not is_cctv_provided(current_cctv_status)
                and not is_cctv_not_provided(current_cctv_status)
                and not is_team_owned_cctv_status(current_cctv_status)
            ):
                updates.append({
                    "range": gspread.utils.rowcol_to_a1(row_number, cctv_status_column),
                    "values": [[CCTV_NOT_PROVIDED_STATUS]],
                })
        else:
            output = work_row_from_pending(pending_row, cogs_cache.get(shipment_id))
            # Append only the bot-owned A:P block. Manual/team columns may be
            # protected and must remain untouched when a new work row is added.
            appended_rows.append([output.get(header, "") for header in bot_headers])

    # Migrate the old assignment marker. PIC_cctv now records assignment;
    # status_cctv_deli is limited to the two CCTV outcomes or a blank value.
    for shipment_id, (row_number, existing_row) in existing.items():
        current_cctv_status = existing_row.get("status_cctv_deli")
        if not is_legacy_assigned_status(current_cctv_status):
            continue
        pending_row = active_rows.get(shipment_id) or {}
        work_type = pending_row.get("type") or existing_row.get("Type")
        full_missing_to = is_full_missing_to(
            pending_row.get("missing_to_ratio") or existing_row.get("Số đơn thiếu / đơn trong TO"),
            work_type,
        )
        replacement = (
            CCTV_NOT_PROVIDED_STATUS
            if is_auto_no_cctv_type(work_type)
            and not full_missing_to
            and str(existing_row.get("PIC_cctv") or "").strip()
            else ""
        )
        updates.append({
            "range": gspread.utils.rowcol_to_a1(row_number, cctv_status_column),
            "values": [[replacement]],
        })

    # A resolved case remains visible for the team, including every manual
    # field. Only the bot-owned status is changed.
    #
    # Leaving active_rows is the whole condition. This used to iterate
    # `resolved_shipments` instead, which only holds orders whose Pending ->
    # NOT_PENDING flip was caught in flight by
    # capture_published_pending_status_changes(). That capture needs the old
    # published PENDING row and the new NOT_PENDING row to coexist in
    # lt_pending_result, but compact_pending_result_history() (run by BOT 2 AND
    # BOT 3) keeps one row per candidate and drops non-PENDING ones, so
    # whichever bot compacts first erases the evidence. The 2026-09-05 DB reset
    # wiped the audit table outright. Result: 852 sheet rows - 9 of them with
    # CCTV already provided - sat at PENDING forever because this loop never
    # even visited them. active_rows is the durable signal; the audit table is
    # not, so it no longer gates the status.
    bot_status_column = headers.index("BOT Status") + 1
    resolved_marked = 0
    hoan_dong_so = 0
    thay_vang_lan_nay = set()
    for shipment_id, (row_number, existing_row) in existing.items():
        if shipment_id in active_rows:
            # Quay lai hang pending thi xoa dau vang mat, dem lai tu dau.
            _vang_khoi_hang_pending.pop(shipment_id, None)
            continue
        current_bot_status = str(existing_row.get("BOT Status") or "").strip().upper()
        # A blank status is a row the bot has not claimed yet (a manual entry,
        # or one appended after this cycle's snapshot). Do not close it.
        if not current_bot_status or current_bot_status == RECONCILED_BOT_STATUS:
            continue

        # Phai vang mat DUNG CLOSE_CONFIRM_CYCLES vong lien tiep moi duoc dong so.
        #
        # Truoc day mot vong la du, nen mot lan cham nham don le cung dong duoc
        # dong. Ops bao 4 don ngay 2026-09-12: SPXVN063724926529,
        # SPXVN061400040529, SPXVN062143041599, SPXVN061510080149 - tat ca deu
        # thanh RECONCILED trong khi van thieu hang that, roi vong sau tu quay ve
        # PENDING (dong nao con trong hang pending bi ghi de BOT Status ve PENDING
        # moi vong). Khong truy duoc nguyen nhan cu cham nham vi
        # compact_pending_result_history() chi giu mot dong ket qua cho moi don,
        # nen bang chung da bi xoa.
        #
        # Dem theo VONG chu khong theo thoi gian: do dai mot vong thay doi tu 8
        # den 25 phut tuy tai, lay moc phut se lan.
        thay_vang_lan_nay.add(shipment_id)
        lan_vang = _vang_khoi_hang_pending.get(shipment_id, 0) + 1
        _vang_khoi_hang_pending[shipment_id] = lan_vang
        if lan_vang < CLOSE_CONFIRM_CYCLES:
            hoan_dong_so += 1
            continue
        # RECONCILED means "the team handled this one", not specifically "CCTV
        # was provided" - 03.Lạc tuyến and 04.Tìm hàng are just as much a human
        # decision as 01. This also covers a row that arrived as 02 and was
        # later changed by the team: what counts is the value at closing time.
        # A filled status_nhan_xet closes the row RECONCILED too (ops rule
        # 2026-09-11), but only while the row is still PENDING, i.e. only for an
        # order leaving the queue right now. Ops chose not to re-open settled
        # rows: 790 rows already sat at RESOLVED with a review note on them, and
        # without this gate the next cycle would have rewritten every one of
        # them. The cctv signal above keeps its own older behaviour.
        next_bot_status = "RESOLVED"
        if is_team_handled_cctv_status(existing_row.get("status_cctv_deli")):
            next_bot_status = RECONCILED_BOT_STATUS
        elif current_bot_status == "PENDING" and has_team_review(
            existing_row.get("status_nhan_xet")
        ):
            next_bot_status = RECONCILED_BOT_STATUS
        if current_bot_status == next_bot_status:
            continue
        resolved_marked += 1
        updates.append({
            "range": gspread.utils.rowcol_to_a1(row_number, bot_status_column),
            "values": [[next_bot_status]],
        })

    # Chi giu dau cua nhung don vang mat o CHINH vong nay. Sheet co hon 17.000
    # dong va phan lon da dong so tu lau; khong don thi bo dem phinh mai.
    for da_xong in [k for k in _vang_khoi_hang_pending if k not in thay_vang_lan_nay]:
        _vang_khoi_hang_pending.pop(da_xong, None)

    for start in range(0, len(updates), 300):
        sheets_batch_update_with_retry(worksheet, updates[start:start + 300])
    if appended_rows:
        # Google Sheets append may infer the entire table range, including
        # protected manual columns. Write the exact bot-owned A:P range
        # instead, leaving every team-maintained column untouched.
        # `updates` above only rewrite existing cells, so the row count from the
        # snapshot read at the top of this function is still current.
        next_row = max(row_count + 1, WORK_DATA_START_ROW)
        required_end_row = next_row + len(appended_rows) - 1
        if worksheet.row_count < required_end_row:
            worksheet.resize(rows=required_end_row)
        for start in range(0, len(appended_rows), 300):
            chunk = appended_rows[start:start + 300]
            end_row = next_row + len(chunk) - 1
            if bot_columns_are_contiguous:
                worksheet.update(
                    values=chunk,
                    range_name=(
                        f"A{next_row}:"
                        f"{gspread.utils.rowcol_to_a1(end_row, BOT_HEADER_COUNT)}"
                    ),
                    value_input_option="USER_ENTERED",
                )
            else:
                # Write one vertical range per bot-owned column. This keeps
                # team columns untouched while reducing thousands of cell
                # updates to one Sheets request per row chunk.
                append_updates = []
                for value_index, header in enumerate(bot_headers):
                    column = headers.index(header) + 1
                    start_cell = gspread.utils.rowcol_to_a1(next_row, column)
                    end_cell = gspread.utils.rowcol_to_a1(end_row, column)
                    append_updates.append({
                        "range": f"{start_cell}:{end_cell}",
                        "values": [[values_row[value_index]] for values_row in chunk],
                    })
                sheets_batch_update_with_retry(worksheet, append_updates)
            next_row = end_row + 1

    print(
        "pending_work sync: "
        f"active={len(active_rows)} | updated={len(updates)} | "
        f"unchanged_rows={rows_unchanged} | appended={len(appended_rows)} | "
        f"resolved_marked={resolved_marked} | hoan dong so cho xac nhan={hoan_dong_so}"
    )
    return existing


def clear_full_missing_to_cctv_status(worksheet, headers, active_rows, values=None):
    """Keep full-missing TO rows blank after assignment and Sheet coercion."""
    if "status_cctv_deli" not in headers:
        return 0

    existing = read_existing_rows(worksheet, headers, values=values)
    status_column = headers.index("status_cctv_deli") + 1
    updates = []
    for shipment_id, pending_row in active_rows.items():
        if not is_full_missing_to(
            pending_row.get("missing_to_ratio"),
            pending_row.get("type"),
        ):
            continue
        existing_item = existing.get(shipment_id)
        if not existing_item:
            continue
        row_number, existing_row = existing_item
        current = existing_row.get("status_cctv_deli")
        if not str(current or "").strip():
            continue
        # A full-missing TO - bulky included - reaches the Sheet with this cell
        # empty on purpose; the team pulls the CCTV footage and records the
        # outcome here. Every value in the current dropdown is therefore a
        # human decision and stays. Only the retired '04. Chia đơn' marker is
        # still cleared. Ops instruction 2026-09-08: for a row that arrives
        # blank, never erase whatever the team puts in it.
        if not is_legacy_assigned_status(current):
            continue
        updates.append({
            "range": gspread.utils.rowcol_to_a1(row_number, status_column),
            "values": [[""]],
        })

    for start in range(0, len(updates), 300):
        sheets_batch_update_with_retry(worksheet, updates[start:start + 300])
    if updates:
        print(f"pending_work full-missing TO: cleared CCTV status on {len(updates)} row(s)")
    return len(updates)


def set_sorting_no_cctv_status(worksheet, headers, active_rows, values=None):
    """A Sorting order that is missing only part of its TO always shows
    '02. Không cung cấp cam' - set it as soon as the row appears, without
    waiting for a PIC. Never overwrites a status a person chose:
    '01. Đã cung cấp cam' or a team-owned investigation outcome."""
    if "status_cctv_deli" not in headers:
        return 0

    existing = read_existing_rows(worksheet, headers, values=values)
    status_column = headers.index("status_cctv_deli") + 1
    updates = []
    for shipment_id, pending_row in active_rows.items():
        if normalize_text(pending_row.get("type")) != "sorting":
            continue
        if is_full_missing_to(pending_row.get("missing_to_ratio"), pending_row.get("type")):
            continue
        existing_item = existing.get(shipment_id)
        if not existing_item:
            continue
        row_number, existing_row = existing_item
        current = existing_row.get("status_cctv_deli")
        if (
            is_cctv_provided(current)
            or is_cctv_not_provided(current)
            or is_team_owned_cctv_status(current)
        ):
            continue
        updates.append({
            "range": gspread.utils.rowcol_to_a1(row_number, status_column),
            "values": [[CCTV_NOT_PROVIDED_STATUS]],
        })

    for start in range(0, len(updates), 300):
        sheets_batch_update_with_retry(worksheet, updates[start:start + 300])
    if updates:
        print(f"pending_work Sorting partial-missing: set '02. Không cung cấp cam' on {len(updates)} row(s)")
    return len(updates)


# Mot batch_update xoa hang nghin dai dong cung luc la request nang; Google tra
# 503 cho chinh no ngay 2026-09-14 khi don 3.459 dong. Cat nho ra, va vi da di tu
# DUOI LEN nen moi mieng deu dung chi so cu - xoa mieng duoi khong lam xe dich
# dong cua mieng tren.
DELETE_ROWS_CHUNK = 400
DELETE_ROWS_RETRY_SECONDS = (5, 15, 40)


def _dem_dong_luoi(worksheet):
    """So dong cua luoi (gridProperties.rowCount). Xoa dong lam so nay giam."""
    try:
        meta = worksheet.spreadsheet.fetch_sheet_metadata()
    except Exception:
        return None
    for sheet in meta.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("sheetId") == worksheet.id:
            return props.get("gridProperties", {}).get("rowCount")
    return None


def delete_rows(worksheet, row_numbers):
    """Delete many rows, bottom to top, in chunks that survive a flaky Sheets API."""
    # Rows 1-2 are protected logically: header and user formula row.
    row_numbers = [row for row in row_numbers if row >= WORK_DATA_START_ROW]
    if not row_numbers:
        return 0

    groups = []
    for row_number in sorted(set(row_numbers), reverse=True):
        if groups and row_number == groups[-1][1] - 1:
            groups[-1] = (groups[-1][0], row_number)
        else:
            groups.append((row_number, row_number))

    requests = []
    for highest_row, lowest_row in groups:
        requests.append({
            "deleteDimension": {
                "range": {
                    "sheetId": worksheet.id,
                    "dimension": "ROWS",
                    "startIndex": lowest_row - 1,
                    "endIndex": highest_row,
                }
            }
        })

    da_xoa = 0
    for start in range(0, len(requests), DELETE_ROWS_CHUNK):
        mieng = requests[start:start + DELETE_ROWS_CHUNK]
        so_dong_mieng = sum(
            req["deleteDimension"]["range"]["endIndex"]
            - req["deleteDimension"]["range"]["startIndex"]
            for req in mieng
        )
        if not _xoa_mot_mieng(worksheet, mieng, so_dong_mieng):
            print(
                f"pending_work cleanup: dung o mieng {start // DELETE_ROWS_CHUNK + 1}, "
                f"da xoa {da_xoa:,} dong. Phan con lai de vong sau tinh lai tu dau."
            )
            break
        da_xoa += so_dong_mieng
    return da_xoa


def _xoa_mot_mieng(worksheet, requests, so_dong_mieng):
    """True neu mieng nay da duoc xoa. False neu bo cuoc - KHONG nem loi.

    Chuyen quan trong: mot batch_update la tat-ca-hoac-khong, nhung HTTP 503 co
    the la "chua chay" MA CUNG co the la "da chay xong, mat duong ve". Thu lai mu
    quang bang dung chi so dong cu trong truong hop thu hai se xoa NHAM dong khac,
    vi moi dong ben duoi da dich len. Nen truoc khi thu lai phai doi chieu
    gridProperties.rowCount: xoa dong lam so nay giam dung bang so dong vua xoa.
    """
    truoc = _dem_dong_luoi(worksheet)
    for attempt in range(len(DELETE_ROWS_RETRY_SECONDS) + 1):
        try:
            worksheet.spreadsheet.batch_update({"requests": requests})
            return True
        except APIError as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            if status_code not in {429, 500, 502, 503, 504}:
                raise
            sau = _dem_dong_luoi(worksheet)
            if truoc is not None and sau is not None and truoc - sau >= so_dong_mieng:
                print(
                    f"Xoa dong: HTTP {status_code} nhung luoi da giam "
                    f"{truoc - sau} dong -> lenh DA chay. Khong thu lai."
                )
                return True
            if attempt >= len(DELETE_ROWS_RETRY_SECONDS):
                print(f"Xoa dong: HTTP {status_code}, het luot thu lai.")
                return False
            if sau is None:
                print(
                    f"Xoa dong: HTTP {status_code} va KHONG doc duoc so dong de doi "
                    "chieu. Dung lai cho an toan thay vi xoa mu."
                )
                return False
            delay = DELETE_ROWS_RETRY_SECONDS[attempt]
            print(
                f"Xoa dong: HTTP {status_code}, luoi chua giam -> lenh chua chay; "
                f"cho {delay}s roi thu lai ({attempt + 1}/{len(DELETE_ROWS_RETRY_SECONDS)})..."
            )
            time.sleep(delay)
            truoc = sau
    return False


# Mot dong xuat hien tren sheet o moc eligible_at = arrived_time + 36h, nen "da
# nam tren sheet N ngay" = arrived_time + 36h + N ngay.
WORK_RETENTION_EXTRA_HOURS = SHEET_EXPORT.PENDING_READY_AFTER_ARRIVED_HOURS

# Chan an toan. Neu mot lan don dep dinh xoa qua nhung nguong nay thi co gi do
# sai - cot Arrived time doc hong, hoac sheet vua bi ghi de - va thu dung lai van
# hon la xoa sach. Ops dan "can than keo bi xoa het".
PURGE_MAX_ROWS_PER_CYCLE = 12000
PURGE_MAX_SHARE_OF_SHEET = 0.60

# Ops chot 2026-09-14: CHI don dong RESOLVED. RECONCILED la dong co nguoi that su
# vao xu ly - o status_cctv_deli hoac status_nhan_xet da duoc dien tay - nen o lai
# tren sheet de doi chieu. Danh sach nay la danh sach TRANG: khong khop thi giu,
# nen mot trang thai moi xuat hien sau nay se mac dinh duoc giu chu khong bi xoa.
#
# Ops bo sung 2026-09-18: RECONCILED cung duoc xoa, NHUNG chi khi status_bao_thieu
# dung bang "1.Hub khong bao thieu". Hub co bao thieu (2/3) hoac o trong thi giu -
# do la viec con phai doi chieu.
PURGEABLE_BOT_STATUSES = {"RESOLVED", RECONCILED_BOT_STATUS}

# Gia tri that tren pending_work, do 2026-09-18 (20.000 dong):
#   "1.Hub không báo thiếu"          9.416  -> duoc xoa
#   (o trong)                        9.156  -> giu
#   "3.Hub báo thiếu đúng timeline"  1.412  -> giu
#   "2.Hub báo thiếu trễ"               16  -> giu
# So khop tuyet doi (chi bo qua hoa/thuong va khoang trang thua): nhan dan bi doi
# thi khong xoa gi ca - huong an toan.
RECONCILED_PURGE_BAO_THIEU = "1.hub không báo thiếu"


def _khop_bao_thieu_duoc_xoa(gia_tri):
    return " ".join(str(gia_tri or "").split()).casefold() == RECONCILED_PURGE_BAO_THIEU


def purge_expired_work_rows(
    worksheet, headers, service, retention_days=WORK_RETENTION_DAYS, values=None,
    dry_run=False,
):
    """Xoa dong da nam tren sheet du lau, va don luon dau vet cua no trong SQLite.

    dry_run=True: tinh het roi in ra, KHONG xoa gi - sheet lan SQLite deu nguyen.
    Dung chung mot than ham voi lan chay that, de con so xem truoc khong bao gio
    lech voi thuc te (PURGE_WORK_NOW.py goi vao day).

    KHONG dung published_at nua. Do duoc 2026-09-14: 25.953 trong 31.933 dong
    khong co published_at (dong ket qua da bi compact_pending_result_history xoa),
    va 0 dong nao trong so con lai qua han - vi published_at bi dong dau lai moi
    lan xuat. Nen ham nay chua tung xoa duoc mot dong nao, va sheet phinh tu
    17.090 len 31.933 dong trong hai ngay.

    Arrived time nam ngay tren sheet, luon co, va khong bao gio bi ghi de.
    """
    now = datetime.now()
    cutoff = now - timedelta(
        days=max(0, int(retention_days)), hours=WORK_RETENTION_EXTRA_HOURS
    )
    existing = read_existing_rows(worksheet, headers, values=values)

    # Chi dua vao trang thai PENDING that su trong SQLite, khong tin mot minh o
    # BOT Status tren sheet: o do co the vua bi dong nham (xem CLOSE_CONFIRM_CYCLES).
    con_pending = set(load_active_pending_rows(service))

    rows_to_delete = []
    shipments_to_purge = []
    giu_vi_con_pending = 0
    giu_vi_trang_thai = 0
    giu_vi_bao_thieu = 0
    for shipment_id, (row_number, row) in existing.items():
        if shipment_id in con_pending:
            giu_vi_con_pending += 1
            continue
        bot_status = str(row.get("BOT Status") or "").strip().upper()
        if bot_status == "PENDING":
            giu_vi_con_pending += 1
            continue
        # Danh sach trang: chi RESOLVED moi duoc don. RECONCILED, o trong, hay bat
        # ky trang thai nao khac deu o lai.
        if bot_status not in PURGEABLE_BOT_STATUSES:
            giu_vi_trang_thai += 1
            continue
        # RECONCILED chi duoc xoa khi Hub KHONG bao thieu (ops 2026-09-18).
        if bot_status == RECONCILED_BOT_STATUS and not _khop_bao_thieu_duoc_xoa(
            row.get("status_bao_thieu")
        ):
            giu_vi_bao_thieu += 1
            continue
        arrived_at = parse_deadline(row.get("Arrived time"))
        # Khong doc duoc gio xe ve thi GIU. Xoa dua tren mot o khong doc duoc la
        # cach nhanh nhat de mat sach du lieu.
        if arrived_at is None or arrived_at > cutoff:
            continue
        rows_to_delete.append(row_number)
        shipments_to_purge.append(shipment_id)

    tong_dong = max(1, len(existing))
    if len(rows_to_delete) > PURGE_MAX_ROWS_PER_CYCLE:
        print(
            f"pending_work cleanup DUNG LAI: dinh xoa {len(rows_to_delete):,} dong, "
            f"vuot tran {PURGE_MAX_ROWS_PER_CYCLE:,}/vong. Khong xoa gi ca."
        )
        return 0
    if len(rows_to_delete) / tong_dong > PURGE_MAX_SHARE_OF_SHEET:
        print(
            f"pending_work cleanup DUNG LAI: dinh xoa {len(rows_to_delete):,}/{tong_dong:,} "
            f"dong ({len(rows_to_delete) / tong_dong:.0%}), vuot nguong "
            f"{PURGE_MAX_SHARE_OF_SHEET:.0%}. Khong xoa gi ca."
        )
        return 0

    if dry_run:
        print(
            f"[XEM TRUOC] se xoa {len(rows_to_delete):,}/{tong_dong:,} dong "
            f"({len(rows_to_delete) / tong_dong:.1%}) voi moc {retention_days} ngay.\n"
            f"           giu {giu_vi_con_pending:,} dong con PENDING, "
            f"{giu_vi_trang_thai:,} dong trang thai khac, "
            f"{giu_vi_bao_thieu:,} dong RECONCILED ma Hub CO bao thieu (hoac o trong).\n"
            f"           moc cat: Arrived time <= {cutoff:%Y-%m-%d %H:%M:%S} "
            f"(= {retention_days} ngay + {WORK_RETENTION_EXTRA_HOURS}h).\n"
            "           KHONG xoa gi ca."
        )
        return 0

    deleted = delete_rows(worksheet, rows_to_delete)
    if deleted:
        print(
            f"pending_work cleanup: xoa {deleted:,} dong RESOLVED / RECONCILED-khong-bao-thieu "
            f"da o tren sheet qua {retention_days} ngay (giu {giu_vi_con_pending:,} dong con PENDING, "
            f"{giu_vi_trang_thai:,} dong trang thai khac, "
            f"{giu_vi_bao_thieu:,} dong RECONCILED ma Hub co bao thieu)"
        )
    # Chi don lich su khi sheet da xoa DU. Neu xoa dang chung, ta khong biet chac
    # ma don nao con o lai, nen de nguyen: vong sau doc lai sheet va tinh lai tu
    # dau. Phan don DB gan nhu khong ton gi (do 2026-09-14: 55 dong), hoan mot
    # vong khong mat mat gi.
    if shipments_to_purge and deleted == len(rows_to_delete):
        purge_settled_history(service, shipments_to_purge)
    elif shipments_to_purge:
        print(
            f"pending_work cleanup: xoa sheet chua tron ({deleted:,}/{len(rows_to_delete):,}), "
            "hoan phan don lich su trong SQLite sang vong sau."
        )
    return deleted


def purge_settled_history(service, shipment_ids):
    """Xoa candidate + ket qua cua nhung don da roi khoi sheet.

    Phai xoa CA HAI. Bo chon lo lay candidate nao CHUA co dong ket qua, nen neu
    chi xoa ket qua thi don do lai du dieu kien de BOT 3 cham lai - nguoc han y
    dinh. Xoa candidate moi la thu that su dung viec cham lai.

    Van giu nguyen lt_pending_status_changed: bang do la nhat ky, khong bi don, va
    la thu duy nhat con lai de truy lai lich su khi co su co.
    """
    ids = sorted({str(s).strip() for s in shipment_ids if str(s).strip()})
    if not ids:
        return 0

    candidate_table = (
        f"`{SHEET_EXPORT.BIGQUERY_PROJECT_ID}.{SHEET_EXPORT.BIGQUERY_DATASET_ID}"
        f".{UNIFIED_PENDING.BIGQUERY_CANDIDATE_TABLE_ID}`"
    )
    result_table = (
        f"`{SHEET_EXPORT.BIGQUERY_PROJECT_ID}.{SHEET_EXPORT.BIGQUERY_DATASET_ID}"
        f".{SHEET_EXPORT.PENDING_RESULT_TABLE_ID}`"
    )

    da_xoa = 0
    for start in range(0, len(ids), 500):
        lo = ids[start:start + 500]
        trong_ngoac = ", ".join("'" + v.replace("'", "''") + "'" for v in lo)

        # Hai bang xoa theo HAI COT khac nhau, va do la chuyen toc do chu khong
        # phai tuy tien:
        #
        #   lt_pending_result   : 17.446 dong, khong co chi muc nao tren
        #                         shipment_id nhung bang nho nen quet ca bang van
        #                         re. Giu chot pending_status ngay trong SQL.
        #   lt_pending_candidate: 1.819.202 dong. Do 2026-09-14 tren mot lo 500 ma
        #                         don: loc theo shipment_id mat 92,7s, loc theo
        #                         order_number mat 0,23s - vi chi co
        #                         idx_pending_candidate_order. Bay lo moi vong la
        #                         chenh nhau 11 phut, nen phai dung order_number.
        #
        # Voi don le thi order_number chinh la ma van don, trung voi cot Shipment
        # ID tren sheet. Dong nao co order_number khac (marker cap TO) thi khong
        # nam trong danh sach nay.
        #
        # Danh sach dua vao day da duoc loc bo don con PENDING o tren roi
        # (con_pending + BOT Status), nen cau IN don gian la du an toan.
        for bang, cot, chot in (
            (result_table, "shipment_id", "AND COALESCE(pending_status, '') != 'PENDING'"),
            (candidate_table, "order_number", ""),
        ):
            service.jobs().query(
                projectId=SHEET_EXPORT.BIGQUERY_PROJECT_ID,
                body={
                    "query": f"""
                    DELETE FROM {bang}
                     WHERE {cot} IN ({trong_ngoac})
                       {chot}
                    """,
                    "useLegacySql": False,
                },
            ).execute()
        da_xoa += len(lo)
    print(f"SQLite cleanup: don candidate + ket qua cua {da_xoa:,} don da roi sheet")
    return da_xoa


def parse_deadline(value):
    text = str(value or "").strip()
    if not text:
        return None
    # The Sheet is Vietnamese-locale, so slash dates are DAY first. %m/%d used
    # to be tried first, which silently misread every date whose day is <= 12:
    # '08/09/2026' (8 Sep) became 9 Aug, roughly two months in the past. Only
    # days >= 13 came out right, because %m/%d fails on month 13 and fell
    # through. Proven 2026-09-08 against the ISO 'Arrived time' column, since
    # deadline_bao_thieu is Arrived + 36h: reading day-first matched exactly on
    # 3,786 of 5,787 rows (the rest are team-edited deadlines), month-first
    # matched 0 and produced offsets of -1,404h and -3,516h.
    #
    # Effect of the bug: deadlines resolved to dates weeks in the past, so
    # nearly every row passed the `deadline < cutoff` test at once and the
    # `candidates.sort(key=deadline)` priority order was meaningless. Rows were
    # assigned earlier than intended, not later.
    #
    # %m/%d is kept as a last resort: it can only fire on a string day-first
    # cannot parse at all (day > 12 in the first field), which is unambiguous.
    for parser in (
        lambda: datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None),
        lambda: datetime.strptime(text, "%d/%m/%Y %H:%M:%S"),
        lambda: datetime.strptime(text, "%Y-%m-%d %H:%M:%S"),
        lambda: datetime.strptime(text, "%m/%d/%Y %H:%M:%S"),
    ):
        try:
            return parser()
        except ValueError:
            continue
    return None


def get_valid_staff(spreadsheet):
    try:
        worksheet = spreadsheet.worksheet(VALID_WORKSHEET_TITLE)
    except gspread.WorksheetNotFound:
        print("BOT 3 assign skipped: tab Valid does not exist")
        return []
    rows = _thu_lai_doc_sheet(worksheet.get_all_records, "tab Valid")
    staff = []
    for row in rows:
        name = str(row.get(VALID_STAFF_COLUMN) or "").strip()
        if name and name not in staff:
            staff.append(name)
    if not staff:
        print("BOT 3 assign skipped: Valid!list_chia_don has no staff")
    return staff


def assign_pending_work(worksheet, headers, spreadsheet, values=None,
                        eligible_times=None):
    staff = get_valid_staff(spreadsheet)
    required = {"PIC_cctv", "status_cctv_deli", "deadline_cctv", "date_chia_don", "LT ID", "BOT Status"}
    missing = [name for name in required if name not in headers]
    if not staff or missing:
        print(f"BOT 3 assign skipped: missing columns {missing}" if missing else "")
        return 0

    values = sheets_get_all_values_with_retry(worksheet) if values is None else values
    records = []
    for row_number, values_row in enumerate(
        values[WORK_DATA_START_ROW - 1:],
        start=WORK_DATA_START_ROW,
    ):
        row = {}
        for index, header in enumerate(headers):
            if header and header not in row:
                row[header] = values_row[index] if index < len(values_row) else ""
        row["_row_number"] = row_number
        records.append(row)

    # Workload steers who gets the next LT group, so it must count work still
    # OUTSTANDING, not work merely assigned.
    #
    # `status_bao_thieu` is the marker, on ops's instruction 2026-09-10: a person
    # who has worked the row records one of `1.Hub không báo thiếu`,
    # `2.Hub báo thiếu trễ` or `3.Hub báo thiếu đúng timeline` there. Any
    # non-blank value counts - never match those strings literally, they carry no
    # space after the digit and ops can add options. The column is in
    # MANUAL_COLUMNS and no bot code ever writes it, so a value in it can only
    # have come from a person.
    #
    # date_cctv / date_nhan_xet are kept as a secondary marker. They are stamped
    # by the team's own onEdit, which never fires for the bot's API writes.
    #
    # Ops reported the counts were wrong and assignment had gone lopsided.
    # Measured on the live sheet the same day, of 419 assigned PENDING rows:
    #   417 already had status_bao_thieu, only 2 were genuinely outstanding
    #   300 also had a date; 117 had status_bao_thieu but no date
    # So the old count said Đạt 222 / Nhân 158 / Duyên 15 when the real figures
    # were 0 / 0 / 0 - the least-loaded rule was steering by noise, and kept
    # skipping people who were completely free.
    handled_columns = [
        name for name in ("status_bao_thieu", "date_cctv", "date_nhan_xet")
        if name in headers
    ]
    if not handled_columns:
        # Not fatal - assignment still runs - but say so, because without these
        # the count silently reverts to "assigned" and skews the split again.
        print(
            "BOT 3 assign warning: none of status_bao_thieu / date_cctv / "
            "date_nhan_xet is on the sheet, so workload counts assigned rows "
            "instead of outstanding ones"
        )

    # Bulky is counted a second time, on its own, because it is balanced on its
    # own count rather than on the shared one (ops rule 2026-09-11, "chia đều
    # những đơn bulky"). One shared counter had let bulky pile onto whoever
    # happened to be light on sorting work; measured the same day, of the
    # outstanding PENDING rows Huyền held 19 bulky out of 33 while Nhung held
    # 0 out of 115.
    workload = {name: 0 for name in staff}
    bulky_workload = {name: 0 for name in staff}
    for row in records:
        pic = str(row.get("PIC_cctv") or "").strip()
        # PIC_cctv is the assignment marker. status_cctv_deli is reserved for
        # the two CCTV outcomes and may legitimately remain blank.
        if pic not in workload:
            continue
        if str(row.get("BOT Status") or "").strip().upper() != "PENDING":
            continue
        if any(str(row.get(name) or "").strip() for name in handled_columns):
            continue
        workload[pic] += 1
        if normalize_text(row.get("Type")) == "bulky":
            bulky_workload[pic] += 1

    cutoff = datetime.now().replace(hour=12, minute=0, second=0, microsecond=0) + timedelta(days=1)
    candidates = []
    now = datetime.now()
    in_assign_shift = ASSIGN_SHIFT_OPEN_HOUR <= now.hour < ASSIGN_SHIFT_CLOSE_HOUR
    held_off_shift = 0

    # Cua so B duoc tha theo dot, nen phai biet CA LO co bao nhieu don.
    #
    # Dem MOI dong cua cua so B, khong loc theo BOT Status.  Lo phai dung yen tu
    # 12:00, neu khong thi moi vong lai dem lai tren so don con PENDING va han
    # muc tut theo.  Vi du cua ops: lo 90 don, 13h chia 30, sau do 20 don tu
    # chuyen NOT_PENDING.  Dem theo don con PENDING thi lo thanh 70, han muc 14h
    # la 47, tru 30 da chia chi con 17 - trong khi y dinh la 30.  Dong da xong
    # van nam tren sheet 5 ngay nen dem ca chung la dem duoc lo goc.
    eligible_times = eligible_times or {}
    b_da_chia = 0
    b_tong = 0
    for row in records:
        cua_so = collection_window(
            eligible_times.get(str(row.get("Shipment ID") or "").strip()), now
        )
        if cua_so != "B":
            continue
        b_tong += 1
        if str(row.get("PIC_cctv") or "").strip():
            b_da_chia += 1
    b_han_muc = wave_quota(b_tong, now)
    b_con_duoc = max(0, b_han_muc - b_da_chia)
    b_giu_lai = 0

    for row in records:
        if str(row.get("BOT Status") or "").strip().upper() != "PENDING":
            continue
        if str(row.get("PIC_cctv") or "").strip():
            continue

        # Lich cua sep, 2026-09-11. Don cua cua so B nam cho den dot 13h/14h/15h,
        # moi dot mot phan ba. Ap dung cho MOI loai don, bulky va TO thieu nguyen
        # bao cung cho dot - ops chon nhu vay.
        cua_so = collection_window(
            eligible_times.get(str(row.get("Shipment ID") or "").strip()), now
        )
        if cua_so is None:
            # Cong bo sau 12:00 hom nay: cho dot 08:00 ngay mai.
            continue
        # Han muc cua cua so B KHONG tru o day. Don o day con phai qua cua so
        # tuoi 36-60h/60-84h va cong tat ca cac chot ben duoi; tru som thi mot
        # don bi loai sau do van an mat mot suat cua dot. Cat sau khi da sap xep.
        row["_cua_so"] = cua_so
        # deadline_cctv is a team-maintained optional override. New rows do
        # not populate it, so fall back to the bot-calculated shortage
        # deadline instead of leaving the entire work queue unassigned.
        deadline = (
            parse_deadline(row.get("deadline_cctv"))
            or parse_deadline(row.get("deadline_bao_thieu"))
        )

        full_missing_to = is_full_missing_to(
            row.get("Số đơn thiếu / đơn trong TO"),
            row.get("Type"),
        )
        # Bulky and whole-missing TOs go to a PIC the moment they reach the
        # Sheet - there is no CCTV judgement to wait for, the team just has to
        # go and look. They therefore skip BOTH the deadline<cutoff gate and
        # the 60h..84h window. Ops rule, confirmed 2026-09-08.
        #
        # Note is_full_missing_to() returns False for Bulky by definition
        # (it excludes work_type == "bulky"), so Bulky must be named here
        # separately or it would fall through to the partial-missing path.
        #
        # This is deliberately checked BEFORE status_cctv_deli: a hand-typed
        # '02. Không cung cấp cam' on one of these rows used to push it into
        # the 60h window. The order type decides, not the CCTV cell.
        assign_on_sight = full_missing_to or normalize_text(row.get("Type")) == "bulky"

        # Stays None for assign-on-sight rows: they have no closing time, so
        # the off-shift safety net below can never apply to them.
        assignment_expires_at = None
        in_assignment_window = False

        # Only partial-missing rows get here with will_be_no_cctv able to be
        # True, so the old "and not full_missing_to" guard is now redundant.
        will_be_no_cctv = not assign_on_sight and (
            is_cctv_not_provided(row.get("status_cctv_deli"))
            or is_auto_no_cctv_type(row.get("Type"))
        )
        if will_be_no_cctv:
            # No-cam orders are assigned inside a 24h window measured from when
            # the LT reached the receiving hub (arrived_time). Fall back to
            # deadline_bao_thieu - 36h when the Arrived time cell is unreadable.
            # Was 84h..108h; moved 24h earlier on 2026-09-07 at the team's
            # request. The window keeps its 24h width, it just opens sooner, so
            # these orders reach a PIC while there is still room to act on them.
            #
            # 2026-09-09: the hubs in EARLY_WINDOW_RECONCILE_HUBS open another
            # day earlier still, at 36h..60h. Width is unchanged, so the
            # off-hours delay analysis of the 60h..84h window carries over.
            arrived_at = parse_deadline(row.get("Arrived time"))
            if arrived_at is None:
                shortage_deadline = parse_deadline(row.get("deadline_bao_thieu"))
                arrived_at = (
                    shortage_deadline - timedelta(hours=36) if shortage_deadline else None
                )
            open_hours, close_hours = assignment_window_hours(
                row.get("Hub/SOC cần đối soát")
            )
            assignment_ready_at = (
                arrived_at + timedelta(hours=open_hours) if arrived_at else None
            )
            assignment_expires_at = (
                arrived_at + timedelta(hours=close_hours) if arrived_at else None
            )
            if (
                assignment_ready_at is None
                or assignment_expires_at is None
                or now < assignment_ready_at
                or now >= assignment_expires_at
            ):
                continue
            in_assignment_window = True

        # "Only hand out work that is due by noon tomorrow." This gate predates
        # the assignment windows and is calibrated for the 60h..84h one, whose
        # opening moment IS deadline_cctv - so it never bites there (measured
        # 2026-09-09: 26 of 26 in-window rows passed it). The 36h..60h window
        # opens a full 24h before deadline_cctv, so the gate silently held those
        # rows back for another day: 8 in-window Big-SOC rows were stuck, and
        # SPXVN068462752479 would have waited until 53.5h instead of the ~38h
        # ops asked for.
        #
        # A row already inside its assignment window has its timing decided by
        # that window, so this older gate no longer applies to it. Rows with no
        # window at all - assign-on-sight, or a team-set CCTV status - keep the
        # gate exactly as before.
        if not assign_on_sight and not in_assignment_window:
            if deadline is None or deadline >= cutoff:
                continue

        # Reaching here with assign_on_sight means the row is a candidate
        # regardless of its deadline, so it may still be unparseable/absent;
        # datetime.min keeps it sortable and puts it at the front of the queue.
        deadline = deadline or datetime.min

        # Outside the assignment shift nothing is handed out: nobody is at a
        # desk, and by morning most of these rows have left the queue on their
        # own. The one exception is a row whose assignment window is about to
        # close, which would otherwise be lost for good.
        if not in_assign_shift:
            if (
                assignment_expires_at is None
                or now < assignment_expires_at - timedelta(hours=ASSIGN_OFF_SHIFT_URGENT_HOURS)
            ):
                held_off_shift += 1
                continue

        candidates.append((deadline, str(row.get("LT ID") or ""), row))
    candidates.sort(key=lambda item: (item[0], item[1], item[2]["_row_number"]))

    # Cat cua so B xuong dung han muc cua dot dang toi. Danh sach da sap theo
    # deadline tang dan, tuc DON CU NHAT TRUOC - dung thu tu ops chon, va cung la
    # thu tu an toan cho deadline cat cam.  Don cua so A khong bi cat.
    giu = []
    con = b_con_duoc
    for muc in candidates:
        if muc[2].get("_cua_so") != "B":
            giu.append(muc)
        elif con > 0:
            con -= 1
            giu.append(muc)
        else:
            b_giu_lai += 1
    candidates = giu

    updates = []
    full_to_assigned = 0
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    assigned_by_group = {}
    for row in records:
        pic = str(row.get("PIC_cctv") or "").strip()
        if pic not in workload:
            continue
        assigned_by_group.setdefault(assignment_group_key(row), pic)

    candidate_groups = {}
    for deadline, lt_id, row in candidates:
        group_key = assignment_group_key(row)
        group = candidate_groups.setdefault(group_key, {"deadline": deadline, "rows": []})
        group["deadline"] = min(group["deadline"], deadline)
        group["rows"].append(row)

    ordered_groups = sorted(
        candidate_groups.items(),
        key=lambda item: (
            item[1]["deadline"],
            # assignment_group_key() returns a string; this used to index into a
            # (LT, TO) tuple and would now sort on its first two characters.
            item[0],
            min(row["_row_number"] for row in item[1]["rows"]),
        ),
    )
    for group_key, group in ordered_groups:
        group_rows = group["rows"]
        # A group is bulky only when every row in it is, so a mixed group would
        # still be balanced on the shared counter. Nothing is mixed today:
        # measured 2026-09-11, all 83 bulky rows on the sheet have a blank LT
        # ID, so assignment_group_key() falls through to TO ID and each bulky
        # row is its own single-row group. Not one group held both kinds. That
        # is why balancing bulky separately costs the "one LT, one person" rule
        # nothing - a bulky row is never inside somebody else's LT.
        bulky_group = all(
            normalize_text(row.get("Type")) == "bulky" for row in group_rows
        )
        chosen = assigned_by_group.get(group_key)
        if chosen not in workload:
            if bulky_group:
                # Fewest bulky first, total load only as a tiebreak, so bulky
                # spreads evenly even when the sorting queue is lopsided.
                chosen = min(
                    staff,
                    key=lambda name: (
                        bulky_workload[name], workload[name], staff.index(name)
                    ),
                )
            else:
                chosen = min(staff, key=lambda name: (workload[name], staff.index(name)))
        # Bulky still counts toward the shared total, so a heavy bulky day does
        # reduce how many LT groups that person is handed next.
        workload[chosen] += len(group_rows)
        if bulky_group:
            bulky_workload[chosen] += len(group_rows)
        assigned_by_group[group_key] = chosen

        for row in group_rows:
            row_number = row["_row_number"]
            full_missing_to = is_full_missing_to(
                row.get("Số đơn thiếu / đơn trong TO"),
                row.get("Type"),
            )
            if full_missing_to:
                full_to_assigned += 1
            cctv_status = (
                CCTV_NOT_PROVIDED_STATUS
                if is_auto_no_cctv_type(row.get("Type"))
                and not full_missing_to
                else ""
            )
            for header, value in (
                ("PIC_cctv", chosen),
                ("status_cctv_deli", cctv_status),
                ("date_chia_don", now_text),
            ):
                updates.append({
                    "range": gspread.utils.rowcol_to_a1(row_number, headers.index(header) + 1),
                    "values": [[value]],
                })

    for start in range(0, len(updates), 300):
        sheets_batch_update_with_retry(worksheet, updates[start:start + 300])
    if held_off_shift:
        print(
            f"BOT 3 held {held_off_shift} order(s) until the next "
            f"{ASSIGN_SHIFT_OPEN_HOUR:02d}:00 pass: outside the "
            f"{ASSIGN_SHIFT_OPEN_HOUR:02d}:00-{ASSIGN_SHIFT_CLOSE_HOUR:02d}:00 "
            "assignment shift"
        )
    print(
        f"BOT 3 assigned {len(candidates)} order(s) in {len(candidate_groups)} LT group(s) "
        f"(full-missing TO: {full_to_assigned}): {workload}"
    )
    print(
        f"BOT 3 lich chia: dot {waves_released(now)}/{len(ASSIGN_WAVE_HOURS)} "
        f"({'/'.join(f'{h}h' for h in ASSIGN_WAVE_HOURS)}) | cua so B {b_tong} don, "
        f"han muc den gio {b_han_muc}, da chia {b_da_chia}, giu lai {b_giu_lai}"
    )
    # Printed separately because the shared figure above hides it: bulky is a
    # small slice of a big queue, and it is the slice ops watches for balance.
    print(f"BOT 3 bulky outstanding per PIC: {bulky_workload}")
    return len(candidates)


class _phase:
    """Print how long each major step of a cycle takes, to locate the bottleneck."""

    def __init__(self, name):
        self.name = name

    def __enter__(self):
        self._started = time.monotonic()
        return self

    def __exit__(self, *exc):
        print(f"[BOT 3 timing] {self.name}: {time.monotonic() - self._started:.1f}s")
        return False


def run_once(
    assign=False,
    retention_days=WORK_RETENTION_DAYS,
    reset_work=False,
    batch_limit=5000,
    fetch_cogs=True,
    cogs_limit=250,
):
    started_at = datetime.now()
    print("=" * 80)
    print(f"BOT 3 Work started: {started_at.isoformat(timespec='seconds')}")
    print(f"BOT 3 Admin Chrome profile: {BOT3_ADMIN_PROFILE}")
    try:
        migrate_legacy_candidate_queue()
    except sqlite3.OperationalError as error:
        if "locked" not in str(error).lower():
            raise
        print("BOT 3 queue migration deferred: SQLite is busy; retry next cycle")
    with _phase("process_due_pending_slice (final BatchSearch/Tracking Info)"):
        process_due_pending_slice(batch_limit=batch_limit)
    with _phase("publish_pending_outputs (COGS + pending sheets)"):
        publish_pending_outputs(fetch_cogs=fetch_cogs, cogs_limit=cogs_limit)

    service = sqlite_store.create_service()
    ensure_cogs_table(service)
    ensure_reconcile_cache_table(service)

    with _phase("load + enrich active pending rows"):
        active_rows = load_active_pending_rows(service)
        enrich_reconciliation_context(service, active_rows)
        cogs_cache = load_cogs_cache(service)
    spreadsheet = open_spreadsheet()
    worksheet = get_or_create_worksheet(spreadsheet)
    headers = ensure_headers(worksheet)
    # Each sheet download below is a full-tab HTTP GET. Read once before the
    # sync writes and once after, and pass the snapshot to every helper instead
    # of re-downloading the tab six times per cycle.
    pre_sync_values = None
    if reset_work:
        clear_pending_work_rows(worksheet)
    else:
        pre_sync_values = sheets_get_all_values_with_retry(worksheet)
        removed = deduplicate_pending_work_rows(worksheet, headers, values=pre_sync_values)
        if removed:
            # Row layout changed; the snapshot is stale.
            pre_sync_values = None
    # load_resolved_shipments() is no longer read here - see sync_pending_work().
    with _phase("sync_pending_work (write pending_work rows)"):
        sync_pending_work(
            worksheet, headers, active_rows, cogs_cache,
            values=pre_sync_values,
        )

    # One fresh read after the sync writes for the remaining steps. The process
    # lock already prevents a concurrent run, so the second historical dedupe
    # pass is no longer needed here.
    # Neu van khong doc duoc sau khi da thu lai: BO QUA phan chia don + don dep
    # cua vong nay chu KHONG nem loi. Toan bo viec nang (BatchSearch, publish,
    # sync_pending_work) da xong va da luu; huy ca vong o day chi lam mat thanh
    # qua do. Chia don chay lai sau 15 phut nua.
    try:
        post_sync_values = sheets_get_all_values_with_retry(worksheet)
    except APIError as exc:
        status_code = getattr(getattr(exc, "response", None), "status_code", None)
        print(
            f"Doc lai pending_work that bai (HTTP {status_code}). Bo qua chia don + "
            "don dep vong nay, phan da ghi van giu nguyen."
        )
        elapsed = (datetime.now() - started_at).total_seconds()
        print(f"BOT 3 Work done (thieu buoc chia don). Elapsed: {elapsed:.0f}s")
        return elapsed

    with _phase("assign + cleanup"):
        # Ba buoc duoi day deu chay lai duoc o vong sau va deu chi ghi len sheet.
        # Mot loi Google o day khong duoc phep huy ca vong: toan bo phan nang
        # (BatchSearch, publish, sync_pending_work) da xong va da luu truoc do,
        # va mot vong chet con ton them 900 giay nam khong - xem sleep o main().
        try:
            if assign:
                assign_pending_work(
                    worksheet, headers, spreadsheet,
                    values=post_sync_values,
                    # Lich chia theo dot phan loai don bang eligible_at (arrived+36h),
                    # KHONG phai published_at - xem chu thich trong
                    # load_pending_eligible_times() ve vi sao published_at khong dung yen.
                    eligible_times=load_pending_eligible_times(service),
                )
        except APIError as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            print(f"Chia don bo qua vong nay (HTTP {status_code}).")
        # Sheet can coerce a ratio such as 1/1 into a date during assignment.
        # Enforce the full-missing rule last using the calculated SQLite context.
        try:
            clear_full_missing_to_cctv_status(worksheet, headers, active_rows, values=post_sync_values)
            set_sorting_no_cctv_status(worksheet, headers, active_rows, values=post_sync_values)
        except APIError as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            print(f"Cap nhat o status_cctv_deli bo qua vong nay (HTTP {status_code}).")
        # Don dep la buoc CUOI CUNG va la buoc it khan nhat. Mot loi o day khong
        # duoc phep huy ca vong - moi thu trong vong deu da ghi xong roi.
        try:
            purge_expired_work_rows(
                worksheet, headers, service,
                retention_days=retention_days, values=post_sync_values,
            )
        except APIError as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            print(f"pending_work cleanup bo qua vong nay (HTTP {status_code}).")

    elapsed = (datetime.now() - started_at).total_seconds()
    print(f"BOT 3 Work done. Elapsed: {elapsed:.0f}s")
    return elapsed


def acquire_bot3_lock():
    """Allow only one BOT 3 process to update pending_work at a time."""
    handle = open(BOT3_LOCK_FILE, "a+b")
    if handle.tell() == 0:
        handle.write(b"0")
        handle.flush()
    handle.seek(0)
    try:
        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
    except OSError:
        handle.close()
        return None
    return handle


def release_bot3_lock(handle):
    if handle is None:
        return
    try:
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
    finally:
        handle.close()


def main():
    parser = argparse.ArgumentParser(description="BOT 3 - Sync and optionally assign pending_work")
    parser.add_argument("--once", action="store_true", help="Chay mot vong roi dung.")
    parser.add_argument("--interval-minutes", type=int, default=30, help="Thoi gian nghi giua hai vong.")
    parser.add_argument("--assign", action="store_true", help="Chia don can deadline_cctv theo Valid!list_chia_don.")
    parser.add_argument("--batch-limit", type=int, default=5000, help="So candidate toi da moi lat; xong lat se xuat Sheet ngay.")
    parser.add_argument("--skip-cogs", action="store_true", help="Khong goi API COGS khi xuat Pending.")
    parser.add_argument("--cogs-limit", type=int, default=250, help="So don Pending toi da can bo sung COGS moi vong.")
    parser.add_argument(
        "--reset-work",
        action="store_true",
        help="Xoa toan bo dong du lieu pending_work, giu hang tieu de va dong bo lai Pending hien tai.",
    )
    parser.add_argument("--retention-days", type=int, default=WORK_RETENTION_DAYS, help="Xoa dong pending_work da xuat Sheet tu N ngay truoc.")
    parser.add_argument("--stop-on-error", action="store_true")
    args = parser.parse_args()

    while True:
        elapsed = 0
        lock_handle = acquire_bot3_lock()
        if lock_handle is None:
            print("BOT 3 is already running; skip this overlapping cycle.")
            if args.once:
                return
            time.sleep(max(1, args.interval_minutes * 60))
            continue
        try:
            elapsed = run_once(
                assign=args.assign,
                retention_days=args.retention_days,
                reset_work=args.reset_work,
                batch_limit=args.batch_limit,
                fetch_cogs=not args.skip_cogs,
                cogs_limit=args.cogs_limit,
            )
            # Reset is a one-time operator action, even if this process is
            # accidentally started without --once.
            args.reset_work = False
        except Exception:
            print("BOT 3 Work failed:")
            traceback.print_exc()
            if args.stop_on_error:
                raise
        finally:
            release_bot3_lock(lock_handle)
        if args.once:
            return
        sleep_seconds = max(0, args.interval_minutes * 60 - int(elapsed))
        print(f"BOT 3 Work sleep {sleep_seconds}s before next cycle.")
        time.sleep(sleep_seconds)


if __name__ == "__main__":
    main()
