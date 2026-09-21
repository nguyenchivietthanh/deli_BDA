r"""BatchSearch lai toan bo don PENDING dang tren sheet, go nhung don da het pending.

CHAY TAY. Khong goi Tracking Info - chi BatchSearch, ~500 don moi lenh.

Khac SWEEP_BATCHSEARCH.py o hai cho, va ca hai deu la de khong xoa nham don
pending that:

  1. KHONG ep ket qua ve PRECHECK. Tab pending_all chi hien ket qua FINAL, nen
     ep PRECHECK la rut luon ca don con pending khoi sheet.

  2. CHI GHI don BatchSearch noi NOT_PENDING. Don van PENDING thi de nguyen
     dong ket qua cu - khong ghi dong moi. Ghi de se dat lai checked_at, day lan
     hoi lai co Tracking Info cua BOT 3 lui them 180 phut, dung vao don dang can
     duoc go nhat.

Vi sao bo Tracking Info ma khong mat don nao: Tracking Info chi chay khi
BatchSearch da noi PENDING, va no chi co the doi PENDING thanh NOT_PENDING, khong
bao gio nguoc lai. Mot ket luan NOT_PENDING cua BatchSearch la ket luan cuoi.

PHAI TAT BOT 3 truoc: cung profile Chrome Admin, va cung ghi lt_pending_result.

    $env:BOT_DELI_TO_TASK_MODE = "1"
    python .\RECHECK_PENDING_BATCHSEARCH.py --xem-truoc
    python .\RECHECK_PENDING_BATCHSEARCH.py                       # don tren sheet
    python .\RECHECK_PENDING_BATCHSEARCH.py --nhom precheck       # don cho FINAL
    python .\RECHECK_PENDING_BATCHSEARCH.py --nhom ca-hai         # ca hai
"""
import argparse
import importlib.util
import msvcrt
import os
import sys
import time
from collections import Counter
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent

# Giong SWEEP_BATCHSEARCH.py: profile Admin cua BOT 3, dat truoc khi nap module.
BOT3_ADMIN_PROFILE = Path(
    os.getenv(
        "BOT_DELI_CHROME_USER_DATA_DIR_BOT3",
        str(Path(os.getenv("LOCALAPPDATA", "")) / "BOT_DELI_FMS_BROWSER_BOT3_ADMIN" / "User Data"),
    )
)
os.environ["BOT_DELI_CHROME_USER_DATA_DIR_ADMIN"] = str(BOT3_ADMIN_PROFILE)
os.environ["BOT_DELI_CHROME_PROFILE_ADMIN"] = os.getenv("BOT_DELI_CHROME_PROFILE_BOT3", "Default")
BOT3_LOCK_FILE = BASE_DIR / ".bot3_work.lock"


def load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


UP = load_module(BASE_DIR / "BOT-Deli_BDA_unified_pending_pipeline.py", "recheck_unified")


def don_pending_tren_sheet(service):
    """Candidate cua dung tap don dang hien tren pending_all.

    Cung dieu kien voi pending_query() trong export_pending_to_sheet.py: ket qua
    moi nhat PENDING, giai doan FINAL/FINAL_READY, sender BDA hoac TO_TRANSIT, da
    qua moc 36h.
    """
    cot = ", ".join("c." + f["name"] for f in UP.candidate_schema_fields())
    return UP.query_bq(
        service,
        f"""
        WITH latest AS (
          SELECT candidate_id, pending_status, processing_stage, source_type, sender, arrived_time,
                 ROW_NUMBER() OVER (PARTITION BY candidate_id ORDER BY checked_at DESC) AS rk
          FROM `{UP.BIGQUERY_PROJECT_ID}.{UP.BIGQUERY_DATASET_ID}.{UP.BIGQUERY_RESULT_TABLE_ID}`
          WHERE result_rule_version = '{UP.RESULT_RULE_VERSION}'
            AND order_number IS NOT NULL AND order_number != ''
            AND order_number != '{UP.SEQUENCE_MARKER_ORDER}'
        )
        SELECT {cot}
        FROM latest r
        JOIN `{UP.BIGQUERY_PROJECT_ID}.{UP.BIGQUERY_DATASET_ID}.{UP.BIGQUERY_CANDIDATE_TABLE_ID}` c
          ON c.candidate_id = r.candidate_id
        WHERE r.rk = 1
          AND r.pending_status = 'PENDING'
          AND (r.processing_stage IS NULL OR r.processing_stage IN ('FINAL_READY', 'FINAL'))
          -- TO_TRANSIT bi loai (2026-09-17): transit ket luan theo CA TO trong
          -- BOT 3. Cong cu nay xet tung don, xoa le vai don se lam phan con lai
          -- cua TO bi BOT 3 coi la "ca TO con thieu".
          AND r.source_type != 'TO_TRANSIT'
          AND LOWER(TRIM(r.sender)) = LOWER('{UP.SOC_CODE}')
          AND r.arrived_time IS NOT NULL
          AND DATETIME_ADD(r.arrived_time, INTERVAL {UP.PENDING_READY_AFTER_ARRIVED_HOURS} HOUR)
              <= CURRENT_DATETIME('Asia/Bangkok')
        """,
        fail_soft=True,
    )


def don_precheck_pending(service):
    """Don song sot dot quet precheck, da qua moc 32h, dang cho BOT 3 cham FINAL.

    Phan lon KHONG co dong ket qua nao - dot quet chi doi queue_stage cua chung.
    Ket luan PENDING cua chung da nhieu gio tuoi; kien nao da nhuc nhich tu do se
    duoc BatchSearch go luon, khong can toi Tracking Info cua BOT 3.
    """
    cot = ", ".join("c." + f["name"] for f in UP.candidate_schema_fields())
    return UP.query_bq(
        service,
        f"""
        SELECT {cot}
        FROM `{UP.BIGQUERY_PROJECT_ID}.{UP.BIGQUERY_DATASET_ID}.{UP.BIGQUERY_CANDIDATE_TABLE_ID}` c
        WHERE c.candidate_rule_version = '{UP.CANDIDATE_RULE_VERSION}'
          AND LOWER(TRIM(c.sender)) = LOWER('{UP.SOC_CODE}')
          AND c.order_number IS NOT NULL AND c.order_number != ''
          AND c.order_number != '{UP.SEQUENCE_MARKER_ORDER}'
          AND c.queue_stage = 'PRECHECK_PENDING'
          AND c.final_check_at IS NOT NULL
          AND c.final_check_at <= CURRENT_DATETIME('Asia/Bangkok')
        """,
        fail_soft=True,
    )


def lay_khoa():
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


def tra_khoa(handle):
    if handle is None:
        return
    try:
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
    finally:
        handle.close()


def main():
    parser = argparse.ArgumentParser(description="BatchSearch lai don PENDING tren sheet")
    parser.add_argument("--xem-truoc", action="store_true", help="Chi dem, khong goi FMS, khong ghi gi.")
    parser.add_argument(
        "--nhom", choices=("sheet", "precheck", "ca-hai"), default="sheet",
        help="sheet = don PENDING dang tren pending_all (mac dinh); "
             "precheck = don PRECHECK_PENDING da qua 32h; ca-hai = gop ca hai.",
    )
    args = parser.parse_args()

    if not UP.deferred_to_expand_mode():
        print("DUNG: BOT_DELI_TO_TASK_MODE chua bat. Dat = 1 roi chay lai.")
        return 1

    service = UP.create_bq_service()
    theo_id = {}
    if args.nhom in ("sheet", "ca-hai"):
        for c in don_pending_tren_sheet(service):
            theo_id[c["candidate_id"]] = c
        print(f"  nhom sheet    : {len(theo_id):,}")
    if args.nhom in ("precheck", "ca-hai"):
        truoc = len(theo_id)
        pc = don_precheck_pending(service)
        for c in pc:
            theo_id.setdefault(c["candidate_id"], c)
        print(f"  nhom precheck : {len(pc):,}  (them moi sau khi bo trung: {len(theo_id) - truoc:,})")
    candidates = list(theo_id.values())
    loai = Counter(c.get("source_type") for c in candidates)
    print("=" * 70)
    print(f"Tong don se BatchSearch ({args.nhom}): {len(candidates):,}   {dict(loai)}")
    print(f"So lenh BatchSearch can goi : {-(-len(candidates) // UP.BATCH_SIZE)} (moi lenh {UP.BATCH_SIZE} don)")
    print("=" * 70)
    if args.xem_truoc or not candidates:
        print("Xem truoc: khong goi FMS, khong ghi gi." if args.xem_truoc else "Khong co don nao.")
        return 0

    khoa = lay_khoa()
    if khoa is None:
        print("DUNG: BOT 3 dang chay (giu .bot3_work.lock). Tat BOT 3 roi chay lai.")
        return 1

    try:
        fms_session = UP.EXPORT_BOT.FmsSession(UP.ADMIN_ROLE)
        t0 = time.time()
        now = datetime.now()
        checked_at = now.strftime("%Y-%m-%d %H:%M:%S")
        ket_qua_go = []
        van_pending = 0
        khong_thay = 0
        ly_do = Counter()

        for i in range(0, len(candidates), UP.BATCH_SIZE):
            lo = candidates[i:i + UP.BATCH_SIZE]
            so_don = sorted({c["order_number"] for c in lo if c.get("order_number")})
            items = UP.search_tracking_batch(fms_session, so_don)
            theo_don = {
                UP.tracking_item_order_number(it): it
                for it in items if UP.tracking_item_order_number(it)
            }
            for c in lo:
                item = theo_don.get(c.get("order_number"))
                if not item:
                    # Khong co du lieu BatchSearch: KHONG ket luan gi, de nguyen.
                    khong_thay += 1
                    continue
                stage = UP.candidate_processing_stage(c, now)
                result = UP.enrich_result(c, item, checked_at, stage)
                if result.get("pending_status") == "NOT_PENDING":
                    ket_qua_go.append(result)
                    ly_do[result.get("result_reason") or ""] += 1
                else:
                    van_pending += 1
            print(f"  lo {i // UP.BATCH_SIZE + 1}: {len(lo)} don | go duoc {len(ket_qua_go):,} | "
                  f"van pending {van_pending:,} | {time.time() - t0:.0f}s")

        da_ghi = UP.load_rows_to_bq(service, ket_qua_go, UP.BIGQUERY_RESULT_TABLE_ID)
        # Ghi nhat ky NGAY, truoc khi BOT 3 compact bang ket qua - ham nay can ca
        # dong PENDING cu lan dong NOT_PENDING moi cung ton tai. Don chua tung len
        # sheet (nhom precheck) khong co dong PENDING cu nen khong sinh nhat ky -
        # dung, vi chung chua bao gio hien cho ops.
        su_kien = UP.capture_published_pending_status_changes(service)
        # Rut candidate da ket thuc khoi hang cho ngay, dung ham BOT 3 goi ngay
        # sau buoc ghi nhat ky. Chi xoa candidate, khong dung bang ket qua, nen
        # khong anh huong nhat ky vua ghi.
        if da_ghi:
            UP.prune_finalized_pending_candidates(service)

        print("\n" + "=" * 70)
        print(f"Kiem tra        : {len(candidates):,} don trong {time.time() - t0:.0f}s")
        print(f"GO khoi pending : {da_ghi:,}")
        print(f"Van PENDING     : {van_pending:,}  (khong ghi gi, de BOT 3 cham tiep co Tracking Info)")
        print(f"BatchSearch khong tra ve: {khong_thay:,}  (de nguyen)")
        print(f"Nhat ky doi trang thai  : {su_kien or 0:,} dong")
        if ly_do:
            print("\nLy do go:")
            for k, v in ly_do.most_common():
                print(f"  {v:>6,}  {k}")
        print("\nBat lai BOT 3. Don tren sheet se roi pending_all o lan xuat sheet ke tiep;"
              " don nhom precheck da rut khoi hang cho ngay.")
        print("=" * 70)
    finally:
        tra_khoa(khoa)
    return 0


if __name__ == "__main__":
    sys.exit(main())
