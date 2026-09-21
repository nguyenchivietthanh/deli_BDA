r"""Quet BatchSearch mot lan cho don ton - CHAY TAY, khong phai mot con bot.

Muc dich: don ~500.000 candidate da qua moc len sheet (arrived + 36h) ma chua
duoc check lan nao. Do 2026-09-14, chi 6,7-10,3% so don can toi Tracking Info;
90% con lai duoc BatchSearch ket luan NOT_PENDING va bien mat han. Bo Tracking
Info trong dot quet nay lam moi 12.000 don ton ~240s thay vi ~1.440s.

Bo Tracking Info KHONG lam mat don nao: trong ca nam nhanh cua
received_tag_at_bda_destination(), Tracking Info chi co the doi PENDING thanh
NOT_PENDING, khong bao gio nguoc lai. Don ma BatchSearch da noi NOT_PENDING thi
Tracking Info khong con gi de noi them.

Sau moi lot:
  - don NOT_PENDING  -> prune_finalized_pending_candidates() xoa han
  - don PENDING      -> queue_stage = PRECHECK_PENDING, next_check_at =
                        final_check_at (da qua) -> BOT 3 uu tien cham
                        Tracking Info cho chung o vong sau
  - don UNKNOWN      -> PRECHECK_UNKNOWN, hen lai sau 15 phut
Ket qua PRECHECK khong bao gio len sheet (export loc processing_stage
IN ('FINAL_READY','FINAL')), nen khong the do PENDING chua loc vao mat cac ban.

Tu 2026-09-19: CHAY SONG SONG VOI BOT 3 DUOC. Dot quet dung profile Chrome rieng
(cua LAY_COGS.py), khong gianh phien dang nhap voi BOT 3. Dung chay LAY_COGS.py
cung luc. Muon dung chung profile BOT 3 (chi khi BOT 3 da tat): --profile bot3.

Moc quet (--moc) quyet dinh khi nao BOT 3 goi Tracking Info cho don song sot:
  --moc precheck (+24h): don PENDING thanh PRECHECK_PENDING voi
      next_check_at = final_check_at, nen BOT 3 CHUA goi Tracking Info trong
      khoang 24-32h; no cham tu moc 32h tro di. Quet som nen loai bot som.
  --moc final (+32h) / eligible (+36h): don song sot da qua moc, BOT 3 lay ngay
      o vong sau va goi Tracking Info luon.

    $env:BOT_DELI_TO_TASK_MODE = "1"
    python .\SWEEP_BATCHSEARCH.py --dry-run
    python .\SWEEP_BATCHSEARCH.py --lot 25000
"""
import argparse
import importlib.util
import os
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent

# PHAI dat TRUOC khi nap module pipeline, giong BOT-Deli_BDA_run_bot3_work.py.
# Khong dat thi vai role "Admin" roi vao profile mac dinh cua BOT 2 - va BOT 2
# van dang chay. Hai tien trinh Chrome cung mot user-data-dir la hong phien dang
# nhap FMS cua ca hai. Dot quet muon profile Admin cua BOT 3, von dang ranh vi
# BOT 3 da duoc tat.
#
# MAC DINH 2026-09-19: dung lai PROFILE CUA LENH LAY COGS
# (BOT_DELI_FMS_BROWSER_COGS), da dang nhap san - dot quet chay SONG SONG voi
# BOT 3 ma khong gianh Chrome, va khong phai dang nhap them lan nua.
# Luu y: dung chay LAY_COGS.py cung luc voi dot quet, vi chung dung chung profile.
# Muon quay ve dung chung profile cua BOT 3 (chi khi BOT 3 DA TAT): --profile bot3.
SWEEP_PROFILE = Path(
    os.getenv(
        "BOT_DELI_CHROME_USER_DATA_DIR_SWEEP",
        str(Path(os.getenv("LOCALAPPDATA", "")) / "BOT_DELI_FMS_BROWSER_COGS" / "User Data"),
    )
)
BOT3_ADMIN_PROFILE = Path(
    os.getenv(
        "BOT_DELI_CHROME_USER_DATA_DIR_BOT3",
        str(Path(os.getenv("LOCALAPPDATA", "")) / "BOT_DELI_FMS_BROWSER_BOT3_ADMIN" / "User Data"),
    )
)
# argparse chay sau khi module pipeline da nap, ma bien moi truong PHAI dat truoc
# do - nen doc thang tu sys.argv o day.
_DUNG_PROFILE_BOT3 = "bot3" in [a.lower() for a in sys.argv]
PROFILE_DANG_DUNG = BOT3_ADMIN_PROFILE if _DUNG_PROFILE_BOT3 else SWEEP_PROFILE
os.environ["BOT_DELI_CHROME_USER_DATA_DIR_ADMIN"] = str(PROFILE_DANG_DUNG)
os.environ["BOT_DELI_CHROME_PROFILE_ADMIN"] = os.getenv("BOT_DELI_CHROME_PROFILE_BOT3", "Default")


def load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


UNIFIED = load_module(BASE_DIR / "BOT-Deli_BDA_unified_pending_pipeline.py", "sweep_unified")
EXPORT_BOT = UNIFIED.EXPORT_BOT


def dem_con_lai(service, mark):
    cot = UNIFIED.SWEEP_MARKS[mark]
    rows = UNIFIED.query_bq(
        service,
        f"""
        SELECT COUNT(*) AS con_lai, MIN(c.arrived_time) AS cu_nhat
        FROM `{UNIFIED.BIGQUERY_PROJECT_ID}.{UNIFIED.BIGQUERY_DATASET_ID}.{UNIFIED.BIGQUERY_CANDIDATE_TABLE_ID}` c
        WHERE c.candidate_rule_version = '{UNIFIED.CANDIDATE_RULE_VERSION}'
          AND LOWER(TRIM(c.sender)) = LOWER('{UNIFIED.SOC_CODE}')
          AND c.order_number IS NOT NULL
          AND c.order_number != ''
          AND c.order_number != '{UNIFIED.SEQUENCE_MARKER_ORDER}'
          AND c.arrived_time IS NOT NULL
          AND c.{cot} IS NOT NULL
          AND c.{cot} <= CURRENT_DATETIME('Asia/Bangkok')
          AND COALESCE(c.queue_stage, 'NEW') = 'NEW'
          AND c.prechecked_at IS NULL
          AND NOT EXISTS (
            SELECT 1
            FROM `{UNIFIED.BIGQUERY_PROJECT_ID}.{UNIFIED.BIGQUERY_DATASET_ID}.{UNIFIED.BIGQUERY_RESULT_TABLE_ID}` r
            WHERE r.result_rule_version = '{UNIFIED.RESULT_RULE_VERSION}'
              AND r.candidate_id = c.candidate_id
          )
        """,
        fail_soft=True,
    )
    row = (rows or [{}])[0]
    return EXPORT_BOT.to_int(row.get("con_lai"), default=0), row.get("cu_nhat")


def main():
    parser = argparse.ArgumentParser(description="Quet BatchSearch mot lan cho don ton")
    parser.add_argument("--lot", type=int, default=25000,
                        help="So candidate moi lot. Tran cung la 25000.")
    parser.add_argument("--so-lot", type=int, default=0,
                        help="Dung sau N lot. 0 = chay den khi het.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Chi dem va in, khong goi FMS, khong ghi gi.")
    parser.add_argument(
        "--moc", choices=sorted(UNIFIED.SWEEP_MARKS), default="eligible",
        help="Moc thoi gian de quet: precheck = xe ve +24h, final = +32h, "
             "eligible = +36h (mac dinh, dung moc len sheet).",
    )
    parser.add_argument(
        "--profile", choices=("sweep", "bot3"), default="sweep",
        help="sweep (mac dinh) = profile cua lenh LAY_COGS, da dang nhap san, chay song song "
             "duoc voi BOT 3. bot3 = dung chung profile cua BOT 3, CHI khi BOT 3 da tat.",
    )
    args = parser.parse_args()

    if not UNIFIED.deferred_to_expand_mode():
        print("DUNG: BOT_DELI_TO_TASK_MODE chua bat. Dat = 1 roi chay lai.")
        return 1

    service = UNIFIED.create_bq_service()
    UNIFIED.ensure_tables(service)
    print(f"Profile Chrome cua dot quet: {PROFILE_DANG_DUNG}")
    if args.profile == "sweep":
        print("  -> profile RIENG, khong gianh Chrome voi BOT 3. Lan dau phai dang nhap FMS bang tay.")
    else:
        print("  -> dung chung profile BOT 3. CHI chay khi BOT 3 da tat, neu khong ca hai deu hong phien.")

    gio_moc = {"precheck": 24, "final": 32, "eligible": 36}[args.moc]
    con_lai, cu_nhat = dem_con_lai(service, args.moc)
    print("=" * 72)
    print(f"Moc quet: {args.moc} = {UNIFIED.SWEEP_MARKS[args.moc]} (xe ve + {gio_moc}h)")
    print(f"Don da qua moc do ma chua check: {con_lai:,}")
    print(f"Xe ve cu nhat trong so do: {cu_nhat}")
    print("=" * 72)
    if args.dry_run:
        print("dry-run: dung tai day, khong goi FMS, khong ghi gi.")
        return 0
    if not con_lai:
        print("Khong con gi de quet.")
        return 0

    lot_size = max(1, min(args.lot, UNIFIED.MAX_BATCH_CANDIDATES_PER_CYCLE))
    fms_session = EXPORT_BOT.FmsSession(UNIFIED.ADMIN_ROLE)

    bat_dau = time.time()
    da_quet = 0
    lot_thu = 0
    while True:
        lot_thu += 1
        if args.so_lot and lot_thu > args.so_lot:
            print(f"\nDu {args.so_lot} lot theo yeu cau, dung lai.")
            break

        candidates = UNIFIED.sweep_candidates_past_eligible(
            service, lot_size, mark=args.moc
        )
        if not candidates:
            print("\nHet don du dieu kien quet.")
            break

        print(f"\n{'=' * 72}\nLOT {lot_thu}: {len(candidates):,} don | "
              f"{datetime.now():%H:%M:%S}\n{'=' * 72}")
        t0 = time.time()
        try:
            UNIFIED.batchsearch_candidates(
                service,
                fms_session,
                candidates=candidates,
                sweep_precheck_only=True,
            )
        except Exception:
            print("Lot nay loi, dung dot quet de xem lai:")
            traceback.print_exc()
            break

        # BAT BUOC sau moi lot. Don NOT_PENDING chi roi khoi hang cho khi bi
        # prune; de lai thi lot sau van thay chung va quet lai vo ich.
        UNIFIED.prune_finalized_pending_candidates(service)

        da_quet += len(candidates)
        giay = time.time() - t0
        # Tru dan thay vi dem lai: cau COUNT phai quet ca bang 1,9 trieu dong,
        # goi sau moi lot la tu them vai phut. Tru dan chinh xac tuyet doi vi moi
        # dong vua quet deu roi khoi tap "NEW + chua co ket qua" - hoac bi prune,
        # hoac doi sang PRECHECK_PENDING/PRECHECK_UNKNOWN.
        con_lai = max(0, con_lai - len(candidates))
        toc_do = da_quet / max(1e-9, time.time() - bat_dau)
        print(f"Lot {lot_thu} xong sau {giay:.0f}s "
              f"({giay / max(1, len(candidates)) * 1000:.0f}ms/don). "
              f"Da quet {da_quet:,} | con lai {con_lai:,}")
        if toc_do > 0 and con_lai:
            print(f"  Toc do {toc_do * 3600:,.0f} don/gio -> "
                  f"con khoang {con_lai / toc_do / 3600:.1f} gio nua")
        if not con_lai:
            print("\nDa quet het.")
            break

    print(f"\n{'=' * 72}")
    print(f"Tong: {da_quet:,} don trong {(time.time() - bat_dau) / 60:.1f} phut")
    print("Bay gio bat lai BOT 3 nhu binh thuong.")
    print("=" * 72)
    return 0


if __name__ == "__main__":
    sys.exit(main())
