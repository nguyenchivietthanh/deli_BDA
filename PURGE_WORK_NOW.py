r"""Don sheet pending_work NGAY, khong doi BOT 3 chay het vong.

Goi thang purge_expired_work_rows() cua BOT 3 - cung mot than ham, cung luat,
cung ba chot an toan (chi RESOLVED, tran 12.000 dong/vong, tran 60% sheet).
Khong chep lai luat o day, de khong bao gio lech voi ban that.

    $env:BOT_DELI_TO_TASK_MODE = "1"
    python .\PURGE_WORK_NOW.py --ngay 5 --xem-truoc     # chi in, khong xoa
    python .\PURGE_WORK_NOW.py --ngay 5                 # xoa that

CHAY DUOC trong khi BOT 3 dang chay: cong cu nay lay dung .bot3_work.lock cua
BOT 3. Neu BOT 3 dang o giua mot vong thi lenh bao ban va thoat, chu khong xoa
chong len - hai tien trinh cung xoa dong theo chi so la xoa nham sang dong khac.
"""
import argparse
import importlib.util
import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).parent

# Dat truoc khi nap module BOT 3, giong RUN_BOT3_FINAL_WORK.ps1.
import os

os.environ.setdefault("BOT_DELI_TO_TASK_MODE", "1")


def load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


B3 = load_module(BASE_DIR / "BOT-Deli_BDA_run_bot3_work.py", "purge_now_bot3")


def main():
    parser = argparse.ArgumentParser(description="Don pending_work ngay lap tuc")
    parser.add_argument(
        "--ngay", type=int, default=B3.WORK_RETENTION_DAYS,
        help=f"So ngay dong duoc nam tren sheet truoc khi xoa (mac dinh {B3.WORK_RETENTION_DAYS}).",
    )
    parser.add_argument(
        "--xem-truoc", action="store_true",
        help="Chi tinh va in ra, khong xoa dong nao.",
    )
    parser.add_argument(
        "--cho", type=int, default=0,
        help="Neu BOT 3 dang giu khoa, cho toi N giay roi thu lai. 0 = khong cho.",
    )
    args = parser.parse_args()

    print(f"Moc: {args.ngay} ngay tren sheet "
          f"(= Arrived time + {B3.WORK_RETENTION_EXTRA_HOURS}h + {args.ngay} ngay)")
    print(f"Trang thai duoc xoa: {sorted(B3.PURGEABLE_BOT_STATUSES)}")

    # Xem truoc khong ghi gi nen khong can khoa - luon chay duoc, ke ca khi BOT 3
    # dang giua vong. Chi lan xoa that moi phai doc quyen.
    khoa = None
    if not args.xem_truoc:
        khoa = B3.acquire_bot3_lock()
        het_han = time.time() + max(0, args.cho)
        while khoa is None and time.time() < het_han:
            print("BOT 3 dang giu khoa, cho 15s...")
            time.sleep(15)
            khoa = B3.acquire_bot3_lock()
        if khoa is None:
            print(
                "DUNG: BOT 3 dang o giua mot vong (giu .bot3_work.lock).\n"
                "     Doi vong do xong, hoac chay lai voi --cho 900."
            )
            return 1

    try:
        service = B3.sqlite_store.create_service()
        worksheet = B3.get_or_create_worksheet(B3.open_spreadsheet())
        headers = B3.ensure_headers(worksheet)
        t0 = time.time()
        deleted = B3.purge_expired_work_rows(
            worksheet, headers, service,
            retention_days=args.ngay,
            dry_run=args.xem_truoc,
        )
        print(f"\nXong sau {time.time() - t0:.0f}s. Da xoa {deleted:,} dong.")
    finally:
        B3.release_bot3_lock(khoa)
    return 0


if __name__ == "__main__":
    sys.exit(main())
