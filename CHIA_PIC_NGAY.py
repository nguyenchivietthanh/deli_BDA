r"""Chia PIC + xuat sheet NGAY, bo qua BatchSearch/Tracking Info.

Dung khi cac ban den gio lam ma BOT 3 dang ket hoac vua loi: lenh nay chi lam
dung phan cac ban can, thuong xong trong 3-6 phut thay vi ca vong 30-60 phut.

Lam gi:
  1. Xuat tab pending_all (khong goi COGS)
  2. Dong bo pending_work (them dong moi, cap nhat o)
  3. Chia PIC theo lich dot, y het buoc --assign cua BOT 3
  4. Sua o status_cctv_deli cho TO thieu nguyen / hang sorting

KHONG lam: BatchSearch, Tracking Info, xoa dong het han. BOT 3 van lo nhung
viec do o vong ke tiep.

    $env:BOT_DELI_TO_TASK_MODE = "1"
    python .\CHIA_PIC_NGAY.py            # BOT 3 phai dang dung
    python .\CHIA_PIC_NGAY.py --cho 900  # doi toi 15 phut cho BOT 3 nghi giua hai vong

Lenh nay can KHOA cua BOT 3 (.bot3_work.lock) de hai ben khong ghi de nhau.
"""
import argparse
import importlib.util
import sys
import time
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent
sys.path.insert(0, str(BASE_DIR))


def load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


B3 = load_module(BASE_DIR / "BOT-Deli_BDA_run_bot3_work.py", "chia_pic_bot3")
APIError = B3.APIError

# Google tra 503/500 kha thuong xuyen o gio cao diem (dung loi vua lam BOT 3 chet
# giua buoc sync sang 20/09). Doi lau hon giua cac lan thu: lenh nay chay tay,
# tha cho them vai phut con hon bao loi roi bat nguoi chay lai.
THU_LAI_GIAY = (20, 45, 90, 150)


def thu_lai(mo_ta, ham):
    for lan, cho in enumerate((*THU_LAI_GIAY, None), start=1):
        try:
            return ham()
        except APIError as exc:
            ma = getattr(getattr(exc, "response", None), "status_code", None)
            if cho is None or ma not in (429, 500, 502, 503, 504):
                raise
            print(f"  {mo_ta} loi HTTP {ma}; cho {cho}s roi thu lai (lan {lan})...", flush=True)
            time.sleep(cho)


def main():
    parser = argparse.ArgumentParser(description="Chia PIC + xuat sheet ngay, bo qua BatchSearch")
    parser.add_argument("--cho", type=int, default=0,
                        help="So giay doi khoa BOT 3 truoc khi bo cuoc (mac dinh 0 = khong doi).")
    parser.add_argument("--khong-xuat-sheet", action="store_true",
                        help="Bo buoc xuat tab pending_all, chi dong bo pending_work va chia PIC.")
    parser.add_argument("--khong-chia", action="store_true",
                        help="Chi xuat sheet + dong bo, KHONG chia PIC.")
    args = parser.parse_args()

    han = time.time() + max(0, args.cho)
    khoa = B3.acquire_bot3_lock()
    while khoa is None and time.time() < han:
        print("BOT 3 dang chay, cho khoa... (Ctrl+C de dung)", flush=True)
        time.sleep(15)
        khoa = B3.acquire_bot3_lock()
    if khoa is None:
        print("KHONG lay duoc khoa: BOT 3 dang chay. Tat BOT 3 roi chay lai, "
              "hoac them --cho 900 de doi no nghi giua hai vong.")
        return 1

    bat_dau = time.time()
    try:
        if not args.khong_xuat_sheet:
            print("[1/4] Xuat tab pending_all...", flush=True)
            with B3._phase("  xuat sheet"):
                thu_lai("xuat sheet", lambda: B3.publish_pending_outputs(fetch_cogs=False))

        print("[2/4] Nap + lam giau don Pending...", flush=True)
        service = B3.sqlite_store.create_service()
        B3.ensure_cogs_table(service)
        B3.ensure_reconcile_cache_table(service)
        with B3._phase("  nap du lieu"):
            active_rows = B3.load_active_pending_rows(service)
            B3.enrich_reconciliation_context(service, active_rows)
            cogs_cache = B3.load_cogs_cache(service)

        spreadsheet = B3.open_spreadsheet()
        worksheet = B3.get_or_create_worksheet(spreadsheet)
        headers = B3.ensure_headers(worksheet)
        values = B3.sheets_get_all_values_with_retry(worksheet)

        print(f"[3/4] Dong bo pending_work ({len(active_rows):,} don Pending)...", flush=True)
        with B3._phase("  sync_pending_work"):
            thu_lai("sync pending_work", lambda: B3.sync_pending_work(
                worksheet, headers, active_rows, cogs_cache, values=values))

        if args.khong_chia:
            print("[4/4] Bo qua chia PIC theo yeu cau.")
        else:
            print("[4/4] Chia PIC...", flush=True)
            values_sau = B3.sheets_get_all_values_with_retry(worksheet)
            with B3._phase("  chia PIC"):
                thu_lai("chia PIC", lambda: B3.assign_pending_work(
                    worksheet, headers, spreadsheet,
                    values=values_sau,
                    eligible_times=B3.load_pending_eligible_times(service),
                ))
            with B3._phase("  cap nhat status_cctv_deli"):
                thu_lai("status_cctv_deli - TO thieu nguyen", lambda: B3.clear_full_missing_to_cctv_status(
                    worksheet, headers, active_rows, values=values_sau))
                thu_lai("status_cctv_deli - sorting", lambda: B3.set_sorting_no_cctv_status(
                    worksheet, headers, active_rows, values=values_sau))

        print(f"\nXong sau {time.time() - bat_dau:.0f}s ({datetime.now():%H:%M:%S}). "
              "BOT 3 van lo BatchSearch/Tracking Info va don dep o vong sau.")
        return 0
    finally:
        B3.release_bot3_lock(khoa)


if __name__ == "__main__":
    sys.exit(main())
