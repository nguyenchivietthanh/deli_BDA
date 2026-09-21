r"""Lay COGS cho danh sach ma don - file rieng, KHONG dung chung voi BOT.

Cung API voi BOT 3 (fleet_order/order/detail/show_sensitive_data, data_field=cogs),
goi qua Chrome dang nhap FMS voi role Admin.

Chrome dung PROFILE RIENG (BOT_DELI_FMS_BROWSER_COGS) de khong tranh profile voi
BOT 2 / BOT 3 dang chay. Lan dau chay: Chrome mo ra, dang nhap FMS bang tay trong
180 giay; cac lan sau tu dung lai phien.

Cach dung:
    $env:PYTHONUTF8 = "1"
    python .\LAY_COGS.py                     # dan ma don vao, Enter o dong trong de bat dau
    python .\LAY_COGS.py --file don.txt      # doc ma don tu file

Dan kieu gi cung duoc (moi dong 1 ma, cach nhau dau phay/tab, copy ca cot tu sheet):
script tu nhat moi chuoi SPX... va bo trung.

Ket qua: cogs_ket_qua\cogs_<gio>.csv (mo bang Excel duoc), ghi tung dong ngay khi lay
xong - tat giua chung khong mat phan da lay. Don da co COGS trong cac file cu cua thu
muc nay se duoc bo qua, nen chay lai la tu lam tiep phan con thieu.

Gioi han: FMS gioi han so lan xem du lieu nhay cam moi ngay tren tai khoan
(API code 200301004 "reached the maximum"). Gap loi nay script DUNG NGAY va ghi phan
chua lay vao cogs_con_lai_<gio>.txt de hom sau chay tiep bang --file.
"""
import argparse
import csv
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

BASE_DIR = Path(__file__).resolve().parent
COGS_PROFILE = Path(
    os.getenv(
        "BOT_DELI_CHROME_USER_DATA_DIR_COGS",
        str(Path(os.getenv("LOCALAPPDATA", "")) / "BOT_DELI_FMS_BROWSER_COGS" / "User Data"),
    )
)
# Phai dat truoc khi import browser_fetch.
os.environ["BOT_DELI_CHROME_USER_DATA_DIR_ADMIN"] = str(COGS_PROFILE)
os.environ["BOT_DELI_CHROME_PROFILE_ADMIN"] = "Default"
sys.path.insert(0, str(BASE_DIR))

import browser_fetch  # noqa: E402

for stream in (sys.stdout, sys.stderr):
    if hasattr(stream, "reconfigure"):
        stream.reconfigure(encoding="utf-8", errors="replace")

COGS_URL = "https://spx.shopee.vn/api/fleet_order/order/detail/show_sensitive_data"
ROLE = "Admin"
OUT_DIR = BASE_DIR / "cogs_ket_qua"
# Ma don co the co chu cai o CUOI (vd SPXVN05109030759B) - lay tron ca cum chu+so.
MA_DON = re.compile(r"\bSPX[A-Z0-9]{8,}", re.IGNORECASE)
COT = ["shipment_id", "cogs", "lay_luc", "loi"]


def doc_ma_don(args):
    if args.file:
        text = Path(args.file).read_text(encoding="utf-8-sig", errors="replace")
    else:
        print("Dan ma don vao (bao nhieu dong cung duoc).")
        print(">>> Dan xong thi bam Enter THEM 1-2 LAN (den khi gap dong trong) de bat dau <<<")
        dong = []
        while True:
            try:
                line = input()
            except EOFError:
                break
            if not line.strip():
                if dong:
                    break
                continue
            dong.append(line)
        text = "\n".join(dong)
    thay = [m.group(0).upper() for m in MA_DON.finditer(text)]
    return list(dict.fromkeys(thay))


def da_co_cogs():
    """shipment_id -> cogs tu cac file ket qua cu."""
    out = {}
    for f in sorted(OUT_DIR.glob("cogs_*.csv")):
        try:
            with open(f, encoding="utf-8-sig", newline="") as fh:
                for row in csv.DictReader(fh):
                    sid = (row.get("shipment_id") or "").strip().upper()
                    val = (row.get("cogs") or "").strip()
                    if sid and val:
                        out[sid] = val
        except OSError:
            continue
    return out


def la_loi_gioi_han(exc):
    text = str(exc).lower()
    return "200301004" in text or "reached the maximum" in text


def lay_cogs(shipment_id):
    url = f"{COGS_URL}?shipment_id={quote(shipment_id)}&data_field=cogs"
    response = browser_fetch.request_json(ROLE, "GET", url, label=f"COGS {shipment_id}", max_retries=3)
    value = (response.get("data") or {}).get("data_detail")
    if value is None or value == "":
        raise RuntimeError("API tra ve data_detail rong")
    return value


def main():
    parser = argparse.ArgumentParser(description="Lay COGS cho danh sach ma don")
    parser.add_argument("--file", help="File chua ma don (txt/csv). Khong truyen thi dan vao terminal.")
    parser.add_argument("--lay-lai", action="store_true", help="Lay lai ca don da co COGS trong file cu.")
    parser.add_argument("--nghi", type=float, default=0.15, help="Giay nghi giua 2 don (mac dinh 0.15, giong BOT).")
    args = parser.parse_args()

    ds = doc_ma_don(args)
    if not ds:
        print("Khong tim thay ma don SPX... nao.")
        return 1
    OUT_DIR.mkdir(exist_ok=True)
    cu = {} if args.lay_lai else da_co_cogs()
    can_lay = [s for s in ds if s not in cu]
    print(f"\nNhan {len(ds):,} ma don | da co COGS tu truoc: {len(ds) - len(can_lay):,} | can lay: {len(can_lay):,}")
    print(f"Chrome profile: {COGS_PROFILE}")
    if not can_lay:
        print("Khong con don nao can lay.")
        return 0

    gio = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_kq = OUT_DIR / f"cogs_{gio}.csv"
    ok = loi = 0
    dung_vi_gioi_han = False
    bat_dau = time.time()
    try:
        with open(file_kq, "w", encoding="utf-8-sig", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=COT)
            w.writeheader()
            for i, sid in enumerate(can_lay, start=1):
                lay_luc = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                try:
                    cogs = lay_cogs(sid)
                    w.writerow({"shipment_id": sid, "cogs": cogs, "lay_luc": lay_luc, "loi": ""})
                    ok += 1
                    print(f"{i}/{len(can_lay)} {sid} = {cogs}")
                except KeyboardInterrupt:
                    raise
                except Exception as exc:
                    if la_loi_gioi_han(exc):
                        dung_vi_gioi_han = True
                        print(f"\n{i}/{len(can_lay)} {sid}: tai khoan da cham gioi han xem COGS hom nay. DUNG.")
                        con_lai = can_lay[i - 1:]
                        break
                    w.writerow({"shipment_id": sid, "cogs": "", "lay_luc": lay_luc, "loi": str(exc)[:300]})
                    loi += 1
                    print(f"{i}/{len(can_lay)} {sid} LOI: {exc}")
                fh.flush()
                if args.nghi > 0:
                    time.sleep(args.nghi)
    except KeyboardInterrupt:
        print("\nDa dung bang Ctrl+C. Phan da lay van nam trong file ket qua.")
    finally:
        try:
            browser_fetch.shutdown_all()
        except Exception:
            pass

    phut = (time.time() - bat_dau) / 60
    print(f"\nXong sau {phut:.1f} phut | lay duoc {ok:,} | loi {loi:,}")
    print(f"Ket qua: {file_kq}")
    if dung_vi_gioi_han:
        file_con = OUT_DIR / f"cogs_con_lai_{gio}.txt"
        file_con.write_text("\n".join(con_lai), encoding="utf-8")
        print(f"Con {len(con_lai):,} don chua lay -> {file_con}")
        print(f"Chay tiep sau: python .\\LAY_COGS.py --file \"{file_con}\"")
    if loi:
        print("Don loi chay lai lenh cu la tu lay lai (chi don chua co COGS moi duoc goi).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
