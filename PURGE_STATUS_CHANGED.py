r"""Don bang nhat ky lt_pending_status_changed, giu N ngay gan nhat.

LUON sao luu ra CSV truoc khi xoa. Bang nay la nhat ky duy nhat khong bi nen -
xoa xong khong con duong truy lai lich su, nen file CSV la thu thay the.

    $env:BOT_DELI_TO_TASK_MODE = "1"
    python .\PURGE_STATUS_CHANGED.py --ngay 3              # xem truoc, khong xoa
    python .\PURGE_STATUS_CHANGED.py --ngay 3 --thuc-hien  # sao luu roi xoa
"""
import argparse
import csv
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent
DB = BASE_DIR / "bot_deli.sqlite3"
BANG = "lt_pending_status_changed"


def main():
    parser = argparse.ArgumentParser(description="Don nhat ky lt_pending_status_changed")
    parser.add_argument("--ngay", type=int, default=3, help="Giu lai N ngay gan nhat (mac dinh 3).")
    parser.add_argument("--thuc-hien", action="store_true", help="Sao luu roi xoa that.")
    parser.add_argument("--thu-muc-sao-luu", default=str(BASE_DIR / "backup"),
                        help="Noi de file CSV sao luu.")
    args = parser.parse_args()
    giu = max(0, int(args.ngay))

    con = sqlite3.connect(DB, timeout=300)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout = 300000")

    # Dong khong co ngay thi GIU - khong xoa dua tren mot o khong doc duoc.
    dk = (
        "status_changed_at IS NOT NULL AND status_changed_at != '' "
        f"AND status_changed_at < datetime('now','localtime','-{giu} days')"
    )

    tong = con.execute(f"SELECT COUNT(*) FROM {BANG}").fetchone()[0]
    xoa = con.execute(f"SELECT COUNT(*) FROM {BANG} WHERE {dk}").fetchone()[0]
    khong_ngay = con.execute(
        f"SELECT COUNT(*) FROM {BANG} WHERE status_changed_at IS NULL OR status_changed_at=''"
    ).fetchone()[0]
    r = con.execute(f"SELECT MIN(status_changed_at) a, MAX(status_changed_at) b FROM {BANG}").fetchone()

    print(f"Bang {BANG}")
    print(f"  tong hien tai        : {tong:,}")
    print(f"  khoang thoi gian     : {r['a']}  ->  {r['b']}")
    print(f"  giu lai {giu} ngay gan nhat: {tong - xoa:,}")
    print(f"  SE XOA               : {xoa:,}  ({xoa / max(1, tong):.0%})")
    print(f"  dong khong co ngay (giu nguyen): {khong_ngay:,}")

    if not args.thuc_hien:
        print("\nXem truoc: chua xoa gi. Them --thuc-hien de sao luu roi xoa.")
        return 0
    if not xoa:
        print("\nKhong co dong nao de xoa.")
        return 0

    thu_muc = Path(args.thu_muc_sao_luu)
    thu_muc.mkdir(parents=True, exist_ok=True)
    ten = thu_muc / f"{BANG}_truoc_{datetime.now():%Y%m%d_%H%M%S}.csv"
    cols = [c[1] for c in con.execute(f"PRAGMA table_info({BANG})")]
    print(f"\nSao luu {xoa:,} dong -> {ten}")
    with open(ten, "w", newline="", encoding="utf-8-sig") as fh:
        w = csv.writer(fh)
        w.writerow(cols)
        n = 0
        for row in con.execute(f"SELECT * FROM {BANG} WHERE {dk}"):
            w.writerow([row[c] for c in cols])
            n += 1
    print(f"  da ghi {n:,} dong, {ten.stat().st_size / 1024 / 1024:.1f} MB")
    if n != xoa:
        print(f"  DUNG: sao luu {n:,} dong nhung dinh xoa {xoa:,}. Khong xoa gi ca.")
        return 1

    cur = con.execute(f"DELETE FROM {BANG} WHERE {dk}")
    con.commit()
    con_lai = con.execute(f"SELECT COUNT(*) FROM {BANG}").fetchone()[0]
    print(f"\nDa xoa {cur.rowcount:,} dong. Con lai {con_lai:,}.")
    print(f"Muon lay lai: doc file CSV o tren.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
