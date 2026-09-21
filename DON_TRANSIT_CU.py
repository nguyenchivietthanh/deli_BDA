r"""Don dong TO transit cu khoi lt_pending_candidate (ops chot 2026-09-17).

Xoa:
  - moi dong nguon BDA_LT_TO_PATH_WITHOUT_BDA (duong di khong co BDA - khong
    phai viec cua BDA, da ngung tao);
  - dong LH_PENDING_TO_TRANSIT co arrived_time truoc TRANSIT_START_ARRIVED_TIME
    (transit cu, bot chi kiem tu ngay do).
KHONG xoa dong nao da co ket qua trong lt_pending_result (do 2026-09-17: 0 dong).

Sao luu ra file CSV nen gzip truoc khi xoa. Xoa tung phan va commit tung phan
de BOT 1 / BOT 2 dang chay khong bi khoa lau. Xong thi tao index
idx_pending_candidate_to (bot can index nay de dem don theo TO transit).

    $env:BOT_DELI_TO_TASK_MODE = "1"
    python .\DON_TRANSIT_CU.py              # xem truoc, khong xoa
    python .\DON_TRANSIT_CU.py --thuc-hien  # sao luu roi xoa
"""
import argparse
import csv
import gzip
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent
DB = BASE_DIR / "bot_deli.sqlite3"
BANG = "lt_pending_candidate"
MOC = "2026-09-18 00:00:00"  # = TRANSIT_START_ARRIVED_TIME trong unified_pending_pipeline
PHAN = 20000

DK = f"""
source_type = 'TO_TRANSIT'
AND (
  COALESCE(candidate_source, '') != 'LH_PENDING_TO_TRANSIT'
  OR arrived_time IS NULL OR arrived_time = '' OR arrived_time < '{MOC}'
)
AND candidate_id NOT IN (
  SELECT candidate_id FROM lt_pending_result
  WHERE source_type = 'TO_TRANSIT' AND candidate_id IS NOT NULL
)
"""


def main():
    parser = argparse.ArgumentParser(description="Don TO transit cu")
    parser.add_argument("--thuc-hien", action="store_true", help="Sao luu roi xoa that.")
    parser.add_argument("--thu-muc-sao-luu", default=str(BASE_DIR / "backup"))
    args = parser.parse_args()

    con = sqlite3.connect(DB, timeout=300)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout = 300000")

    print(f"Bang {BANG}, moc transit moi: arrived_time >= {MOC}")
    print("  nguon | xoa | giu")
    for r in con.execute(f"""
        SELECT candidate_source,
               SUM(CASE WHEN {DK} THEN 1 ELSE 0 END) AS xoa,
               SUM(CASE WHEN {DK} THEN 0 ELSE 1 END) AS giu
        FROM {BANG} WHERE source_type = 'TO_TRANSIT' GROUP BY candidate_source"""):
        print(f"  {r['candidate_source']:<28} {r['xoa']:>10,} {r['giu']:>8,}")
    tong = con.execute(f"SELECT COUNT(*) FROM {BANG}").fetchone()[0]
    ids = [row[0] for row in con.execute(f"SELECT rowid FROM {BANG} WHERE {DK}")]
    print(f"  tong bang: {tong:,} | SE XOA: {len(ids):,} ({len(ids) / max(1, tong):.0%})")

    if not args.thuc_hien:
        print("\nXem truoc: chua xoa gi. Them --thuc-hien de sao luu roi xoa.")
        return 0
    if not ids:
        print("\nKhong co dong nao de xoa.")
    else:
        thu_muc = Path(args.thu_muc_sao_luu)
        thu_muc.mkdir(parents=True, exist_ok=True)
        ten = thu_muc / f"transit_cu_truoc_{datetime.now():%Y%m%d_%H%M%S}.csv.gz"
        cols = [c[1] for c in con.execute(f"PRAGMA table_info({BANG})")]
        print(f"\nSao luu {len(ids):,} dong -> {ten}")
        n = 0
        with gzip.open(ten, "wt", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(cols)
            for i in range(0, len(ids), PHAN):
                phan = ids[i:i + PHAN]
                q = f"SELECT * FROM {BANG} WHERE rowid IN ({','.join('?' * len(phan))})"
                for row in con.execute(q, phan):
                    w.writerow([row[c] for c in cols])
                    n += 1
        print(f"  da ghi {n:,} dong, {ten.stat().st_size / 1024 / 1024:.1f} MB")
        if n != len(ids):
            print(f"  DUNG: sao luu {n:,} dong nhung dinh xoa {len(ids):,}. Khong xoa gi ca.")
            return 1

        da_xoa = 0
        bat_dau = time.time()
        for i in range(0, len(ids), PHAN):
            phan = ids[i:i + PHAN]
            # Kiem lai dieu kien ngay luc xoa: dong da doi (vd vua co ket qua) thi giu.
            cur = con.execute(
                f"DELETE FROM {BANG} WHERE rowid IN ({','.join('?' * len(phan))}) AND {DK}", phan
            )
            con.commit()
            da_xoa += cur.rowcount
            print(f"  xoa {da_xoa:,}/{len(ids):,} ({time.time() - bat_dau:.0f}s)", flush=True)
        print(f"\nDa xoa {da_xoa:,} dong. Con lai {con.execute(f'SELECT COUNT(*) FROM {BANG}').fetchone()[0]:,}.")

    print("\nTao index idx_pending_candidate_to (neu chua co)...")
    t = time.time()
    con.execute(f'CREATE INDEX IF NOT EXISTS "idx_pending_candidate_to" ON {BANG} ("to_number", "trip_number")')
    con.commit()
    print(f"  xong ({time.time() - t:.0f}s)")
    print("File DB chua nho lai tren dia (SQLite giu cho trong de dung lai) - binh thuong.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
