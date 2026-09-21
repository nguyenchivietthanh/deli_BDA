"""Read-only DB health check: run this on any machine to see if bot_deli.sqlite3
is bloating. Safe to run any time, even while the 3 bots are running - it only
opens the database in read-only mode and never writes anything.

Usage:
    python BOT-Deli_BDA_check_bloat.py

Paste the full output back to Claude to have it interpreted.
"""

import sqlite3
import pathlib
import datetime

DB = pathlib.Path(__file__).resolve().parent / "bot_deli.sqlite3"
CANDIDATE_RULE_VERSION = "pending_candidate_v20260809_unified_v2"
RESULT_RULE_VERSION = "pending_result_v20260826_mass_returned_to_bda_v8"


def main():
    if not DB.exists():
        print(f"Khong tim thay: {DB}")
        return

    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=15)
    con.execute("PRAGMA busy_timeout=10000")

    def q(label, sql):
        print(f"\n{label}")
        try:
            for row in con.execute(sql).fetchall():
                print("   ", row)
        except sqlite3.OperationalError as exc:
            print("   (bo qua)", exc)

    print("=" * 70)
    print("BOT DELI - kiem tra phinh du lieu")
    print("=" * 70)

    for suffix, note in ((".sqlite3", "main"), (".sqlite3-wal", "WAL"), (".sqlite3-shm", "shm")):
        p = DB.with_suffix(suffix) if suffix != ".sqlite3" else DB
        if suffix != ".sqlite3":
            p = DB.parent / (DB.name + suffix.replace(".sqlite3", ""))
        if p.exists():
            print(f"{note}: {p.stat().st_size / 1024 / 1024:.1f} MiB  ({p})")
        else:
            print(f"{note}: khong ton tai")

    q("page_count / freelist_count:", "SELECT * FROM (SELECT (SELECT * FROM pragma_page_count()) pc, (SELECT * FROM pragma_freelist_count()) fc)")

    q("So dong moi bang:", """
        SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name
    """)
    for (table,) in con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall():
        try:
            count = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            print(f"  - {table}: {count:,} rows")
        except sqlite3.OperationalError as exc:
            print(f"  - {table}: loi {exc}")

    q("lt_pending_candidate - rule version:",
      "SELECT candidate_rule_version, COUNT(*) FROM lt_pending_candidate GROUP BY 1")
    q("lt_pending_candidate - queue_stage:",
      "SELECT COALESCE(queue_stage,'<null>'), COUNT(*) FROM lt_pending_candidate GROUP BY 1 ORDER BY 2 DESC")
    q("lt_pending_candidate - candidate_source (deferred mode phai toan LT_UNIT_TO_SORTING):",
      "SELECT candidate_source, COUNT(*) FROM lt_pending_candidate GROUP BY 1 ORDER BY 2 DESC")
    q("lt_pending_candidate - do tuoi arrived_time:", """
        SELECT CASE
          WHEN arrived_time IS NULL THEN 'null'
          WHEN arrived_time >= datetime('now','+7 hours','-1 day') THEN '0-1 ngay'
          WHEN arrived_time >= datetime('now','+7 hours','-2 day') THEN '1-2 ngay'
          WHEN arrived_time >= datetime('now','+7 hours','-3 day') THEN '2-3 ngay'
          ELSE '>3 ngay' END, COUNT(*)
        FROM lt_pending_candidate GROUP BY 1 ORDER BY 2 DESC
    """)
    q("lt_pending_candidate - min/max arrived_time:",
      "SELECT MIN(arrived_time), MAX(arrived_time) FROM lt_pending_candidate")

    q("lt_pending_result - rule version:",
      "SELECT result_rule_version, COUNT(*) FROM lt_pending_result GROUP BY 1")
    q("lt_pending_result - so dong vs candidate_id rieng (phinh append-only?):",
      "SELECT COUNT(*), COUNT(DISTINCT candidate_id) FROM lt_pending_result")
    q("lt_pending_result - da check trong 60/180 phut gan day (BatchSearch con chay khong):", """
        SELECT
          SUM(CASE WHEN checked_at >= datetime('now','+7 hours','-60 minutes') THEN 1 ELSE 0 END),
          SUM(CASE WHEN checked_at >= datetime('now','+7 hours','-180 minutes') THEN 1 ELSE 0 END)
        FROM lt_pending_result
    """)

    q("Bang staging cu (phai KHONG con neu deferred mode dang bat):",
      "SELECT name FROM sqlite_master WHERE type='table' AND name='lt_to_sorting_pending_candidate'")

    for table, key in (
        ("bot3_cogs_cache", "shipment_id"),
        ("bot3_reconcile_arrival_cache", "cache_key"),
    ):
        q(f"{table} - so dong vs {key} rieng (phinh append-only nhe, khong khan cap):",
          f'SELECT COUNT(*), COUNT(DISTINCT "{key}") FROM "{table}"')

    print("\n" + "=" * 70)
    print("Xong. Dan toan bo output nay cho Claude.")


if __name__ == "__main__":
    main()
