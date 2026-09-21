r"""Xem hang doi RA CHANG (Handover) - de biet co can nang --handover-limit khong.

Chang = mot lan LT do hang tai mot tram. Chang chua ra thi cac TO trong do chua
thanh ung vien, nen don chi vao chu trinh khi da gia.

    $env:PYTHONUTF8 = "1"
    python .\XEM_HANG_DOI_CHANG.py

Doc thuan tuy, khong ghi gi. Chay duoc ca khi bot dang chay.
"""
import sqlite3
import sys
from pathlib import Path

DB = Path(__file__).parent / "bot_deli.sqlite3"
CHUA_RA = """
with latest as (
  select *, row_number() over (partition by trip_id, sequence_number order by observed_at desc) rn
  from lt_handover_trip
  where source='handover' and observed_at >= datetime('now','localtime','-3 days')
)
select {cot}
from latest l
where rn=1 and sequence_number is not null and arrived_time is not null
  and not exists (select 1 from lt_unit u
                  where u.trip_id=l.trip_id and u.sequence_number=l.sequence_number
                    and u.source_type in ('HANDOVER','ENDED'))
  and not exists (select 1 from lt_to_sorting_detail_state s
                  where s.trip_id=l.trip_id and s.sequence_number=l.sequence_number
                    and s.source='HANDOVER_SEQUENCE_COMPLETE')
"""


def main():
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=600)
    r = con.execute(CHUA_RA.format(cot="""count(*) chang, count(distinct trip_id) chuyen,
        min(arrived_time) cu_nhat,
        round((julianday('now','localtime')-julianday(min(arrived_time)))*24,1) tre_gio""")).fetchone()
    chang, chuyen, cu_nhat, tre = r
    print(f"Chang chua ra : {chang:,}  (thuoc {chuyen:,} chuyen)")
    print(f"Cu nhat       : {cu_nhat}  -> tre {tre} gio" if cu_nhat else "Cu nhat       : -")

    print("\nToc do ra chang theo gio (24h qua):")
    rows = con.execute("""select substr(checked_at,1,13) gio, count(*) n
        from lt_to_sorting_detail_state
        where source='HANDOVER_SEQUENCE_COMPLETE'
          and checked_at >= datetime('now','localtime','-24 hours')
        group by 1 order by 1""").fetchall()
    for gio, n in rows:
        print(f"   {gio}  {n:>4}")
    tong = sum(n for _, n in rows)
    print(f"   -> tong 24h: {tong:,} chang (~{tong / 24:.0f}/gio)")

    if chang:
        if tong:
            print(f"\nVoi toc do nay, {chang:,} chang ton can ~{chang / max(1, tong / 24):.0f} gio de ra het.")
        print("Nhieu gio ra DUNG bang --handover-limit => tran do dang chan, nang no len.")
    else:
        print("\nKhong ton dong: giu nguyen --handover-limit.")

    print("\n10 chang cu nhat dang cho:")
    for x in con.execute(CHUA_RA.format(cot="trip_number, sequence_number, arrived_time")
                         + " order by arrived_time limit 10"):
        print("   ", tuple(x))
    return 0


if __name__ == "__main__":
    sys.exit(main())
