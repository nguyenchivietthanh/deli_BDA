"""Xem nhanh du lieu cac bang lt_in_* trong bot_deli.sqlite3 - khong can cai
them phan mem gi (sqlite3 co san trong Python).

Vi du:
  python BOT_Pick_BDA_sqlite_info.py                 # liet ke bang + so dong
  python BOT_Pick_BDA_sqlite_info.py lt_in_trip       # in 20 dong moi nhat
  python BOT_Pick_BDA_sqlite_info.py lt_in_trip 50    # in 50 dong moi nhat
"""

import sqlite3
import sys
from pathlib import Path


DATABASE_FILE = Path(__file__).resolve().parent / "bot_deli.sqlite3"


def list_tables(conn):
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'lt_in_%' ORDER BY name"
    ).fetchall()
    print(f"SQLite database: {DATABASE_FILE}")
    if not tables:
        print("Chua co bang lt_in_* nao - chay BOT_Pick_BDA_arrived_LT.py truoc.")
        return
    for (table_name,) in tables:
        count = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
        print(f"- {table_name}: {count:,} dong")


def print_table(conn, table_name, limit):
    try:
        columns = [row[1] for row in conn.execute(f'PRAGMA table_info("{table_name}")')]
    except sqlite3.OperationalError:
        columns = []
    if not columns:
        print(f"Khong tim thay bang '{table_name}'.")
        return
    order_by = "rowid DESC"
    rows = conn.execute(f'SELECT * FROM "{table_name}" ORDER BY {order_by} LIMIT ?', (limit,)).fetchall()
    print(f"Bang {table_name} - {len(rows)} dong moi nhat (cot: {', '.join(columns)})")
    for row in rows:
        print(dict(zip(columns, row)))


def main():
    if not DATABASE_FILE.exists():
        print(f"SQLite database chua duoc tao: {DATABASE_FILE}")
        print("Chay BOT_Pick_BDA_arrived_LT.py --once truoc de tao database.")
        return

    args = sys.argv[1:]
    with sqlite3.connect(DATABASE_FILE) as conn:
        if not args:
            list_tables(conn)
            return
        table_name = args[0]
        limit = int(args[1]) if len(args) > 1 else 20
        print_table(conn, table_name, limit)


if __name__ == "__main__":
    main()
