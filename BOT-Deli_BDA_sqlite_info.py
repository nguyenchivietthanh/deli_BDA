from pathlib import Path

import sqlite3


DATABASE_FILE = Path(__file__).resolve().parent / "bot_deli.sqlite3"


def main():
    if not DATABASE_FILE.exists():
        print(f"SQLite database chua duoc tao: {DATABASE_FILE}")
        return

    with sqlite3.connect(DATABASE_FILE) as conn:
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).fetchall()
        print(f"SQLite database: {DATABASE_FILE}")
        print(f"File size: {DATABASE_FILE.stat().st_size / 1024 / 1024:.2f} MB")
        for (table_name,) in tables:
            count = conn.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
            print(f"- {table_name}: {count:,} rows")


if __name__ == "__main__":
    main()
