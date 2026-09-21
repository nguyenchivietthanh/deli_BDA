import argparse
import importlib.util
import traceback
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
ARRIVED_LT_BOT_PATH = BASE_DIR / "BOT-Deli_BDA_arrived_LT.py"
EXPORT_LT_UNIT_BOT_PATH = BASE_DIR / "BOT-Deli_BDA_export_LT_unit_to_BQ.py"


def load_module(path, module_name):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ARRIVED_LT_BOT = load_module(ARRIVED_LT_BOT_PATH, "bot_deli_bda_arrived_lt")
EXPORT_LT_UNIT_BOT = load_module(EXPORT_LT_UNIT_BOT_PATH, "bot_deli_bda_export_lt_unit_to_bq")


def run_cycle(lt_limit=None, export_limit=None, skip_lt=False, skip_export=False, lt_from=None, lt_to=None):
    cycle_started_at = datetime.now()
    print("=" * 80)
    print(f"Ingest pipeline started: {cycle_started_at.isoformat(timespec='seconds')}")

    if not skip_lt:
        print("\n[1/2] Get arrived LT to Google Sheet")
        ARRIVED_LT_BOT.run_once(limit=lt_limit, window_from=lt_from, window_to=lt_to)
    else:
        print("\n[1/2] Skip get arrived LT")

    if not skip_export:
        print("\n[2/2] Export LT units to BigQuery")
        EXPORT_LT_UNIT_BOT.run_once(limit=export_limit)
    else:
        print("\n[2/2] Skip export LT units")

    elapsed = (datetime.now() - cycle_started_at).total_seconds()
    print(f"Ingest pipeline done. Elapsed: {elapsed:.0f}s")
    return elapsed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Chay 1 vong roi dung.")
    parser.add_argument("--interval-minutes", type=int, default=60, help="So phut giua 2 vong pipeline.")
    parser.add_argument("--lt-limit", type=int, default=None, help="Gioi han so LT lay vao Sheet trong moi vong.")
    parser.add_argument("--lt-from", default=None, help="Moc arrived_time bat dau. Vi du: '2026-08-10 00:00:00'.")
    parser.add_argument("--lt-to", default=None, help="Moc arrived_time ket thuc. Vi du: '2026-08-10 22:00:00'.")
    parser.add_argument("--export-limit", type=int, default=None, help="Gioi han so LT export trong moi vong.")
    parser.add_argument("--skip-lt", action="store_true", help="Bo qua buoc lay LT vao sheet.")
    parser.add_argument("--skip-export", action="store_true", help="Bo qua buoc export LT unit len BigQuery.")
    args = parser.parse_args()

    interval_seconds = max(1, args.interval_minutes) * 60
    while True:
        elapsed = 0
        try:
            elapsed = run_cycle(
                lt_limit=args.lt_limit,
                export_limit=args.export_limit,
                skip_lt=args.skip_lt,
                skip_export=args.skip_export,
                lt_from=args.lt_from,
                lt_to=args.lt_to,
            )
        except Exception:
            print("Ingest pipeline failed:")
            traceback.print_exc()

        if args.once:
            return

        sleep_seconds = max(0, interval_seconds - int(elapsed))
        print(f"Sleep {sleep_seconds} seconds before next ingest pipeline cycle.")
        ARRIVED_LT_BOT.ti.sleep(sleep_seconds)


if __name__ == "__main__":
    main()
