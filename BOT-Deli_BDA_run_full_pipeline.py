import argparse
import importlib.util
import traceback
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
INGEST_PIPELINE_PATH = BASE_DIR / "BOT-Deli_BDA_run_ingest_pipeline.py"
PENDING_PIPELINE_PATH = BASE_DIR / "BOT-Deli_BDA_run_pending_pipeline.py"


def load_module(path, module_name):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


INGEST_PIPELINE = load_module(INGEST_PIPELINE_PATH, "bot_deli_bda_run_ingest_pipeline")
PENDING_PIPELINE = load_module(PENDING_PIPELINE_PATH, "bot_deli_bda_run_pending_pipeline")


def run_cycle(
    lt_limit=None,
    export_limit=None,
    bulky_limit=None,
    to_limit=None,
    batch_limit=None,
    skip_lt=False,
    skip_export=False,
    skip_bulky=False,
    skip_to=False,
    skip_batchsearch=False,
    skip_sheet=False,
    stop_on_error=False,
    lt_from=None,
    lt_to=None,
    include_lt_unit_fallback=True,
):
    cycle_started_at = datetime.now()
    print("=" * 80)
    print(f"Full pipeline started: {cycle_started_at.isoformat(timespec='seconds')}")

    ingest_elapsed = 0
    pending_elapsed = 0

    try:
        print("\n[A] Ingest pipeline - BDA role")
        ingest_elapsed = INGEST_PIPELINE.run_cycle(
            lt_limit=lt_limit,
            export_limit=export_limit,
            skip_lt=skip_lt,
            skip_export=skip_export,
            lt_from=lt_from,
            lt_to=lt_to,
        )
    except Exception:
        print("Full pipeline: ingest step failed:")
        traceback.print_exc()
        if stop_on_error:
            raise

    try:
        print("\n[B] Pending pipeline - Admin role")
        pending_elapsed = PENDING_PIPELINE.run_cycle(
            bulky_limit=bulky_limit,
            to_limit=to_limit,
            batch_limit=batch_limit,
            skip_bulky=skip_bulky,
            skip_to=skip_to,
            skip_batchsearch=skip_batchsearch,
            skip_sheet=skip_sheet,
            include_lt_unit_fallback=include_lt_unit_fallback,
        )
    except Exception:
        print("Full pipeline: pending step failed:")
        traceback.print_exc()
        if stop_on_error:
            raise

    elapsed = (datetime.now() - cycle_started_at).total_seconds()
    print(
        "Full pipeline done. "
        f"Elapsed: {elapsed:.0f}s | ingest: {ingest_elapsed:.0f}s | pending: {pending_elapsed:.0f}s"
    )
    return elapsed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Chay 1 vong full pipeline roi dung.")
    parser.add_argument("--interval-minutes", type=int, default=15, help="So phut giua 2 vong full pipeline.")
    parser.add_argument("--lt-limit", type=int, default=None, help="Gioi han so LT lay vao Sheet trong moi vong ingest.")
    parser.add_argument("--lt-from", default=None, help="Moc arrived_time bat dau. Vi du: '2026-08-10 00:00:00'.")
    parser.add_argument("--lt-to", default=None, help="Moc arrived_time ket thuc. Vi du: '2026-08-10 22:00:00'.")
    parser.add_argument("--export-limit", type=int, default=None, help="Gioi han so LT export trong moi vong ingest.")
    parser.add_argument("--bulky-limit", type=int, default=None, help="Gioi han so LT check bulky pending trong moi vong pending.")
    parser.add_argument("--to-limit", type=int, default=None, help="Gioi han so TO Sorting check trong moi vong pending.")
    parser.add_argument("--batch-limit", type=int, default=None, help="Gioi han candidate pending; BOT tu cap toi da 2000 moi vong.")
    parser.add_argument("--skip-lt", action="store_true", help="Bo qua buoc lay LT vao sheet.")
    parser.add_argument("--skip-export", action="store_true", help="Bo qua buoc export LT unit len BigQuery.")
    parser.add_argument("--skip-bulky", action="store_true", help="Bo qua buoc check Bulky pending.")
    parser.add_argument("--skip-to", action="store_true", help="Bo qua buoc check TO Sorting pending.")
    parser.add_argument("--skip-batchsearch", action="store_true", help="Chi tao candidate pending, khong BatchSearch ket qua cuoi.")
    parser.add_argument("--skip-sheet", action="store_true", help="Bo qua buoc export pending ra Google Sheet.")
    parser.add_argument(
        "--include-lt-unit-fallback",
        action="store_true",
        default=True,
        help="Quet TO Sorting trong lt_unit (mac dinh bat).",
    )
    parser.add_argument("--stop-on-error", action="store_true", help="Neu 1 buoc loi thi dung luon thay vi chay tiep buoc sau.")
    args = parser.parse_args()

    interval_seconds = max(1, args.interval_minutes) * 60
    while True:
        elapsed = 0
        try:
            elapsed = run_cycle(
                export_limit=args.export_limit,
                lt_limit=args.lt_limit,
                bulky_limit=args.bulky_limit,
                to_limit=args.to_limit,
                batch_limit=args.batch_limit,
                skip_lt=args.skip_lt,
                skip_export=args.skip_export,
                skip_bulky=args.skip_bulky,
                skip_to=args.skip_to,
                skip_batchsearch=args.skip_batchsearch,
                skip_sheet=args.skip_sheet,
                stop_on_error=args.stop_on_error,
                lt_from=args.lt_from,
                lt_to=args.lt_to,
                include_lt_unit_fallback=args.include_lt_unit_fallback,
            )
        except Exception:
            print("Full pipeline failed:")
            traceback.print_exc()

        if args.once:
            return

        sleep_seconds = max(0, interval_seconds - int(elapsed))
        print(f"Sleep {sleep_seconds} seconds before next full pipeline cycle.")
        INGEST_PIPELINE.ARRIVED_LT_BOT.ti.sleep(sleep_seconds)


if __name__ == "__main__":
    main()
