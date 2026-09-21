import argparse
import importlib.util
import traceback
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
UNIFIED_PENDING_BOT_PATH = BASE_DIR / "BOT-Deli_BDA_unified_pending_pipeline.py"
EXPORT_PENDING_SHEET_BOT_PATH = BASE_DIR / "BOT-Deli_BDA_export_pending_to_sheet.py"


def load_module(path, module_name):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


UNIFIED_PENDING_BOT = load_module(UNIFIED_PENDING_BOT_PATH, "bot_deli_bda_unified_pending_pipeline")
EXPORT_PENDING_SHEET_BOT = load_module(EXPORT_PENDING_SHEET_BOT_PATH, "bot_deli_bda_export_pending_to_sheet")


def run_cycle(
    bulky_limit=None,
    to_limit=None,
    batch_limit=None,
    skip_bulky=False,
    skip_to=False,
    skip_batchsearch=False,
    skip_sheet=False,
    force_recheck_pending=False,
    force_recheck_lh_pending=False,
    include_lt_unit_fallback=True,
    include_bda_prefilter=True,
    clear_bda_staging=False,
    fetch_cogs=False,
    cogs_limit=250,
    return_result=False,
):
    cycle_started_at = datetime.now()
    print("=" * 80)
    print(f"Pending pipeline started: {cycle_started_at.isoformat(timespec='seconds')}")

    print("\n[1/2] Unified pending candidate + BatchSearch")
    unified_result = UNIFIED_PENDING_BOT.run_once(
        lt_limit=bulky_limit,
        to_limit=to_limit,
        batch_limit=batch_limit,
        skip_lh_pending=skip_bulky,
        skip_lt_unit_to=skip_to,
        skip_batchsearch=skip_batchsearch,
        force_recheck_pending=force_recheck_pending,
        force_recheck_lh_pending=force_recheck_lh_pending,
        include_lt_unit_fallback=include_lt_unit_fallback,
        include_bda_prefilter=include_bda_prefilter,
    )

    if not skip_sheet:
        if unified_result.get("batchsearch_completed"):
            # Preserve orders that were already published as Pending and were
            # resolved by this new BatchSearch before compacting result history.
            UNIFIED_PENDING_BOT.capture_published_pending_status_changes(
                UNIFIED_PENDING_BOT.create_bq_service()
            )
        print("\n[2/2] Export pending result to Google Sheet")
        EXPORT_PENDING_SHEET_BOT.run_once(
            fetch_missing_cogs=fetch_cogs,
            cogs_limit=cogs_limit,
        )
        if unified_result.get("batchsearch_completed"):
            # Never discard BDA TO staging when its merge unexpectedly returns no rows.
            # Keeping it is preferable to losing candidates before their T+18h check.
            merged_bda_to_staging = (
                clear_bda_staging
                and unified_result.get("bda_staging_merged", False)
            )
            UNIFIED_PENDING_BOT.clear_completed_candidate_staging(
                UNIFIED_PENDING_BOT.create_bq_service(),
                clear_to_staging=merged_bda_to_staging,
                clear_bulky_staging=True,
            )
            if not merged_bda_to_staging:
                print("Kept TO staging: BDA TO candidates were not merged in this cycle")
        UNIFIED_PENDING_BOT.prune_finalized_pending_candidates(
            UNIFIED_PENDING_BOT.create_bq_service()
        )
        UNIFIED_PENDING_BOT.compact_pending_result_history(
            UNIFIED_PENDING_BOT.create_bq_service()
        )
    else:
        print("\n[2/2] Skip export pending result to Google Sheet")

    elapsed = (datetime.now() - cycle_started_at).total_seconds()
    print(f"Pending pipeline done. Elapsed: {elapsed:.0f}s")
    if return_result:
        return {
            "elapsed": elapsed,
            "unified_result": unified_result,
        }
    return elapsed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Chay 1 vong roi dung.")
    parser.add_argument("--interval-minutes", type=int, default=15, help="So phut giua 2 vong pipeline.")
    parser.add_argument("--bulky-limit", type=int, default=None, help="Gioi han so LT collect tu Handover/lt_unit trong moi vong.")
    parser.add_argument("--to-limit", type=int, default=None, help="Gioi han so TO Sorting collect tu lt_unit trong moi vong.")
    parser.add_argument("--batch-limit", type=int, default=None, help="Gioi han candidate batchsearch; BOT tu cap toi da 2000 moi vong.")
    parser.add_argument("--skip-bulky", action="store_true", help="Bo qua collect candidate tu API pending inbound LH.")
    parser.add_argument("--skip-to", action="store_true", help="Bo qua collect TO Sorting tu lt_unit.")
    parser.add_argument(
        "--include-lt-unit-fallback",
        action="store_true",
        default=True,
        help="Quet TO Sorting trong lt_unit (mac dinh bat).",
    )
    parser.add_argument("--skip-batchsearch", action="store_true", help="Chi tao candidate, khong BatchSearch ket qua cuoi.")
    parser.add_argument("--skip-sheet", action="store_true", help="Bo qua buoc export pending tu BigQuery ra Google Sheet.")
    parser.add_argument("--fetch-cogs", action="store_true", help="Lay COGS cho Pending cuoi truoc khi xuat Sheet.")
    parser.add_argument("--cogs-limit", type=int, default=250, help="Toi da Pending can goi COGS moi vong.")
    parser.add_argument(
        "--force-recheck-pending",
        action="store_true",
        help="Recheck ngay cac ket qua PENDING cu, bo qua chu ky 120 phut.",
    )
    parser.add_argument(
        "--keep-bda-staging",
        action="store_true",
        help="Khong xoa staging BDA sau export Sheet; dung khi BOT BDA dang chay song song.",
    )
    args = parser.parse_args()

    interval_seconds = max(1, args.interval_minutes) * 60
    while True:
        elapsed = 0
        try:
            elapsed = run_cycle(
                bulky_limit=args.bulky_limit,
                to_limit=args.to_limit,
                batch_limit=args.batch_limit,
                skip_bulky=args.skip_bulky,
                skip_to=args.skip_to,
                skip_batchsearch=args.skip_batchsearch,
                skip_sheet=args.skip_sheet,
                force_recheck_pending=args.force_recheck_pending,
                include_lt_unit_fallback=args.include_lt_unit_fallback,
                clear_bda_staging=not args.keep_bda_staging,
                fetch_cogs=args.fetch_cogs,
                cogs_limit=args.cogs_limit,
            )
        except Exception:
            print("Pending pipeline failed:")
            traceback.print_exc()

        if args.once:
            return

        sleep_seconds = max(0, interval_seconds - int(elapsed))
        print(f"Sleep {sleep_seconds} seconds before next pending pipeline cycle.")
        UNIFIED_PENDING_BOT.EXPORT_BOT.LT_BOT.ti.sleep(sleep_seconds)


if __name__ == "__main__":
    main()
