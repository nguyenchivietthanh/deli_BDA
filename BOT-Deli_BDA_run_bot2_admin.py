"""BOT 2: Admin pending worker.

Only uses the Admin FMS account/profile. It expands Handover inbound TOs,
collects Pending Inbound for both Ended and Handover LTs and promotes
candidates into SQLite. BatchSearch, final results and Google Sheets belong
to the independent BOT 3 worker.
"""

import argparse
import importlib.util
import os
import time
import traceback
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_ADMIN_PROFILE = Path(os.getenv("LOCALAPPDATA", "")) / "BOT_DELI_FMS_BROWSER_ADMIN" / "User Data"
os.environ.setdefault("BOT_DELI_CHROME_USER_DATA_DIR_ADMIN", str(DEFAULT_ADMIN_PROFILE))
os.environ.setdefault("BOT_DELI_CHROME_PROFILE_ADMIN", "Default")


def load_module(path, module_name):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


EXPORT_BOT = load_module(BASE_DIR / "BOT-Deli_BDA_export_LT_unit_to_BQ.py", "bot2_export_lt_unit")
UNIFIED_BOT = load_module(BASE_DIR / "BOT-Deli_BDA_unified_pending_pipeline.py", "bot2_unified_pending")
PENDING_BOT = load_module(BASE_DIR / "BOT-Deli_BDA_run_pending_pipeline.py", "bot2_pending_runner")
# Was 2, to split work with BOT 1 (which takes trips with exactly 1 arrived stop
# after BDA - see BOT1_HANDOVER_ARRIVED_SEQUENCE_COUNT). That split leaked:
# BOT 1 reads the LIVE FMS handover list, while this worker reads the durable
# DB queue, so any 1-context trip BOT 1 missed in its moment was orphaned - no
# worker would ever expand it. Measured 2026-09-06: 104 of 347 queued sequences
# had exactly 1 context, they were the oldest, and since the queue is
# ORDER BY arrived_time LIMIT --handover-limit they permanently occupied every
# slot -> this step did zero useful work per cycle and the queue only grew
# (260 -> 348 over ~10h despite raising --handover-limit 2 -> 5).
# 1 lets this worker pick up whatever BOT 1 did not get to; BOT 1 still grabs
# them first from its live fetch, and only_unexpanded + the
# HANDOVER_SEQUENCE_COMPLETE marker keep the two from doing the same work twice.
BOT2_HANDOVER_MIN_ARRIVED_SEQUENCES = 1
DEFAULT_PRIORITY_BATCH_LIMIT = 5000
DEFAULT_BACKGROUND_HANDOVER_LIMIT = 2
DEFAULT_BACKGROUND_LH_LIMIT = 10
DEFAULT_BACKGROUND_TO_LIMIT = 250


def expand_handover_inbound(limit=None):
    service = UNIFIED_BOT.create_bq_service()
    UNIFIED_BOT.ensure_tables(service)
    handover_trips = UNIFIED_BOT.get_stored_handover_trip_candidates(
        service,
        limit=limit,
        only_unexpanded=True,
    )
    total_ready = len(handover_trips)
    handover_trips = [
        trip for trip in handover_trips
        if len(EXPORT_BOT.handover_export_contexts(trip)) >= BOT2_HANDOVER_MIN_ARRIVED_SEQUENCES
    ]
    moved_to_bot1 = total_ready - len(handover_trips)
    print(
        "Admin Handover LT ready to expand "
        f"(>= {BOT2_HANDOVER_MIN_ARRIVED_SEQUENCES} arrived sequence): {len(handover_trips)} "
        f"| BOT 1 short Handover: {moved_to_bot1}"
    )
    if not handover_trips:
        return 0
    admin_session = EXPORT_BOT.FmsSession("Admin")
    return EXPORT_BOT.export_handover_trips_to_lt_unit(handover_trips, admin_session, service)


def finalize_pending_outputs(fetch_cogs=False, cogs_limit=250):
    """Publish one consolidated Sheet snapshot after one or more queue batches."""
    service = UNIFIED_BOT.create_bq_service()
    UNIFIED_BOT.capture_published_pending_status_changes(service)
    PENDING_BOT.EXPORT_PENDING_SHEET_BOT.run_once(
        fetch_missing_cogs=fetch_cogs,
        cogs_limit=cogs_limit,
    )
    # TO staging belongs to BOT 1. Bulky staging is produced only by this
    # Admin worker and is safe to clear after its durable queue + Sheet sync.
    UNIFIED_BOT.clear_completed_candidate_staging(
        UNIFIED_BOT.create_bq_service(),
        clear_to_staging=False,
        clear_bulky_staging=True,
    )
    UNIFIED_BOT.prune_finalized_pending_candidates(
        UNIFIED_BOT.create_bq_service()
    )
    UNIFIED_BOT.compact_pending_result_history(
        UNIFIED_BOT.create_bq_service()
    )


def drain_ready_batchsearch_queue(batch_limit, fetch_cogs, cogs_limit):
    """Finish every candidate already due before the next Admin cycle.

    Each pass retains the configured batch limit so FMS receives manageable
    BatchSearch requests.  The readiness query naturally excludes freshly
    checked PENDING rows until their normal recheck interval is due.
    """
    service = UNIFIED_BOT.create_bq_service()
    UNIFIED_BOT.ensure_tables(service)
    batches = 0
    total_processed = 0

    while True:
        ready = UNIFIED_BOT.latest_candidates_to_batch(service, limit=1)
        if not ready:
            break

        batches += 1
        print(
            "\n[Drain] BatchSearch queue still has due candidate(s). "
            f"Run backlog batch {batches} with limit={batch_limit or 'default'}."
        )
        result = PENDING_BOT.run_cycle(
            skip_bulky=True,
            skip_to=True,
            batch_limit=batch_limit,
            skip_sheet=True,
            include_lt_unit_fallback=False,
            include_bda_prefilter=False,
            clear_bda_staging=False,
            return_result=True,
        )
        processed = int(result["unified_result"].get("batchsearch_inserted") or 0)
        total_processed += processed
        if processed <= 0:
            print(
                "[Drain] No BatchSearch result was written in this pass; stop "
                "draining to avoid looping forever. The next cycle will retry."
            )
            break

    if total_processed:
        print(
            "[Drain] All processed batches are durable in SQLite. "
            "Refresh Google Sheet once for the whole drain."
        )
        finalize_pending_outputs(
            fetch_cogs=fetch_cogs,
            cogs_limit=cogs_limit,
        )

    print(
        "[Drain] Due BatchSearch queue finished. "
        f"batches={batches} | processed={total_processed}"
    )
    return total_processed


def run_urgent_lane(batch_limit, fetch_cogs=False, cogs_limit=250, publish=True):
    """Process due precheck/final candidates before or after background work."""
    effective_limit = batch_limit or DEFAULT_PRIORITY_BATCH_LIMIT
    service = UNIFIED_BOT.create_bq_service()
    UNIFIED_BOT.ensure_tables(service)
    if not UNIFIED_BOT.latest_candidates_to_batch(service, limit=1):
        print("\n[Urgent lane] No candidate is due; continue with background work.")
        return {
            "elapsed": 0,
            "unified_result": {"batchsearch_inserted": 0},
        }
    print(
        "\n[Urgent lane] BatchSearch due candidates first "
        f"(limit={effective_limit}; FINAL, PENDING recheck, UNKNOWN, then PRECHECK)."
    )
    return PENDING_BOT.run_cycle(
        skip_bulky=True,
        skip_to=True,
        batch_limit=effective_limit,
        include_lt_unit_fallback=False,
        include_bda_prefilter=False,
        clear_bda_staging=False,
        skip_sheet=not publish,
        fetch_cogs=fetch_cogs,
        cogs_limit=cogs_limit,
        return_result=True,
    )


def run_cycle(
    bulky_limit=None,
    to_limit=None,
    batch_limit=None,
    handover_limit=None,
    force_recheck_lh_pending=False,
    **_legacy_options,
):
    started_at = datetime.now()
    print("=" * 80)
    print(f"BOT 2 Admin started: {started_at.isoformat(timespec='seconds')}")
    print(f"Admin Chrome profile: {os.environ['BOT_DELI_CHROME_USER_DATA_DIR_ADMIN']}")

    background_handover_limit = handover_limit or DEFAULT_BACKGROUND_HANDOVER_LIMIT
    background_bulky_limit = bulky_limit or DEFAULT_BACKGROUND_LH_LIMIT
    background_to_limit = to_limit or DEFAULT_BACKGROUND_TO_LIMIT
    print(
        "\n[Background lane] bounded slice: "
        f"handover_lt={background_handover_limit} | "
        f"pending_inbound_lt={background_bulky_limit} | to={background_to_limit}"
    )
    print("[1/3] Admin - Expand inbound TO for stored Handover LT")
    expand_handover_inbound(limit=background_handover_limit)

    print("\n[2/3] Admin - Pending Inbound for Ended/Handover")
    PENDING_BOT.run_cycle(
        bulky_limit=background_bulky_limit,
        skip_to=True,
        batch_limit=batch_limit,
        force_recheck_lh_pending=force_recheck_lh_pending,
        include_lt_unit_fallback=True,
        # BOT 1 promotes BDA staging after its own TO expansion. Bot 2 must
        # not merge or clear the same staging table concurrently.
        include_bda_prefilter=False,
        # BOT 1 may write new BDA staging while this worker is checking.
        clear_bda_staging=False,
        skip_batchsearch=True,
        skip_sheet=True,
    )

    print("\n[3/3] Admin - bounded TO Detail slice")
    PENDING_BOT.run_cycle(
        to_limit=background_to_limit,
        skip_bulky=True,
        batch_limit=batch_limit,
        include_lt_unit_fallback=True,
        include_bda_prefilter=False,
        clear_bda_staging=False,
        skip_batchsearch=True,
        skip_sheet=True,
    )

    print("\nBOT 2 collection complete. BOT 3 will process the SQLite queue independently.")

    elapsed = (datetime.now() - started_at).total_seconds()
    print(f"BOT 2 Admin done. Elapsed: {elapsed:.0f}s")
    return elapsed


def main():
    parser = argparse.ArgumentParser(description="BOT 2 - Admin pending worker")
    parser.add_argument("--once", action="store_true", help="Chay mot vong roi dung.")
    parser.add_argument("--interval-minutes", type=int, default=15, help="Thoi gian nghi giua hai vong.")
    parser.add_argument("--handover-limit", type=int, default=None, help="Gioi han LT Handover expand moi vong.")
    parser.add_argument("--bulky-limit", type=int, default=None, help="Gioi han LT Pending Inbound moi vong.")
    parser.add_argument("--to-limit", type=int, default=None, help="Gioi han TO Detail moi vong.")
    parser.add_argument("--batch-limit", type=int, default=None, help="Gioi han candidate BatchSearch moi vong.")
    parser.add_argument(
        "--priority-batch-limit",
        type=int,
        default=DEFAULT_PRIORITY_BATCH_LIMIT,
        help="So candidate toi da cho moi lan khan (dau va cuoi vong); mac dinh 5000.",
    )
    parser.add_argument(
        "--drain-ready-before-next-cycle",
        action="store_true",
        help=(
            "Drain het candidate da den luot truoc background, kiem tra lai "
            "sau background, roi moi sang vong Admin tiep theo."
        ),
    )
    parser.add_argument(
        "--force-recheck-pending-inbound",
        action="store_true",
        help="Ep goi lai Pending Inbound bo qua checkpoint sequence; chi dung de kiem tra/sua du lieu.",
    )
    parser.add_argument(
        "--run-bot3-work",
        action="store_true",
        help="Sau BOT 2, dong bo Pending cuoi cung sang tab pending_work.",
    )
    parser.add_argument(
        "--skip-cogs",
        action="store_true",
        help="Khong goi API COGS trong BOT 2 truoc khi xuat Sheet.",
    )
    parser.add_argument(
        "--cogs-limit",
        type=int,
        default=250,
        help="So don Pending toi da BOT 2 goi COGS moi vong.",
    )
    parser.add_argument(
        "--bot3-assign",
        action="store_true",
        help="Khi chay BOT 3, chia don theo Valid!list_chia_don va deadline_cctv.",
    )
    parser.add_argument(
        "--bot3-retention-days",
        type=int,
        default=5,
        help="Xoa dong pending_work da duoc BOT xuat Sheet tu N ngay truoc.",
    )
    parser.add_argument(
        "--error-retry-seconds",
        type=int,
        default=90,
        help="Sau loi bat ngo, thu lai sau N giay thay vi cho het interval.",
    )
    parser.add_argument("--stop-on-error", action="store_true")
    args = parser.parse_args()

    while True:
        elapsed = 0
        failed = False
        try:
            elapsed = run_cycle(
                bulky_limit=args.bulky_limit,
                to_limit=args.to_limit,
                batch_limit=args.batch_limit,
                handover_limit=args.handover_limit,
                force_recheck_lh_pending=args.force_recheck_pending_inbound,
            )
        except Exception:
            failed = True
            print("BOT 2 Admin failed:")
            traceback.print_exc()
            if args.stop_on_error:
                raise
        if args.once:
            return
        if failed:
            sleep_seconds = max(15, args.error_retry_seconds)
        else:
            sleep_seconds = max(0, args.interval_minutes * 60 - int(elapsed))
        print(f"BOT 2 Admin sleep {sleep_seconds}s before next cycle.")
        time.sleep(sleep_seconds)


if __name__ == "__main__":
    main()
