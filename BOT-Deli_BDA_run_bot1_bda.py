"""BOT 1: BDA collector.

Only uses the BDA FMS account/profile. It captures Ended and Handover LTs in
the same cycle, expands Ended TOs, and stages suspicious TO orders for Admin.
"""

import argparse
import importlib.util
import json
import os
import time
import traceback
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_BDA_PROFILE = Path(os.getenv("LOCALAPPDATA", "")) / "BOT_DELI_FMS_BROWSER_BDA" / "User Data"
os.environ.setdefault("BOT_DELI_CHROME_USER_DATA_DIR_BDA", str(DEFAULT_BDA_PROFILE))
os.environ.setdefault("BOT_DELI_CHROME_PROFILE_BDA", "Default")

# A trip can disappear from the Handover tab as soon as it becomes Completed.
# Reconcile a bounded number of those recently departed Handover trips per BDA
# cycle so their final receiving sequence is not lost outside the normal Ended
# arrived-time window.
HANDOVER_TO_ENDED_RECONCILE_LIMIT = 40
BOT1_HANDOVER_ARRIVED_SEQUENCE_COUNT = 1


def load_module(path, module_name):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ARRIVED_BOT = load_module(BASE_DIR / "BOT-Deli_BDA_arrived_LT.py", "bot1_arrived_lt")
EXPORT_BOT = load_module(BASE_DIR / "BOT-Deli_BDA_export_LT_unit_to_BQ.py", "bot1_export_lt_unit")
UNIFIED_BOT = load_module(BASE_DIR / "BOT-Deli_BDA_unified_pending_pipeline.py", "bot1_unified_pending")


def promote_ended_to_candidates(service):
    """Move BOT 1's completed Ended-TO staging into the durable queue."""
    if UNIFIED_BOT.deferred_to_expand_mode():
        print("Deferred-expand mode: skip Ended-TO staging promote (BOT 2 expands lt_unit late)")
        return 0
    staged_rows = UNIFIED_BOT.collect_bda_to_detail_candidates(service)
    if not staged_rows:
        print("BDA TO staging: khong co candidate moi can promote")
        return 0

    promoted = UNIFIED_BOT.store_candidates(service, staged_rows, [])[0]
    UNIFIED_BOT.clear_completed_candidate_staging(
        service,
        clear_to_staging=True,
        clear_bulky_staging=False,
    )
    print(f"BDA TO staging promoted to lt_pending_candidate: {promoted}")
    return promoted


def short_handover_trips_for_bda(trips):
    """BOT 1 expands only Handover trips with exactly one arrived stop after BDA."""
    selected = []
    for trip in trips or []:
        arrived_sequence_count = len(EXPORT_BOT.handover_export_contexts(trip))
        if arrived_sequence_count == BOT1_HANDOVER_ARRIVED_SEQUENCE_COUNT:
            selected.append(trip)
    return selected


def normalize_status(value):
    if value is None:
        return ""
    return str(value).strip().casefold()


def is_completed_trip_detail(detail_data):
    """Return true only when the current trip detail explicitly says Ended."""
    data = (detail_data or {}).get("data") or {}
    for field in (
        "trip_status",
        "display_status",
        "display_status_v2",
        "action_status",
        "status",
    ):
        value = data.get(field)
        if value in (90, "90"):
            return True
        text = normalize_status(value)
        if text in {"completed", "ended"}:
            return True
    return False


def query_rows(service, query):
    return UNIFIED_BOT.query_bq(service, query, fail_soft=True)


def departed_handover_trip_rows(service, current_handover_trip_ids, limit):
    """Find recent stored Handover trips which are no longer on the live tab."""
    current_ids_sql = ", ".join(json.dumps(str(trip_id)) for trip_id in sorted(current_handover_trip_ids))
    not_current_clause = f"WHERE trip_id NOT IN ({current_ids_sql})" if current_ids_sql else ""
    return query_rows(service, f"""
        WITH latest AS (
          SELECT
            *,
            ROW_NUMBER() OVER (
              PARTITION BY trip_id, sequence_number
              ORDER BY observed_at DESC
            ) AS rn
          FROM `{UNIFIED_BOT.BIGQUERY_PROJECT_ID}.{UNIFIED_BOT.BIGQUERY_DATASET_ID}.{UNIFIED_BOT.BIGQUERY_HANDOVER_TRIP_TABLE_ID}`
          WHERE source = 'handover'
        ),
        trip_latest AS (
          SELECT
            trip_id,
            MAX(trip_number) AS trip_number,
            MAX(station) AS station,
            MAX(to_station) AS to_station,
            MAX(trip_station_json) AS trip_station_json,
            MAX(observed_at) AS observed_at
          FROM latest
          WHERE rn = 1
          GROUP BY trip_id
        )
        SELECT *
        FROM trip_latest
        {not_current_clause}
        ORDER BY observed_at DESC, trip_number
        LIMIT {int(limit)}
    """)


def stored_handover_trip_rows_by_number(service, trip_numbers):
    values = [str(value).strip() for value in (trip_numbers or []) if str(value).strip()]
    if not values:
        return []
    trip_numbers_sql = ", ".join(json.dumps(value) for value in values)
    return query_rows(service, f"""
        WITH latest AS (
          SELECT
            *,
            ROW_NUMBER() OVER (
              PARTITION BY trip_id, sequence_number
              ORDER BY observed_at DESC
            ) AS rn
          FROM `{UNIFIED_BOT.BIGQUERY_PROJECT_ID}.{UNIFIED_BOT.BIGQUERY_DATASET_ID}.{UNIFIED_BOT.BIGQUERY_HANDOVER_TRIP_TABLE_ID}`
          WHERE source = 'handover'
        )
        SELECT
          trip_id,
          MAX(trip_number) AS trip_number,
          MAX(station) AS station,
          MAX(to_station) AS to_station,
          MAX(trip_station_json) AS trip_station_json,
          MAX(observed_at) AS observed_at
        FROM latest
        WHERE rn = 1
          AND trip_number IN ({trip_numbers_sql})
        GROUP BY trip_id
    """)


def existing_ended_sequence_keys(service):
    rows = query_rows(service, f"""
        SELECT DISTINCT trip_id, sequence_number
        FROM `{UNIFIED_BOT.BIGQUERY_PROJECT_ID}.{UNIFIED_BOT.BIGQUERY_DATASET_ID}.{UNIFIED_BOT.BIGQUERY_ENDED_TRIP_TABLE_ID}`
    """)
    return {
        f"{row.get('trip_id') or ''}|{row.get('sequence_number') or ''}"
        for row in rows
        if row.get("trip_id") and row.get("sequence_number") is not None
    }


def reconciliation_rows_from_detail(trip, detail_data, known_sequence_keys):
    data = (detail_data or {}).get("data") or {}
    stations = data.get("trip_station") or data.get("trip_stations") or []
    if not stations:
        return []

    fallback = {
        "id": trip.get("trip_id"),
        "trip_number": trip.get("trip_number"),
        "loaded_station_name": trip.get("station") or UNIFIED_BOT.SOC_CODE,
    }
    points = ARRIVED_BOT.build_trip_arrival_points(detail_data, fallback)
    rows = []
    for point in points:
        sequence_number = point.get("sequence_number")
        sequence_key = f"{trip.get('trip_id')}|{sequence_number}"
        if not sequence_number or sequence_key in known_sequence_keys:
            continue
        arrived_time = point.get("arrived_time")
        if not arrived_time:
            continue
        rows.append({
            "_trip_id": str(trip.get("trip_id") or ""),
            "trip_number": trip.get("trip_number") or "",
            "sequence_number": sequence_number,
            "station": point.get("station") or UNIFIED_BOT.SOC_CODE,
            "to_station": point.get("to_station") or "",
            "departed_time": ARRIVED_BOT.format_datetime(point.get("departed_time")),
            "sta": ARRIVED_BOT.format_datetime(point.get("sta")),
            "arrived_time": ARRIVED_BOT.format_datetime(arrived_time),
            "unseal_time": ARRIVED_BOT.format_datetime(point.get("unseal_time")),
            "unseal_operator": point.get("unseal_operator") or "",
            "unloaded_time": ARRIVED_BOT.format_datetime(point.get("unloaded_time")),
            "unloaded_operator": point.get("unloaded_operator") or "",
            "total_to": "",
            "total_order": "",
            "to_packed": "",
            "order_bulky": "",
            "order_packed": "",
        })
        known_sequence_keys.add(sequence_key)
    return rows


def append_reconciled_ended_rows_to_sheet(rows):
    if not rows:
        return 0
    worksheet = ARRIVED_BOT.open_target_sheet()
    headers, next_id, existing_sequence_keys = ARRIVED_BOT.sheet_context(worksheet)
    appendable = []
    for row in rows:
        sequence_key = f"{row.get('trip_number')}|{row.get('sequence_number')}"
        if sequence_key in existing_sequence_keys:
            continue
        row["id"] = next_id
        next_id += 1
        existing_sequence_keys.add(sequence_key)
        appendable.append(row)
    if not appendable:
        return 0
    pushed = ARRIVED_BOT.append_rows(worksheet, headers, appendable)
    print(f"Reconciled Handover -> Ended pushed to LT Sheet: {pushed} rows")
    return pushed


def reconcile_departed_handover_to_ended(
    service,
    fms_session,
    current_handover_trips,
    force_trip_numbers=None,
):
    """Promote recently disappeared Handover trips once FMS confirms Completed."""
    current_ids = {
        str(trip.get("trip_id") or trip.get("id"))
        for trip in current_handover_trips
        if trip.get("trip_id") or trip.get("id")
    }
    candidates = departed_handover_trip_rows(
        service,
        current_ids,
        HANDOVER_TO_ENDED_RECONCILE_LIMIT,
    )
    forced_rows = stored_handover_trip_rows_by_number(service, force_trip_numbers)
    candidates_by_trip_id = {
        str(row.get("trip_id") or ""): row
        for row in candidates + forced_rows
        if row.get("trip_id")
    }
    candidates = list(candidates_by_trip_id.values())
    if not candidates:
        return 0

    known_sequence_keys = existing_ended_sequence_keys(service)
    reconciled_rows = []
    for trip in candidates:
        trip_id = str(trip.get("trip_id") or "")
        trip_number = trip.get("trip_number") or trip_id
        detail_data = EXPORT_BOT.fetch_handover_trip_detail(fms_session, trip_id, trip_number)
        if not is_completed_trip_detail(detail_data):
            continue
        rows = reconciliation_rows_from_detail(trip, detail_data, known_sequence_keys)
        if rows:
            reconciled_rows.extend(rows)
            print(f"Reconciled Handover -> Ended {trip_number}: {len(rows)} new sequence(s)")

    if not reconciled_rows:
        return 0
    UNIFIED_BOT.store_ended_trip_rows(service, reconciled_rows)
    append_reconciled_ended_rows_to_sheet(reconciled_rows)
    print(f"Handover -> Ended reconciliation stored: {len(reconciled_rows)} sequence row(s)")
    return len(reconciled_rows)


def run_cycle(
    lt_limit=None,
    export_limit=None,
    lt_from=None,
    lt_to=None,
    reconcile_trip_numbers=None,
):
    started_at = datetime.now()
    print("=" * 80)
    print(f"BOT 1 BDA started: {started_at.isoformat(timespec='seconds')}")
    print(f"BDA Chrome profile: {os.environ['BOT_DELI_CHROME_USER_DATA_DIR_BDA']}")

    print("\n[1/3] BDA - Get Ended LT to Google Sheet")
    ended_rows = ARRIVED_BOT.run_once(limit=lt_limit, window_from=lt_from, window_to=lt_to)
    ended_service = UNIFIED_BOT.create_bq_service()
    UNIFIED_BOT.ensure_tables(ended_service)
    UNIFIED_BOT.store_ended_trip_rows(ended_service, ended_rows)

    # Store both LT sources before any slow TO expansion. BOT 2 can therefore
    # begin its Admin work from a complete Ended + Handover snapshot while BOT
    # 1 gives the stable Ended workload its priority.
    print("\n[2/3] BDA - Capture and store Handover LT (Admin expands inbound TO later)")
    handover_service = EXPORT_BOT.create_bq_service()
    EXPORT_BOT.ensure_tables(handover_service)
    handover_session = EXPORT_BOT.FmsSession(EXPORT_BOT.SOC_CODE)
    handover_trips = EXPORT_BOT.fetch_handover_trip_candidates(
        handover_session,
        limit=export_limit,
    )
    EXPORT_BOT.store_handover_trips(handover_service, handover_trips)
    print("Stored Ended + Handover LT. Admin worker can start from both sources now.")

    # Ended LTs are the stable, deadline-bearing workload. Expand and promote
    # them before spending time on the moving Handover list.
    print("\n[3/3] BDA - Expand Ended TO, stage, then promote suspicious orders")
    EXPORT_BOT.run_once(
        limit=export_limit,
        capture_handover=False,
        expand_handover_with_admin=False,
    )
    promote_ended_to_candidates(ended_service)

    short_handover_trips = short_handover_trips_for_bda(handover_trips)
    if short_handover_trips:
        print(
            "BDA expands short Handover LT "
            f"(= {BOT1_HANDOVER_ARRIVED_SEQUENCE_COUNT} arrived sequence): {len(short_handover_trips)}"
        )
        EXPORT_BOT.export_handover_trips_to_lt_unit(
            short_handover_trips,
            handover_session,
            handover_service,
            collect_candidates=True,
        )
    else:
        print("BDA short Handover LT ready to expand: 0")

    if export_limit is None or reconcile_trip_numbers:
        reconcile_departed_handover_to_ended(
            ended_service,
            handover_session,
            handover_trips,
            force_trip_numbers=reconcile_trip_numbers,
        )
    else:
        print("Skip Handover -> Ended reconciliation in limited test run.")

    elapsed = (datetime.now() - started_at).total_seconds()
    print(f"BOT 1 BDA done. Elapsed: {elapsed:.0f}s")
    return elapsed


def main():
    parser = argparse.ArgumentParser(description="BOT 1 - BDA Ended/Handover collector")
    parser.add_argument("--once", action="store_true", help="Chay mot vong roi dung.")
    parser.add_argument("--interval-minutes", type=int, default=15, help="Thoi gian nghi giua hai vong.")
    parser.add_argument("--lt-limit", type=int, default=None, help="Gioi han LT Ended ghi Sheet moi vong.")
    parser.add_argument("--export-limit", type=int, default=None, help="Gioi han LT Ended ra TO moi vong.")
    parser.add_argument("--lt-from", default=None, help="Moc arrived bat dau, chi dung khi --once.")
    parser.add_argument("--lt-to", default=None, help="Moc arrived ket thuc, chi dung khi --once.")
    parser.add_argument(
        "--reconcile-trip",
        action="append",
        default=[],
        help="Doi soat mot LT Handover cu da chuyen Ended. Co the dung nhieu lan.",
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
                lt_limit=args.lt_limit,
                export_limit=args.export_limit,
                lt_from=args.lt_from,
                lt_to=args.lt_to,
                reconcile_trip_numbers=args.reconcile_trip,
            )
        except Exception:
            failed = True
            print("BOT 1 BDA failed:")
            traceback.print_exc()
            if args.stop_on_error:
                raise
        if args.once:
            return
        if failed:
            sleep_seconds = max(15, args.error_retry_seconds)
        else:
            sleep_seconds = max(0, args.interval_minutes * 60 - int(elapsed))
        print(f"BOT 1 BDA sleep {sleep_seconds}s before next cycle.")
        time.sleep(sleep_seconds)


if __name__ == "__main__":
    main()
