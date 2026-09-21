import argparse
import importlib.util
from collections import Counter
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
UNIFIED_PATH = BASE_DIR / "BOT-Deli_BDA_unified_pending_pipeline.py"


def load_module(path, module_name):
    spec = importlib.util.spec_from_file_location(module_name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


BOT = load_module(UNIFIED_PATH, "bot_deli_bda_unified_pending_pipeline")


def normalize_bound(value, label):
    parsed = BOT.parse_datetime_value(value)
    if not parsed:
        raise ValueError(f"{label} khong dung dinh dang YYYY-MM-DD HH:MM:SS")
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def sql_text(value):
    return str(value).replace("'", "''")


def load_candidates(service, arrived_from, arrived_to):
    columns = ",\n      ".join(
        f"c.{field['name']}" for field in BOT.candidate_schema_fields()
    )
    query = f"""
    WITH latest_candidate AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY candidate_id
          ORDER BY candidate_checked_at DESC
        ) AS candidate_latest_rank
      FROM `{BOT.BIGQUERY_PROJECT_ID}.{BOT.BIGQUERY_DATASET_ID}.{BOT.BIGQUERY_CANDIDATE_TABLE_ID}`
      WHERE order_number IS NOT NULL
        AND order_number != ''
        AND order_number != '{BOT.SEQUENCE_MARKER_ORDER}'
        AND arrived_time >= '{sql_text(arrived_from)}'
        AND arrived_time < '{sql_text(arrived_to)}'
    )
    SELECT
      {columns}
    FROM latest_candidate c
    WHERE c.candidate_latest_rank = 1
    ORDER BY c.arrived_time, c.trip_number, c.to_number, c.order_number
    """
    return BOT.query_bq(service, query, fail_soft=False)


def tracking_item_by_order(items):
    return {
        BOT.tracking_item_order_number(item): item
        for item in items
        if BOT.tracking_item_order_number(item)
    }


def run_test(arrived_from, arrived_to, batch_size):
    service = BOT.create_bq_service()
    BOT.ensure_tables(service)
    candidates = load_candidates(service, arrived_from, arrived_to)
    print(f"Pair-rule test candidates: {len(candidates)}")
    if not candidates:
        return

    fms_session = BOT.EXPORT_BOT.FmsSession(BOT.ADMIN_ROLE)
    terminal_after_arrived = 0
    remaining_suspect = 0
    batchsearch_missing = 0
    terminal_status_counts = Counter()

    for batch_index, batch in enumerate(BOT.chunked(candidates, batch_size), start=1):
        order_numbers = sorted({row["order_number"] for row in batch if row.get("order_number")})
        items = BOT.search_tracking_batch(fms_session, order_numbers)
        item_by_order = tracking_item_by_order(items)

        for candidate in batch:
            item = item_by_order.get(candidate.get("order_number"))
            if not item:
                batchsearch_missing += 1
                remaining_suspect += 1
                continue

            status_code = BOT.EXPORT_BOT.to_int(item.get("order_status"), default=-1)
            status = BOT.status_name(item, status_code)
            arrived_at = BOT.parse_datetime_value(candidate.get("arrived_time"))
            received_at = BOT.parse_datetime_value(BOT.first_value(
                item,
                ["current_station_received_time", "current_station_receive_time"],
            ))
            is_terminal_after_arrived = (
                BOT.is_hard_not_pending_status(status, status_code)
                and bool(arrived_at and received_at and received_at > arrived_at)
            )
            if is_terminal_after_arrived:
                terminal_after_arrived += 1
                terminal_status_counts[status or str(status_code)] += 1
            else:
                remaining_suspect += 1

        print(
            f"Batch {batch_index}: processed={len(batch)} | "
            f"removed_by_pair_rule={terminal_after_arrived} | "
            f"remaining={remaining_suspect}"
        )

    print("=" * 80)
    print(f"Arrived window: {arrived_from} -> {arrived_to}")
    print(f"Before pair rule: {len(candidates)} candidate rows")
    print(f"NOT_PENDING by terminal AND received-after-arrived: {terminal_after_arrived}")
    print(f"Remaining after pair rule only: {remaining_suspect}")
    print(f"BatchSearch missing: {batchsearch_missing}")
    if terminal_status_counts:
        print("Removed status breakdown:")
        for status, total in terminal_status_counts.most_common():
            print(f"  - {status}: {total}")
    print("No Tracking Detail, no SQLite write, and no Google Sheet export were performed.")


def main():
    parser = argparse.ArgumentParser(
        description="Test BatchSearch terminal AND received-after-arrived rule only."
    )
    parser.add_argument("--arrived-from", required=True, help="YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--arrived-to", required=True, help="YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--batch-size", type=int, default=250, help="So don moi BatchSearch request.")
    args = parser.parse_args()

    arrived_from = normalize_bound(args.arrived_from, "arrived-from")
    arrived_to = normalize_bound(args.arrived_to, "arrived-to")
    if arrived_to <= arrived_from:
        raise ValueError("arrived-to phai sau arrived-from")
    run_test(arrived_from, arrived_to, max(1, min(args.batch_size, 500)))


if __name__ == "__main__":
    main()
