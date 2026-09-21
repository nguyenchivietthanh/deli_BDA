"""BOT_Pick_BDA - Buoc 2: ra tung chuyen LT (dang NOT_PARSED trong lt_in_trip)
thanh danh sach TO thuc nhan tai BDA, luu vao bang lt_in_unit.

Dung API "TO trong 1 chuyen LT" (type=inbound) voi actual_unloaded_sequence_number
lay tu sequence_number da xac dinh o Buoc 1. Chi giu lai cac dong la TO
(to_number bat dau bang "TO"); cac kien SPX le khong di kem TO thuoc ve luong
pending_inbound (type=pending) se lam o Buoc 3, khong dua vao day.

lt_in_unit.parsed_status o day LUON la NOT_PARSED khi moi ghi: no nghia la
"da ra MANIFEST (goi general_to/detail/search) cho TO nay hay chua", va se
duoc Buoc 3 chuyen thanh PARSED sau khi ra xong tung TO. Buoc nay chi chiu
trach nhiem cho lt_in_trip.parsed_status (da lay danh sach TO cua LT nay hay
chua) - UPDATE truc tiep dong da co trong lt_in_trip, khong insert dong moi.
"""

import argparse
import time
import traceback

import BOT_Pick_BDA_arrived_LT as ARRIVED_BOT


BIGQUERY_PROJECT_ID = ARRIVED_BOT.BIGQUERY_PROJECT_ID
BIGQUERY_DATASET_ID = ARRIVED_BOT.BIGQUERY_DATASET_ID
TRIP_TABLE_ID = ARRIVED_BOT.TRIP_TABLE_ID
UNIT_TABLE_ID = "lt_in_unit"

PAGE_COUNT = 100
RUN_INTERVAL_SECONDS = 60 * 60
DEFAULT_TRIP_LIMIT = 200


# --------------------------------------------------------------- SQLite/BQ

def create_bq_service():
    return ARRIVED_BOT.create_bq_service()


def unit_table_schema_fields():
    return [
        {"name": "trip_number", "type": "STRING"},
        {"name": "trip_id", "type": "STRING"},
        {"name": "sequence_number", "type": "INTEGER"},
        {"name": "station", "type": "STRING"},
        {"name": "to_station", "type": "STRING"},
        {"name": "arrived_time", "type": "DATETIME"},
        {"name": "item_type", "type": "STRING"},
        {"name": "to_number", "type": "STRING"},
        {"name": "to_path", "type": "STRING"},
        {"name": "sender", "type": "STRING"},
        {"name": "receiver", "type": "STRING"},
        {"name": "to_parcel_quantity", "type": "INTEGER"},
        {"name": "loaded_station_name", "type": "STRING"},
        {"name": "unloaded_station_name", "type": "STRING"},
        {"name": "parsed_status", "type": "STRING"},
        {"name": "parsed_at", "type": "DATETIME"},
        {"name": "exported_at", "type": "DATETIME"},
    ]


def ensure_tables(service):
    service.store.ensure_table(UNIT_TABLE_ID, unit_table_schema_fields())
    service.store.query(
        f'CREATE INDEX IF NOT EXISTS idx_lt_in_unit_trip_seq '
        f'ON "{UNIT_TABLE_ID}" (trip_id, sequence_number)'
    )
    service.store.query(
        f'CREATE INDEX IF NOT EXISTS idx_lt_in_unit_to_number '
        f'ON "{UNIT_TABLE_ID}" (to_number)'
    )
    service.store.query(
        f'CREATE INDEX IF NOT EXISTS idx_lt_in_unit_parsed '
        f'ON "{UNIT_TABLE_ID}" (parsed_status, to_number)'
    )


def query_rows(service, sql):
    _fields, rows = service.store.query(sql)
    return rows


def insert_rows(service, rows, table_id, schema_fields):
    if not rows:
        return 0
    service.store.insert_rows(table_id, rows, schema_fields)
    return len(rows)


# ------------------------------------------------------------ lt_in_trip IO

def get_unparsed_trips(service, limit=None):
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    query = f"""
    WITH latest AS (
      SELECT *,
        ROW_NUMBER() OVER (
          PARTITION BY trip_id, sequence_number
          ORDER BY observed_at DESC
        ) AS rn
      FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{TRIP_TABLE_ID}`
    )
    SELECT
      trip_number, trip_id, sequence_number, station, to_station,
      arrived_time, is_last_station
    FROM latest
    WHERE rn = 1 AND parsed_status = 'NOT_PARSED'
    ORDER BY arrived_time
    {limit_clause}
    """
    return query_rows(service, query)


def mark_trip_parsed(service, trip, parsed_at):
    ARRIVED_BOT.update_trip_parsed_status(
        service,
        trip.get("trip_id"),
        trip.get("sequence_number"),
        "PARSED",
        parsed_at,
    )


# ------------------------------------------------------------ lt_in_unit IO

def load_known_to_numbers(service, trip_id, sequence_number):
    rows = query_rows(
        service,
        f"""
        SELECT DISTINCT to_number
        FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{UNIT_TABLE_ID}`
        WHERE trip_id = '{trip_id}' AND sequence_number = {int(sequence_number)}
        """,
    )
    return {
        ARRIVED_BOT.to_text(row.get("to_number"))
        for row in rows
        if ARRIVED_BOT.to_text(row.get("to_number"))
    }


# --------------------------------------------------------------------- FMS

def fetch_inbound_to_list(trip_id, trip_number, sequence_number):
    url_template = (
        "https://spx.shopee.vn/api/admin/transportation/trip/history/loading/list"
        f"?trip_id={trip_id}&pageno={{page}}&count={{count}}"
        f"&actual_unloaded_sequence_number={sequence_number}&type=inbound"
    )
    return ARRIVED_BOT.fetch_list(
        url_template,
        count=PAGE_COUNT,
        label=f"TO trong LT (inbound) - {trip_number} - seq {sequence_number}",
    )


def source_to_number(row):
    return ARRIVED_BOT.to_text(ARRIVED_BOT.first_value(row, [
        "to_number", "parcel_number", "scan_number", "tracking_number",
    ]))


def build_unit_rows(trip, inbound_rows, known_to_numbers, exported_at):
    rows = []
    seen = set(known_to_numbers)
    for source_row in inbound_rows:
        to_number = source_to_number(source_row)
        if not to_number.startswith("TO") or to_number in seen:
            continue
        seen.add(to_number)

        sender = ARRIVED_BOT.to_text(ARRIVED_BOT.first_value(source_row, [
            "sender_station_name", "sender_station", "loaded_station_name",
        ])) or trip.get("station") or ""
        receiver = ARRIVED_BOT.to_text(ARRIVED_BOT.first_value(source_row, [
            "receiver_station_name", "receive_station_name",
            "unloaded_station_name", "actual_unloaded_station_name",
        ])) or ARRIVED_BOT.SOC_CODE
        to_path = ARRIVED_BOT.to_text(ARRIVED_BOT.first_value(source_row, [
            "to_path", "path", "route", "route_name", "transportation_path",
        ]))
        if not to_path:
            to_path = f"{sender}>{receiver}" if sender and receiver else (sender or receiver)
        quantity = ARRIVED_BOT.to_int(ARRIVED_BOT.first_value(source_row, [
            "number_of_order", "to_parcel_quantity", "to_quantity",
        ]), default=0) or None

        rows.append({
            "trip_number": trip.get("trip_number") or "",
            "trip_id": ARRIVED_BOT.to_text(trip.get("trip_id")),
            "sequence_number": ARRIVED_BOT.to_int(trip.get("sequence_number"), default=0),
            "station": trip.get("station") or "",
            "to_station": ARRIVED_BOT.SOC_CODE,
            "arrived_time": trip.get("arrived_time"),
            "item_type": "TO_SORTING",
            "to_number": to_number,
            "to_path": to_path,
            "sender": sender,
            "receiver": receiver,
            "to_parcel_quantity": quantity,
            "loaded_station_name": ARRIVED_BOT.to_text(ARRIVED_BOT.first_value(
                source_row, ["loaded_station_name"]
            )) or sender,
            "unloaded_station_name": ARRIVED_BOT.to_text(ARRIVED_BOT.first_value(
                source_row, ["unloaded_station_name", "actual_unloaded_station_name"]
            )) or receiver,
            "parsed_status": "NOT_PARSED",
            "parsed_at": None,
            "exported_at": exported_at,
        })
    return rows


# --------------------------------------------------------------------- run

def run_once(limit=None):
    service = create_bq_service()
    ensure_tables(service)

    trips = get_unparsed_trips(service, limit=limit or DEFAULT_TRIP_LIMIT)
    print(f"Chuyen LT can ra danh sach TO (NOT_PARSED): {len(trips)}")

    total_unit_rows = 0
    parsed_trip_count = 0
    exported_at = ARRIVED_BOT.now_text()

    for trip in trips:
        trip_id = ARRIVED_BOT.to_text(trip.get("trip_id"))
        trip_number = trip.get("trip_number")
        sequence_number = ARRIVED_BOT.to_int(trip.get("sequence_number"), default=0)
        if not trip_id or sequence_number <= 0:
            continue
        try:
            inbound_rows = fetch_inbound_to_list(trip_id, trip_number, sequence_number)
            known_to_numbers = load_known_to_numbers(service, trip_id, sequence_number)
            unit_rows = build_unit_rows(trip, inbound_rows, known_to_numbers, exported_at)
            inserted = insert_rows(service, unit_rows, UNIT_TABLE_ID, unit_table_schema_fields())
            total_unit_rows += inserted
            mark_trip_parsed(service, trip, exported_at)
            parsed_trip_count += 1
            print(
                f"Ra LT {trip_number} seq {sequence_number}: {len(inbound_rows)} dong tra ve, "
                f"{inserted} TO moi ghi vao {UNIT_TABLE_ID}"
            )
        except Exception:
            print(f"Loi khi ra LT {trip_number} seq {sequence_number}, se retry vong sau:")
            traceback.print_exc()

    print(
        f"Da danh dau PARSED: {parsed_trip_count}/{len(trips)} chuyen. "
        f"Tong dong {UNIT_TABLE_ID} moi: {total_unit_rows}"
    )
    return total_unit_rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Chay 1 vong roi dung.")
    parser.add_argument("--limit", type=int, default=None, help="So chuyen LT xu ly moi vong.")
    parser.add_argument("--interval-minutes", type=int, default=RUN_INTERVAL_SECONDS // 60)
    args = parser.parse_args()

    if args.once or args.limit:
        run_once(limit=args.limit)
        return

    interval_seconds = max(1, args.interval_minutes) * 60
    while True:
        try:
            run_once()
        except Exception:
            print("BOT_Pick_BDA_export_LT_unit that bai:")
            traceback.print_exc()
        print(f"Nghi {interval_seconds}s truoc chu ky tiep theo.")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()
