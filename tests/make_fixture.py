"""Small correctness data only: never use this fixture for benchmark numbers."""
from datetime import datetime, timedelta
from pathlib import Path


def make_fixture(root):
    import pyarrow as pa
    import pyarrow.parquet as pq
    root = Path(root)
    trips = root / "trips"
    trips.mkdir(parents=True, exist_ok=False)
    pickup = datetime(2023, 1, 1)
    good = dict(VendorID=2, tpep_pickup_datetime=pickup,
                tpep_dropoff_datetime=pickup + timedelta(minutes=10),
                passenger_count=1, trip_distance=3.0, PULocationID=1,
                DOLocationID=2, fare_amount=10.0, total_amount=12.0)
    micro = {**good, "tpep_pickup_datetime": pickup + timedelta(hours=1, microseconds=123456),
             "tpep_dropoff_datetime": pickup + timedelta(hours=1, seconds=2, microseconds=123457),
             "trip_distance": 1.0}
    zero_fare = {**good, "fare_amount": -0.0}
    bad = [{**good, **change} for change in [
        {"VendorID": None}, {"passenger_count": 1.5}, {"trip_distance": 0.0},
        {"trip_distance": float("nan")}, {"trip_distance": float("inf")},
        {"tpep_pickup_datetime": None}, {"tpep_dropoff_datetime": pickup - timedelta(seconds=1)}]]
    missing_zone = {**good, "PULocationID": 999}
    first = [good, micro, zero_fare, missing_zone, *bad]
    second = [good, micro, {**zero_fare, "fare_amount": 0.0}]
    for name, rows, small_ints, float_passengers in [
        ("first.parquet", first, False, True), ("second.parquet", second, True, False)]:
        from common import arrow_schema
        fields = []
        for field in arrow_schema("base"):
            dtype = field.type
            if field.name in ["VendorID", "PULocationID", "DOLocationID"] and small_ints:
                dtype = pa.int32()
            if field.name == "passenger_count" and float_passengers:
                dtype = pa.float64()
            fields.append(pa.field(field.name, dtype))
        pq.write_table(pa.Table.from_pylist(rows, schema=pa.schema(fields)), trips / name,
                       row_group_size=2)
    (root / "zones.csv").write_text('LocationID,Borough,Zone,service_zone\n1,Queens,Zone A,Boro Zone\n2,Brooklyn,Zone B,Boro Zone\n')
    return first, second


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    make_fixture(parser.parse_args().output)
