#!/usr/bin/env python3
"""Exact multiset parity checks, using temporary SQLite storage rather than full-data RAM."""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
import tempfile
from collections import defaultdict
from pathlib import Path

from common import (BASE_COLUMNS, FLOAT_COLUMNS, OUTPUT_COLUMNS, TIME_COLUMNS, ZONE_COLUMNS, arrow_schema,
                    average_speed, canonical_key, parquet_files, write_json)


def files_by_bucket(root):
    grouped = defaultdict(list)
    for path in parquet_files(root):
        parent = path.parent
        if parent.name.startswith("bucket="):
            grouped[int(parent.name.split("=", 1)[1])].append(path)
        elif path.name == "empty.parquet":
            grouped[-1].append(path)
        else:
            raise ValueError(f"Expected bucket=N output directories: {path}")
    if not grouped:
        raise ValueError(f"No Parquet output files in {root}")
    return grouped


def encode_row(row, bucket, bucket_count, expected_bucket=None):
    if any(row[name] is None for name in OUTPUT_COLUMNS):
        raise ValueError("Unexpected null output value.")
    if any(not math.isfinite(row[name]) for name in FLOAT_COLUMNS):
        raise ValueError("Unexpected NaN/infinite output value.")
    key = canonical_key([row[name] for name in BASE_COLUMNS])
    if bucket >= 0 and expected_bucket != bucket:
        raise ValueError("Row was exported into the wrong hash bucket.")
    duration = row[TIME_COLUMNS[1]] - row[TIME_COLUMNS[0]]
    micros = (duration.days * 86400 + duration.seconds) * 1_000_000 + duration.microseconds
    expected_duration = float(micros) / 1_000_000.0
    if expected_duration <= 0 or row["trip_distance"] <= 0:
        raise ValueError("Nonpositive duration/distance in output.")
    if row["duration_seconds"] != expected_duration:
        raise ValueError("Incorrect duration_seconds.")
    if row["pickup_hour"] != row[TIME_COLUMNS[0]].hour:
        raise ValueError("Incorrect pickup_hour.")
    if row["average_speed_mph"] != average_speed(row["trip_distance"], expected_duration):
        raise ValueError("Incorrect average_speed_mph.")
    return json.dumps([key, *[row[name] for name in ZONE_COLUMNS],
                       float(row["duration_seconds"]).hex(), int(row["pickup_hour"]),
                       float(row["average_speed_mph"]).hex()], separators=(",", ":"))


def compare(left, right, batch_rows=8192):
    import pyarrow.parquet as pq
    left, right = Path(left), Path(right)
    first = json.loads((left / "_run.json").read_text())
    second = json.loads((right / "_run.json").read_text())
    for metadata in [first, second]:
        if metadata["status"] != "complete":
            raise ValueError("Both runs must have completed successfully.")
    for field in ["contract", "input_manifest", "lookup_sha256"]:
        if first[field] != second[field]:
            raise ValueError(f"Runs used different {field}.")
    buckets = first["configuration"]["buckets"]
    if buckets != second["configuration"]["buckets"]:
        raise ValueError("Runs used different bucket counts.")
    groups = [files_by_bucket(left), files_by_bucket(right)]
    total_rows = [0, 0]
    with tempfile.TemporaryDirectory(prefix="taxi-parity-") as temporary:
        connection = sqlite3.connect(str(Path(temporary) / "parity.sqlite"))
        try:
            connection.execute("PRAGMA cache_size=-16384")
            connection.execute("PRAGMA temp_store=FILE")
            for bucket in sorted(set(groups[0]) | set(groups[1])):
                connection.execute("CREATE TABLE rows (value TEXT PRIMARY KEY, lhs INTEGER NOT NULL, rhs INTEGER NOT NULL)")
                for side in [0, 1]:
                    column = "lhs" if side == 0 else "rhs"
                    sql = ("INSERT INTO rows VALUES (?, ?, ?) ON CONFLICT(value) DO UPDATE SET "
                           f"{column}={column}+1")
                    for path in groups[side].get(bucket, []):
                        parquet = pq.ParquetFile(str(path))
                        if not parquet.schema_arrow.remove_metadata().equals(arrow_schema("output")):
                            raise ValueError(f"Unexpected output schema in {path}")
                        for batch in parquet.iter_batches(batch_size=batch_rows, columns=OUTPUT_COLUMNS, use_threads=False):
                            import pandas as pd
                            hash_values = pd.util.hash_pandas_object(
                                batch.select(BASE_COLUMNS).to_pandas(), index=False,
                                hash_key="0123456789abcdef").to_numpy(dtype="uint64")
                            expected_buckets = hash_values % buckets
                            records = [(encode_row(row, bucket, buckets, int(expected_buckets[i])),
                                        int(side == 0), int(side == 1))
                                       for i, row in enumerate(batch.to_pylist())]
                            connection.executemany(sql, records)
                            total_rows[side] += len(records)
                    connection.commit()
                mismatch = connection.execute("SELECT value,lhs,rhs FROM rows WHERE lhs != rhs LIMIT 1").fetchone()
                duplicate = connection.execute("SELECT value,lhs,rhs FROM rows WHERE lhs > 1 OR rhs > 1 LIMIT 1").fetchone()
                if mismatch:
                    raise ValueError(f"Output mismatch in bucket {bucket}: counts={mismatch[1:]}, row={mismatch[0]}")
                if duplicate:
                    raise ValueError(f"Duplicate output row in bucket {bucket}: counts={duplicate[1:]}")
                connection.execute("DROP TABLE rows")
                connection.commit()
                # Reuse disk pages for the next bucket; RAM is kept to a small batch/cache.
        finally:
            connection.close()
    if total_rows != [first["output_rows"], second["output_rows"]]:
        raise ValueError("Output files no longer match run metadata row counts.")
    return {"exact_match": True, "left_rows": total_rows[0], "right_rows": total_rows[1],
            "bucket_count": buckets}


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--left", type=Path, required=True, help="Completed Spark output directory")
    cli.add_argument("--right", type=Path, required=True, help="Completed Ray output directory")
    cli.add_argument("--batch-rows", type=int, default=8192)
    cli.add_argument("--report", type=Path)
    args = cli.parse_args()
    if args.batch_rows <= 0:
        cli.error("batch-rows must be positive")
    result = compare(args.left, args.right, args.batch_rows)
    if args.report:
        write_json(args.report, result)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
