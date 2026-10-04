"""Shared, explicit cleaning rules and bounded-memory run utilities."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import re
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
INTEGER_COLUMNS = ["VendorID", "passenger_count", "PULocationID", "DOLocationID"]
FLOAT_COLUMNS = ["trip_distance", "fare_amount", "total_amount"]
TIME_COLUMNS = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
BASE_COLUMNS = ["VendorID", *TIME_COLUMNS, "passenger_count", "trip_distance",
                "PULocationID", "DOLocationID", "fare_amount", "total_amount"]
ZONE_COLUMNS = ["pickup_borough", "pickup_zone", "dropoff_borough", "dropoff_zone"]
OUTPUT_COLUMNS = BASE_COLUMNS + ZONE_COLUMNS + [
    "duration_seconds", "pickup_hour", "average_speed_mph"]
MIB = 1024 * 1024
CONTRACT = "taxi-clean-v1"


def arrow_schema(kind="base"):
    import pyarrow as pa
    fields = []
    for name in BASE_COLUMNS:
        dtype = pa.int64() if name in INTEGER_COLUMNS else (
            pa.timestamp("us") if name in TIME_COLUMNS else pa.float64())
        if kind == "raw":
            dtype = pa.string() if name in TIME_COLUMNS else pa.float64()
        fields.append(pa.field(name, dtype))
    if kind == "staging":
        fields.extend([pa.field("_key", pa.string()), pa.field("_bucket", pa.int32())])
    if kind == "output":
        fields.extend(pa.field(name, pa.string()) for name in ZONE_COLUMNS)
        fields.extend([pa.field("duration_seconds", pa.float64()),
                       pa.field("pickup_hour", pa.int64()),
                       pa.field("average_speed_mph", pa.float64())])
    return pa.schema(fields)


def canonical_key(values):
    """Exact normalized values, independent of file name and process hash seed."""
    tokens = []
    for name, value in zip(BASE_COLUMNS, values):
        if name in INTEGER_COLUMNS:
            tokens.append(str(int(value)))
        elif name in FLOAT_COLUMNS:
            tokens.append(float(value).hex())
        else:
            tokens.append(value.isoformat(timespec="microseconds"))
    return json.dumps(tokens, separators=(",", ":"))


def bucket_for(key, buckets):
    return int.from_bytes(hashlib.sha256(key.encode("utf-8")).digest()[:8], "big") % buckets


def clean_batch(table, buckets):
    """Same batch cleaner called by Spark mapInArrow and Ray map_batches.

    Integer fields must be integral signed 32-bit values (stored as int64).
    Timestamp values are naive TLC wall times, truncated to microseconds,
    in [1678-01-01, 2262-01-01). Numeric NaNs/infinities and signed zero
    differences are normalized consistently before deduplication.
    """
    import numpy as np
    import pandas as pd
    import pyarrow as pa
    frame = table.select(BASE_COLUMNS).to_pandas()
    valid = pd.Series(True, index=frame.index)
    for name in INTEGER_COLUMNS + FLOAT_COLUMNS:
        values = pd.to_numeric(frame[name], errors="coerce").astype("float64")
        valid &= np.isfinite(values)
        if name in INTEGER_COLUMNS:
            valid &= (values == np.trunc(values)) & (values >= -(2**31)) & (values < 2**31)
        frame[name] = values.mask(values == 0, 0.0)
    for name in TIME_COLUMNS:
        values = pd.to_datetime(frame[name], errors="coerce", format="mixed")
        if isinstance(values.dtype, pd.DatetimeTZDtype):
            raise ValueError("Timezone-aware input is unsupported; TLC wall times must be naive.")
        values = values.dt.as_unit("us")
        valid &= values.notna() & (values >= pd.Timestamp("1678-01-01")) & (
            values < pd.Timestamp("2262-01-01"))
        frame[name] = values
    valid &= frame["trip_distance"] > 0
    valid &= frame[TIME_COLUMNS[1]] > frame[TIME_COLUMNS[0]]
    frame = frame.loc[valid, BASE_COLUMNS].copy()
    for name in INTEGER_COLUMNS:
        frame[name] = frame[name].astype("int64")
    keys = [canonical_key(row) for row in frame.itertuples(index=False, name=None)]
    frame["_key"] = keys
    frame["_bucket"] = [bucket_for(key, buckets) for key in keys]
    return pa.Table.from_pandas(frame, schema=arrow_schema("staging"), preserve_index=False).replace_schema_metadata(None)


def average_speed(distance, duration):
    if duration <= 0 or not math.isfinite(distance):
        raise ValueError("Speed requires finite distance and positive duration.")
    return round(float(distance) * 3600.0 / float(duration), 6)


def add_features(table, identity=False):
    import pyarrow as pa
    import pyarrow.compute as pc
    pickup = pc.cast(table[TIME_COLUMNS[0]], pa.int64()).to_numpy()
    dropoff = pc.cast(table[TIME_COLUMNS[1]], pa.int64()).to_numpy()
    durations = (dropoff - pickup).astype("float64") / 1_000_000.0
    distances = table["trip_distance"].to_numpy()
    speeds = [float(d) if identity else average_speed(d, t)
              for d, t in zip(distances, durations)]
    columns = {name: table[name] for name in BASE_COLUMNS + ZONE_COLUMNS}
    columns.update(duration_seconds=pa.array(durations, type=pa.float64()),
                   pickup_hour=pc.cast(pc.hour(table[TIME_COLUMNS[0]]), pa.int64()),
                   average_speed_mph=pa.array(speeds, type=pa.float64()))
    return pa.table(columns, schema=arrow_schema("output"))


def load_zones(path):
    zones, seen = [], set()
    with Path(path).open(newline="", encoding="utf-8-sig") as stream:
        for row in csv.DictReader(stream):
            location = int(row["LocationID"])
            if location in seen:
                raise ValueError(f"Duplicate lookup LocationID: {location}")
            if not row.get("Borough") or not row.get("Zone"):
                raise ValueError(f"Missing zone/borough for LocationID {location}")
            seen.add(location)
            zones.append((location, row["Borough"], row["Zone"]))
    if not zones:
        raise ValueError("Zone lookup is empty.")
    return zones


def parquet_files(path):
    path = Path(path)
    files = [path] if path.is_file() else path.rglob("*.parquet")
    return sorted(p for p in files if p.suffix == ".parquet" and not p.name.startswith((".", "_")))


def parquet_rows(files):
    import pyarrow.parquet as pq
    return sum(pq.ParquetFile(str(path)).metadata.num_rows for path in files)


def write_json(path, value):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")


def parser(framework):
    result = argparse.ArgumentParser(description=f"Bounded-memory {framework} taxi cleaner")
    result.add_argument("--input", type=Path, default=ROOT / "data/raw/trips")
    result.add_argument("--lookup", type=Path, default=ROOT / "data/raw/taxi_zone_lookup.csv")
    result.add_argument("--run-id", default=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"))
    result.add_argument("--output", type=Path)
    result.add_argument("--staging", type=Path)
    result.add_argument("--buckets", type=int, default=0,
                        help="0: automatically choose at least 128 from raw row count")
    result.add_argument("--batch-rows", type=int, default=8192)
    result.add_argument("--block-mib", type=int, default=16)
    result.add_argument("--max-bucket-mib", type=int, default=128,
                        help="Guard based on a conservative 512-byte-per-row working-set estimate")
    result.add_argument("--shuffle-partitions", type=int, default=8)
    result.add_argument("--concurrency", type=int, default=2)
    result.add_argument("--max-files", type=int, help="Development subset, recorded in run metadata")
    result.add_argument("--allow-local", action="store_true", help="Correctness tests only; not a cluster benchmark")
    return result


def prepare_run(args, framework):
    import pyarrow.parquet as pq
    if not re.fullmatch(r"[A-Za-z0-9_-]+", args.run_id):
        raise ValueError("run-id may contain only letters, digits, underscores and hyphens.")
    for name in ["batch_rows", "block_mib", "max_bucket_mib", "shuffle_partitions", "concurrency"]:
        if getattr(args, name) <= 0:
            raise ValueError(f"{name} must be positive.")
    if args.buckets < 0 or (args.max_files is not None and args.max_files <= 0):
        raise ValueError("buckets must be nonnegative; max-files must be positive.")
    files = parquet_files(args.input.resolve())
    if args.max_files:
        files = files[:args.max_files]
    if not files:
        raise ValueError(f"No Parquet inputs in {args.input}")
    manifest = []
    for path in files:
        data = pq.ParquetFile(str(path))
        missing = set(BASE_COLUMNS) - set(data.schema_arrow.names)
        if missing:
            raise ValueError(f"{path.name} lacks columns: {sorted(missing)}")
        for name in TIME_COLUMNS:
            if getattr(data.schema_arrow.field(name).type, "tz", None):
                raise ValueError(f"{path.name}: {name} must contain naive TLC wall times, not timezone-aware timestamps.")
        manifest.append({"path": str(path), "bytes": path.stat().st_size,
                         "rows": data.metadata.num_rows})
    rows = sum(item["rows"] for item in manifest)
    if args.buckets == 0:
        needed = max(128, math.ceil(rows * 512 / (args.max_bucket_mib * MIB)))
        args.buckets = 1 << (needed - 1).bit_length()
    args.output = (args.output or ROOT / f"data/output/{framework}/{args.run_id}").resolve()
    args.staging = (args.staging or ROOT / f"data/staging/{framework}/{args.run_id}").resolve()
    inputs = [args.input.resolve(), args.lookup.resolve()]
    roots = [args.output, args.staging]
    for root in roots:
        if root.exists():
            raise FileExistsError(f"Refusing to overwrite {root}; choose a new run-id/path.")
        for source in inputs:
            if root == source or root in source.parents or source in root.parents:
                raise ValueError("Output/staging must not overlap inputs.")
    if roots[0] in roots[1].parents or roots[1] in roots[0].parents or roots[0] == roots[1]:
        raise ValueError("Output and staging must be separate directories.")
    zones = load_zones(args.lookup)
    for root in roots:
        root.mkdir(parents=True, exist_ok=False)
    configuration = {key: str(value) if isinstance(value, Path) else value
                     for key, value in vars(args).items()}
    metadata = {"contract": CONTRACT, "framework": framework, "run_id": args.run_id,
                "status": "running", "configuration": configuration,
                "input_manifest": manifest, "input_bytes": sum(x["bytes"] for x in manifest),
                "raw_rows": rows, "lookup_sha256": hashlib.sha256(args.lookup.read_bytes()).hexdigest(),
                "started_utc": datetime.now(timezone.utc).isoformat(),
                "benchmark_eligible": not args.allow_local,
                "timing_boundary": "Ingestion through final export; includes bucket staging I/O. "
                                   "Excludes dependency/cluster startup, preflight and UDF profiling."}
    write_json(args.output / "_run.json", metadata)
    return files, zones, metadata


def staged_buckets(args):
    root = args.staging / "parquet"
    return [(index, parquet_files(root / f"_bucket={index}")) for index in range(args.buckets)]


def check_bucket(files, args):
    rows = parquet_rows(files)
    estimate = rows * 512
    if estimate > args.max_bucket_mib * MIB:
        raise MemoryError(f"Bucket has {rows:,} rows (~{estimate / MIB:.1f} MiB estimated). "
                          "Start a new run with more --buckets. This guard is an estimate, not a hard RAM limit.")
    return rows


def ensure_nonempty_layout(output):
    import pyarrow as pa
    import pyarrow.parquet as pq
    if not parquet_files(output):
        pq.write_table(pa.Table.from_batches([], schema=arrow_schema("output")),
                       str(output / "empty.parquet"), compression="snappy")


def finish_run(args, metadata, started, cleaned_rows, bucket_results):
    ensure_nonempty_layout(args.output)
    metadata.update(status="complete", total_seconds=time.perf_counter() - started,
                    timed_end_epoch=time.time(), cleaned_rows=cleaned_rows,
                    output_rows=parquet_rows(parquet_files(args.output)), buckets=bucket_results,
                    finished_utc=datetime.now(timezone.utc).isoformat())
    write_json(args.output / "_run.json", metadata)
    print(json.dumps({key: metadata[key] for key in ["framework", "run_id", "total_seconds", "output_rows"]}))
