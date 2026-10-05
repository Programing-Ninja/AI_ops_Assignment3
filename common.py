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
        fields.extend([pa.field("_key", pa.int64()), pa.field("_bucket", pa.int32())])
    if kind == "output":
        fields.extend(pa.field(name, pa.string()) for name in ZONE_COLUMNS)
        fields.extend([pa.field("duration_seconds", pa.float64()),
                       pa.field("pickup_hour", pa.int64()),
                       pa.field("average_speed_mph", pa.float64())])
    return pa.schema(fields)


def canonical_key(values):
    """Exact normalized row serialization used by the parity validator."""
    tokens = []
    for name, value in zip(BASE_COLUMNS, values):
        if name in INTEGER_COLUMNS:
            tokens.append(str(int(value)))
        elif name in FLOAT_COLUMNS:
            tokens.append(float(value).hex())
        else:
            tokens.append(value.isoformat(timespec="microseconds"))
    return json.dumps(tokens, separators=(",", ":"))


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
    # Fixed-key pandas hashing is vectorized and stable across processes. The
    # hash only selects a disk bucket; deduplication groups by every normalized
    # source column as well, so a hash collision cannot merge different trips.
    hashes = pd.util.hash_pandas_object(
        frame[BASE_COLUMNS], index=False, hash_key="0123456789abcdef").to_numpy(dtype="uint64")
    frame["_key"] = hashes.view("int64")
    frame["_bucket"] = (hashes % buckets).astype("int32")
    return pa.Table.from_pandas(frame, schema=arrow_schema("staging"), preserve_index=False).replace_schema_metadata(None)


def average_speed(distance, duration):
    if duration <= 0 or not math.isfinite(distance):
        raise ValueError("Speed requires finite distance and positive duration.")
    return round(float(distance) * 3600.0 / float(duration), 6)


def add_features(table, identity=False):
    import pyarrow as pa
    import pyarrow.compute as pc
    # Ray's hash shuffle can return timestamp arrays with second precision.
    # Normalize the unit before interpreting the integer representation.
    pickup_us = pc.cast(table[TIME_COLUMNS[0]], pa.timestamp("us"), safe=False)
    dropoff_us = pc.cast(table[TIME_COLUMNS[1]], pa.timestamp("us"), safe=False)
    pickup = pc.cast(pickup_us, pa.int64()).to_numpy()
    dropoff = pc.cast(dropoff_us, pa.int64()).to_numpy()
    durations = (dropoff - pickup).astype("float64") / 1_000_000.0
    distances = table["trip_distance"].to_numpy()
    speeds = [float(d) if identity else average_speed(d, t)
              for d, t in zip(distances, durations)]
    columns = {name: table[name] for name in BASE_COLUMNS + ZONE_COLUMNS}
    columns.update(duration_seconds=pa.array(durations, type=pa.float64()),
                   pickup_hour=pc.cast(pc.hour(table[TIME_COLUMNS[0]]), pa.int64()),
                   average_speed_mph=pa.array(speeds, type=pa.float64()))
    return pa.table(columns, schema=arrow_schema("output"))


def add_zone_names(table, zones):
    """Enrich Arrow batches from the small zone dimension without distributed joins."""
    import pyarrow as pa
    import pyarrow.compute as pc
    zone_ids = pa.array([zone[0] for zone in zones], type=pa.int64())
    pickup_index = pc.index_in(table["PULocationID"], value_set=zone_ids)
    dropoff_index = pc.index_in(table["DOLocationID"], value_set=zone_ids)
    valid = pc.and_(pc.is_valid(pickup_index), pc.is_valid(dropoff_index))
    kept = table.filter(valid)
    pickup_index = pc.cast(pc.filter(pickup_index, valid), pa.int64())
    dropoff_index = pc.cast(pc.filter(dropoff_index, valid), pa.int64())
    boroughs = pa.array([zone[1] for zone in zones], type=pa.string())
    names = pa.array([zone[2] for zone in zones], type=pa.string())
    kept = kept.append_column("pickup_borough", pc.take(boroughs, pickup_index))
    kept = kept.append_column("pickup_zone", pc.take(names, pickup_index))
    kept = kept.append_column("dropoff_borough", pc.take(boroughs, dropoff_index))
    kept = kept.append_column("dropoff_zone", pc.take(names, dropoff_index))
    return kept


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
    result.add_argument("--reuse-staging", type=Path,
                        help="Reuse completed staged Parquet from a previous run; excludes staging from benchmark timing")
    result.add_argument("--buckets", type=int, default=0,
                        help="0: automatically choose at least 128 from raw row count")
    result.add_argument("--batch-rows", type=int, default=32768)
    result.add_argument("--block-mib", type=int, default=32)
    result.add_argument("--max-bucket-mib", type=int, default=128,
                        help="Guard based on a conservative 512-byte-per-row working-set estimate")
    result.add_argument("--shuffle-partitions", type=int, default=8)
    result.add_argument("--concurrency", type=int, default=2)
    result.add_argument("--max-files", type=int, help="Development subset, recorded in run metadata")
    result.add_argument("--max-uncompressed-gb", type=float,
                        help="Use a sorted whole-file prefix within this decimal GB Parquet uncompressed-size budget")
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
    if args.max_uncompressed_gb is not None and args.max_uncompressed_gb <= 0:
        raise ValueError("max-uncompressed-gb must be positive.")
    if args.max_files is not None and args.max_uncompressed_gb is not None:
        raise ValueError("Choose only one of --max-files or --max-uncompressed-gb.")
    files = parquet_files(args.input.resolve())
    if args.max_files:
        files = files[:args.max_files]
    if not files:
        raise ValueError(f"No Parquet inputs in {args.input}")
    manifest = []
    uncompressed_budget = (int(args.max_uncompressed_gb * 1_000_000_000)
                           if args.max_uncompressed_gb is not None else None)
    for path in files:
        data = pq.ParquetFile(str(path))
        missing = set(BASE_COLUMNS) - set(data.schema_arrow.names)
        if missing:
            raise ValueError(f"{path.name} lacks columns: {sorted(missing)}")
        for name in TIME_COLUMNS:
            if getattr(data.schema_arrow.field(name).type, "tz", None):
                raise ValueError(f"{path.name}: {name} must contain naive TLC wall times, not timezone-aware timestamps.")
        uncompressed_bytes = sum(data.metadata.row_group(i).total_byte_size
                                 for i in range(data.metadata.num_row_groups))
        if uncompressed_budget is not None and manifest and (
                sum(item["uncompressed_bytes"] for item in manifest) + uncompressed_bytes
                > uncompressed_budget):
            break
        if uncompressed_budget is not None and uncompressed_bytes > uncompressed_budget:
            raise ValueError(f"{path.name} alone exceeds --max-uncompressed-gb budget.")
        manifest.append({"path": str(path), "bytes": path.stat().st_size,
                         "uncompressed_bytes": uncompressed_bytes,
                         "rows": data.metadata.num_rows})
    if not manifest:
        raise ValueError("The uncompressed-size budget did not select any Parquet files.")
    files = [Path(item["path"]) for item in manifest]
    rows = sum(item["rows"] for item in manifest)
    if args.buckets == 0:
        needed = max(128, math.ceil(rows * 512 / (args.max_bucket_mib * MIB)))
        args.buckets = 1 << (needed - 1).bit_length()
    args.output = (args.output or ROOT / f"data/output/{framework}/{args.run_id}").resolve()
    staging_source = args.reuse_staging or args.staging
    args.staging = (staging_source or ROOT / f"data/staging/{framework}/{args.run_id}").resolve()
    inputs = [args.input.resolve(), args.lookup.resolve()]
    if args.output.exists():
        raise FileExistsError(f"Refusing to overwrite {args.output}; choose a new run-id/path.")
    if args.reuse_staging is None and args.staging.exists():
        raise FileExistsError(f"Refusing to overwrite {args.staging}; choose a new run-id/path.")
    if args.reuse_staging is not None and not (args.staging / "parquet").is_dir():
        raise FileNotFoundError(f"No completed staged Parquet at {args.staging / 'parquet'}")
    for root in [args.output, args.staging]:
        for source in inputs:
            if root == source or root in source.parents or source in root.parents:
                raise ValueError("Output/staging must not overlap inputs.")
    if args.output in args.staging.parents or args.staging in args.output.parents or args.output == args.staging:
        raise ValueError("Output and staging must be separate directories.")
    zones = load_zones(args.lookup)
    args.output.mkdir(parents=True, exist_ok=False)
    if args.reuse_staging is None:
        args.staging.mkdir(parents=True, exist_ok=False)
    configuration = {key: str(value) if isinstance(value, Path) else value
                     for key, value in vars(args).items()}
    metadata = {"contract": CONTRACT, "framework": framework, "run_id": args.run_id,
                "status": "running", "configuration": configuration,
                "input_manifest": manifest, "input_bytes": sum(x["bytes"] for x in manifest),
                "input_uncompressed_bytes": sum(x["uncompressed_bytes"] for x in manifest),
                "raw_rows": rows, "lookup_sha256": hashlib.sha256(args.lookup.read_bytes()).hexdigest(),
                "started_utc": datetime.now(timezone.utc).isoformat(),
                "benchmark_eligible": not args.allow_local and args.reuse_staging is None,
                "timing_boundary": (("Deduplication through final export; staged input reused, staging excluded. "
                                     if args.reuse_staging is not None else
                                     "Ingestion through final export; includes bucket staging I/O. ") +
                                    "Excludes dependency/cluster startup, preflight and UDF profiling.")}
    write_json(args.output / "_run.json", metadata)
    return files, zones, metadata


def staged_buckets(args):
    root = args.staging / "parquet"
    return [(index, parquet_files(root / f"_bucket={index}")) for index in range(args.buckets)]


def staged_manifest(args):
    """List staged fragments once and enforce the estimated per-bucket bound."""
    import pyarrow.parquet as pq
    grouped = {}
    counts = {}
    root = args.staging / "parquet"
    for path in root.glob("_bucket=*/*.parquet"):
        bucket = int(path.parent.name.split("=", 1)[1])
        grouped.setdefault(bucket, []).append(path)
        counts[bucket] = counts.get(bucket, 0) + pq.ParquetFile(str(path)).metadata.num_rows
    for bucket, rows in counts.items():
        estimate = rows * 512
        if estimate > args.max_bucket_mib * MIB:
            raise MemoryError(f"Bucket {bucket} has {rows:,} rows (~{estimate / MIB:.1f} MiB estimated). "
                              "Increase --buckets and use a new run ID.")
    return grouped, counts


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
