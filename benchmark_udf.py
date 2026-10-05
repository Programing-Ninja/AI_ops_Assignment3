#!/usr/bin/env python3
"""Profile the shared speed function separately from full pipeline timing."""
from __future__ import annotations

import argparse
import gc
import json
import statistics
import time
from pathlib import Path

from common import average_speed, parquet_files, parquet_rows, write_json


def ray_speed_batch(table):
    import pyarrow as pa
    return pa.table({"speed": pa.array([average_speed(d, t) for d, t in zip(
        table["trip_distance"].to_numpy(), table["duration_seconds"].to_numpy())], type=pa.float64())})


def run_spark(args, buckets):
    from pyspark import StorageLevel
    from pyspark.sql import functions as F, types as T
    from spark_clean import connect_spark
    spark = connect_spark(args)
    results = []
    speed_udf = F.udf(average_speed, T.DoubleType())
    try:
        for paths in buckets:
            frame = spark.read.parquet(*[str(path) for path in paths]).select(
                "trip_distance", "duration_seconds").persist(StorageLevel.MEMORY_AND_DISK)
            frame.count()  # Materialize the bucket before any profiling timers.
            try:
                for trial in range(args.repeats + 1):
                    values = {}
                    for mode in (["baseline", "udf"] if trial % 2 == 0 else ["udf", "baseline"]):
                        started = time.perf_counter()
                        column = F.col("trip_distance") if mode == "baseline" else speed_udf(
                            "trip_distance", "duration_seconds")
                        checksum = frame.select(column.alias("speed")).agg(F.sum("speed")).collect()[0][0]
                        values[f"{mode}_seconds"] = time.perf_counter() - started
                        values[f"{mode}_checksum"] = checksum
                    if trial:
                        results.append({"bucket": str(paths[0].parent), "trial": trial, **values})
            finally:
                frame.unpersist(blocking=True)
    finally:
        spark.stop()
    return results


def run_ray(args, buckets):
    import ray
    from ray.data.aggregate import Sum
    from ray_clean import connect_ray
    from ray_clean import StreamingTaxiParquet
    connect_ray(args)
    results = []
    try:
        for paths in buckets:
            frame = ray.data.read_datasource(
                StreamingTaxiParquet(paths, args.batch_rows,
                                     columns=["trip_distance", "duration_seconds"],
                                     schema_kind="output"),
                concurrency=args.concurrency,
                override_num_blocks=max(1, len(paths))).materialize()
            for trial in range(args.repeats + 1):
                values = {}
                for mode in (["baseline", "udf"] if trial % 2 == 0 else ["udf", "baseline"]):
                    started = time.perf_counter()
                    transformed = frame if mode == "baseline" else frame.map_batches(
                        ray_speed_batch, batch_format="pyarrow", batch_size=args.batch_rows,
                        concurrency=args.concurrency)
                    column = "trip_distance" if mode == "baseline" else "speed"
                    reduced = transformed.groupby(None, num_partitions=1).aggregate(
                        Sum(column, alias_name="checksum")).take(1)
                    checksum = reduced[0]["checksum"] if reduced else None
                    values[f"{mode}_seconds"] = time.perf_counter() - started
                    values[f"{mode}_checksum"] = checksum
                if trial:
                    results.append({"bucket": str(paths[0].parent), "trial": trial, **values})
            del frame
            gc.collect()
    finally:
        ray.shutdown()
    return results


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--framework", choices=["spark", "ray"], required=True)
    cli.add_argument("--input", type=Path, required=True, help="Completed, parity-validated pipeline output")
    cli.add_argument("--report", type=Path, required=True)
    cli.add_argument("--run-id", default="udf-profile", help="Spark application label")
    cli.add_argument("--master")
    cli.add_argument("--address", default="auto")
    cli.add_argument("--allow-local", action="store_true")
    cli.add_argument("--repeats", type=int, default=3)
    cli.add_argument("--max-buckets", type=int, default=8,
                     help="Profile the first non-empty output buckets to keep the diagnostic bounded")
    cli.add_argument("--batch-rows", type=int, default=8192)
    cli.add_argument("--block-mib", type=int, default=16)
    cli.add_argument("--shuffle-partitions", type=int, default=8)
    cli.add_argument("--concurrency", type=int, default=2)
    args = cli.parse_args()
    if min(args.repeats, args.max_buckets, args.batch_rows, args.block_mib,
           args.shuffle_partitions, args.concurrency) <= 0:
        cli.error("Sizing and repeat parameters must be positive")
    if args.report.exists():
        raise FileExistsError(f"Refusing to overwrite {args.report}")
    metadata = json.loads((args.input / "_run.json").read_text())
    if metadata["status"] != "complete" or metadata["output_rows"] <= 0:
        raise ValueError("Input must be a completed pipeline run with at least one output row.")
    args.buckets = metadata["configuration"]["buckets"]
    buckets = [parquet_files(path) for path in sorted(args.input.glob("bucket=*"))]
    buckets = [paths for paths in buckets if paths and parquet_rows(paths) > 0]
    buckets = buckets[:args.max_buckets]
    results = run_spark(args, buckets) if args.framework == "spark" else run_ray(args, buckets)
    totals = [{"trial": trial, "baseline_seconds": sum(r["baseline_seconds"] for r in results if r["trial"] == trial),
               "udf_seconds": sum(r["udf_seconds"] for r in results if r["trial"] == trial)}
              for trial in range(1, args.repeats + 1)]
    report = {"framework": args.framework, "source_run": metadata["run_id"],
              "benchmark_eligible": not args.allow_local and metadata["benchmark_eligible"],
              "method": f"First {len(buckets)} non-empty buckets; per-bucket cached input; one warm-up; "
                        "alternating native sum baseline and Python speed sum. "
                        "Stage times include scheduling, serialization and aggregation; not isolated JVM overhead.",
              "trials": totals, "bucket_measurements": results,
              "median_udf_seconds": statistics.median(r["udf_seconds"] for r in totals),
              "median_baseline_seconds": statistics.median(r["baseline_seconds"] for r in totals)}
    write_json(args.report, report)
    print(json.dumps({key: report[key] for key in ["framework", "median_udf_seconds", "median_baseline_seconds"]}))


if __name__ == "__main__":
    main()
