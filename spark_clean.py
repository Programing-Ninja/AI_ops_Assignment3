#!/usr/bin/env python3
"""Spark pipeline. Connects to a manually configured cluster; never launches one."""
from __future__ import annotations

import sys
import time

from common import (BASE_COLUMNS, INTEGER_COLUMNS, TIME_COLUMNS, OUTPUT_COLUMNS,
                    ROOT, MIB, average_speed, finish_run, parser, prepare_run, staged_manifest, write_json)


def spark_schema(kind="base"):
    from pyspark.sql import types as T
    fields = []
    for name in BASE_COLUMNS:
        dtype = T.LongType() if name in INTEGER_COLUMNS else (
            T.TimestampNTZType() if name in TIME_COLUMNS else T.DoubleType())
        fields.append(T.StructField(name, dtype, True))
    if kind == "staging":
        fields.extend([T.StructField("_key", T.LongType()), T.StructField("_bucket", T.IntegerType())])
    return T.StructType(fields)


def connect_spark(args):
    from pyspark.sql import SparkSession
    builder = (SparkSession.builder.appName(f"Taxi-{args.run_id}")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.timestampType", "TIMESTAMP_NTZ")
               .config("spark.sql.execution.arrow.maxRecordsPerBatch", args.batch_rows)
               .config("spark.sql.parquet.columnarReaderBatchSize", min(args.batch_rows, 4096))
               .config("spark.sql.files.maxPartitionBytes", args.block_mib * MIB)
               .config("spark.sql.shuffle.partitions", max(args.buckets, args.shuffle_partitions))
               .config("spark.sql.autoBroadcastJoinThreshold", -1)
               .config("spark.sql.adaptive.enabled", "false")
               .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS"))
    if args.master:
        builder = builder.master(args.master)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    master = spark.sparkContext.master
    if master.startswith("local") and not args.allow_local:
        spark.stop()
        raise ValueError("Use a standalone cluster master, or --allow-local for a correctness test.")
    if not master.startswith("local"):
        # Trigger allocation outside the measured interval, then wait for two executors.
        spark.range(2).repartition(2).count()
        deadline = time.monotonic() + 45
        while spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1 < 2:
            if time.monotonic() > deadline:
                spark.stop()
                raise RuntimeError("Two Spark executors are required. Check your manually started workers.")
            time.sleep(1)
    # Ship the shared functions for both the pipeline and the separate UDF profiler.
    spark.sparkContext.addPyFile(str(ROOT / "common.py"))
    return spark


def main():
    import pyspark
    from pyspark.sql import functions as F, types as T
    cli = parser("spark")
    cli.add_argument("--master", help="spark://HEAD:7077; inherited from spark-submit if omitted")
    args = cli.parse_args()
    spark = connect_spark(args)
    metadata = None
    try:
        files, zones, metadata = prepare_run(args, "spark")
        metadata["versions"] = {"python": sys.version.split()[0], "pyspark": pyspark.__version__}
        metadata["cluster"] = {"master": spark.sparkContext.master,
                               "executor_count": max(0, spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1)}
        buckets = args.buckets
        def cleaner(iterator):
            from common import clean_batch
            for batch in iterator:
                yield from clean_batch(batch, buckets).to_batches()
        metadata["timed_start_epoch"] = time.time()
        started = time.perf_counter()
        if args.reuse_staging is None:
            stage_started = started
            raw = None
            # Read monthly files separately; their physical Parquet types differ.
            for path in files:
                part = spark.read.parquet(str(path)).select(*[
                    F.col(name).cast("string" if name in TIME_COLUMNS else "double").alias(name)
                    for name in BASE_COLUMNS])
                raw = part if raw is None else raw.unionByName(part)
            staged = raw.mapInArrow(cleaner, spark_schema("staging"))
            staged.repartition(args.buckets, "_bucket").write.mode("errorifexists").option(
                "compression", "snappy").partitionBy("_bucket").parquet(str(args.staging / "parquet"))
            metadata["staging_seconds"] = time.perf_counter() - stage_started
        else:
            metadata["staging_seconds"] = 0.0
        bucket_files, bucket_rows = staged_manifest(args)
        staged_files = [str(path) for paths in bucket_files.values() for path in paths]
        metadata["staged_file_count"] = len(staged_files)
        cleaned_rows = sum(bucket_rows.values())
        # Read all staged fragments in one execution plan, then deduplicate
        # globally. Explicit file paths include Spark's underscore-prefixed
        # partition directories; recover the deterministic bucket from path.
        frame = spark.read.parquet(*staged_files).withColumn(
            "_bucket", F.regexp_extract(F.input_file_name(), r"_bucket=(\d+)/", 1).cast("int"))
        frame = frame.dropDuplicates(BASE_COLUMNS)
        zone_schema = T.StructType([T.StructField("LocationID", T.LongType()),
                                    T.StructField("Borough", T.StringType()), T.StructField("Zone", T.StringType())])
        lookup = spark.createDataFrame(zones, schema=zone_schema)
        pickup = F.broadcast(lookup.select(F.col("LocationID").alias("PULocationID"),
                            F.col("Borough").alias("pickup_borough"), F.col("Zone").alias("pickup_zone")))
        dropoff = F.broadcast(lookup.select(F.col("LocationID").alias("DOLocationID"),
                            F.col("Borough").alias("dropoff_borough"), F.col("Zone").alias("dropoff_zone")))
        frame = frame.join(pickup, "PULocationID", "inner").join(dropoff, "DOLocationID", "inner")
        duration = (F.unix_micros(F.col(TIME_COLUMNS[1]).cast(T.TimestampType())) -
                    F.unix_micros(F.col(TIME_COLUMNS[0]).cast(T.TimestampType()))) / F.lit(1_000_000.0)
        frame = frame.withColumn("duration_seconds", duration).withColumn(
            "pickup_hour", F.hour(TIME_COLUMNS[0]).cast("long"))
        # Match Python's binary-float rounding at exact decimal halfway values.
        speed = F.udf(average_speed, T.DoubleType())(
            F.col("trip_distance"), F.col("duration_seconds"))
        final = frame.withColumn("average_speed_mph", speed).select(
            *OUTPUT_COLUMNS, F.col("_bucket").alias("bucket"))
        final.write.mode("append").option("compression", "snappy").partitionBy(
            "bucket").parquet(str(args.output))
        finish_run(args, metadata, started, cleaned_rows, [])
    except Exception as error:
        if metadata is not None:
            metadata.update(status="failed", error=str(error))
            write_json(args.output / "_run.json", metadata)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
