#!/usr/bin/env python3
"""Ray Data pipeline using a manually configured cluster and bounded disk buckets."""
from __future__ import annotations

import gc
import sys
import time

from common import (BASE_COLUMNS, MIB, ROOT, add_features, arrow_schema, check_bucket, clean_batch,
                    finish_run, parser, prepare_run, staged_buckets, write_json)


def connect_ray(args):
    import ray
    from ray.data.context import DataContext, ShuffleStrategy
    if not hasattr(ray.data.Dataset, "join"):
        raise RuntimeError("This Ray version has no Dataset.join. Install requirements.txt in your Conda environment.")
    if args.address == "local":
        if not args.allow_local:
            raise ValueError("--address local requires --allow-local and is only for correctness tests.")
        ray.init(num_cpus=args.concurrency, object_store_memory=128 * MIB, include_dashboard=False)
    else:
        # address='auto' fails if the user has not started the cluster; no silent local fallback.
        ray.init(address=args.address, runtime_env={"py_modules": [str(ROOT / "common.py"), str(ROOT / "ray_io.py")]})
    nodes = [node for node in ray.nodes() if node["Alive"]]
    compute_nodes = [node for node in nodes if node["Resources"].get("CPU", 0) > 0]
    if not args.allow_local and len(compute_nodes) != 2:
        ray.shutdown()
        raise RuntimeError(f"Expected exactly 2 compute nodes; found {len(compute_nodes)}. "
                           "Start two workers and set the head's --num-cpus=0 manually.")
    context = DataContext.get_current()
    context.target_max_block_size = args.block_mib * MIB
    context.target_min_block_size = min(MIB, context.target_max_block_size)
    context.read_op_min_num_blocks = args.concurrency
    context.shuffle_strategy = ShuffleStrategy.HASH_SHUFFLE
    context.max_hash_shuffle_aggregators = args.concurrency
    context.enable_progress_bars = False
    context.enable_operator_progress_bars = False
    context.execution_options.resource_limits = context.execution_options.resource_limits.copy(cpu=args.concurrency)
    return [{"ip": node["NodeManagerAddress"], "cpus": node["Resources"].get("CPU", 0)} for node in nodes]


def joined_bucket(paths, zones, args):
    import pyarrow as pa
    import ray
    from ray.data.aggregate import Count
    frame = ray.data.read_parquet([str(path) for path in paths], columns=BASE_COLUMNS + ["_key"],
                                  concurrency=args.concurrency)
    # Global within each hash bucket, including rows in different files/blocks.
    frame = frame.groupby(BASE_COLUMNS + ["_key"], num_partitions=args.shuffle_partitions).aggregate(
        Count(alias_name="_duplicates")).drop_columns(["_duplicates"]).materialize()
    pickup = pa.table({"PULocationID": pa.array([z[0] for z in zones], type=pa.int64()),
                       "pickup_borough": pa.array([z[1] for z in zones]),
                       "pickup_zone": pa.array([z[2] for z in zones])})
    dropoff = pa.table({"DOLocationID": pa.array([z[0] for z in zones], type=pa.int64()),
                        "dropoff_borough": pa.array([z[1] for z in zones]),
                        "dropoff_zone": pa.array([z[2] for z in zones])})
    frame = frame.join(ray.data.from_arrow(pickup), join_type="inner", on=("PULocationID",),
                       num_partitions=args.shuffle_partitions).materialize()
    # A fully unmatched pickup bucket has no output blocks/schema for a second join.
    if frame.count() == 0:
        return None
    frame = frame.join(ray.data.from_arrow(dropoff), join_type="inner", on=("DOLocationID",),
                       num_partitions=args.shuffle_partitions).materialize()
    return frame if frame.count() else None


def main():
    import ray
    from ray_io import StreamingTaxiParquet
    cli = parser("ray")
    cli.add_argument("--address", default="auto", help="Existing Ray cluster address; default auto")
    args = cli.parse_args()
    cluster = connect_ray(args)
    metadata = None
    try:
        files, zones, metadata = prepare_run(args, "ray")
        metadata["versions"] = {"python": sys.version.split()[0], "ray": ray.__version__}
        metadata["cluster"] = cluster
        metadata["timed_start_epoch"] = time.time()
        started = time.perf_counter()
        raw = ray.data.read_datasource(StreamingTaxiParquet(files, args.batch_rows),
                                      concurrency=args.concurrency)
        staged = raw.map_batches(clean_batch, batch_format="pyarrow", batch_size=args.batch_rows,
                                  fn_kwargs={"buckets": args.buckets}, concurrency=args.concurrency)
        staged.write_parquet(str(args.staging / "parquet"), partition_cols=["_bucket"],
                             compression="snappy", row_group_size=args.batch_rows,
                             concurrency=args.concurrency)
        metadata["staging_seconds"] = time.perf_counter() - started
        (args.staging / "ray-staging-stats.txt").write_text(staged.stats())
        del raw, staged
        gc.collect()
        cleaned_rows, results = 0, []
        for index, paths in staged_buckets(args):
            if not paths:
                continue
            bucket_started = time.perf_counter()
            count = check_bucket(paths, args)
            cleaned_rows += count
            print(f"Ray bucket {index + 1}/{args.buckets}: {count:,} cleaned rows", flush=True)
            joined = joined_bucket(paths, zones, args)
            if joined is None:
                results.append({"bucket": index, "cleaned_rows": count,
                                "seconds": time.perf_counter() - bucket_started})
                continue
            final = joined.map_batches(add_features, batch_format="pyarrow", batch_size=args.batch_rows,
                                        concurrency=args.concurrency)
            final.write_parquet(str(args.output / f"bucket={index}"), compression="snappy",
                                row_group_size=args.batch_rows, concurrency=args.concurrency)
            (args.staging / f"ray-bucket-{index}-stats.txt").write_text(final.stats())
            results.append({"bucket": index, "cleaned_rows": count,
                            "seconds": time.perf_counter() - bucket_started})
            del joined, final
            gc.collect()
        finish_run(args, metadata, started, cleaned_rows, results)
    except Exception as error:
        if metadata is not None:
            metadata.update(status="failed", error=str(error))
            write_json(args.output / "_run.json", metadata)
        raise
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
