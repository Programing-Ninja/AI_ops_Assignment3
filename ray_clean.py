#!/usr/bin/env python3
"""Ray Data pipeline using a manually configured cluster and bounded disk buckets."""
from __future__ import annotations

import gc
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq
from ray.data.block import BlockMetadata
from ray.data.datasource import Datasource, ReadTask

from common import (BASE_COLUMNS, MIB, ROOT, add_features, add_zone_names, clean_batch,
                    arrow_schema, finish_run, parser, prepare_run, staged_manifest, write_json)


class StreamingTaxiParquet(Datasource):
    def __init__(self, files, batch_rows, columns=None, schema_kind="raw"):
        self.files = [str(path) for path in files]
        self.batch_rows = batch_rows
        self.schema = arrow_schema(schema_kind)
        self.columns = list(columns or BASE_COLUMNS)
        self.output_schema = pa.schema([self.schema.field(name) for name in self.columns])

    def estimate_inmemory_data_size(self):
        return sum(pq.ParquetFile(path).metadata.num_rows * 128 for path in self.files)

    def get_read_tasks(self, parallelism):
        tasks = []
        for filename in self.files:
            rows = pq.ParquetFile(filename).metadata.num_rows
            def read_file(path=filename, limit=self.batch_rows, selected=self.columns,
                          schema=self.output_schema):
                for batch in pq.ParquetFile(path).iter_batches(
                        batch_size=limit, columns=selected, use_threads=False):
                    yield pa.Table.from_batches([batch]).cast(schema, safe=False)
            size = rows * (128 if self.schema.equals(arrow_schema("raw")) else 512)
            metadata = BlockMetadata(num_rows=rows, size_bytes=size, exec_stats=None,
                                     input_files=[filename])
            tasks.append(ReadTask(read_file, metadata, schema=self.output_schema))
        return tasks


class StreamingBucketedParquet(Datasource):
    def __init__(self, bucket_files, batch_rows):
        self.bucket_files = {int(bucket): [str(path) for path in paths]
                             for bucket, paths in bucket_files.items() if paths}
        self.batch_rows = batch_rows
        self.schema = arrow_schema("staging")
        self.physical_columns = BASE_COLUMNS + ["_key"]

    def estimate_inmemory_data_size(self):
        return None

    def get_read_tasks(self, parallelism):
        tasks = []
        for bucket, paths in sorted(self.bucket_files.items()):
            def read_bucket(files=paths, bucket_id=bucket, limit=self.batch_rows,
                            schema=self.schema, columns=self.physical_columns):
                fragments = []
                for filename in files:
                    for batch in pq.ParquetFile(filename).iter_batches(
                            batch_size=limit, columns=columns, use_threads=False):
                        fragments.append(pa.Table.from_batches([batch]))
                if not fragments:
                    return
                table = pa.concat_tables(fragments, promote_options="default")
                table = table.append_column("_bucket", pa.array(
                    [bucket_id] * table.num_rows, type=pa.int32()))
                unique = table.to_pandas().drop_duplicates(subset=BASE_COLUMNS, keep="first")
                table = pa.Table.from_pandas(unique, preserve_index=False).cast(schema, safe=False)
                print(f"Ray bucket {bucket_id + 1}: {table.num_rows:,} rows after exact dedup",
                      flush=True)
                for offset in range(0, table.num_rows, limit):
                    yield table.slice(offset, limit)
            metadata = BlockMetadata(num_rows=None, size_bytes=None, exec_stats=None,
                                     input_files=paths)
            tasks.append(ReadTask(read_bucket, metadata, schema=self.schema))
        return tasks


def connect_ray(args):
    import ray
    from ray.data.context import DataContext
    if args.address == "local":
        if not args.allow_local:
            raise ValueError("--address local requires --allow-local and is only for correctness tests.")
        ray.init(num_cpus=args.concurrency, object_store_memory=128 * MIB, include_dashboard=False)
    else:
        # address='auto' fails if the user has not started the cluster; no silent local fallback.
        ray.init(address=args.address, runtime_env={"py_modules": [str(ROOT / "common.py"), str(ROOT / "ray_clean.py")]})
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
    context.enable_progress_bars = False
    context.enable_operator_progress_bars = False
    context.execution_options.resource_limits = context.execution_options.resource_limits.copy(cpu=args.concurrency)
    return [{"ip": node["NodeManagerAddress"], "cpus": node["Resources"].get("CPU", 0)} for node in nodes]


def main():
    import ray
    from ray_clean import StreamingBucketedParquet, StreamingTaxiParquet
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
        if args.reuse_staging is None:
            raw = ray.data.read_datasource(
                StreamingTaxiParquet(files, args.batch_rows),
                concurrency=args.concurrency,
                override_num_blocks=max(args.concurrency, len(files)))
            staged = raw.map_batches(clean_batch, batch_format="pyarrow", batch_size=args.batch_rows,
                                      fn_kwargs={"buckets": args.buckets}, concurrency=args.concurrency)
            staged.write_parquet(str(args.staging / "parquet"), partition_cols=["_bucket"],
                                 compression="snappy", row_group_size=args.batch_rows,
                                 min_rows_per_file=args.batch_rows * 2,
                                 concurrency=args.concurrency)
            metadata["staging_seconds"] = time.perf_counter() - started
            (args.staging / "ray-staging-stats.txt").write_text(staged.stats())
            del raw, staged
            gc.collect()
        else:
            metadata["staging_seconds"] = 0.0
        bucket_files, bucket_rows = staged_manifest(args)
        # One Ray task owns each bounded bucket, streams its fragments, performs
        # exact pandas deduplication, and yields bounded Arrow batches. Identical
        # rows cannot cross bucket boundaries because the bucket hash is stable.
        frame = ray.data.read_datasource(
            StreamingBucketedParquet(bucket_files, args.batch_rows),
            concurrency=args.concurrency, override_num_blocks=max(args.buckets, args.concurrency))
        metadata["staged_file_count"] = sum(len(paths) for paths in bucket_files.values())
        cleaned_rows = sum(bucket_rows.values())
        def enrich_and_feature(table):
            enriched = add_zone_names(table, zones)
            bucket = enriched["_bucket"]
            output = add_features(enriched)
            return output.append_column("bucket", bucket)
        final = frame.map_batches(enrich_and_feature, batch_format="pyarrow",
                                  batch_size=args.batch_rows, concurrency=args.concurrency)
        final.write_parquet(str(args.output), partition_cols=["bucket"], compression="snappy",
                            row_group_size=args.batch_rows, concurrency=args.concurrency)
        finish_run(args, metadata, started, cleaned_rows, [])
    except Exception as error:
        if metadata is not None:
            metadata.update(status="failed", error=str(error))
            write_json(args.output / "_run.json", metadata)
        raise
    finally:
        ray.shutdown()


if __name__ == "__main__":
    main()
