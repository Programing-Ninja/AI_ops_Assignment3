# DA3408 Assignment 3: Spark vs. Ray

Two equivalent taxi preprocessing pipelines for Spark and Ray Data. Both stage cleaned rows into deterministic disk buckets, deduplicate exact normalized trip rows, enrich pickup/drop-off IDs from the zone table, calculate duration/hour/speed, and export Snappy Parquet. All staging I/O is included in end-to-end timing.

## 1. Existing Conda environment

```bash
conda activate aiops
cd '/home/harshvardhan/sem5/AI_OPS/assignment 3/submission'
python -m pip install -r requirements.txt
python -c 'import sys, ray, pyspark, pyarrow; print(sys.executable); print(ray.__version__, pyspark.__version__, pyarrow.__version__)'
export PYSPARK_PYTHON="$(python -c 'import sys; print(sys.executable)')"
```

Python 3.11 and Java 17 are recommended. Install matching requirements on every node. The initial `aiops` environment had Ray 2.3.0, PySpark 4.2.0 and no PyArrow: it needs these dependency changes. The pinned Spark/Ray versions provide the APIs used here. A separate Spark binary installation must match the PySpark version. `matplotlib` is used only by the result summarizer.

## 2. Data

The downloaded set is under `data/raw/trips/`, with the zone CSV at `data/raw/taxi_zone_lookup.csv`. The timed Spark/Ray pair uses the first four sorted whole Parquet files (Jan–Apr 2023), about 0.345 GB uncompressed, to keep both runs within a 30-minute budget on the available laptop. The rest of the downloaded data stays unchanged.

```bash
# Preserve and validate existing files; download only missing files.
python download_data.py --months 2023-01 2023-02

# The benchmark run uses --max-uncompressed-gb 0.35 for a deterministic four-file prefix.
python download_data.py --target-gb 2
```

The downloader uses official [TLC data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), trying 2023, 2024, 2022, then 2021. It records actual bytes, row counts and SHA-256 checksums in `data/raw/input_manifest.json`. It never generates benchmark data or overwrites existing raw files.

## 3. Manually configure the clusters

The assignment requires you to perform network setup and cluster configuration yourself. No script here provisions machines, launches worker clusters or configures firewalls. Follow the official [Spark standalone](https://spark.apache.org/docs/3.5.7/spark-standalone.html) and [Ray manual cluster](https://docs.ray.io/en/latest/cluster/vms/user-guides/launching-clusters/on-premises.html) instructions.

Use one coordinator and two worker nodes, with one compute CPU per worker for this memory budget. Configure the Ray head with zero schedulable CPUs; the script verifies two live compute nodes. Spark verifies at least two executors before starting its timer. Confirm two active worker entries yourself in the UIs. On one physical laptop, Ray nodes need separate containers/VMs; two Python tasks on one Ray node are not two cluster workers. Document the shared physical hardware in the report.

All nodes must see the same absolute input/staging/output paths through shared storage or a shared volume. Keep cluster dependencies identical. The scripts ship shared Python modules to workers. Use the same hardware for both frameworks and run them separately.

For Spark, start with 768 MiB executor heap, 768 MiB driver heap, one core per executor, two total application cores, and reduced master/worker daemon heaps (for example 128 MiB each). Java heaps exclude Python/native process memory. For Ray, choose small object stores when manually starting nodes and a local disk spill directory with enough space; its worker/object-store settings are not hard total-process memory caps. Close memory-heavy applications first: the observed 2–3 GiB available RAM is tight for either complete cluster. Use more RAM or other worker machines if cluster overhead itself cannot fit.

## 4. Full pipeline commands

After manually starting the Spark cluster, run from the head (replace the master address):

```bash
spark-submit \
  --master spark://HEAD_IP:7077 \
  --deploy-mode client \
  --driver-memory 768m \
  --executor-memory 768m \
  --executor-cores 1 \
  --conf spark.cores.max=2 \
  --conf spark.driver.host=HEAD_IP \
  spark_clean.py --run-id spark01
```

Stop Spark yourself, manually start Ray, then run on its head:

```bash
python ray_clean.py --address auto --run-id ray01
```

Outputs are `data/output/spark/spark01/` and `data/output/ray/ray01/`. Staging is `data/staging/FRAMEWORK/RUN_ID/`. New run IDs are required for every trial; existing directories are never overwritten.

Defaults: 32,768 rows per batch, 32 MiB target blocks, two concurrent tasks, and a 512 MiB estimated per-bucket budget. The bucket count is automatically chosen from input row count and the budget, with a minimum of 128. Both frameworks therefore select the same count for the same inputs. Set `--buckets N` to override it. The guard estimates 512 bytes per cleaned row; it is not an enforced limit on decoder, Python, Ray, or JVM memory.

Use `--max-uncompressed-gb N` to select a deterministic sorted prefix of complete Parquet files whose uncompressed row-group metadata fits within N decimal GB. The selected paths, row counts and compressed/uncompressed byte counts are recorded in `_run.json`. Use the same cap for Spark and Ray when comparing a bounded subset.

Ray creates one bounded streaming read task per bucket. Each task reads that bucket's fragments, deduplicates exactly on all normalized source columns in pandas, then yields bounded Arrow batches for vectorized zone enrichment and feature export. This makes the 128 independent buckets run in parallel across the two workers, without a large global shuffle or repeated dataset-plan startup. Spark reads the staged bucket files in one plan, deduplicates in its distributed DataFrame plan, broadcasts the small zone dimension for both lookups, and exports bucket-partitioned Parquet.

The current Docker setup gives the Ray workers two CPUs each; the run command limits pipeline concurrency to two to reduce OOM risk while keeping both workers active. See `runner.md` for cluster commands, monitoring, recovery, and measured-run ordering.

The downloaded TLC months have large row groups and may have different physical integer types. Spark reads each file separately and casts before unioning it. Ray uses `StreamingTaxiParquet`, a Ray Data datasource backed by `ParquetFile.iter_batches`, rather than allocating a whole monthly table. Decoder buffers and task/runtime overhead can still exceed batch size. Avoid full-dataset `collect()`, `to_pandas()`, `materialize()` or caching.

Temporary files remain for inspection; remove only your completed run's staging manually when no longer needed. Keep one output pair initially, and check available disk before repeated full-data runs.

## 5. Cleaning contract and parity

- Project the same nine columns, discard null/NaN/infinite values, reject nonintegral/out-of-range integer fields, nonpositive distance and nonpositive duration.
- Integer fields must fit signed 32-bit values and are stored as int64. Timestamps are naive TLC wall times at microsecond precision, limited to `[1678-01-01, 2262-01-01)`; timezone-aware inputs are rejected. Negative zero becomes positive zero.
- Duplicate identity is all nine normalized trip columns. A fixed-key, vectorized pandas 64-bit hash routes each normalized row to a bucket. Exact deduplication still compares every source column, so different trip values never merge because of a hash collision.
- Validate unique lookup IDs. Inner join pickup/drop-off IDs; unmatched trips are discarded. Export joined borough/zone fields, duration, pickup hour and speed rounded with the same Python function to six decimal places.
- The zone table is small. Spark uses explicit broadcast joins; Ray uses equivalent vectorized Arrow lookup operations rather than redistributing the large trip side. Describe this honestly as large-fact-to-small-dimension enrichment, not a join between two large tables.

```bash
python validate_outputs.py \
  --left data/output/spark/spark01 \
  --right data/output/ray/ray01 \
  --report artifacts/parity01.json
```

The validator checks run manifests, schema, nulls, feature values, bucket placement, exact row equality and duplicates. It compares complete row multisets using disk-backed SQLite, one bucket at a time. File names, order and file counts need not match. A successful validation is required before interpreting performance results.

## 6. Real resource measurements

In a separate terminal on each physical host, start monitoring before a pipeline run, then stop with Ctrl+C after export:

```bash
python monitor_resources.py --framework spark --run-id spark01 \
  --node worker1 --output artifacts/spark01-worker1.csv
```

Repeat for worker2 and optionally the head; use matching framework/run IDs. Each file records UTC, CPU normalized to 0–100% per host, memory as total minus available, and swap usage. Samples include other host processes: report this measurement scope. Synchronize clocks and use one monitor per physical host. If both worker containers share one laptop, monitor the laptop once and do not sum duplicated host memory readings.

The PDF also allows `top`; you can retain a raw `top -b -d 1` log alongside this CSV. Capture Spark master UI `8080` (two active workers), running Spark application UI `4040`, and Ray dashboard `8265` (head + two workers, task/resource activity).

## 7. Separate Python UDF experiment

Use the same parity-validated output directory as input for both profilers. Only distance/duration columns are loaded, one bucket at a time, and materialized before the profiling timers. There is one warm-up per bucket and three measured trials with alternating baseline/UDF order.

```bash
spark-submit --master spark://HEAD_IP:7077 --driver-memory 768m \
  --executor-memory 768m --executor-cores 1 --conf spark.cores.max=2 \
  --conf spark.driver.host=HEAD_IP benchmark_udf.py \
  --framework spark --input data/output/spark/spark01 \
  --report artifacts/spark-udf01.json

python benchmark_udf.py --framework ray --address auto \
  --input data/output/spark/spark01 --report artifacts/ray-udf01.json
```

The UDF output is consumed by a sum, so Spark cannot prune it. Spark uses a regular scalar Python UDF; Ray calls the same scalar function inside bounded Arrow batches. Stage times include task scheduling, serialization and the final aggregation. The native distance-sum baseline gives context; subtracting it does **not** isolate JVM communication or guarantee a positive difference. Do not include these separate profiling trials in end-to-end timings.

## 8. Trials and plots

Warm up each framework once, then perform at least three measured real-cluster runs with fresh IDs; alternate framework order and keep input files/settings fixed. Exclude warm-ups from this command:

```bash
python summarize_benchmarks.py \
  --runs data/output/spark/spark01 data/output/spark/spark02 data/output/spark/spark03 \
         data/output/ray/ray01 data/output/ray/ray02 data/output/ray/ray03 \
  --output artifacts/comparison
```

To include resource peaks, add `--resources PATH_TO_EACH_CSV ... --worker-nodes worker1 worker2`. The summarizer trims logs to each timed interval, uses synchronized one-second bins with all selected physical hosts present, and reports the peak simultaneous memory sum and CPU weighted by host CPU count. Missing samples remain missing. It writes a trial CSV, JSON summary and graphs using real measurements. It rejects local-test runs and incompatible inputs/settings. Pipeline `_run.json` files record status, versions, configuration, actual time/rows, cluster information and input manifests.

## 9. Correctness checks without a distributed cluster

```bash
python -m unittest discover -s tests -v
python tests/make_fixture.py /tmp/taxi-fixture

spark-submit --master 'local[2]' --driver-memory 768m spark_clean.py \
  --allow-local --input /tmp/taxi-fixture/trips --lookup /tmp/taxi-fixture/zones.csv \
  --output /tmp/taxi-fixture/spark-output --staging /tmp/taxi-fixture/spark-staging \
  --buckets 4 --shuffle-partitions 2 --run-id fixture-spark

python ray_clean.py --address local --allow-local \
  --input /tmp/taxi-fixture/trips --lookup /tmp/taxi-fixture/zones.csv \
  --output /tmp/taxi-fixture/ray-output --staging /tmp/taxi-fixture/ray-staging \
  --buckets 4 --shuffle-partitions 2 --run-id fixture-ray

python validate_outputs.py --left /tmp/taxi-fixture/spark-output --right /tmp/taxi-fixture/ray-output
```

The deliberately small fixture includes cross-file duplicates, mismatched Parquet types, signed zero, invalid numbers, missing values, fractional-second trips and unknown zones. It is only a correctness test, never a synthetic benchmark. Local runs are marked `benchmark_eligible=false`.

## Submission and AI attribution

Submit these scripts, actual benchmark logs/graphs, required screenshots, and your own 3–5 page report with a measured winner and AI-first/BI-first discussion. Raw/staging/output data are ignored by Git. Copy selected measured artifacts into your submission before committing; `artifacts/` is ignored by default to prevent accidental large generated commits.

Performance Tuning Note to adapt accurately in your report: “AI assisted with the implementation and suggested deterministic disk-backed buckets, bounded Arrow reads, limited concurrency and column projection to accommodate limited RAM. These settings were applied to both frameworks. Cluster/network setup was performed manually; reported performance values came from our actual runs.” No winner or benchmark numbers are supplied in advance.
