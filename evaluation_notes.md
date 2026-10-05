# Assignment 3: in-class evaluation guide

This guide follows the five categories in the assignment rubric. Use `runner.md` for the exact environment and run commands. The results below are taken from the completed run metadata, resource logs, validator report, and UDF profiles. Do not present the cancelled `a3_ray05` attempt or the reused-stage diagnostic as a benchmark result.

## The 100-point rubric and what to show

| Rubric category | Weight | What to explain | Evidence to have ready |
|---|---:|---|---|
| Cluster orchestration | 25 | We manually configured a coordinator and two workers for Spark and Ray. The workers run in Docker containers on one shared laptop, so this is a local cluster, not three separate physical machines. | Spark Master UI at port 8080 showing two active workers; Spark application UI at 4040 showing the active application and two executors; Ray Dashboard at 8265 showing its head and two worker nodes/resources. Save screenshots under `artifacts/a3_01/screenshots/`. |
| Pipeline parity | 25 | Both frameworks use the same whole-file input prefix, normalization rules, row validity checks, exact duplicate identity, pickup/drop-off lookup semantics, derived columns, and Parquet output. | Completed `_run.json` files, `validate_outputs.py` report with `exact_match: true`, matching output row counts and schemas, plus the fixture test result. |
| Performance analysis | 20 | Compare total wall time from ingestion through export, and peak CPU/RAM sampled on the shared host during each timed interval. Explain the hardware, data cap, worker resources, and run count. | `benchmark_runs.csv`, `summary.json`, execution-time and resource-peak graphs, matching host-monitor CSVs, and the pipeline logs. Say whether each framework has one run or repeated runs. |
| UDF deep-dive | 15 | Explain Spark’s scalar Python UDF path versus Ray’s Arrow-batch Python path, then discuss what the timings do and do not establish. | `spark-udf.json`, `ray-udf.json`, `udf_summary.csv`, `udf_comparison.png`, and the corresponding UI screenshots if available. |
| Documentation | 15 | Present a concise 3–5 page report with the system, method, parity evidence, benchmark visuals, UDF discussion, a justified winner, and the required AI tuning note. | Final PDF in `artifacts/a3_01/`, source charts/tables, screenshots, and the runnable scripts/README/runner guide. |

## A short opening explanation

“I built the same taxi-trip preprocessing pipeline in Spark and Ray Data, then ran both on manually configured two-worker clusters. The input is a deterministic set of four complete NYC Yellow Taxi monthly Parquet files capped at 0.35 decimal GB uncompressed. I reduced the original 2 GB target after the first Ray run showed that it could not meet the 30-minute total budget on this laptop. The code standardizes and validates trip fields, routes equal rows to deterministic disk buckets, removes duplicates, enriches pickup and drop-off IDs from the taxi-zone table, calculates trip duration, pickup hour, and average speed, and exports Parquet. I compare measured end-to-end time and host CPU/memory use, verify that outputs match exactly, and separately profile the Python speed transformation. The final framework recommendation will follow the measured results on this machine.”

## Pipeline and data details

The selected input prefix contains four monthly files (January–April 2023), **12,672,737 raw rows**, and **345,261,338 uncompressed Parquet row-group bytes** (about 0.345 decimal GB). The full download has 42 files and about 4.019 GB uncompressed. Both commands use `--max-uncompressed-gb 0.35`, which selects the same sorted whole-file prefix for Spark and Ray. Compressed download size and uncompressed Parquet size are different quantities. The earlier 2 GB Ray run was cancelled after roughly nine minutes when its read/stage rate showed that it would miss the time budget; do not count that partial run as a benchmark.

The shared cleaning contract projects the nine trip columns, converts fields to consistent types, rejects null/nonfinite or invalid values, normalizes signed zero, and parses TLC timestamps as naive wall-clock timestamps with microsecond precision. A fixed-key hash gives each normalized row a deterministic key and one of 128 buckets. Equal rows therefore reach the same bucket even when they came from different files or batches. The hash chooses the bucket; deduplication compares all nine normalized columns, so different trips cannot be merged because of a hash collision.

Staging writes cleaned rows to Snappy Parquet. The 512 MiB bucket guard estimates working memory at 512 bytes per staged row; it is a guard based on observed row counts, not a hard limit on Python, Ray, or JVM memory. Ray reads the many small fragments as one bounded task per bucket, deduplicates each bucket exactly in pandas, and yields Arrow batches to the feature/export stage. This avoids 128 sequential Ray shuffle plans and keeps each in-memory deduplication unit bounded. The per-bucket diagnostic on the saved full-size Ray stage deduplicated 518,762 rows from 236 fragments in about 0.31 seconds on the host, with peak process RSS about 316 MiB; this is a local cached/read-path check, not the distributed benchmark time.

Spark reads each monthly source separately because Parquet physical types vary across months, casts to a common schema, and stages by bucket. It then reads staged files in one plan, removes duplicates, uses explicit broadcast lookup joins for the small zone table, computes the features, and writes bucket-partitioned Parquet. Ray performs equivalent zone enrichment with vectorized Arrow lookups rather than shuffling trip data twice to join a small dimension table. Both are inner-lookup semantics: a trip with an unknown pickup or drop-off zone is excluded.

The phrase “heavy join” in the assignment should be explained accurately. The fact side is the large trip table; the supplied taxi-zone table is a small dimension, so Spark broadcasts it and Ray maps the same lookup over Arrow batches. It is a large-to-small enrichment, not a join between two large fact tables. Do not describe the zone file as large.

## Rubric talking points

### Cluster orchestration

The assignment calls for two workers in each cluster. Spark and Ray each use one coordinator plus two worker containers, with shared input/output storage. Ray’s current cluster status showed three active nodes and four total compute CPUs; the Ray head has zero task CPUs and the two workers provide the compute capacity. Spark’s worker/executor configuration is in `runner.md`. The containers share one physical laptop, so the resource plots measure host-wide activity and include the operating system and any remaining background activity.

Show the screenshots while the clusters are active: Spark Master at `http://127.0.0.1:8080`, Spark app UI at `http://127.0.0.1:4040`, and Ray Dashboard at `http://127.0.0.1:8265`. The Spark application UI can disappear after the job exits. A Ray dashboard screenshot should show the node/resource view and active task/data activity, not only an empty dashboard.

### Pipeline parity

The deterministic file prefix and shared cleaner prevent input or rule differences from affecting the comparison. Dedup identity is the normalized nine-column trip row. Unknown zone IDs are removed by inner lookup semantics. The final fields include the trip columns, pickup/drop-off borough and zone, `duration_seconds`, `pickup_hour`, and `average_speed_mph` rounded to six decimals. Spark calls the shared Python `average_speed` function as a scalar UDF so its floating-point halfway behavior exactly matches Ray; native Spark `bround` differed on a real row.

The correctness fixture includes cross-file duplicates, differing Parquet types, signed zero, null/nonfinite and invalid values, fractional-second timestamps, and unknown zone IDs. The Spark and Ray fixture outputs each contained three valid rows and passed `validate_outputs.py` with an exact match. This verifies the rewritten plan on a small case; the measured outputs must also pass the validator before performance results count as parity evidence.

### Performance analysis

The required end-to-end timer starts after cluster connection and input preflight and covers ingestion, cleaning, staging, deduplication, lookup enrichment, features, and Parquet export. It excludes cluster startup, dependencies, validation, and the separate UDF profile. The resource monitor samples the shared laptop during this interval; report peak CPU and memory from the CSV/summarizer and explain that these are host-wide, not per-worker measurements. Ray used a 64 MiB target block to reduce small partition-file overhead, while Spark used 32 MiB; both used the same 32,768-row batches, 128 buckets, two concurrent tasks, and two worker cores.

Do not compare a normal measured run with a `--reuse-staging` diagnostic. Reuse-stage runs have `benchmark_eligible: false` because they exclude ingestion and staging. The cancelled `a3_ray05` run completed staging but its original serial bucket finalization was stopped after measuring approximately 100 seconds per bucket. The 2 GB `a3_ray07` run was cancelled after the read/stage rate made the 30-minute target unattainable, so the benchmark uses four monthly files. The first Spark run (`a3_spark01`) completed but failed exact validation because native `bround` differed from Python on a halfway speed value; it is excluded, and the correct Spark run (`a3_spark02`) uses the shared Python function. In `a3_ray08`, one worker OOM-killed a task and Ray spilled 8.2 GiB, but the task retried, all stages completed, and the measured output was written. Only the complete, parity-validated runs belong in the comparison.

| Measurement | Ray | Spark |
|---|---:|---:|
| Completed measured run ID | `a3_ray08` | `a3_spark02` |
| Input files / uncompressed bytes | 4 / 345,261,338 | same |
| Raw rows | 12,672,737 | same |
| Output rows | 12,191,845 | 12,191,845 |
| Total seconds, ingestion to export | 315.17 | 115.81 |
| Staging seconds | 266.11 | 23.36 |
| Target block size | 64 MiB | 32 MiB |
| Peak host CPU / memory | 13.2% / 5.99 GiB | 29.0% / 4.71 GiB |
| Full-output exact parity | Exact match | Exact match |

This is one measured pair, not a repeated-run ranking. On this machine Spark was **2.72× faster** end to end and used less peak host memory, while Ray showed lower peak CPU, one retried worker OOM, and 8.2 GiB of object spilling. The target block sizes differed (64 MiB for Ray and 32 MiB for Spark) as an explicit framework-specific tuning; the data prefix, buckets, batches, concurrency, workers, and CPU allocation matched.

State whether the comparison uses one measured pair or repeated pairs. With only one pair, call the result a single-run observation rather than a stable performance ranking. Discuss OOMs, spilling, disk use, and background load only when they appear in the collected evidence.

### UDF deep-dive

The custom calculation is average speed: distance in miles times 3,600 divided by duration in seconds, rounded to six decimals. The full Spark pipeline invokes this Python function through a scalar Spark UDF; Ray applies the same Python function to Arrow batches. The separate profiler caches/materializes the first eight non-empty output buckets, alternates a native sum baseline with the Python speed calculation, warms up, and records three measured repetitions. Its durations include scheduling, serialization, and aggregation. Baseline subtraction estimates additional transformation cost; it does not isolate JVM communication and can be negative.

Report the measured median baseline and UDF times for each framework, along with trial spread. Then explain what is plausible from the execution models: Spark crosses between JVM and Python for a scalar UDF, while Ray Data already operates with Python workers and batches. Avoid claiming Ray automatically wins; the measured result, batch size, framework version, cluster resources, and profiler limits determine the conclusion.

For this run, the first eight buckets were profiled for three measured repetitions after warm-up. Spark's median baseline was **2.01 s** and Python UDF stage **16.32 s** (estimated extra stage cost 14.31 s). Ray's median baseline was **2.04 s** and Arrow-batch Python stage **2.88 s** (estimated extra 0.84 s). These are totals over the sampled buckets, not per-row latency; scheduling, serialization, aggregation and the baseline design limit causal interpretation.

### Documentation and winner conclusion

The required report is 3–5 pages. A practical structure is: (1) objective, dataset, and manually configured clusters; (2) shared pipeline and parity method; (3) runtime/resource comparison with graphs; (4) UDF deep-dive; (5) conclusion and AI tuning disclosure. Include the two-worker Spark Master and Ray dashboard evidence, table the exact input/versions/resources, and label every run count and timing boundary.

For the measured ETL workload, Spark is the clear single-run winner on runtime (115.81 s versus Ray's 315.17 s) and peak memory (4.71 GiB versus 5.99 GiB). For BI-first reporting and structured SQL, choose Spark based on this result and its optimizer. For an AI-first Python feature pipeline, Ray's Arrow-batch UDF profile was substantially faster than Spark's scalar UDF (2.88 s versus 16.32 s on the sampled buckets), so Ray is the better fit when Python-native composition matters; be candid that its full pipeline was slower and used more host memory here. These are conditional recommendations from one local-cluster pair, not general rankings.

Include an accurate Performance Tuning Note such as: “AI assistance suggested bounded Arrow reads, deterministic disk-backed buckets, and avoiding repeated per-bucket distributed shuffle plans to fit the laptop’s memory and reduce task overhead. The shared cleaning contract and exact parity check were retained. Cluster/network configuration was performed manually, and all reported measurements were collected from actual runs.” Adjust this wording to match the final changes and your course’s attribution requirements.

## Likely questions

**Why 128 buckets?** The larger 72 million-row input could not safely be materialized as one in-memory batch. For the selected 12.67 million-row prefix, 128 buckets hold about 95,000 staged rows apiece on average, well below the 512 MiB estimated working-set guard. It also keeps duplicates for the same normalized row together.

**Why did the earlier Ray run take so long?** The first design launched 128 separate read, shuffle, join, and write plans serially; the measured cost was roughly 100 seconds per bucket after warm-up. The revised design deduplicates each bucket locally in a bounded Ray task and vectorizes the small lookup over Arrow batches. The completed four-file run took 315.17 seconds end to end.

**Why do Spark and Ray not use the same join API?** The logical lookup and inner-join semantics are the same. Spark’s explicit broadcast join suits a small dimension table; Ray’s Arrow batch lookup avoids redistributing the much larger trip side. The exact row validator checks the result.

**How do you know the reported numbers are real?** They come from archived run metadata, pipeline logs, host monitoring, the exact parity report, and the separate profiler outputs. The UI evidence screenshots were captured and have named insertion slots in `report.tex`.

**What are the comparison’s limits?** Both worker groups share one laptop and its disk, RAM, and CPU. The data is a 0.345 GB uncompressed whole-file prefix selected to meet the 30-minute run budget. This measures these versions and this machine; it does not establish a universal framework ranking.
