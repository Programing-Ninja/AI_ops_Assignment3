# Assignment 3: commands to finish the submission

Run these steps in order. The three Docker containers and network already exist, the data is downloaded, and Ray currently has **three active nodes**. This guide does not recreate the Docker network, image, or containers.

Order: **check data → Ray warm-up and measured run → switch to Spark → Spark warm-up and measured run → validate parity → optional extra trials → Spark UDF experiment → switch to Ray → Ray UDF experiment → graphs → screenshots/report → GitHub**.

The assignment PDF requires actual measurements for both frameworks; it does not specify three full-pipeline trials. One complete measured pair covers those measurements. Two additional pairs are recommended if time and disk permit. The separate UDF experiment uses three measured trials automatically.

All commands below use fresh run IDs beginning with `a3_`. If any of these IDs already exists, choose a new ID and update its monitor, validation, and summary paths too. A failed run cannot be resumed by reusing its ID.

## 0. Start with a clean Ray cluster and the revised memory settings

Run this section first. It terminates any old pipeline in the three containers and starts Ray with the same manually configured network and node addresses. Mounted data remains intact. Ctrl+C on `docker exec` alone does not reliably stop the job inside the container.

```bash
source /home/harshvardhan/miniconda3/etc/profile.d/conda.sh
conda activate aiops
cd '/home/harshvardhan/sem5/AI_OPS/assignment 3/submission'
mkdir -p artifacts/a3_01/logs artifacts/a3_01/screenshots artifacts/a3_01/run_metadata
sudo docker update --memory 2g --memory-swap 4g taxi-head
sudo docker update --cpus 2 taxi-worker1 taxi-worker2
free -h
```

The head uses a 2 GiB RAM limit and up to 4 GiB combined RAM and swap; each worker retains its 1.5 GiB RAM limit. Give each worker two CPU slots while retaining those memory limits. Close unnecessary applications. Do not disable Ray's memory monitor or enlarge the object store just to silence its generic warning.

Start the head, check it, then start the workers:

```bash
sudo docker restart taxi-worker1 taxi-worker2 taxi-head

sudo docker exec -d taxi-head bash -c \
'mkdir -p /scratch/ray-spill && exec ray start \
  --head --node-ip-address=172.28.53.10 --port=6379 --num-cpus=0 \
  --object-store-memory=134217728 \
  --object-spilling-directory=/scratch/ray-spill \
  --include-dashboard=true --dashboard-host=0.0.0.0 --dashboard-port=8265 \
  --disable-usage-stats --block > /scratch/ray-start.log 2>&1'
```

Wait for the head to start. Check its log and status; proceed to the worker commands only once these succeed:

```bash
sudo docker exec taxi-head tail -n 30 /scratch/ray-start.log
sudo docker exec taxi-head ray status --address=172.28.53.10:6379
```

Then start both workers:

```bash
sudo docker exec -d taxi-worker1 bash -c \
'mkdir -p /scratch/ray-spill && exec ray start \
  --address=172.28.53.10:6379 --node-ip-address=172.28.53.11 --num-cpus=2 \
  --object-store-memory=134217728 \
  --object-spilling-directory=/scratch/ray-spill \
  --disable-usage-stats --block > /scratch/ray-start.log 2>&1'

sudo docker exec -d taxi-worker2 bash -c \
'mkdir -p /scratch/ray-spill && exec ray start \
  --address=172.28.53.10:6379 --node-ip-address=172.28.53.12 --num-cpus=2 \
  --object-store-memory=134217728 \
  --object-spilling-directory=/scratch/ray-spill \
  --disable-usage-stats --block > /scratch/ray-start.log 2>&1'
```

After a few seconds:

```bash
sudo docker exec taxi-head ray status --address=172.28.53.10:6379
```

Proceed only with **three active nodes and four total compute CPUs**. Creating the spill directories above prevents the worker startup failure encountered earlier.


If startup fails or memory kills recur:

```bash
sudo docker stats --no-stream taxi-head taxi-worker1 taxi-worker2
sudo docker exec taxi-head bash -c 'tail -n 100 /tmp/ray/session_latest/logs/raylet.out'
sudo docker exec taxi-worker1 tail -n 50 /scratch/ray-start.log
sudo docker exec taxi-worker2 tail -n 50 /scratch/ray-start.log
```

The early attempts `a3_ray_warmup` and `a3_ray01`–`a3_ray04` did not complete reliably. `a3_ray05` completed staging but was cancelled during its serial 128-plan finalization; its staged Parquet is retained for diagnosis. `a3_ray06_trial` was cancelled because its input cap did not match the reused stage. `a3_ray06_trial2` completed finalization on the real cluster in about 146 seconds, but reused staging and is ineligible as an end-to-end benchmark. The 2 GB `a3_ray07` attempt was also cancelled after about nine minutes because the first few files were taking too long; its partial staging is not reusable. The current implementation performs exact dedup locally within each bounded bucket, in parallel across Ray workers, and uses vectorized Arrow zone enrichment. Use `a3_ray08` below with the reduced four-file prefix. Keep concurrency at **2** to reduce worker memory kills; 128 buckets retain the 512 MiB estimated working-set guard.

## 1. Prepare two host terminals and check the dataset

In **both terminals**, run:

```bash
source /home/harshvardhan/miniconda3/etc/profile.d/conda.sh
conda activate aiops
cd '/home/harshvardhan/sem5/AI_OPS/assignment 3/submission'
```

Use terminal A for pipeline commands and terminal B for resource monitoring. The pipelines run inside `taxi-head`, where this directory is mounted at `/srv/taxi`; the monitor runs on the physical laptop.

In terminal A:

```bash
mkdir -p artifacts/a3_01/logs artifacts/a3_01/screenshots artifacts/a3_01/run_metadata
df -h . /tmp
free -h
sudo docker ps --filter name=taxi-
sudo docker exec taxi-head ray status --address=172.28.53.10:6379

# Refresh the compressed-download manifest without fetching more months.
python download_data.py --target-gb 2
cp data/raw/input_manifest.json artifacts/a3_01/input_manifest.json

python - <<'PY'
import json
from pathlib import Path
manifest = json.loads(Path('data/raw/input_manifest.json').read_text())
rows = sum(item['rows'] for item in manifest['files'])
print(f"Files: {len(manifest['files'])}")
print(f"Input: {manifest['total_bytes'] / 1e9:.3f} GB (decimal)")
print(f"Raw rows: {rows:,}")
assert Path('data/raw/taxi_zone_lookup.csv').is_file()
PY
```

At the time this guide was written, the download contained **42 files, 2,549,830,572 compressed bytes, and 150,862,145 raw rows**. Their Parquet metadata reports about **4.019 GB uncompressed**. Keep the download and lookup unchanged throughout the comparison; the measured commands select the same sorted whole-file prefix capped at 0.35 decimal GB uncompressed.

The first 22 files (1.932 GB, 72,297,926 rows) proved too slow on this shared laptop: the 2 GB Ray attempt was cancelled after roughly nine minutes with only the first few files processed. To fit both main framework runs inside a 30-minute budget, use the first **four complete monthly files**: **345,261,338 uncompressed bytes (0.345 GB) and 12,672,737 raw rows**. The same `--max-uncompressed-gb 0.35` cap selects that exact prefix for both frameworks. Keep 128 disk buckets, 32,768-row batches, concurrency 2, and the 512 MiB per-bucket estimate guard. Ray uses a 64 MiB target block to reduce partition-file overhead; Spark uses 32 MiB. This framework-specific block tuning is recorded in the result table; the other sizing and cluster settings match. The guard limits estimated per-task working data; it is not a hard cap on decoder or runtime allocations.

The following check reports the whole-file prefix selected by the same 0.35 GB budget:

```bash
python - <<'PY'
from pathlib import Path
import pyarrow.parquet as pq
files = sorted(Path('data/raw/trips').glob('*.parquet'))
used = rows = count = 0
for path in files:
    parquet = pq.ParquetFile(path)
    size = sum(parquet.metadata.row_group(i).total_byte_size
               for i in range(parquet.metadata.num_row_groups))
    if used + size > 350_000_000:
        break
    used += size
    rows += parquet.metadata.num_rows
    count += 1
print(f'Selected files: {count}; uncompressed: {used:,} bytes; raw rows: {rows:,}')
assert used > 0 and used <= 350_000_000
PY
```

The laptop currently has about **29 GB free**, shared by the project and `/tmp`. Staging, shuffle/spill files, and outputs can be much larger than the compressed inputs. Check disk between runs and delete only the completed run's staging when instructed below. If space becomes insufficient, stop and provide more storage before retrying with new IDs.

## 2. Run Ray first

### 2a. One-file warm-up, excluded from the final comparison

Terminal A:

```bash
set -o pipefail
mkdir -p artifacts/a3_01/logs
sudo docker exec taxi-head python ray_clean.py \
  --address 172.28.53.10:6379 \
  --run-id a3_ray_warmup_v5 --max-files 1 --buckets 8 --max-bucket-mib 512 \
  --batch-rows 32768 --block-mib 32 --concurrency 2 --shuffle-partitions 1 \
  2>&1 | tee artifacts/a3_01/logs/ray-warmup-v4.log
```

Wait for success. The last JSON line should contain a time and output row count. Check the recorded status:

```bash
python - <<'PY'
import json
from pathlib import Path
m = json.loads(Path('data/output/ray/a3_ray_warmup_v5/_run.json').read_text())
assert m['status'] == 'complete', m
assert m['output_rows'] > 0, m
print('Ray warm-up complete')
PY
```

After that check passes, remove only this disposable warm-up's data:

```bash
rm -r -- data/staging/ray/a3_ray_warmup_v5 data/output/ray/a3_ray_warmup_v5
```

### 2b. Start monitoring before the measured run

Terminal B:

```bash
top -b -d 1 > artifacts/a3_01/logs/ray08-top.log &
TAXI_TOP_PID=$!
python monitor_resources.py \
  --framework ray --run-id a3_ray08 --node laptop --interval 1 \
  --output artifacts/a3_01/ray08-host.csv
# After the pipeline finishes, press Ctrl+C to stop the Python monitor.
# Then run these two lines to stop the background top logger:
kill "$TAXI_TOP_PID"
wait "$TAXI_TOP_PID" 2>/dev/null || true
```

Leave this terminal monitoring while terminal A runs the pipeline. Use **one host monitor**, since all three containers share one physical laptop. Summing three host readings would count the same memory three times. Keep other application activity low and comparable across frameworks.

### 2c. Run the complete Ray pipeline

Terminal A:

```bash
set -o pipefail
sudo docker exec taxi-head python ray_clean.py \
  --address 172.28.53.10:6379 \
  --run-id a3_ray08 --max-uncompressed-gb 0.35 --buckets 128 --max-bucket-mib 512 \
  --batch-rows 32768 --block-mib 64 --concurrency 2 --shuffle-partitions 1 \
  2>&1 | tee artifacts/a3_01/logs/ray08.log
```

**While it runs**, open <http://127.0.0.1:8265> and save screenshots showing the head, both workers, and task/resource activity under `artifacts/a3_01/screenshots/`. Waiting until the process finishes can lose useful activity evidence.

Capture two Ray views for the rubric: first the Nodes/Resources view showing the head and both workers active; then, after the log starts printing `Ray bucket ... rows after exact dedup`, capture the dashboard's live task/resource activity. Save them as `ray-nodes.png` and `ray-active.png` before the run finishes.

When the command exits successfully, stop both loggers in terminal B as described above. Then, in terminal A:

```bash
python - <<'PY'
import json
from pathlib import Path
m = json.loads(Path('data/output/ray/a3_ray08/_run.json').read_text())
assert m['status'] == 'complete' and m['benchmark_eligible'], m
assert m['output_rows'] > 0, m
assert m['input_uncompressed_bytes'] <= 350_000_000, m
print({k: m[k] for k in ['framework', 'input_bytes', 'input_uncompressed_bytes', 'raw_rows', 'output_rows', 'total_seconds']})
PY
mkdir -p artifacts/a3_01/run_metadata/ray08
cp data/output/ray/a3_ray08/_run.json artifacts/a3_01/run_metadata/ray08/
```

After the check passes, reclaim Ray staging space. **Keep the output** for parity:

```bash
rm -r -- data/staging/ray/a3_ray08
df -h . /tmp
```

## 3. Switch from Ray to Spark

Stop any monitor first. The following restart terminates Ray on all three containers. Run it only after the Ray pipeline has finished. Files in the shared project and scratch mounts remain.

Terminal A:

```bash
sudo docker restart taxi-worker1 taxi-worker2 taxi-head

sudo docker exec -d taxi-head bash -c \
'exec "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.master.Master \
  --host 172.28.53.10 --port 7077 --webui-port 8080 \
  > /scratch/spark-master.log 2>&1'

sudo docker exec -d taxi-worker1 bash -c \
'mkdir -p /scratch/spark-work /scratch/spark-local && \
  exec "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.worker.Worker \
  --host 172.28.53.11 --port 7078 --webui-port 8081 \
  --cores 1 --memory 768m --work-dir /scratch/spark-work \
  spark://172.28.53.10:7077 > /scratch/spark-worker.log 2>&1'

sudo docker exec -d taxi-worker2 bash -c \
'mkdir -p /scratch/spark-work /scratch/spark-local && \
  exec "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.worker.Worker \
  --host 172.28.53.12 --port 7078 --webui-port 8081 \
  --cores 1 --memory 768m --work-dir /scratch/spark-work \
  spark://172.28.53.10:7077 > /scratch/spark-worker.log 2>&1'
```

Open <http://127.0.0.1:8080>. Wait until **two workers are ALIVE**, each with one core and 768 MiB configured worker memory. Save a master UI screenshot. If a worker does not register, read its log before proceeding:

```bash
sudo docker exec taxi-worker1 tail -n 50 /scratch/spark-worker.log
sudo docker exec taxi-worker2 tail -n 50 /scratch/spark-worker.log
```

Do not run both frameworks simultaneously on this laptop. Cluster startup is outside the measured ingestion-to-export interval.

## 4. Run Spark

### 4a. One-file warm-up, excluded from the final comparison

Terminal A:

```bash
set -o pipefail
mkdir -p artifacts/a3_01/logs
sudo docker exec taxi-head spark-submit \
  --master spark://172.28.53.10:7077 --deploy-mode client \
  --driver-memory 768m --executor-memory 768m --executor-cores 1 \
  --conf spark.cores.max=2 \
  --conf spark.driver.host=172.28.53.10 \
  --conf spark.driver.bindAddress=0.0.0.0 \
  spark_clean.py --run-id a3_spark_warmup --max-files 1 --buckets 128 \
  --batch-rows 32768 --block-mib 32 --concurrency 2 --shuffle-partitions 8 \
  2>&1 | tee artifacts/a3_01/logs/spark-warmup.log

python - <<'PY'
import json
from pathlib import Path
m = json.loads(Path('data/output/spark/a3_spark_warmup/_run.json').read_text())
assert m['status'] == 'complete', m
assert m['output_rows'] > 0, m
print('Spark warm-up complete')
PY
```

Only after success:

```bash
rm -r -- data/staging/spark/a3_spark_warmup data/output/spark/a3_spark_warmup
```

### 4b. Start monitoring

Terminal B:

```bash
top -b -d 1 > artifacts/a3_01/logs/spark02-top.log &
TAXI_TOP_PID=$!
python monitor_resources.py \
  --framework spark --run-id a3_spark02 --node laptop --interval 1 \
  --output artifacts/a3_01/spark02-host.csv
# After export finishes: Ctrl+C, then stop top with these lines.
kill "$TAXI_TOP_PID"
wait "$TAXI_TOP_PID" 2>/dev/null || true
```

### 4c. Run the complete Spark pipeline

Terminal A:

```bash
set -o pipefail
sudo docker exec taxi-head spark-submit \
  --master spark://172.28.53.10:7077 --deploy-mode client \
  --driver-memory 768m --executor-memory 768m --executor-cores 1 \
  --conf spark.cores.max=2 \
  --conf spark.driver.host=172.28.53.10 \
  --conf spark.driver.bindAddress=0.0.0.0 \
  spark_clean.py --run-id a3_spark02 --max-uncompressed-gb 0.35 --buckets 128 --max-bucket-mib 512 \
  --batch-rows 32768 --block-mib 32 --concurrency 2 --shuffle-partitions 1 \
  2>&1 | tee artifacts/a3_01/logs/spark02.log
```

**While the application runs**, save screenshots of:

- <http://127.0.0.1:8080>: two active workers and the running application.
- <http://127.0.0.1:4040>: Jobs/Stages and the Executors tab with two executors.

Take the Spark Master screenshot before submitting the measured job, once it shows two active workers. Take the port 4040 screenshots immediately after the application appears: one on Jobs/Stages and one on Executors. Save them as `spark-master.png`, `spark-jobs.png`, and `spark-executors.png`. Port 4040 may disappear as soon as the application ends.

The application UI on 4040 normally disappears when `spark-submit` exits. Save its evidence before completion.

After successful export, stop the terminal B loggers. Then, in terminal A:

```bash
python - <<'PY'
import json
from pathlib import Path
m = json.loads(Path('data/output/spark/a3_spark02/_run.json').read_text())
assert m['status'] == 'complete' and m['benchmark_eligible'], m
assert m['output_rows'] > 0, m
assert m['input_uncompressed_bytes'] <= 350_000_000, m
print({k: m[k] for k in ['framework', 'input_bytes', 'input_uncompressed_bytes', 'raw_rows', 'output_rows', 'total_seconds']})
PY
mkdir -p artifacts/a3_01/run_metadata/spark02
cp data/output/spark/a3_spark02/_run.json artifacts/a3_01/run_metadata/spark02/
```

After the check passes, remove only completed staging, keeping the output:

```bash
rm -r -- data/staging/spark/a3_spark02
df -h . /tmp
```

## 5. Validate the complete outputs before using the timings

Terminal A, on the host:

```bash
set -o pipefail
python validate_outputs.py \
  --left data/output/spark/a3_spark02 \
  --right data/output/ray/a3_ray08 \
  --batch-rows 8192 --report artifacts/a3_01/parity01.json \
  2>&1 | tee artifacts/a3_01/logs/parity01.log
```

Expect exit code zero and `"exact_match": true` in the report. This reads both complete datasets in batches and compares exact rows through disk-backed SQLite; it can take substantial time for 150 million raw rows. Do not interrupt it simply because it has little console output. Validation is outside the pipeline timings.

If it fails, resolve the mismatch and rerun the affected pipelines with new IDs before interpreting performance. Keep `data/output/spark/a3_spark02` for **both** UDF experiments. Do not delete the output pair yet.

## 6. Optional: obtain three measured full-pipeline trials per framework

Skip this section if completing one measured pair. Spark is currently running, so the extra order can be:

| Order | Framework | Pipeline run ID | Host CSV | Top log |
| --- | --- | --- | --- | --- |
| 1 | Spark | `a3_spark03` | `spark03-host.csv` | `spark03-top.log` |
| 2 | Ray | `a3_ray09` | `ray09-host.csv` | `ray09-top.log` |
| 3 | Spark | `a3_spark04` | `spark04-host.csv` | `spark04-top.log` |
| 4 | Ray | `a3_ray10` | `ray10-host.csv` | `ray10-top.log` |

Reuse the exact measured pipeline and monitor commands in steps 2b–2c and 4b–4c, replacing the run IDs and file names with the table entries. Keep all sizing flags identical within each framework; the 64 MiB Ray and 32 MiB Spark target blocks are the intentional framework-specific tuning. Do not repeat the warm-ups. Use step 0's Ray startup block when switching to Ray and step 3 when switching back to Spark.

Validate each complete pair with these exact commands:

```bash
python validate_outputs.py \
  --left data/output/spark/a3_spark02 --right data/output/ray/a3_ray09 \
  --report artifacts/a3_01/parity02.json

python validate_outputs.py \
  --left data/output/spark/a3_spark04 --right data/output/ray/a3_ray10 \
  --report artifacts/a3_01/parity03.json
```

Run the first validation after pair 02 completes, before starting pair 03. Run the second after pair 03 completes. Stop monitors after each pipeline, copy `_run.json` into the corresponding evidence directory, and remove completed staging as in the first pair.

To conserve disk, after **pair 02 has passed validation**, archive its metadata and delete only that additional pair's outputs. The first pair stays available for UDF profiling:

```bash
mkdir -p artifacts/a3_01/run_metadata/spark02 artifacts/a3_01/run_metadata/ray09
cp data/output/spark/a3_spark02/_run.json artifacts/a3_01/run_metadata/spark02/
cp data/output/ray/a3_ray09/_run.json artifacts/a3_01/run_metadata/ray09/
rm -r -- data/output/spark/a3_spark02 data/output/ray/a3_ray09
```

After **pair 03 has passed validation**, do the same:

```bash
mkdir -p artifacts/a3_01/run_metadata/spark04 artifacts/a3_01/run_metadata/ray10
cp data/output/spark/a3_spark04/_run.json artifacts/a3_01/run_metadata/spark04/
cp data/output/ray/a3_ray10/_run.json artifacts/a3_01/run_metadata/ray10/
rm -r -- data/output/spark/a3_spark04 data/output/ray/a3_ray10
```

The summarizer can use these archived directories because it reads the original `_run.json` measurements. Keep parity reports, CSV samples, screenshots, and logs as evidence. These archived directories are metadata records, not retained Parquet datasets. Disk capacity is still a constraint while processing any one pair.

## 7. Run the separate Spark UDF experiment, then switch to Ray

Spark should still be running. Both UDF experiments must use the same parity-validated Spark output from pair 01.

Terminal A:

```bash
set -o pipefail
sudo docker exec taxi-head spark-submit \
  --master spark://172.28.53.10:7077 --deploy-mode client \
  --driver-memory 768m --executor-memory 768m --executor-cores 1 \
  --conf spark.cores.max=2 \
  --conf spark.driver.host=172.28.53.10 \
  --conf spark.driver.bindAddress=0.0.0.0 \
  benchmark_udf.py --framework spark --run-id a3_spark_udf \
  --input data/output/spark/a3_spark02 \
  --report artifacts/a3_01/spark-udf.json --repeats 3 \
  --max-buckets 8 --batch-rows 4096 --block-mib 8 --concurrency 2 --shuffle-partitions 8 \
  2>&1 | tee artifacts/a3_01/logs/spark-udf.log
```

Wait for it to finish and save any additional 4040 screenshots while it runs. Do not use this job's elapsed time as the full pipeline time.

Now stop Spark and switch back to Ray by running **only the startup commands from step 0**, starting with `sudo docker restart taxi-worker1 taxi-worker2 taxi-head`. Wait for three active Ray nodes and four total CPUs before step 8. Do not repeat the data preflight or pipeline warm-up.

## 8. Run the Ray UDF experiment

Terminal A:

```bash
set -o pipefail
sudo docker exec taxi-head python benchmark_udf.py \
  --framework ray --address 172.28.53.10:6379 \
  --input data/output/spark/a3_spark02 \
  --report artifacts/a3_01/ray-udf.json --repeats 3 \
  --max-buckets 8 --batch-rows 4096 --block-mib 8 --concurrency 2 --shuffle-partitions 8 \
  2>&1 | tee artifacts/a3_01/logs/ray-udf.log
```

Save an additional dashboard screenshot during activity if needed. Each profiler loads one bucket at a time, warms it up, and measures three trials of a native baseline and the Python speed transformation. The reported stage times include scheduling, serialization, and aggregation. A baseline subtraction is an estimate of extra stage cost; it does not isolate JVM communication and can be negative.

## 9. Generate the pipeline graphs and comparison table

On the host in terminal A, for **one measured pair**:

```bash
python summarize_benchmarks.py \
  --runs artifacts/a3_01/run_metadata/spark02 artifacts/a3_01/run_metadata/ray08 \
  --resources artifacts/a3_01/spark02-host.csv artifacts/a3_01/ray08-host.csv \
  --worker-nodes laptop --output artifacts/a3_01/comparison
```

If you completed **all three measured pairs**, use this command instead:

```bash
python summarize_benchmarks.py \
  --runs artifacts/a3_01/run_metadata/spark02 artifacts/a3_01/run_metadata/spark03 \
         artifacts/a3_01/run_metadata/spark04 artifacts/a3_01/run_metadata/ray08 \
         artifacts/a3_01/run_metadata/ray09 artifacts/a3_01/run_metadata/ray10 \
  --resources artifacts/a3_01/spark02-host.csv artifacts/a3_01/spark03-host.csv \
              artifacts/a3_01/spark04-host.csv artifacts/a3_01/ray08-host.csv \
              artifacts/a3_01/ray09-host.csv artifacts/a3_01/ray10-host.csv \
  --worker-nodes laptop --output artifacts/a3_01/comparison
```

Choose one command. The output directory must be new. For a later revised summary, use a new directory such as `comparison_v2`.

Generated files:

- `comparison/benchmark_runs.csv`: actual pipeline seconds, output rows, peak CPU, and peak memory per trial.
- `comparison/summary.json`: number of trials and median/min/max time per framework.
- `comparison/execution_time.png`: execution time graph.
- `comparison/resource_peaks.png`: CPU and memory graphs.

The resource summaries include only samples inside each recorded pipeline interval. CPU is normalized to 0–100% for the whole physical laptop; memory includes the OS and other applications. State this scope in the report. Missing samples must be fixed or described as missing, rather than reported as zero. With one measured trial, describe it as a single-trial result; it does not establish run-to-run variability.

## 10. Generate a UDF table and graph from the measured JSON files

Run this on the host. It only plots values already recorded by the profilers:

```bash
python - <<'PY'
import csv
import json
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

root = Path('artifacts/a3_01')
rows = []
for framework in ['spark', 'ray']:
    report = json.loads((root / f'{framework}-udf.json').read_text())
    assert report['benchmark_eligible'], 'Only use real-cluster measurements'
    rows.append({
        'framework': framework,
        'buckets': 8,
        'trials': len(report['trials']),
        'median_baseline_seconds': report['median_baseline_seconds'],
        'median_udf_seconds': report['median_udf_seconds'],
        'udf_minus_baseline_seconds': report['median_udf_seconds'] - report['median_baseline_seconds'],
    })
with (root / 'udf_summary.csv').open('w', newline='') as stream:
    writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)

x = list(range(len(rows)))
fig, ax = plt.subplots(figsize=(7, 4))
ax.bar([v - 0.18 for v in x], [r['median_baseline_seconds'] for r in rows],
       width=0.36, label='Native distance-sum baseline')
ax.bar([v + 0.18 for v in x], [r['median_udf_seconds'] for r in rows],
       width=0.36, label='Python speed stage')
ax.set_xticks(x, [r['framework'] for r in rows])
ax.set_ylabel('Median summed bucket-stage time (seconds)')
ax.set_title('UDF profile: first 8 buckets, 3 trials')
ax.legend()
fig.tight_layout()
fig.savefig(root / 'udf_comparison.png', dpi=160)
plt.close(fig)
print(json.dumps(rows, indent=2))
PY
```

The profiler totals cover only the first eight non-empty output buckets. They exclude bucket loading and materialization, so they are a separate experiment from ingestion-to-export timing.

## 11. Write and export the 3–5 page report

The rubric-aligned report source is `report.tex`; the captured screenshots from `images/` have been copied to the named paths below and are embedded in the four-page PDF. Recompile after changing the images or report text:

```bash
pdflatex -interaction=nonstopmode -halt-on-error -output-directory artifacts/a3_01 report.tex
```

The screenshot paths are `artifacts/a3_01/screenshots/spark-master.png`, `spark-app.png`, `ray-nodes.png`, and `ray-active.png`. A workable report order is:

1. **Data and setup:** dataset size/months/rows, machine RAM/CPU, software versions from `_run.json`, the manually configured Docker network, one head and two workers per framework. Explain that all containers share the same laptop. Include master/dashboard screenshots.
2. **Pipeline and parity:** ingestion, shared cleaning rules, duplicate removal, pickup/drop-off zone joins, timestamp handling, Python speed function, and Parquet export. Reference `parity01.json` and output row counts. The small zone lookup is broadcast to Spark workers.
3. **Performance:** paste the runtime/resource table and graphs. Define the timing boundary: ingestion through export, including disk staging; excluding cluster startup, preflight, validation, and the separate UDF experiment. State the trial count and host-wide monitoring scope. Discuss spill/swap and disk I/O if visible in the logs.
4. **UDF deep dive:** include `udf_summary.csv` and `udf_comparison.png`. Discuss scalar Python execution in Spark versus the Ray batch implementation, including the measured stage overhead and the limits of baseline subtraction.
5. **Conclusion and attribution:** name the winner for this measured workload, supported by the values. Discuss which framework you would choose for an AI-first project versus a BI-first project. Include a Performance Tuning Note and relevant limitations.

Adapt this attribution accurately:

> AI assisted with the pipeline implementation and suggested column projection, bounded Arrow reads, deterministic disk buckets, and limited concurrency to fit the laptop's RAM. Both frameworks used the same cleaning contract, input files, bucket count, batch size, concurrency, and worker CPU allocation. Ray used a larger target block to reduce small partition-file overhead. Initial cluster/network setup was performed manually. AI also assisted with runtime diagnostics and correctness-test execution. All reported results came from actual runs.

Do not invent results or assume Ray must win the UDF experiment. If only one pair was measured, limit the winner claim to that trial on this hardware.

Before submission, make sure the inserted screenshots show **Spark master with two active workers, Spark application UI on 4040, and Ray dashboard resource/activity views with the head and two workers**.

## 12. Put the code and selected evidence in GitHub

The Git repository is the `submission/` directory. Raw data, staging, pipeline outputs, and `artifacts/` are ignored. Explicitly add only the selected evidence; do not force-add the dataset.

After saving your screenshots and report:

```bash
git status --short
git remote -v

git add runner.md README.md Dockerfile.cluster requirements.txt \
  common.py spark_clean.py ray_clean.py download_data.py \
  monitor_resources.py benchmark_udf.py validate_outputs.py summarize_benchmarks.py \
  tests .gitignore

git add -f artifacts/a3_01/input_manifest.json artifacts/a3_01/run_metadata \
  artifacts/a3_01/parity01.json artifacts/a3_01/comparison \
  artifacts/a3_01/spark02-host.csv artifacts/a3_01/ray08-host.csv \
  artifacts/a3_01/spark-udf.json artifacts/a3_01/ray-udf.json \
  artifacts/a3_01/udf_summary.csv artifacts/a3_01/udf_comparison.png \
  artifacts/a3_01/screenshots artifacts/a3_01/report.pdf report.tex

git diff --cached --stat
git commit -m "Add measured Spark and Ray assignment results"
git push -u origin HEAD
```

If you ran extra trials, also add `parity02.json`, `parity03.json`, and the four additional `*-host.csv` files before committing. Their metadata is already covered by the directory above. Keep the raw console/top logs locally; include selected logs in GitHub if useful and reasonably sized.

If `git remote -v` shows no `origin`, create your GitHub repository and set its actual URL with `git remote add origin YOUR_REPOSITORY_URL` before pushing. Submit the repository URL, screenshots, and the PDF as requested by the course.

## If a run fails

- Stop its resource monitor and top logger, retain the error log, and inspect the recorded `_run.json` status if it exists.
- For a missing worker, inspect `/scratch/ray-start.log` or `/scratch/spark-worker.log` in that worker container before rerunning.
- For a bucket memory guard failure or an out-of-memory failure, investigate the logs. More buckets reduce per-bucket work; if you increase `--buckets`, apply the new value to both frameworks and use new IDs for the comparison.
- For a full disk, preserve raw files and evidence, and reclaim only identified temporary/completed staging files or move to larger shared storage. Container restarts do not remove mounted spill/shuffle files.
- Never use `--allow-local`, `--address local`, or `local[2]` in measured commands. The 0.35 GB uncompressed subset is intentionally bounded for the 30-minute total run budget; the one-file warm-ups are excluded from summaries.

Do not continue to the final comparison unless the measured runs completed, their outputs passed parity, and the relevant resource/UDF evidence was collected.
