#!/usr/bin/env python3
"""Summarize only completed real-cluster runs and plot actual measured times."""
import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path

from common import write_json


def resource_peaks(metadata, resources, nodes):
    samples = defaultdict(dict)
    for path in resources:
        with path.open(newline="") as stream:
            for row in csv.DictReader(stream):
                if (row["framework"], row["run_id"]) != (metadata["framework"], metadata["run_id"]):
                    continue
                if row["node"] not in nodes:
                    continue
                stamp = float(row["epoch_seconds"])
                if metadata["timed_start_epoch"] <= stamp <= metadata["timed_end_epoch"]:
                    slot = samples[math.floor(stamp)]
                    if row["node"] in slot:
                        raise ValueError("Multiple samples per host/second: use one 1-second monitor per physical host.")
                    slot[row["node"]] = row
    complete = [group for group in samples.values() if set(group) == set(nodes)]
    if not complete:
        return {"peak_cpu_percent": None, "peak_memory_gib": None, "resource_samples": 0}
    cpus = [sum(float(r["cpu_percent"]) * int(r["cpu_count"]) for r in group.values()) /
            sum(int(r["cpu_count"]) for r in group.values()) for group in complete]
    memory = [sum(int(r["memory_used_bytes"]) for r in group.values()) / 1024**3 for group in complete]
    return {"peak_cpu_percent": max(cpus), "peak_memory_gib": max(memory), "resource_samples": len(complete)}


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--runs", type=Path, nargs="+", required=True, help="Output run directories")
    cli.add_argument("--resources", type=Path, nargs="*", default=[])
    cli.add_argument("--worker-nodes", nargs="+", help="Distinct physical host labels to include in resource peaks")
    cli.add_argument("--output", type=Path, required=True, help="New report directory")
    args = cli.parse_args()
    if args.resources and not args.worker_nodes:
        cli.error("--worker-nodes is required with --resources")
    if args.worker_nodes and len(set(args.worker_nodes)) != len(args.worker_nodes):
        cli.error("Each physical host must be listed once")
    rows, groups, identities = [], defaultdict(list), set()
    reference = None
    for directory in args.runs:
        metadata = json.loads((directory / "_run.json").read_text())
        if metadata["status"] != "complete" or not metadata["benchmark_eligible"]:
            raise ValueError(f"{directory}: only completed cluster benchmark runs may be summarized.")
        identity = (metadata["framework"], metadata["run_id"])
        if identity in identities:
            raise ValueError("A run was listed twice.")
        identities.add(identity)
        signature = (metadata["contract"], metadata["input_manifest"], metadata["lookup_sha256"], metadata["output_rows"],
                     {k: metadata["configuration"][k] for k in ["buckets", "batch_rows", "shuffle_partitions", "concurrency"]})
        if reference is not None and signature != reference:
            raise ValueError("Runs have different data, cleaning contracts or memory settings.")
        reference = signature
        row = {key: metadata[key] for key in ["framework", "run_id", "input_bytes", "output_rows", "total_seconds"]}
        row["block_mib"] = metadata["configuration"]["block_mib"]
        row.update(resource_peaks(metadata, args.resources, args.worker_nodes or []))
        rows.append(row)
        groups[row["framework"]].append(row["total_seconds"])
    if args.output.exists():
        raise FileExistsError(f"Refusing to overwrite {args.output}")
    args.output.mkdir(parents=True)
    with (args.output / "benchmark_runs.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    summary = {name: {"trials": len(values), "median_seconds": statistics.median(values),
                      "minimum_seconds": min(values), "maximum_seconds": max(values)}
               for name, values in groups.items()}
    write_json(args.output / "summary.json", {"frameworks": summary,
               "resource_scope": args.worker_nodes or [],
               "resource_method": "1-second bins with all selected physical hosts present; weighted CPU and synchronized memory sum."})
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    names = sorted(groups)
    medians = [statistics.median(groups[name]) for name in names]
    errors = [[median - min(groups[name]) for name, median in zip(names, medians)],
              [max(groups[name]) - median for name, median in zip(names, medians)]]
    fig, axis = plt.subplots(figsize=(6, 4))
    axis.bar(names, medians, yerr=errors, capsize=6, color=["#E87924", "#2679BE"][:len(names)])
    axis.set_ylabel("Ingestion-to-export time (seconds)")
    axis.set_title("Actual cluster runs: median and observed range")
    fig.tight_layout()
    fig.savefig(args.output / "execution_time.png", dpi=160)
    plt.close(fig)
    if args.resources:
        fig, axes = plt.subplots(1, 2, figsize=(9, 4))
        for axis, key, title in zip(axes, ["peak_cpu_percent", "peak_memory_gib"], ["Peak CPU (%)", "Peak memory (GiB)"]):
            values = [max((r[key] for r in rows if r["framework"] == name and r[key] is not None), default=0) for name in names]
            # Missing samples are reported as missing, never as a fabricated zero measurement.
            if any(not any(r[key] is not None for r in rows if r["framework"] == name) for name in names):
                axis.text(0.5, 0.5, "Insufficient synchronized samples", ha="center", transform=axis.transAxes)
            else:
                axis.bar(names, values)
            axis.set_title(title)
        fig.tight_layout()
        fig.savefig(args.output / "resource_peaks.png", dpi=160)
        plt.close(fig)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
