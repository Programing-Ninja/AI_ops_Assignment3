#!/usr/bin/env python3
"""Sample real physical-host CPU/memory once per interval, until Ctrl+C."""
import argparse
import csv
import signal
import socket
import threading
import time
from datetime import datetime, timezone
from pathlib import Path


def main():
    import psutil
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--output", type=Path, required=True)
    cli.add_argument("--framework", choices=["spark", "ray"], required=True)
    cli.add_argument("--run-id", required=True)
    cli.add_argument("--node", default=socket.gethostname())
    cli.add_argument("--interval", type=float, default=1)
    cli.add_argument("--duration", type=float, help="Optional maximum seconds")
    args = cli.parse_args()
    if args.interval <= 0 or (args.duration is not None and args.duration <= 0):
        cli.error("interval and duration must be positive")
    stopped = threading.Event()
    for sig in [signal.SIGINT, signal.SIGTERM]:
        signal.signal(sig, lambda *_: stopped.set())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    fields = ["utc", "epoch_seconds", "framework", "run_id", "node", "cpu_percent", "cpu_count",
              "memory_used_bytes", "memory_available_bytes", "memory_total_bytes", "swap_used_bytes"]
    psutil.cpu_percent(interval=None)
    started = time.monotonic()
    with args.output.open("x", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        print(f"Monitoring {args.node}; stop with Ctrl+C. CPU is normalized to 0–100% per host.", flush=True)
        while not stopped.wait(args.interval):
            memory = psutil.virtual_memory()
            writer.writerow({"utc": datetime.now(timezone.utc).isoformat(), "epoch_seconds": time.time(),
                             "framework": args.framework, "run_id": args.run_id, "node": args.node,
                             "cpu_percent": psutil.cpu_percent(interval=None),
                             "cpu_count": psutil.cpu_count(),
                             "memory_used_bytes": memory.total - memory.available,
                             "memory_available_bytes": memory.available, "memory_total_bytes": memory.total,
                             "swap_used_bytes": psutil.swap_memory().used})
            stream.flush()
            if args.duration and time.monotonic() - started >= args.duration:
                break


if __name__ == "__main__":
    main()
