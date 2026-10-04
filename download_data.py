#!/usr/bin/env python3
"""Download real TLC data on demand; existing input files are preserved."""
from __future__ import annotations

import argparse
import hashlib
import math
import re
import tempfile
import urllib.request
from pathlib import Path

from common import ROOT, parquet_files, write_json

BASE_URL = "https://d37ci6vzurychx.cloudfront.net"


def download(url, target, parquet=False):
    import pyarrow.parquet as pq
    if target.exists():
        if parquet:
            pq.ParquetFile(str(target))
        print(f"Keeping existing {target.name}", flush=True)
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading {url}", flush=True)
    with tempfile.NamedTemporaryFile(dir=target.parent, prefix=".download-", delete=False) as stream:
        temporary = Path(stream.name)
        try:
            request = urllib.request.Request(url, headers={"User-Agent": "DA3408-taxi-duel/1.0"})
            with urllib.request.urlopen(request, timeout=120) as response:
                while chunk := response.read(1024 * 1024):
                    stream.write(chunk)
            stream.flush()
            if parquet:
                pq.ParquetFile(str(temporary))
            # Hard link creates a new destination atomically and never overwrites an existing file.
            target.hardlink_to(temporary)
        finally:
            temporary.unlink(missing_ok=True)
    print(f"Saved {target.name}: {target.stat().st_size:,} bytes", flush=True)


def main():
    import pyarrow.parquet as pq
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--raw-dir", type=Path, default=ROOT / "data/raw")
    selection = cli.add_mutually_exclusive_group()
    selection.add_argument("--months", nargs="+", help="Example: 2023-01 2023-02")
    selection.add_argument("--target-gb", type=float, help="Download until total trip bytes reach this many decimal GB")
    args = cli.parse_args()
    if args.target_gb is not None and (not math.isfinite(args.target_gb) or args.target_gb <= 0):
        cli.error("--target-gb must be finite and positive")
    trips = args.raw_dir / "trips"
    trips.mkdir(parents=True, exist_ok=True)
    download(f"{BASE_URL}/misc/taxi_zone_lookup.csv", args.raw_dir / "taxi_zone_lookup.csv")
    months = args.months or ([f"{year}-{month:02d}" for year in [2023, 2024, 2022, 2021]
                            for month in range(1, 13)] if args.target_gb else ["2023-01", "2023-02"])
    for month in months:
        if not re.fullmatch(r"20\d{2}-(0[1-9]|1[0-2])", month):
            cli.error(f"Invalid month: {month}")
        if args.target_gb and sum(path.stat().st_size for path in parquet_files(trips)) >= args.target_gb * 1e9:
            break
        name = f"yellow_tripdata_{month}.parquet"
        download(f"{BASE_URL}/trip-data/{name}", trips / name, parquet=True)
    manifest = []
    for path in parquet_files(trips):
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                digest.update(chunk)
        manifest.append({"filename": path.name, "bytes": path.stat().st_size,
                         "rows": pq.ParquetFile(str(path)).metadata.num_rows, "sha256": digest.hexdigest()})
    total = sum(row["bytes"] for row in manifest)
    write_json(args.raw_dir / "input_manifest.json", {"total_bytes": total, "files": manifest})
    print(f"Trip data: {len(manifest)} files, {total / 1e9:.3f} GB (decimal).")
    if args.target_gb and total < args.target_gb * 1e9:
        raise SystemExit("Available candidate months did not reach the target; request additional --months.")


if __name__ == "__main__":
    main()
