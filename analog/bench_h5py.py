#!/usr/bin/env python3
"""h5py (C libhdf5) baseline for the many-file analog scan.

Emits one JSON object on stdout, same shape as hdf5-df-bench.
"""
from __future__ import annotations

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import h5py
import numpy as np

VALUES = "Machine/GpuMetric[0]/Values"


def _count_one(path: Path) -> int:
    with h5py.File(path, "r") as f:
        return int(np.asarray(f[VALUES]).size)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--work-dir", type=Path, required=True)
    p.add_argument("--workers", type=int, default=0, help="0 = serial")
    args = p.parse_args()
    files = sorted(args.work_dir.glob("part-*.h5"))
    if not files:
        raise SystemExit(f"no part-*.h5 in {args.work_dir}")
    t0 = time.perf_counter()
    if args.workers <= 1:
        n = sum(_count_one(f) for f in files)
    else:
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            n = sum(ex.map(_count_one, files))
    ms = (time.perf_counter() - t0) * 1000.0
    nbytes = sum(f.stat().st_size for f in files)
    print(
        json.dumps(
            {
                "engine": "h5py",
                "workers": max(args.workers, 1),
                "files": len(files),
                "rows": n,
                "ms": round(ms, 3),
                "MBps": round((nbytes / 1_000_000.0) / (ms / 1000.0), 3) if ms else 0,
            }
        )
    )


if __name__ == "__main__":
    main()
