#!/usr/bin/env python3
"""SysML v2 / SDG machine analog (OTel resource + GPU parts).

Shape (depth 4, 7 groups, 5 datasets, 56 attrs) matches the cybersec HDF5
fingerprint. Vocabulary is **not** PRODML/DAS. Groups are a host Machine
(SysML part) with GpuDevice parts and an OTel instrumentation Scope.
PRODML/WITSML remain in SDG for other sources; they are not this analog.
"""
from __future__ import annotations

import argparse
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

try:
    import h5py
except ImportError as e:  # pragma: no cover
    raise SystemExit("h5py is required") from e

SCHEMA_VERSION = "sdg-sysml-machine-hdf5/1.0"
SDG = "https://signals.zndx.org/sdg#"

SERIES_BLOCK_TARGET_BYTES = 8 * 1024 * 1024


def series_block_rows(n_time: int) -> int:
    return max(1, SERIES_BLOCK_TARGET_BYTES // (n_time * 2))


def _fix(node, name: str, s: str) -> None:
    b = s.encode("ascii", "replace")
    node.attrs.create(name, np.bytes_(b), dtype=np.dtype(f"S{max(1, len(b))}"))


def _pair(node, name: str, value: float, unit: str) -> None:
    node.attrs.create(name, np.float64(value))
    _fix(node, name + ".unit", unit)


def _uuid() -> str:
    return str(uuid.uuid4())


def _iso(ns: int) -> str:
    dt = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=ns // 1000)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f") + "+00:00"


def synth_signal(n_series: int, n_time: int, rng: np.random.Generator) -> np.ndarray:
    """int16 stand-in for instantaneous GPU power (watts, scaled)."""
    t = np.arange(n_time, dtype=np.float32)
    base = rng.uniform(80, 250, size=(n_series, 1)).astype(np.float32)
    sig = base + rng.normal(0, 12, size=(n_series, n_time)).astype(np.float32)
    phase = rng.uniform(0, 2 * np.pi, size=(n_series, 1)).astype(np.float32)
    sig += 40.0 * np.sin(2 * np.pi * 2.0 * t / max(n_time, 1) + phase)
    info = np.iinfo(np.int16)
    return np.clip(np.rint(sig), info.min, info.max).astype(np.int16)


def write_analog(
    path: Path,
    *,
    n_series: int,
    n_time: int,
    seed: int = 1,
    values: np.ndarray | None = None,
    timestamps: np.ndarray | None = None,
    step_ns: int = 100_000_000,
    start_ns: int = 1_700_000_000_000_000_000,
) -> Path:
    rng = np.random.default_rng(seed)
    if values is None:
        values = synth_signal(n_series, n_time, rng)
    else:
        values = np.asarray(values)
        if values.ndim != 2:
            raise ValueError("values must be rank-2 (n_series, n_time)")
        n_series, n_time = int(values.shape[0]), int(values.shape[1])
    if timestamps is None:
        timestamps = start_ns + np.arange(n_time, dtype=np.int64) * step_ns
    else:
        timestamps = np.asarray(timestamps, dtype=np.int64)
        if timestamps.shape != (n_time,):
            raise ValueError("timestamps length must equal n_time")
        start_ns = int(timestamps[0]) if n_time else start_ns
        if n_time > 1:
            step_ns = int(timestamps[1] - timestamps[0])
    end_ns = int(timestamps[-1]) if n_time else start_ns
    path.parent.mkdir(parents=True, exist_ok=True)

    with h5py.File(path, "w", libver="latest") as f:
        _fix(f, "collection.uuid", _uuid())

        machine = f.create_group("Machine")
        _fix(machine, "rdfs.seeAlso", f"{SDG}HostMachine")
        _fix(machine, "skos.broader", f"{SDG}SYSML_STRUCTURE")
        _fix(machine, "collection.traceable_id", f"sdg://machine/{_uuid()}")
        _fix(machine, "collection.id", _uuid())
        _fix(machine, "collection.start.time", _iso(start_ns))
        _fix(machine, "schema.version", SCHEMA_VERSION)
        _pair(machine, "gpu.sm.clock", 1.41e9, "Hz")
        _pair(machine, "gpu.memory.clock", 1.001e9, "Hz")
        _pair(machine, "gpu.tdp", 400.0, "W")
        _pair(machine, "sample.rate.max", 10.0, "Hz")
        _pair(machine, "sample.rate.min", 0.0, "Hz")
        _pair(machine, "export.timeout", 5.0, "s")
        machine.attrs.create("series.count", np.int64(n_series))
        machine.attrs.create("start.series.index", np.int64(0))
        machine.attrs.create("values.are.delta", np.bool_(False))

        scope = machine.create_group("Scope")
        _fix(scope, "rdfs.seeAlso", f"{SDG}OtelInstrumentationScope")
        _fix(scope, "part.uuid", _uuid())
        scope.attrs.create("batch.max.size", np.int32(1024))
        scope.attrs.create("queue.capacity", np.int32(2048))
        scope.attrs.create("export.retry.count", np.int32(5))
        scope.attrs.create("sampling.ratio", np.float64(1.0))
        scope.attrs.create("compression.ratio", np.float64(2.0))
        scope.attrs.create("data.transposed", np.bool_(True))
        scope.attrs.create("values.relative", np.bool_(False))
        win_dt = np.dtype([("StartIndex", "<i4"), ("EndIndex", "<i4"), ("Stride", "<i4")])
        n_win = max(1, n_time // 32)
        edges = np.linspace(0, n_time, n_win + 1).astype(np.int32)
        windows = np.zeros(n_win, dtype=win_dt)
        windows["StartIndex"], windows["EndIndex"], windows["Stride"] = edges[:-1], edges[1:], 1
        scope.create_dataset("Windows", data=windows)

        gpu = machine.create_group("GpuDevice[0]")
        _fix(gpu, "rdfs.seeAlso", f"{SDG}GpuDevice")
        _fix(gpu, "skos.broader", f"{SDG}SYSML_STRUCTURE")
        _fix(gpu, "host.uuid", _uuid())
        _fix(gpu, "device.uuid", _uuid())
        anchor_dt = np.dtype(
            [("SeriesIndex", "<i8"), ("DeviceIndex", "<f8"), ("Offset", "<f8")]
        )
        for ai in range(2):
            port = gpu.create_group(f"PortAnchors[{ai}]")
            _fix(port, "reference.frame", "pci-bdf")
            _fix(port, "scope.note", "anchor-pair" if ai == 0 else "all-devices")
            npts = 2 if ai == 0 else n_series
            anchor = np.zeros(npts, dtype=anchor_dt)
            anchor["SeriesIndex"] = np.linspace(0, max(n_series - 1, 0), npts).astype(np.int64)
            anchor["DeviceIndex"] = np.linspace(0.0, float(max(n_series - 1, 0)), npts)
            anchor["Offset"] = 0.0
            port.create_dataset("SeriesAnchor", data=anchor)

        metric = machine.create_group("GpuMetric[0]")
        metric.attrs.create("series.count", np.int64(n_series))
        metric.attrs.create("start.series.index", np.int64(0))
        _pair(metric, "output.data.rate", 10.0, "Hz")
        _fix(metric, "value.unit", "W")
        _fix(metric, "metric.uuid", _uuid())

        values = metric.create_dataset("Values", data=np.asarray(values, dtype=np.int16))
        values.attrs.create("count", np.int64(n_series) * np.int64(n_time))
        values.attrs.create("start.index", np.int64(0))
        _fix(values, "dimensions", "gpu,time")
        _fix(values, "part.start.time", _iso(start_ns))
        _fix(values, "part.end.time", _iso(end_ns))

        ts = metric.create_dataset("Timestamps", data=np.asarray(timestamps, dtype=np.int64))
        ts.attrs.create("count", np.int64(n_time))
        ts.attrs.create("start.index", np.int64(0))
        _fix(ts, "part.start.time", _iso(start_ns))
        _fix(ts, "part.end.time", _iso(end_ns))
        _fix(ts, "start.time", _iso(start_ns))
        _fix(ts, "unit", "ns")

    return path


def count_tree(path: Path) -> dict[str, int]:
    n_groups = 1
    n_datasets = 0
    n_attrs = 0
    max_depth = 0
    with h5py.File(path, "r") as f:
        n_attrs += len(f.attrs)

        def walk(name: str, obj) -> None:
            nonlocal n_groups, n_datasets, n_attrs, max_depth
            depth = name.count("/") + 1
            max_depth = max(max_depth, depth)
            n_attrs += len(obj.attrs)
            if isinstance(obj, h5py.Group):
                n_groups += 1
            else:
                n_datasets += 1

        f.visititems(walk)
    return {
        "groups": n_groups,
        "datasets": n_datasets,
        "attrs": n_attrs,
        "depth": max_depth,
    }


def write_warehouse_analog(path: Path, samples: list[dict]) -> Path:
    """Pack Kudu-grain GPU rows into the SysML analog (Values = watts int16).

    ``samples`` items: ts_ns, gpu_index, power_w, util_pct, mem_used_mb, temp_c.
    Extra series rows 6..23 hold util/mem/temp so the fingerprint tree shape
    stays 7 groups / 5 datasets when n_gpu<=6 and we only write Values.
    """
    if not samples:
        raise ValueError("write_warehouse_analog: no samples")
    by_ts: dict[int, dict[int, dict]] = {}
    gpus: set[int] = set()
    for s in samples:
        ts = int(s["ts_ns"])
        gi = int(s["gpu_index"])
        gpus.add(gi)
        by_ts.setdefault(ts, {})[gi] = s
    times = sorted(by_ts)
    n_gpu = max(gpus) + 1
    n_time = len(times)
    # 4 measure planes stacked as series: power, util, mem, temp
    n_series = n_gpu * 4
    mat = np.zeros((n_series, n_time), dtype=np.int16)
    info = np.iinfo(np.int16)
    for t_i, ts in enumerate(times):
        for gi, s in by_ts[ts].items():
            mat[gi, t_i] = int(np.clip(round(float(s["power_w"])), info.min, info.max))
            mat[n_gpu + gi, t_i] = int(
                np.clip(round(float(s["util_pct"])), info.min, info.max)
            )
            mat[2 * n_gpu + gi, t_i] = int(
                np.clip(round(float(s["mem_used_mb"])), info.min, info.max)
            )
            mat[3 * n_gpu + gi, t_i] = int(
                np.clip(round(float(s["temp_c"])), info.min, info.max)
            )
    out = write_analog(
        path,
        n_series=n_series,
        n_time=n_time,
        values=mat,
        timestamps=np.asarray(times, dtype=np.int64),
    )
    with h5py.File(out, "a") as f:
        f["Machine/GpuMetric[0]/Values"].attrs.create(
            "warehouse.n_gpu", np.int64(n_gpu)
        )
    return out


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--n-series", type=int, default=8, help="GPU count (Values rows)")
    p.add_argument("--n-time", type=int, default=64)
    args = p.parse_args()
    out = write_analog(args.out, n_series=args.n_series, n_time=args.n_time)
    print(count_tree(out), out)


if __name__ == "__main__":
    main()
