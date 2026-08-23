#!/usr/bin/env python3
"""SysML v2 / SDG-grounded HDF5 analog (PRODML-DAS tree shape).

Cardinality must match the design fingerprint: depth 4, 7 groups, 5 datasets,
56 attributes. Group names are SysML/SDG, not OTel. SHA is not compared to
the OTel analog.
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

SCHEMA_VERSION = "sdg-sysml-hdf5/1.0"
SDG = "https://signals.zndx.org/sdg#"

# Design K20: 8 MiB series-blocks on int16 Values. CI analog is small; the
# formula is still recorded so later files share the same grid.
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
    t = np.arange(n_time, dtype=np.float32)
    base = rng.uniform(40, 120, size=(n_series, 1)).astype(np.float32)
    sig = base + rng.normal(0, 8, size=(n_series, n_time)).astype(np.float32)
    phase = rng.uniform(0, 2 * np.pi, size=(n_series, 1)).astype(np.float32)
    sig += 15.0 * np.sin(2 * np.pi * 3.0 * t / max(n_time, 1) + phase)
    info = np.iinfo(np.int16)
    return np.clip(np.rint(sig), info.min, info.max).astype(np.int16)


def write_analog(path: Path, *, n_series: int, n_time: int, seed: int = 1) -> Path:
    rng = np.random.default_rng(seed)
    step_ns = 10_000_000
    start_ns = 1_700_000_000_000_000_000
    end_ns = start_ns + n_time * step_ns
    path.parent.mkdir(parents=True, exist_ok=True)

    with h5py.File(path, "w", libver="latest") as f:
        _fix(f, "collection.uuid", _uuid())

        sysg = f.create_group("AcquisitionSystem")
        _fix(sysg, "rdfs.seeAlso", f"{SDG}ProdmlDasAcquisition")
        _fix(sysg, "skos.broader", f"{SDG}PRODML_DISTRIBUTED_SENSING")
        _fix(sysg, "collection.traceable_id", f"sdg://acquisition/{_uuid()}")
        _fix(sysg, "collection.id", _uuid())
        _fix(sysg, "collection.start.time", _iso(start_ns))
        _fix(sysg, "schema.version", SCHEMA_VERSION)
        _pair(sysg, "interrogator.pulse.rate", 10000.0, "Hz")
        _pair(sysg, "gauge.length", 10.0, "m")
        _pair(sysg, "spatial.resolution", 1.0, "m")
        _pair(sysg, "sample.rate.max", 100.0, "Hz")
        _pair(sysg, "sample.rate.min", 0.0, "Hz")
        _pair(sysg, "export.timeout", 5.0, "s")
        sysg.attrs.create("series.count", np.int64(n_series))
        sysg.attrs.create("start.series.index", np.int64(0))
        sysg.attrs.create("values.are.delta", np.bool_(False))

        interrogator = sysg.create_group("Interrogator")
        _fix(interrogator, "rdfs.seeAlso", f"{SDG}SysmlPartUsage")
        _fix(interrogator, "part.uuid", _uuid())
        interrogator.attrs.create("batch.max.size", np.int32(1024))
        interrogator.attrs.create("queue.capacity", np.int32(2048))
        interrogator.attrs.create("export.retry.count", np.int32(5))
        interrogator.attrs.create("sampling.ratio", np.float64(1.0))
        interrogator.attrs.create("compression.ratio", np.float64(2.0))
        interrogator.attrs.create("data.transposed", np.bool_(True))
        interrogator.attrs.create("values.relative", np.bool_(False))
        win_dt = np.dtype([("StartIndex", "<i4"), ("EndIndex", "<i4"), ("Stride", "<i4")])
        n_win = max(1, n_time // 32)
        edges = np.linspace(0, n_time, n_win + 1).astype(np.int32)
        windows = np.zeros(n_win, dtype=win_dt)
        windows["StartIndex"], windows["EndIndex"], windows["Stride"] = edges[:-1], edges[1:], 1
        interrogator.create_dataset("Windows", data=windows)

        fiber = sysg.create_group("FiberOpticalPath[0]")
        _fix(fiber, "rdfs.seeAlso", f"{SDG}ProdmlFiberOpticalPath")
        _fix(fiber, "skos.broader", f"{SDG}SYSML_STRUCTURE")
        _fix(fiber, "well.uuid", _uuid())
        _fix(fiber, "path.uuid", _uuid())
        anchor_dt = np.dtype(
            [("SeriesIndex", "<i8"), ("MeasuredDepth", "<f8"), ("Offset", "<f8")]
        )
        for ai in range(2):
            loc = fiber.create_group(f"LocusAnchors[{ai}]")
            _fix(loc, "reference.frame", "well-md")
            _fix(loc, "scope.note", "anchor-pair" if ai == 0 else "all-loci")
            npts = 2 if ai == 0 else n_series
            anchor = np.zeros(npts, dtype=anchor_dt)
            anchor["SeriesIndex"] = np.linspace(0, max(n_series - 1, 0), npts).astype(np.int64)
            anchor["MeasuredDepth"] = np.linspace(0.0, float(n_series), npts)
            anchor["Offset"] = 0.0
            loc.create_dataset("SeriesAnchor", data=anchor)

        das = sysg.create_group("DasRaw[0]")
        das.attrs.create("series.count", np.int64(n_series))
        das.attrs.create("start.series.index", np.int64(0))
        _pair(das, "output.data.rate", 100.0, "Hz")
        _fix(das, "value.unit", "counts")
        _fix(das, "das.uuid", _uuid())

        values = das.create_dataset("Values", data=synth_signal(n_series, n_time, rng))
        values.attrs.create("count", np.int64(n_series) * np.int64(n_time))
        values.attrs.create("start.index", np.int64(0))
        _fix(values, "dimensions", "locus,time")
        _fix(values, "part.start.time", _iso(start_ns))
        _fix(values, "part.end.time", _iso(end_ns))

        ts = das.create_dataset(
            "Timestamps",
            data=(start_ns + np.arange(n_time, dtype=np.int64) * step_ns),
        )
        ts.attrs.create("count", np.int64(n_time))
        ts.attrs.create("start.index", np.int64(0))
        _fix(ts, "part.start.time", _iso(start_ns))
        _fix(ts, "part.end.time", _iso(end_ns))
        _fix(ts, "start.time", _iso(start_ns))
        _fix(ts, "unit", "ns")

    return path


def count_tree(path: Path) -> dict[str, int]:
    import h5py

    n_groups = 1  # root
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


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--n-series", type=int, default=8)
    p.add_argument("--n-time", type=int, default=64)
    args = p.parse_args()
    out = write_analog(args.out, n_series=args.n_series, n_time=args.n_time)
    print(count_tree(out), out)


if __name__ == "__main__":
    main()
