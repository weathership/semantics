"""Signals tier1 HDF5 layout (schema_version 2): writer and probe.

Guru: #SL.00000029.HDF5SIGNAL

    /signal  @schema_version=2 @epoch_hour @ts_min @ts_max @series_min @series_max @n_rows
      /int | /dec
        ts        int64 [T]      sorted, unique         (predicate index)
        series_id int64 [S]      sorted                 (predicate index)
        src int8[S]  gpu int8[S] gpu_null uint8[S]  inst int16[S] inst_null uint8[S]
        values    int64 [S][T]   dec: UNSCALED at @scale (same representation as Kudu)
        present   uint8 [S][T]   a gap is stated, never imputed

Integer sources stay integers (no float widening). Decimals are stored as their
unscaled integer plus ``@scale`` -- bit-for-bit what Kudu holds, so tier0 and
tier1 round-trip exactly. Chunking is one series-hour per chunk so a time
window read touches only the chunks it needs; shuffle+gzip does well on
slowly varying fixed-point telemetry because the high bytes are near constant.
"""

from __future__ import annotations

import argparse
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

import h5py
import numpy as np

SCHEMA_VERSION = 2
ROOT = "signal"
DEC_SCALE = 6  # DECIMAL(18,6) in signal_tier0/tier1

_CHUNK_T = 3600
# A lane is one signal_tier0 primary-key stream: (series_id, gpu, inst). DCGM
# writes all N GPUs of a metric at one ts under one series_id, so keying a lane
# by series_id alone collapsed those N rows onto one cell and dropped N-1 of them
# (the same 5-of-6-GPU loss the Kudu PK fix cured on the write side). Null gpu/inst
# get a reserved key sentinel so a "lacks gpu" lane stays distinct and sortable.
_NULL_KEY = -(2**31)


def _chunks(n_s: int, n_t: int) -> tuple[int, int]:
    return (1, max(1, min(_CHUNK_T, n_t)))


def _lane_key(r: dict[str, Any]) -> tuple[int, int, int]:
    """(series_id, gpu, inst) identity of a row's lane; null gpu/inst -> sentinel."""
    g = r.get("gpu")
    i = r.get("inst")
    return (
        int(r["series_id"]),
        int(g) if g is not None else _NULL_KEY,
        int(i) if i is not None else _NULL_KEY,
    )


def _plane(
    grp: h5py.Group,
    rows: list[dict[str, Any]],
    *,
    dec: bool,
) -> tuple[int, int]:
    """Write one plane (int or dec). Returns (n_series, n_time).

    One row per lane (series_id, gpu, inst) x time cell. Lanes are sorted by the
    full key, so series_id stays non-decreasing (with duplicates for a multi-GPU
    metric) and the reader's series_id bounds/binary search still hold.
    """
    ts_set: set[int] = set()
    lane_set: set[tuple[int, int, int]] = set()
    for r in rows:
        ts_set.add(int(r["ts_ns"]))
        lane_set.add(_lane_key(r))
    ts = np.array(sorted(ts_set), dtype=np.int64)
    lanes = sorted(lane_set)  # by (series_id, gpu, inst) -> series_id non-decreasing
    t_idx = {int(t): i for i, t in enumerate(ts)}
    s_idx = {lane: i for i, lane in enumerate(lanes)}
    n_s, n_t = len(lanes), len(ts)

    values = np.zeros((n_s, n_t), dtype=np.int64)
    present = np.zeros((n_s, n_t), dtype=np.uint8)
    src = np.zeros(n_s, dtype=np.int8)
    sids = np.array([lane[0] for lane in lanes], dtype=np.int64)
    gpu = np.zeros(n_s, dtype=np.int8)
    gpu_null = np.ones(n_s, dtype=np.uint8)
    inst = np.zeros(n_s, dtype=np.int16)
    inst_null = np.ones(n_s, dtype=np.uint8)
    for k, (_sid, gk, ik) in enumerate(lanes):
        if gk != _NULL_KEY:
            gpu[k] = gk
            gpu_null[k] = 0
        if ik != _NULL_KEY:
            inst[k] = ik
            inst_null[k] = 0

    for r in rows:
        i = s_idx[_lane_key(r)]
        j = t_idx[int(r["ts_ns"])]
        if dec:
            d = r["val_d"]
            d = d if isinstance(d, Decimal) else Decimal(str(d))
            q = d.scaleb(DEC_SCALE)
            if q != q.to_integral_value():
                raise ValueError(
                    f"#SL.00000029.HDF5SIGNAL val_d {d} has more than {DEC_SCALE} "
                    "decimal places; ingest must normalise to DECIMAL(18,6)"
                )
            values[i, j] = int(q)
        else:
            values[i, j] = int(r["val_i"])
        present[i, j] = 1
        src[i] = int(r["src"])  # constant within a lane (a series_id's source)

    grp.create_dataset("ts", data=ts)
    grp.create_dataset("series_id", data=sids)
    grp.create_dataset("src", data=src)
    grp.create_dataset("gpu", data=gpu)
    grp.create_dataset("gpu_null", data=gpu_null)
    grp.create_dataset("inst", data=inst)
    grp.create_dataset("inst_null", data=inst_null)
    ch = _chunks(n_s, n_t)
    v = grp.create_dataset(
        "values", data=values, chunks=ch, shuffle=True, compression="gzip", compression_opts=4
    )
    if dec:
        v.attrs.create("scale", np.int32(DEC_SCALE))
    grp.create_dataset(
        "present", data=present, chunks=ch, shuffle=True, compression="gzip", compression_opts=4
    )
    grp.attrs.create("n_series", np.int64(n_s))
    grp.attrs.create("n_time", np.int64(n_t))
    return n_s, n_t


def write_signal_hdf5(path: Path, rows: Iterable[dict[str, Any]], *, epoch_hour: int) -> Path:
    """Write one settled hour of signal_tier0 rows.

    Row keys: ts_ns, series_id, src, gpu, inst, val_i, val_d (exactly one of
    val_i / val_d non-None per row, per the series' vtype).
    """
    rows = list(rows)
    if not rows:
        raise ValueError("#SL.00000029.HDF5SIGNAL no rows to write")
    ints = [r for r in rows if r.get("val_i") is not None]
    decs = [r for r in rows if r.get("val_d") is not None]
    if len(ints) + len(decs) != len(rows):
        raise ValueError(
            "#SL.00000029.HDF5SIGNAL a row must carry exactly one of val_i / val_d"
        )
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    ts_all = [int(r["ts_ns"]) for r in rows]
    sid_all = [int(r["series_id"]) for r in rows]
    with h5py.File(path, "w") as f:
        sig = f.create_group(ROOT)
        sig.attrs.create("schema_version", np.int32(SCHEMA_VERSION))
        sig.attrs.create("epoch_hour", np.int32(epoch_hour))
        sig.attrs.create("ts_min", np.int64(min(ts_all)))
        sig.attrs.create("ts_max", np.int64(max(ts_all)))
        sig.attrs.create("series_min", np.int64(min(sid_all)))
        sig.attrs.create("series_max", np.int64(max(sid_all)))
        sig.attrs.create("n_rows", np.int64(len(rows)))
        if ints:
            _plane(sig.create_group("int"), ints, dec=False)
        if decs:
            _plane(sig.create_group("dec"), decs, dec=True)
    return path


def read_bounds(path: Path) -> dict[str, int]:
    """File-level bounds for Iceberg manifest metrics."""
    with h5py.File(path, "r") as f:
        a = f[ROOT].attrs
        return {
            "epoch_hour": int(a["epoch_hour"]),
            "ts_min": int(a["ts_min"]),
            "ts_max": int(a["ts_max"]),
            "series_min": int(a["series_min"]),
            "series_max": int(a["series_max"]),
            "n_rows": int(a["n_rows"]),
        }


def is_signal_layout(f: Any) -> bool:
    return ROOT in f and int(f[ROOT].attrs.get("schema_version", 0)) == SCHEMA_VERSION


def _demo_rows(epoch_hour: int, n_t: int) -> list[dict[str, Any]]:
    """Deterministic fixture: 2 int series + 1 dec series, with a gap."""
    base = epoch_hour * 3600 * 1_000_000_000
    out: list[dict[str, Any]] = []
    for k in range(n_t):
        ts = base + k * 1_000_000_000
        out.append({"ts_ns": ts, "series_id": 101, "src": 0, "gpu": 0, "inst": None,
                    "val_i": 40 + k, "val_d": None})
        if k % 7 != 3:  # gap every 7th sample on series 102
            out.append({"ts_ns": ts, "series_id": 102, "src": 0, "gpu": 1, "inst": None,
                        "val_i": 1000 + 3 * k, "val_d": None})
        out.append({"ts_ns": ts, "series_id": 201, "src": 1, "gpu": None, "inst": 8081,
                    "val_i": None, "val_d": Decimal(k) / Decimal(16)})
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--demo", type=Path, help="write a deterministic fixture here")
    p.add_argument("--epoch-hour", type=int, default=496560)
    p.add_argument("--n-time", type=int, default=64)
    a = p.parse_args(argv)
    if a.demo:
        out = write_signal_hdf5(a.demo, _demo_rows(a.epoch_hour, a.n_time), epoch_hour=a.epoch_hour)
        print(out, read_bounds(out))
        return 0
    p.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
