"""SemDF: Polarisfork semantics in Apache Arrow field metadata."""

from __future__ import annotations

import json
from typing import Any, Mapping

SCHEMA_VERSION = "org.zndx.semdf.version"
SCHEMA_CATALOG = "org.zndx.semdf.catalog"
MEASURE_IRI = "org.zndx.semdf.measure_iri"
UNIT = "org.zndx.semdf.unit"
QUANTITY_KIND = "org.zndx.semdf.quantity_kind"
GRAIN = "org.zndx.semdf.grain"
AGGREGATIONS = "org.zndx.semdf.aggregations"
JOIN_KEYS = "org.zndx.semdf.join_keys"
ROLE = "org.zndx.semdf.role"

GURU_NO_METADATA = "#SL.00000001.NOSEMDF"
GURU_ILLEGAL_AGG = "#SL.00000002.ILLEGALAGG"
GURU_POLYMORPHIC = "#SL.00000008.POLYVALUE"

VERSION = "1"


class SemdfError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code} {message}")
        self.code = code


def _b(s: str) -> bytes:
    return s.encode("utf-8")


def measure_metadata(
    *,
    measure_iri: str,
    unit: str,
    quantity_kind: str,
    grain: str,
    aggregations: list[str],
    join_keys: list[str] | None = None,
) -> dict[bytes, bytes]:
    meta: dict[bytes, bytes] = {
        _b(MEASURE_IRI): _b(measure_iri),
        _b(UNIT): _b(unit),
        _b(QUANTITY_KIND): _b(quantity_kind),
        _b(GRAIN): _b(grain),
        _b(AGGREGATIONS): _b(json.dumps([a.upper() for a in aggregations])),
    }
    if join_keys is not None:
        meta[_b(JOIN_KEYS)] = _b(json.dumps(join_keys))
    return meta


def _decode_meta(meta: Mapping[Any, Any] | None) -> dict[str, str]:
    if not meta:
        return {}
    out: dict[str, str] = {}
    for k, v in meta.items():
        key = k.decode("utf-8") if isinstance(k, (bytes, bytearray)) else str(k)
        val = v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else str(v)
        out[key] = val
    return out


def check_aggregation(field_meta: Mapping[Any, Any] | None, op: str) -> None:
    """COUNT and projection are always legal. Other ops must be in aggregations."""
    op_u = op.upper()
    if op_u in {"COUNT", "COUNT_STAR", "PROJECT"}:
        return
    decoded = _decode_meta(field_meta)
    if not decoded:
        raise SemdfError(GURU_NO_METADATA, "column has no SemDF field metadata")
    if MEASURE_IRI not in decoded:
        raise SemdfError(GURU_POLYMORPHIC, "polymorphic value column without measure binding")
    raw = decoded.get(AGGREGATIONS)
    if raw is None:
        raise SemdfError(GURU_ILLEGAL_AGG, f"aggregation {op_u} is not legal for this measure")
    try:
        allowed = [str(x).upper() for x in json.loads(raw)]
    except json.JSONDecodeError as e:
        raise SemdfError(GURU_ILLEGAL_AGG, f"aggregations metadata is not JSON: {e}") from e
    if op_u not in allowed:
        raise SemdfError(GURU_ILLEGAL_AGG, f"aggregation {op_u} is not legal for this measure")
