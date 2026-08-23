"""Layout probe: SysML machine (GPU) analog first, then OTel ResourceMetrics."""

from __future__ import annotations

from typing import Any

GURU_NOIRI = "#SL.00000004.NOIRI"
GURU_LAYOUT = "#SL.00000005.LAYOUT"

MACHINE_METRIC = "Machine/GpuMetric[0]"
MACHINE_VALUES = "Machine/GpuMetric[0]/Values"


class LayoutError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code} {message}")
        self.code = code


def _decode(val: Any) -> str:
    if isinstance(val, bytes):
        return val.decode("utf-8", "replace").rstrip("\x00")
    return str(val)


def probe_metric_group(f: Any) -> tuple[str, Any]:
    """Return (kind, group that contains Values+Timestamps)."""
    if "Machine" in f:
        machine = f["Machine"]
        if "rdfs.seeAlso" not in machine.attrs:
            raise LayoutError(GURU_NOIRI, "Machine missing rdfs.seeAlso")
        if MACHINE_METRIC not in f or "Values" not in f[MACHINE_METRIC]:
            raise LayoutError(GURU_LAYOUT, "SysML machine tree missing GpuMetric[0]/Values")
        return "sdg_machine", f[MACHINE_METRIC]

    if "ResourceMetrics" in f:
        rm = f["ResourceMetrics"]
        for name in ("Metric[0]", "Metric_0"):
            if name in rm and "Values" in rm[name]:
                return "otel", rm[name]
        for name in rm:
            obj = rm[name]
            if hasattr(obj, "keys") and "Values" in obj:
                return "otel", obj
        raise LayoutError(GURU_LAYOUT, "OTel ResourceMetrics has no Values")

    raise LayoutError(
        GURU_LAYOUT,
        "neither SysML Machine/GpuMetric nor OTel ResourceMetrics",
    )


def see_also_iri(f: Any) -> str | None:
    if "Machine" in f and "rdfs.seeAlso" in f["Machine"].attrs:
        return _decode(f["Machine"].attrs["rdfs.seeAlso"])
    return None
