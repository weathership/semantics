"""Layout probe: SysML/SDG analog first, then OTel. Gaps fail with guru codes."""

from __future__ import annotations

from typing import Any

GURU_NOIRI = "#SL.00000004.NOIRI"
GURU_LAYOUT = "#SL.00000005.LAYOUT"

SYSML_DAS = "AcquisitionSystem/DasRaw[0]"
SYSML_VALUES = "AcquisitionSystem/DasRaw[0]/Values"


class LayoutError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code} {message}")
        self.code = code


def _decode(val: Any) -> str:
    if isinstance(val, bytes):
        return val.decode("utf-8", "replace").rstrip("\x00")
    return str(val)


def probe_metric_group(f: Any) -> tuple[str, Any]:
    """Return (kind, group_that_contains Values+Timestamps)."""
    if "AcquisitionSystem" in f:
        sysg = f["AcquisitionSystem"]
        if "rdfs.seeAlso" not in sysg.attrs:
            raise LayoutError(GURU_NOIRI, "AcquisitionSystem missing rdfs.seeAlso")
        if SYSML_DAS not in f or "Values" not in f[SYSML_DAS]:
            raise LayoutError(GURU_LAYOUT, "SysML tree missing DasRaw[0]/Values")
        return "sdg_sysml", f[SYSML_DAS]

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

    raise LayoutError(GURU_LAYOUT, "neither SysML AcquisitionSystem nor OTel ResourceMetrics")


def see_also_iri(f: Any) -> str | None:
    if "AcquisitionSystem" in f and "rdfs.seeAlso" in f["AcquisitionSystem"].attrs:
        return _decode(f["AcquisitionSystem"].attrs["rdfs.seeAlso"])
    return None
