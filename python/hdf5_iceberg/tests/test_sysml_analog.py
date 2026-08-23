from __future__ import annotations

from pathlib import Path

import pytest

from hdf5_iceberg.audit import audit_local_hdf5
from hdf5_iceberg.layout import GURU_LAYOUT, GURU_NOIRI, LayoutError, probe_metric_group
from hdf5_iceberg.semdf_frame import values_table

FIXTURE = Path(__file__).resolve().parents[3] / "analog" / "fixtures" / "sdg_sysml_small.h5"


def test_audit_sysml_fixture() -> None:
    assert FIXTURE.exists(), FIXTURE
    desc = audit_local_hdf5(str(FIXTURE))
    assert desc.n_series == 8
    assert desc.n_time == 64
    assert desc.dtype == "int16"
    assert desc.tags.get("layout_kind") == "sdg_sysml"
    assert "ProdmlDasAcquisition" in (desc.tags.get("seeAlso") or "")


def test_missing_see_also(tmp_path: Path) -> None:
    import h5py

    dest = tmp_path / "noiri.h5"
    dest.write_bytes(FIXTURE.read_bytes())
    with h5py.File(dest, "a") as f:
        del f["AcquisitionSystem"].attrs["rdfs.seeAlso"]
    with pytest.raises(LayoutError) as ei:
        audit_local_hdf5(str(dest))
    assert ei.value.code == GURU_NOIRI


def test_unknown_layout(tmp_path: Path) -> None:
    import h5py

    p = tmp_path / "empty.h5"
    with h5py.File(p, "w") as f:
        f.create_group("Nope")
    with pytest.raises(LayoutError) as ei:
        with h5py.File(p, "r") as f:
            probe_metric_group(f)
    assert ei.value.code == GURU_LAYOUT


def test_semdf_sum_refused() -> None:
    from semdf import GURU_ILLEGAL_AGG, SemdfError, check_aggregation

    table = values_table(str(FIXTURE))
    meta = table.schema.field("value").metadata
    check_aggregation(meta, "AVG")
    with pytest.raises(SemdfError) as ei:
        check_aggregation(meta, "SUM")
    assert ei.value.code == GURU_ILLEGAL_AGG
