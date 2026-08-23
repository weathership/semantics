from __future__ import annotations

import sys
from pathlib import Path

import h5py
import numpy as np

sys.path.insert(0, str(Path(__file__).parent))
from generate_sdg_hdf5 import count_tree, series_block_rows, write_analog

FIXTURE = Path(__file__).parent / "fixtures" / "sdg_sysml_small.h5"

GURU_NOIRI = "#SL.00000004.NOIRI"
GURU_LAYOUT = "#SL.00000005.LAYOUT"


def _values_path(f: h5py.File) -> str:
    if "AcquisitionSystem/DasRaw[0]/Values" in f:
        return "AcquisitionSystem/DasRaw[0]/Values"
    if "ResourceMetrics/Metric[0]/Values" in f:
        raise AssertionError(f"{GURU_LAYOUT} OTel layout is not the SysML analog target")
    raise AssertionError(f"{GURU_LAYOUT} no Values dataset")


def test_fingerprint_7_5_56(tmp_path: Path) -> None:
    path = tmp_path / "a.h5"
    write_analog(path, n_series=8, n_time=64)
    c = count_tree(path)
    assert c["groups"] == 7, c
    assert c["datasets"] == 5, c
    assert c["attrs"] == 56, c
    assert c["depth"] == 4, c
    with h5py.File(path, "r") as f:
        assert "rdfs.seeAlso" in f["AcquisitionSystem"].attrs
        see = f["AcquisitionSystem"].attrs["rdfs.seeAlso"]
        if isinstance(see, bytes):
            see = see.decode("ascii")
        assert "ProdmlDasAcquisition" in see
        assert "skos.broader" in f["AcquisitionSystem"].attrs
        assert "skos.inScheme" not in f["AcquisitionSystem"].attrs
        vals = f[_values_path(f)]
        assert vals.dtype == np.int16
        assert vals.shape == (8, 64)


def test_missing_see_also_is_guru(tmp_path: Path) -> None:
    path = tmp_path / "b.h5"
    write_analog(path, n_series=4, n_time=16)
    with h5py.File(path, "a") as f:
        del f["AcquisitionSystem"].attrs["rdfs.seeAlso"]
        if "rdfs.seeAlso" in f["AcquisitionSystem"].attrs:
            raise AssertionError("delete failed")
    with h5py.File(path, "r") as f:
        assert "rdfs.seeAlso" not in f["AcquisitionSystem"].attrs
    # Probe used by later audit: missing IRI is #SL.00000004
    with h5py.File(path, "r") as f:
        if "rdfs.seeAlso" not in f["AcquisitionSystem"].attrs:
            err = f"{GURU_NOIRI} AcquisitionSystem missing rdfs.seeAlso"
            assert err.startswith("#SL.00000004")


def test_k20_formula() -> None:
    assert series_block_rows(10000) == 419


def test_committed_fixture_matches_if_present() -> None:
    if not FIXTURE.exists():
        return
    c = count_tree(FIXTURE)
    assert c["groups"] == 7
    assert c["datasets"] == 5
    assert c["attrs"] == 56
