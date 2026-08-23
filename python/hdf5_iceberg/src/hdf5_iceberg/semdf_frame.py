"""Attach SemDF Arrow metadata to a Values hyperslab (Python/Dask handoff)."""

from __future__ import annotations

import numpy as np

from hdf5_iceberg.layout import probe_metric_group, see_also_iri

# Default analog DAS: intensive-style — SUM is illegal.
_DAS_AGG = ["AVG", "MIN", "MAX", "COUNT"]


def values_table(path: str, *, series_lo: int = 0, series_hi: int | None = None):
    """Return a pyarrow Table of `value` with org.zndx.semdf.* field metadata."""
    import h5py
    import pyarrow as pa

    from semdf import measure_metadata

    with h5py.File(path, "r") as f:
        kind, met = probe_metric_group(f)
        iri = see_also_iri(f) or "https://signals.zndx.org/sdg#ProdmlDasRawDataSet"
        vals = met["Values"]
        n = vals.shape[0]
        hi = n if series_hi is None else series_hi
        slab = np.asarray(vals[series_lo:hi, :])
    flat = slab.reshape(-1)
    meta = measure_metadata(
        measure_iri=iri,
        unit="counts",
        quantity_kind="intensive",
        grain="locus×time",
        aggregations=_DAS_AGG,
    )
    field = pa.field("value", pa.from_numpy_dtype(flat.dtype), metadata=meta)
    return pa.Table.from_arrays([flat], schema=pa.schema([field]))
