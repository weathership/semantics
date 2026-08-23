# hdf5_iceberg

Python HDF5 Iceberg library (complete-path home: `weathership/semantics`).

Layout probe: **SysML `Machine/GpuMetric[0]/Values` first**, then OTel `ResourceMetrics`, else `#SL.00000005`. Missing `rdfs.seeAlso` on `Machine` → `#SL.00000004`.

External Layer A URIs stay GET-only. Warehouse-owned HDF5 **write** is in this program (not a Non-Goal). SemDF attach: `hdf5_iceberg.semdf_frame.values_table`.

Standalone **isolated Iceberg-oriented metadata plane** (pointer tables) plus dataset access:

```python
from hdf5_iceberg import DatasetProvider, MetadataProvider, register_root

data = DatasetProvider(roots=["s3://lab-data/datasets/hdf5/"], readonly=True)
meta = MetadataProvider(
    warehouse="s3://cyberphy-md/iceberg/warehouse",
    endpoint_url="http://127.0.0.1:9010",
)
result = register_root(data, meta, adapter="product_prefix")
print(result.n_registered, result.n_reused)
```

## Design laws

1. **Dataset plane is Layer A** — never mutated; GET only.
2. **Metadata plane is Layer B** — all writes under a warehouse prefix you choose (e.g. `cyberphy-md`).
3. **Layout adapters** discover candidates; **file audit** supplies geometry/time (path tags are soft).
4. **Semantic stubs** emit **Turtle + SHACL-Core** (and optional Manchester), not JSON-LD — kvasir-friendly.

## Install

```bash
# monorepo
uv pip install -e packages/hdf5_iceberg

# air-gap image: tree copied to PYTHONPATH as top-level `hdf5_iceberg`
```

## Public API

| Symbol | Role |
|--------|------|
| `DatasetProvider` | RO roots + adapter; list/audit descriptors |
| `MetadataProvider` | RW warehouse; pointer-table parquet (+ optional PyIceberg) |
| `register_root` | scan → audit → idempotent register |

No `cybersec` / `cyberphy` import path.
