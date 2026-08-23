# semantics

Scientific semantic layer for Signals projects: **SemDF** (Arrow field metadata)
and **HDF5 Iceberg** (Python, JVM, Rust) with SDG/SysML bindings.

This is **not** a warehouse SQL engine (that is Impala + `impala_fdw`).
This is **not** Metabase projection (`mbengine` consumes SemDF).
This is **not** Gaius Engine `/sci`.

| Path | Role |
|------|------|
| `crates/semdf` | Rust SemDF keys + illegal-aggregation checks |
| `python/semdf` | Python/pyarrow SemDF |
| `python/hdf5_iceberg` | Complete HDF5 library (wave 1+) |
| `crates/hdf5-df` | DataFusion TableProvider (wave 2) |
| `java/iceberg-hdf5` | Iceberg `FormatModel` (wave 2) |
| `analog/` | SysML **machine** HDF5 analog (host + GPU + OTel scope; 7/5/56) |
| `ontology/scratch/` | SDG TTL for HostMachine / GpuDevice (Aegir-gated corpora later) |

```bash
just semantic-ci
```

Apache-2.0. Origin: `git@github.com:weathership/semantics.git`.
