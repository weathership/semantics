# SysML machine analog (OpenTelemetry + GPU parts)

Shape is the cybersec HDF5 fingerprint: **7 groups / 5 datasets / 56 attrs**, depth 4.
Names are **not** PRODML/DAS. The tree is a SysML **Machine** (host) with **GpuDevice**
parts and an OTel **Scope**. IRIs live in `ontology/scratch/sdg-machine.ttl`
(`sdg:HostMachine`, `sdg:GpuDevice`, `sdg:GpuPower`) under SYSML_STRUCTURE.

Cybersec files that still use `ResourceMetrics` remain a valid **OTel** layout in
`hdf5_iceberg.layout`. WITSML/PRODML stay in SDG for other sources.

```bash
just analog
just test
just hdf5-df-bench          # local many-file: hdf5-pure+DataFusion vs h5py
just hdf5-df-bench-rustfs   # same objects under s3://signals-dataproducts/iceberg/bench/...
```

`hdf5-df-bench --target rustfs` **fails** with `#SL.00000020.NORUSTFS` if Signals RustFS `:9010` is down (no skip). Objects land under `s3://signals-dataproducts/iceberg/bench/hdf5-df/`; the timed scan is GET from RustFS then hdf5-pure. Polarisfork is not required for this reader bench.

The 8×64 analog is ~11 KiB/file — h5py often wins that regime (C startup vs DataFusion). The comparison that matters is **many Iceberg objects** and **K20 8 MiB series-blocks**; bump `--n-files` / analog size before claiming a win.
