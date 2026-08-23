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
```
