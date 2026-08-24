# iceberg-hdf5 (JVM)

Pure-Java read of the SysML machine analog via [jhdf](https://github.com/jamesmudd/jhdf) `0.13.0` (no HDF Group C, no CISD JNI).

```text
/Machine/GpuMetric[0]/Values  →  nGpu × nTime int16
```

```bash
just java-test   # JDK 17+ (Signals devenv profile is 21)
```

`Hdf5GenericFormatModel` implements Iceberg `FormatModel<Record, Schema>` (`FileFormat.HDF5`, `.h5`). Analog read via `readBuilder` yields 512 generic records. `writeBuilder` fails `#SL.00000019.HDF5WRITE` (not a silent no-op).

Iceberg fork for this work is **`~/local/src/wxs/signals/components/iceberg`** (`rch/asf-iceberg`, branch `rch/devenv`). Do not use `cldr/signals` as the working tree.

`FormatModelRegistry.register` belongs on Impala's classpath (Parquet models load in the registry static init). From-source jhdf SHA vendor is follow-on.
