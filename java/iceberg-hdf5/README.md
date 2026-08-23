# iceberg-hdf5 (JVM)

Pure-Java read of the SysML machine analog via [jhdf](https://github.com/jamesmudd/jhdf) `0.13.0` (no HDF Group C, no CISD JNI).

```text
/Machine/GpuMetric[0]/Values  →  nGpu × nTime int16
```

```bash
just java-test   # JDK 17+ (Signals devenv profile is 21)
```

`FormatModel` registration on Impala `impala-iceberg-runtime` is the next Java slice. From-source jhdf SHA vendor is also follow-on (this pin is Maven Central 0.13.0).
