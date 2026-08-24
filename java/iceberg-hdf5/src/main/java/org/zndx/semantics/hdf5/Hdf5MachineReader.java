package org.zndx.semantics.hdf5;

import io.jhdf.HdfFile;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Reads the SysML machine analog ({@code /Machine/GpuMetric[0]/Values}) with
 * jamesmudd jhdf (pure Java, no HDF Group C / CISD JNI).
 *
 * <p>Iceberg {@code FormatModel} registration is the next slice; this class is
 * the JVM analog of Python {@code hdf5_iceberg} and Rust {@code hdf5-df}.
 */
public final class Hdf5MachineReader {

  public static final String GURU_JHDF = "#SL.00000021.JHDF";
  public static final String MACHINE = "/Machine";
  public static final String VALUES = "/Machine/GpuMetric[0]/Values";

  private Hdf5MachineReader() {}

  public static final class Window {
    public final int nGpu;
    public final int nTime;
    public final short[] values; // row-major nGpu × nTime
    public final long[] timestamps; // nTime, 0 if analog has no Timestamps

    Window(int nGpu, int nTime, short[] values) {
      this(nGpu, nTime, values, new long[nTime]);
    }

    Window(int nGpu, int nTime, short[] values, long[] timestamps) {
      this.nGpu = nGpu;
      this.nTime = nTime;
      this.values = values;
      this.timestamps = timestamps == null ? new long[nTime] : timestamps;
    }

    public int size() {
      return nGpu * nTime;
    }

    public long timestampAt(int t) {
      if (t < 0 || t >= timestamps.length) {
        return 0L;
      }
      return timestamps[t];
    }
  }

  public static Window readValues(Path path) {
    Objects.requireNonNull(path, "path");
    try (HdfFile file = new HdfFile(path.toFile())) {
      Node machine = file.getChild("Machine");
      if (!(machine instanceof Group)) {
        throw guru("missing Machine group");
      }
      Dataset ds = file.getDatasetByPath(VALUES);
      if (ds == null) {
        throw guru("missing " + VALUES);
      }
      int[] dims = ds.getDimensions();
      if (dims.length != 2) {
        throw guru("Values rank " + dims.length + " want 2");
      }
      Object raw = ds.getData();
      short[] values = flatten(raw, dims[0], dims[1]);
      long[] timestamps = new long[dims[1]];
      try {
        Dataset ts = file.getDatasetByPath("/Machine/GpuMetric[0]/Timestamps");
        if (ts != null) {
          Object traw = ts.getData();
          if (traw instanceof long[]) {
            long[] src = (long[]) traw;
            System.arraycopy(src, 0, timestamps, 0, Math.min(src.length, timestamps.length));
          } else if (traw instanceof long[][]) {
            long[][] src = (long[][]) traw;
            int n = Math.min(src.length > 0 ? src[0].length : 0, timestamps.length);
            if (src.length > 0) {
              System.arraycopy(src[0], 0, timestamps, 0, n);
            }
          }
        }
      } catch (RuntimeException ignored) {
        // analog fixtures without Timestamps keep zeros
      }
      return new Window(dims[0], dims[1], values, timestamps);
    } catch (RuntimeException e) {
      if (e.getMessage() != null && e.getMessage().startsWith("#SL.")) {
        throw e;
      }
      throw guru(e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
    }
  }

  private static short[] flatten(Object raw, int nGpu, int nTime) {
    int n = nGpu * nTime;
    short[] out = new short[n];
    if (raw instanceof short[][]) {
      short[][] m = (short[][]) raw;
      int i = 0;
      for (short[] row : m) {
        System.arraycopy(row, 0, out, i, row.length);
        i += row.length;
      }
      if (i != n) {
        throw guru("short[][] size " + i + " want " + n);
      }
      return out;
    }
    if (raw instanceof int[][]) {
      int[][] m = (int[][]) raw;
      int i = 0;
      for (int[] row : m) {
        for (int v : row) {
          out[i++] = (short) v;
        }
      }
      return out;
    }
    if (raw instanceof short[]) {
      short[] m = (short[]) raw;
      if (m.length != n) {
        throw guru("short[] len " + m.length + " want " + n);
      }
      return m;
    }
    throw guru("unsupported Values Java type " + raw.getClass().getName());
  }

  private static IllegalStateException guru(String message) {
    return new IllegalStateException(GURU_JHDF + " " + message);
  }
}
