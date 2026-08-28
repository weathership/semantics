package org.zndx.semantics.hdf5;

import io.jhdf.HdfFile;
import io.jhdf.api.Attribute;
import io.jhdf.api.Dataset;
import io.jhdf.api.Group;
import io.jhdf.api.Node;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Reads the signals tier1 layout ({@code /signal}, {@code @schema_version = 2})
 * with jhdf, materialising only the rows a predicate needs.
 *
 * <pre>
 * /signal
 *   @schema_version = 2, @epoch_hour, @ts_min, @ts_max, @series_min, @series_max
 *   /int | /dec
 *     ts        int64 [T]       sorted, unique             (predicate index)
 *     series_id int64 [S]       sorted                     (predicate index)
 *     src int8[S], gpu int8[S], inst int16[S]              per-series scalars
 *     values    int64 [S][T]    (dec: unscaled, @scale)
 *     present   uint8 [S][T]    gaps stated, never imputed
 * </pre>
 *
 * <p>A {@code ts_ns} range becomes two binary searches on {@code ts} and one
 * hyperslab per selected series ({@code Dataset.getData(offset, shape)}); a
 * {@code series_id} set becomes a binary search on {@code series_id}. Nothing
 * outside the intersection is read from the file. Exactness is the caller's
 * residual evaluation on each emitted row.
 */
public final class Hdf5SignalReader {

  public static final String GURU = "#SL.00000029.HDF5SIGNAL";
  public static final String ROOT = "/signal";
  public static final int SCHEMA_VERSION = 2;

  private Hdf5SignalReader() {}

  /** One emitted sample. {@code valD} is null for int series and vice versa. */
  public static final class Row {
    public final long tsNs;
    public final long seriesId;
    public final byte src;
    public final Byte gpu;
    public final Short inst;
    public final Long valI;
    public final BigDecimal valD;

    Row(long tsNs, long seriesId, byte src, Byte gpu, Short inst, Long valI, BigDecimal valD) {
      this.tsNs = tsNs;
      this.seriesId = seriesId;
      this.src = src;
      this.gpu = gpu;
      this.inst = inst;
      this.valI = valI;
      this.valD = valD;
    }
  }

  /** Bounds a caller extracted from its predicate; null means unbounded. */
  public static final class Window {
    public Long tsLo; // inclusive
    public Long tsHi; // inclusive
    public Set<Long> seriesIds; // null = all
    public Integer epochHour; // equality, null = any

    public static Window all() {
      return new Window();
    }
  }

  /** True if the file carries the v2 signal layout. */
  public static boolean isSignalLayout(Path path) {
    try (HdfFile f = new HdfFile(path.toFile())) {
      Node n = f.getByPath(ROOT);
      if (!(n instanceof Group)) {
        return false;
      }
      Attribute v = n.getAttribute("schema_version");
      return v != null && toLong(v.getData()) == SCHEMA_VERSION;
    } catch (RuntimeException e) {
      return false;
    }
  }

  public static List<Row> read(Path path, Window w) {
    Objects.requireNonNull(path, "path");
    Objects.requireNonNull(w, "window");
    List<Row> out = new ArrayList<>();
    try (HdfFile f = new HdfFile(path.toFile())) {
      Node root = f.getByPath(ROOT);
      if (!(root instanceof Group)) {
        throw guru("missing " + ROOT);
      }
      Group sig = (Group) root;
      if (w.epochHour != null) {
        Attribute eh = sig.getAttribute("epoch_hour");
        if (eh != null && toLong(eh.getData()) != w.epochHour.longValue()) {
          return out; // this file is a different hour
        }
      }
      // File-level bounds: skip the whole file when the window cannot intersect.
      Attribute tsMin = sig.getAttribute("ts_min");
      Attribute tsMax = sig.getAttribute("ts_max");
      if (tsMin != null && tsMax != null) {
        long lo = toLong(tsMin.getData());
        long hi = toLong(tsMax.getData());
        if ((w.tsLo != null && w.tsLo > hi) || (w.tsHi != null && w.tsHi < lo)) {
          return out;
        }
      }
      readGroup(sig, "int", w, out, false);
      readGroup(sig, "dec", w, out, true);
    } catch (RuntimeException e) {
      if (e.getMessage() != null && e.getMessage().startsWith("#SL.")) {
        throw e;
      }
      throw guru(e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
    }
    return out;
  }

  private static void readGroup(Group sig, String name, Window w, List<Row> out, boolean dec) {
    Node n = sig.getChild(name);
    if (!(n instanceof Group)) {
      return; // a file may hold only one of the two planes
    }
    Group g = (Group) n;
    long[] ts = longs(dataset(g, "ts").getData());
    if (ts.length == 0) {
      return;
    }
    long[] series = longs(dataset(g, "series_id").getData());
    byte[] src = bytes(dataset(g, "src").getData());
    byte[] gpu = bytes(dataset(g, "gpu").getData());
    short[] inst = shorts(dataset(g, "inst").getData());
    byte[] gpuNull = optionalBytes(g, "gpu_null");
    byte[] instNull = optionalBytes(g, "inst_null");
    Dataset values = dataset(g, "values");
    Dataset present = dataset(g, "present");
    int scale = 0;
    if (dec) {
      Attribute sc = values.getAttribute("scale");
      if (sc == null) {
        throw guru("dec/values missing @scale");
      }
      scale = (int) toLong(sc.getData());
    }

    // Time window → contiguous [t0, t1] on the sorted ts vector.
    int t0 = 0;
    int t1 = ts.length - 1;
    if (w.tsLo != null) {
      t0 = lowerBound(ts, w.tsLo);
    }
    if (w.tsHi != null) {
      t1 = upperBoundInclusive(ts, w.tsHi);
    }
    if (t0 > t1) {
      return;
    }
    int nT = t1 - t0 + 1;

    // Series set → row indices on the sorted series_id vector.
    int[] rows;
    if (w.seriesIds == null) {
      rows = new int[series.length];
      for (int i = 0; i < rows.length; i++) {
        rows[i] = i;
      }
    } else {
      List<Integer> sel = new ArrayList<>();
      for (long sid : w.seriesIds) {
        int i = Arrays.binarySearch(series, sid);
        if (i < 0) {
          continue;
        }
        // series_id is sorted but NOT unique: one metric with N GPUs is N lanes
        // sharing a series_id (lanes keyed by (series_id,gpu,inst)). binarySearch
        // lands on one of them; select the whole contiguous run so a series
        // filter returns every GPU, not just one.
        int lo = i;
        int hi = i;
        while (lo > 0 && series[lo - 1] == sid) {
          lo--;
        }
        while (hi + 1 < series.length && series[hi + 1] == sid) {
          hi++;
        }
        for (int k = lo; k <= hi; k++) {
          sel.add(k);
        }
      }
      rows = sel.stream().mapToInt(Integer::intValue).toArray();
      Arrays.sort(rows);
    }
    if (rows.length == 0) {
      return;
    }

    for (int r : rows) {
      long[] vals = longs(values.getData(new long[] {r, t0}, new int[] {1, nT}));
      byte[] pres = bytes(present.getData(new long[] {r, t0}, new int[] {1, nT}));
      Byte gpuV = (gpuNull != null && gpuNull[r] != 0) ? null : Byte.valueOf(gpu[r]);
      Short instV = (instNull != null && instNull[r] != 0) ? null : Short.valueOf(inst[r]);
      for (int k = 0; k < nT; k++) {
        if (pres[k] == 0) {
          continue; // a gap stays a gap
        }
        long tsNs = ts[t0 + k];
        if (dec) {
          out.add(new Row(tsNs, series[r], src[r], gpuV, instV, null,
              BigDecimal.valueOf(vals[k], scale)));
        } else {
          out.add(new Row(tsNs, series[r], src[r], gpuV, instV, vals[k], null));
        }
      }
    }
  }

  // ---- helpers -------------------------------------------------------

  private static Dataset dataset(Group g, String name) {
    Node n = g.getChild(name);
    if (!(n instanceof Dataset)) {
      throw guru("missing dataset " + g.getPath() + "/" + name);
    }
    return (Dataset) n;
  }

  private static byte[] optionalBytes(Group g, String name) {
    Node n = g.getChild(name);
    return (n instanceof Dataset) ? bytes(((Dataset) n).getData()) : null;
  }

  /** First index with ts[i] >= key. */
  private static int lowerBound(long[] a, long key) {
    int lo = 0;
    int hi = a.length;
    while (lo < hi) {
      int mid = (lo + hi) >>> 1;
      if (a[mid] < key) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return lo;
  }

  /** Last index with ts[i] <= key, or -1. */
  private static int upperBoundInclusive(long[] a, long key) {
    int lo = 0;
    int hi = a.length;
    while (lo < hi) {
      int mid = (lo + hi) >>> 1;
      if (a[mid] <= key) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return lo - 1;
  }

  private static long toLong(Object o) {
    if (o instanceof Number) {
      return ((Number) o).longValue();
    }
    if (o instanceof long[]) {
      return ((long[]) o)[0];
    }
    if (o instanceof int[]) {
      return ((int[]) o)[0];
    }
    throw guru("attribute type " + o.getClass().getName());
  }

  private static long[] longs(Object raw) {
    return anyInts(raw);
  }

  /**
   * jhdf widens unsigned and small integer datasets on read (uint8 → int[],
   * int8 → byte[], int16 → short[]), and a hyperslab of a 2-D dataset comes
   * back as a 2-D array with one row. Accept any of those shapes.
   */
  private static long[] anyInts(Object raw) {
    if (raw instanceof byte[]) {
      byte[] m = (byte[]) raw;
      long[] out = new long[m.length];
      for (int i = 0; i < m.length; i++) {
        out[i] = m[i];
      }
      return out;
    }
    if (raw instanceof short[]) {
      short[] m = (short[]) raw;
      long[] out = new long[m.length];
      for (int i = 0; i < m.length; i++) {
        out[i] = m[i];
      }
      return out;
    }
    if (raw instanceof int[]) {
      int[] m = (int[]) raw;
      long[] out = new long[m.length];
      for (int i = 0; i < m.length; i++) {
        out[i] = m[i];
      }
      return out;
    }
    if (raw instanceof long[]) {
      return (long[]) raw;
    }
    if (raw instanceof byte[][] || raw instanceof short[][] || raw instanceof int[][]
        || raw instanceof long[][]) {
      Object[] rows = (Object[]) raw;
      return rows.length == 0 ? new long[0] : anyInts(rows[0]);
    }
    throw guru("expected integer data, got " + raw.getClass().getName());
  }

  private static byte[] bytes(Object raw) {
    long[] v = anyInts(raw);
    byte[] out = new byte[v.length];
    for (int i = 0; i < v.length; i++) {
      out[i] = (byte) v[i];
    }
    return out;
  }

  private static short[] shorts(Object raw) {
    long[] v = anyInts(raw);
    short[] out = new short[v.length];
    for (int i = 0; i < v.length; i++) {
      out[i] = (short) v[i];
    }
    return out;
  }

  private static IllegalStateException guru(String message) {
    return new IllegalStateException(GURU + " " + message);
  }
}
