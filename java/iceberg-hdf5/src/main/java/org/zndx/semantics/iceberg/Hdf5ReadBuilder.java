package org.zndx.semantics.iceberg;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.zndx.semantics.hdf5.Hdf5MachineReader;
import org.zndx.semantics.hdf5.Hdf5SignalReader;

/**
 * Iceberg ReadBuilder over HDF5 analog files.
 *
 * <p>Two physical layouts:
 *
 * <ul>
 *   <li>{@code /signal} (schema_version 2) — signals tier1. The filter is
 *       honoured: {@code ts_ns} bounds and {@code series_id} sets narrow the
 *       hyperslab the reader touches, then the whole bound expression is
 *       re-evaluated on every emitted row so the result is exact, not merely
 *       narrowed.
 *   <li>{@code /Machine/GpuMetric[0]} — the legacy gpu_metrics_tier1 analog.
 *       Read in full, then filtered row-by-row with the same evaluator, so a
 *       filtered read of legacy history is correct even though it is not
 *       narrowed at the file.
 * </ul>
 */
final class Hdf5ReadBuilder implements ReadBuilder<Record, Schema> {
  private final InputFile inputFile;
  private Schema icebergSchema;
  private Expression filter = Expressions.alwaysTrue();
  private boolean caseSensitive = false;
  private long splitStart = 0;
  private long splitLength = -1;

  Hdf5ReadBuilder(InputFile inputFile) {
    this.inputFile = inputFile;
  }

  @Override
  public ReadBuilder<Record, Schema> split(long start, long length) {
    this.splitStart = start;
    this.splitLength = length;
    return this;
  }

  @Override
  public ReadBuilder<Record, Schema> project(Schema schema) {
    this.icebergSchema = schema;
    return this;
  }

  @Override
  public ReadBuilder<Record, Schema> engineProjection(Schema schema) {
    if (this.icebergSchema == null) {
      this.icebergSchema = schema;
    }
    return this;
  }

  @Override
  public ReadBuilder<Record, Schema> caseSensitive(boolean caseSensitive) {
    this.caseSensitive = caseSensitive;
    return this;
  }

  @Override
  public ReadBuilder<Record, Schema> filter(Expression filter) {
    this.filter = filter == null ? Expressions.alwaysTrue() : filter;
    return this;
  }

  @Override
  public ReadBuilder<Record, Schema> set(String key, String value) {
    return this;
  }

  @Override
  public ReadBuilder<Record, Schema> reuseContainers() {
    return this;
  }

  @Override
  public ReadBuilder<Record, Schema> recordsPerBatch(int rowsPerBatch) {
    return this;
  }

  @Override
  public ReadBuilder<Record, Schema> idToConstant(Map<Integer, ?> idToConstant) {
    return this;
  }

  @Override
  public ReadBuilder<Record, Schema> withNameMapping(NameMapping nameMapping) {
    return this;
  }

  @Override
  public CloseableIterable<Record> build() {
    if (icebergSchema == null) {
      throw new IllegalStateException("project(Schema) must be set before build()");
    }
    // An HDF5 file is one scan range. Iceberg's split contract is "rows whose
    // start offset falls in [start, start+length)"; a non-zero start therefore
    // owns none of them, which keeps a planner that ever splits from reading
    // the file twice.
    if (splitStart > 0) {
      return CloseableIterable.empty();
    }
    Path tmp = materialize();
    try {
      Expression bound = bind(filter);
      // Evaluator binds internally and rejects an already-bound tree, so it
      // takes the unbound filter; the visitor below takes the bound one.
      Evaluator residual = new Evaluator(icebergSchema.asStruct(), filter, caseSensitive);
      List<Record> rows;
      if (Hdf5SignalReader.isSignalLayout(tmp)) {
        rows = readSignal(tmp, bound);
      } else {
        rows = readLegacy(tmp);
      }
      if (bound.equals(Expressions.alwaysTrue())) {
        return CloseableIterable.withNoopClose(rows);
      }
      List<Record> kept = new ArrayList<>(rows.size());
      for (Record r : rows) {
        if (residual.eval(r)) {
          kept.add(r);
        }
      }
      return CloseableIterable.withNoopClose(kept);
    } finally {
      cleanup(tmp);
    }
  }

  private Expression bind(Expression expr) {
    if (expr == null) {
      return Expressions.alwaysTrue();
    }
    return Binder.bind(icebergSchema.asStruct(), expr, caseSensitive);
  }

  // ---- /signal layout --------------------------------------------------

  private List<Record> readSignal(Path path, Expression bound) {
    Hdf5SignalReader.Window w = ExpressionVisitors.visit(bound, new WindowVisitor());
    List<Hdf5SignalReader.Row> rows = Hdf5SignalReader.read(path, w);
    List<Record> out = new ArrayList<>(rows.size());
    List<String> names = new ArrayList<>();
    for (int f = 0; f < icebergSchema.columns().size(); f++) {
      names.add(icebergSchema.columns().get(f).name());
    }
    for (Hdf5SignalReader.Row r : rows) {
      GenericRecord rec = GenericRecord.create(icebergSchema);
      for (int f = 0; f < names.size(); f++) {
        switch (names.get(f)) {
          case "epoch_hour":
            rec.set(f, (int) (r.tsNs / 1_000_000_000L / 3600L));
            break;
          case "ts_ns":
            rec.set(f, r.tsNs);
            break;
          case "series_id":
            rec.set(f, r.seriesId);
            break;
          case "src":
            rec.set(f, (int) r.src);
            break;
          case "gpu":
            rec.set(f, r.gpu == null ? null : Integer.valueOf(r.gpu));
            break;
          case "inst":
            rec.set(f, r.inst == null ? null : Integer.valueOf(r.inst));
            break;
          case "val_i":
            rec.set(f, r.valI);
            break;
          case "val_d":
            rec.set(f, r.valD);
            break;
          default:
            rec.set(f, null);
        }
      }
      out.add(rec);
    }
    return out;
  }

  /**
   * Turns the AND-tree of a bound predicate into the reader's window. Anything
   * it cannot narrow on (OR, NOT, other columns) is left to the residual
   * evaluator — narrowing is an optimisation, exactness comes from the
   * evaluator, so the visitor only ever tightens, never decides.
   */
  private static final class WindowVisitor
      extends ExpressionVisitors.BoundExpressionVisitor<Hdf5SignalReader.Window> {
    private final Hdf5SignalReader.Window w = Hdf5SignalReader.Window.all();

    @Override
    public Hdf5SignalReader.Window alwaysTrue() {
      return w;
    }

    @Override
    public Hdf5SignalReader.Window alwaysFalse() {
      w.seriesIds = new HashSet<>(); // nothing can match
      return w;
    }

    @Override
    public Hdf5SignalReader.Window not(Hdf5SignalReader.Window result) {
      return w; // residual handles it
    }

    @Override
    public Hdf5SignalReader.Window and(
        Hdf5SignalReader.Window left, Hdf5SignalReader.Window right) {
      return w; // both sides already tightened the shared window
    }

    @Override
    public Hdf5SignalReader.Window or(
        Hdf5SignalReader.Window left, Hdf5SignalReader.Window right) {
      // An OR could widen; the children tightened w in place, which is unsafe
      // under OR. Reset to unbounded and let the residual decide.
      w.tsLo = null;
      w.tsHi = null;
      w.seriesIds = null;
      w.epochHour = null;
      return w;
    }

    @Override
    public <T> Hdf5SignalReader.Window predicate(BoundPredicate<T> pred) {
      return super.predicate(pred);
    }

    @Override
    public <T> Hdf5SignalReader.Window predicate(UnboundPredicate<T> pred) {
      return w; // unbound predicates cannot narrow
    }

    private static String name(BoundReference<?> ref) {
      return ref.field().name();
    }

    @Override
    public <T> Hdf5SignalReader.Window lt(BoundReference<T> ref, Literal<T> lit) {
      if ("ts_ns".equals(name(ref))) {
        tightenHi(((Number) lit.value()).longValue() - 1);
      }
      return w;
    }

    @Override
    public <T> Hdf5SignalReader.Window ltEq(BoundReference<T> ref, Literal<T> lit) {
      if ("ts_ns".equals(name(ref))) {
        tightenHi(((Number) lit.value()).longValue());
      }
      return w;
    }

    @Override
    public <T> Hdf5SignalReader.Window gt(BoundReference<T> ref, Literal<T> lit) {
      if ("ts_ns".equals(name(ref))) {
        tightenLo(((Number) lit.value()).longValue() + 1);
      }
      return w;
    }

    @Override
    public <T> Hdf5SignalReader.Window gtEq(BoundReference<T> ref, Literal<T> lit) {
      if ("ts_ns".equals(name(ref))) {
        tightenLo(((Number) lit.value()).longValue());
      }
      return w;
    }

    @Override
    public <T> Hdf5SignalReader.Window eq(BoundReference<T> ref, Literal<T> lit) {
      String n = name(ref);
      if ("ts_ns".equals(n)) {
        long v = ((Number) lit.value()).longValue();
        tightenLo(v);
        tightenHi(v);
      } else if ("series_id".equals(n)) {
        Set<Long> one = new HashSet<>();
        one.add(((Number) lit.value()).longValue());
        intersectSeries(one);
      } else if ("epoch_hour".equals(n)) {
        w.epochHour = ((Number) lit.value()).intValue();
      }
      return w;
    }

    @Override
    public <T> Hdf5SignalReader.Window in(BoundReference<T> ref, Set<T> set) {
      if ("series_id".equals(name(ref))) {
        Set<Long> ids = new HashSet<>();
        for (T v : set) {
          ids.add(((Number) v).longValue());
        }
        intersectSeries(ids);
      }
      return w;
    }

    @Override
    public <T> Hdf5SignalReader.Window notIn(BoundReference<T> ref, Set<T> set) {
      return w;
    }

    @Override
    public <T> Hdf5SignalReader.Window isNull(BoundReference<T> ref) {
      return w;
    }

    @Override
    public <T> Hdf5SignalReader.Window notNull(BoundReference<T> ref) {
      return w;
    }

    private void tightenLo(long v) {
      w.tsLo = (w.tsLo == null) ? v : Math.max(w.tsLo, v);
    }

    private void tightenHi(long v) {
      w.tsHi = (w.tsHi == null) ? v : Math.min(w.tsHi, v);
    }

    private void intersectSeries(Set<Long> ids) {
      if (w.seriesIds == null) {
        w.seriesIds = ids;
      } else {
        w.seriesIds.retainAll(ids);
      }
    }
  }

  // ---- legacy /Machine layout -------------------------------------------

  private List<Record> readLegacy(Path tmp) {
    Hdf5MachineReader.Window w = Hdf5MachineReader.readValues(tmp);
    boolean warehouse =
        icebergSchema.findField("power_w") != null
            || icebergSchema.findField("ts_ns") != null;
    int nGpu = w.nGpu;
    int planes = 1;
    if (warehouse && w.nGpu % 4 == 0 && w.nGpu != 8) {
      nGpu = w.nGpu / 4;
      planes = 4;
    }
    List<Record> rows = new ArrayList<>(nGpu * w.nTime);
    for (int g = 0; g < nGpu; g++) {
      for (int t = 0; t < w.nTime; t++) {
        GenericRecord rec = GenericRecord.create(icebergSchema);
        int base = g * w.nTime + t;
        short power = w.values[base];
        short util = planes > 1 ? w.values[(nGpu + g) * w.nTime + t] : 0;
        short mem = planes > 2 ? w.values[(2 * nGpu + g) * w.nTime + t] : 0;
        short temp = planes > 3 ? w.values[(3 * nGpu + g) * w.nTime + t] : 0;
        long ts = w.timestampAt(t);
        for (int f = 0; f < icebergSchema.columns().size(); f++) {
          String name = icebergSchema.columns().get(f).name();
          switch (name) {
            case "gpu_index":
              rec.set(f, g);
              break;
            case "sample":
              rec.set(f, t);
              break;
            case "value":
              rec.set(f, (int) power);
              break;
            case "epoch_hour":
              rec.set(f, (int) (ts / 1_000_000_000L / 3600L));
              break;
            case "ts_ns":
              rec.set(f, ts);
              break;
            case "power_w":
              rec.set(f, (float) power);
              break;
            case "util_pct":
              rec.set(f, (float) util);
              break;
            case "mem_used_mb":
              rec.set(f, (float) mem);
              break;
            case "temp_c":
              rec.set(f, (float) temp);
              break;
            default:
              rec.set(f, null);
          }
        }
        rows.add(rec);
      }
    }
    return rows;
  }

  // ---- io ------------------------------------------------------------------

  private Path materialize() {
    String loc = inputFile.location();
    if (loc != null && (loc.startsWith("/") || loc.startsWith("file:"))) {
      Path p = Path.of(loc.startsWith("file:") ? loc.substring("file:".length()) : loc);
      if (Files.isRegularFile(p)) {
        return p;
      }
    }
    try {
      Path tmp = Files.createTempFile("iceberg-hdf5-", ".h5");
      try (InputStream in = inputFile.newStream()) {
        Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING);
      }
      tmp.toFile().deleteOnExit();
      return tmp;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void cleanup(Path tmp) {
    String loc = inputFile.location();
    boolean local = loc != null && (loc.startsWith("/") || loc.startsWith("file:"));
    if (local) {
      return; // the caller's file, not our temp copy
    }
    try {
      Files.deleteIfExists(tmp);
    } catch (IOException ignored) {
      // temp analog copy
    }
  }
}
