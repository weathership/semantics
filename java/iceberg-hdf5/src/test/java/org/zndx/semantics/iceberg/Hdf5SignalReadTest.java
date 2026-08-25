package org.zndx.semantics.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Predicate evaluation on the /signal layout. Fixture from
 * {@code python -m hdf5_iceberg.signal_layout --demo}: 64 instants at 1 Hz from
 * epoch_hour 496560; int series 101 (all present) and 102 (gap at k%7==3);
 * dec series 201 = k/16.
 */
class Hdf5SignalReadTest {
  private static final long HOUR = 496560L;
  private static final long BASE_NS = HOUR * 3600L * 1_000_000_000L;
  private static final int N_T = 64;

  private static Path fixture() {
    String prop = System.getProperty("signal.fixture");
    Path p =
        (prop != null && !prop.isBlank())
            ? Path.of(prop)
            : Path.of(System.getProperty("user.dir"))
                .resolve("../../analog/fixtures/signal_v2_small.h5")
                .normalize();
    Assumptions.assumeTrue(Files.isRegularFile(p), "signal fixture missing: " + p);
    return p;
  }

  static Schema signalSchema() {
    return new Schema(
        Types.NestedField.required(1, "epoch_hour", Types.IntegerType.get()),
        Types.NestedField.required(2, "ts_ns", Types.LongType.get()),
        Types.NestedField.required(3, "series_id", Types.LongType.get()),
        Types.NestedField.required(4, "src", Types.IntegerType.get()),
        Types.NestedField.optional(5, "gpu", Types.IntegerType.get()),
        Types.NestedField.optional(6, "inst", Types.IntegerType.get()),
        Types.NestedField.optional(7, "val_i", Types.LongType.get()),
        Types.NestedField.optional(8, "val_d", Types.DecimalType.of(18, 6)));
  }

  private static ReadBuilder<Record, Schema> rb() {
    return Hdf5GenericFormatModel.create()
        .readBuilder(org.apache.iceberg.Files.localInput(fixture().toFile()))
        .project(signalSchema());
  }

  private static int count(ReadBuilder<Record, Schema> b) throws Exception {
    int n = 0;
    try (CloseableIterable<Record> it = b.build()) {
      for (Record ignored : it) {
        n++;
      }
    }
    return n;
  }

  @Test
  void unfilteredReadsEverythingPresent() throws Exception {
    // 64 (s101) + 64 - 10 gaps (s102: k%7==3 → k=3,10,...,59 = 9... compute) + 64 (s201)
    int gaps = 0;
    for (int k = 0; k < N_T; k++) {
      if (k % 7 == 3) gaps++;
    }
    assertEquals(3 * N_T - gaps, count(rb()));
  }

  @Test
  void tsRangeIsExact() throws Exception {
    long lo = BASE_NS + 10L * 1_000_000_000L;
    long hi = BASE_NS + 19L * 1_000_000_000L; // exclusive
    int n = count(rb().filter(Expressions.and(
        Expressions.greaterThanOrEqual("ts_ns", lo), Expressions.lessThan("ts_ns", hi))));
    // k in [10,19): 9 instants; s102 gap at k=10,17 → 9+7+9
    assertEquals(9 + 7 + 9, n);
  }

  @Test
  void seriesInNarrowsAndDecimalIsExact() throws Exception {
    Set<Long> ids = new HashSet<>();
    ids.add(201L);
    int n = 0;
    try (CloseableIterable<Record> it =
        rb().filter(Expressions.in("series_id", ids)).build()) {
      for (Record r : it) {
        n++;
        assertEquals(201L, r.getField("series_id"));
        BigDecimal d = (BigDecimal) r.getField("val_d");
        long k = ((Long) r.getField("ts_ns") - BASE_NS) / 1_000_000_000L;
        // k/16 is exactly representable at scale 6 — no float on the path.
        assertEquals(0, d.compareTo(BigDecimal.valueOf(k).divide(BigDecimal.valueOf(16))));
        assertEquals(6, d.scale());
      }
    }
    assertEquals(N_T, n);
  }

  @Test
  void valuePredicateIsResidualEvaluated() throws Exception {
    // Not narrowable at the file (val_i is payload) — must still be exact.
    int n = count(rb().filter(Expressions.and(
        Expressions.equal("series_id", 101L), Expressions.greaterThan("val_i", 100L))));
    // s101 val_i = 40+k > 100 → k > 60 → k=61,62,63
    assertEquals(3, n);
  }

  @Test
  void wrongHourIsEmptyAndSplitTailIsEmpty() throws Exception {
    assertEquals(0, count(rb().filter(Expressions.equal("epoch_hour", (int) HOUR + 1))));
    assertEquals(0, count(rb().split(1, 10)));
    assertTrue(count(rb().split(0, 10)) > 0);
  }
}
