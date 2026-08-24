package org.zndx.semantics.iceberg;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.zndx.semantics.hdf5.Hdf5MachineReader;

final class Hdf5ReadBuilder implements ReadBuilder<Record, Schema> {
  private final InputFile inputFile;
  private Schema icebergSchema;

  Hdf5ReadBuilder(InputFile inputFile) {
    this.inputFile = inputFile;
  }

  @Override
  public ReadBuilder<Record, Schema> split(long start, long length) {
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
    return this;
  }

  @Override
  public ReadBuilder<Record, Schema> filter(Expression filter) {
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
    Path tmp = materialize();
    try {
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
      return CloseableIterable.withNoopClose(rows);
    } finally {
      try {
        Files.deleteIfExists(tmp);
      } catch (IOException ignored) {
        // temp analog copy
      }
    }
  }

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
}
