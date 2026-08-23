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
      List<Record> rows = new ArrayList<>(w.size());
      int i = 0;
      for (int g = 0; g < w.nGpu; g++) {
        for (int t = 0; t < w.nTime; t++) {
          GenericRecord rec = GenericRecord.create(icebergSchema);
          rec.set(0, g);
          rec.set(1, t);
          rec.set(2, (int) w.values[i++]);
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
