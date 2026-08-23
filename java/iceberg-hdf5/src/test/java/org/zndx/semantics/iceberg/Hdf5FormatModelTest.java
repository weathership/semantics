package org.zndx.semantics.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class Hdf5FormatModelTest {

  private static Path fixture() {
    String prop = System.getProperty("analog.fixture");
    if (prop != null && !prop.isBlank()) {
      return Path.of(prop);
    }
    return Path.of(System.getProperty("user.dir"))
        .resolve("../../analog/fixtures/sdg_machine_small.h5")
        .normalize();
  }

  static Schema analogSchema() {
    return new Schema(
        Types.NestedField.required(1, "gpu_index", Types.IntegerType.get()),
        Types.NestedField.required(2, "sample", Types.IntegerType.get()),
        Types.NestedField.required(3, "value", Types.IntegerType.get()));
  }

  @Test
  void modelReadsAnalog() throws Exception {
    assertTrue(FileFormat.HDF5.addExtension("part").endsWith(".h5"));
    ReadBuilder<Record, Schema> rb =
        Hdf5GenericFormatModel.create()
            .readBuilder(Files.localInput(fixture().toFile()));
    int n = 0;
    try (CloseableIterable<Record> it = rb.project(analogSchema()).build()) {
      for (Record ignored : it) {
        n++;
      }
    }
    assertEquals(512, n);
  }

  @Test
  void writeBuilderIsGuruNotSilent() {
    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () ->
                Hdf5GenericFormatModel.create()
                    .writeBuilder(null));
    assertTrue(e.getMessage().contains(Hdf5GenericFormatModel.GURU_WRITE));
  }
}
