package org.zndx.semantics.hdf5;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class Hdf5MachineReaderTest {

  private static Path fixture() {
    String prop = System.getProperty("analog.fixture");
    if (prop != null && !prop.isBlank()) {
      return Path.of(prop).toAbsolutePath().normalize();
    }
    return Path.of(System.getProperty("user.dir"))
        .resolve("../../analog/fixtures/sdg_machine_small.h5")
        .normalize();
  }

  @Test
  void analogShapeAndCount() {
    Path p = fixture();
    assertTrue(p.toFile().isFile(), () -> "missing analog " + p);
    Hdf5MachineReader.Window w = Hdf5MachineReader.readValues(p);
    assertEquals(8, w.nGpu);
    assertEquals(64, w.nTime);
    assertEquals(512, w.size());
  }

  @Test
  void missingFileIsGuru() {
    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () -> Hdf5MachineReader.readValues(Path.of("/no/such/machine.h5")));
    assertTrue(e.getMessage().contains(Hdf5MachineReader.GURU_JHDF), e.getMessage());
  }
}
