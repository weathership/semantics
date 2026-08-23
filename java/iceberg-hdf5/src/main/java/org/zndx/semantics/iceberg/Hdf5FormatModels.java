package org.zndx.semantics.iceberg;

import org.apache.iceberg.formats.FormatModelRegistry;

/** Registers the HDF5 generic object model. Call once from engine/Impala bootstrap. */
public final class Hdf5FormatModels {
  private Hdf5FormatModels() {}

  public static void register() {
    FormatModelRegistry.register(Hdf5GenericFormatModel.create());
  }
}
