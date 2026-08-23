package org.zndx.semantics.iceberg;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.formats.FormatModel;
import org.apache.iceberg.formats.ModelWriteBuilder;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.io.InputFile;

/**
 * Iceberg File Format API model for SysML machine HDF5 ({@code format=hdf5}, {@code .h5}).
 *
 * <p>Object model is Iceberg generic {@link Record}. Register via {@link
 * org.apache.iceberg.formats.FormatModelRegistry#register(FormatModel)}.
 */
public final class Hdf5GenericFormatModel implements FormatModel<Record, Schema> {

  public static final String GURU_WRITE = "#SL.00000019.HDF5WRITE";

  private Hdf5GenericFormatModel() {}

  public static Hdf5GenericFormatModel create() {
    return new Hdf5GenericFormatModel();
  }

  @Override
  public FileFormat format() {
    return FileFormat.HDF5;
  }

  @Override
  public Class<? extends Record> type() {
    return Record.class;
  }

  @Override
  public Class<Schema> schemaType() {
    return Schema.class;
  }

  @Override
  public ReadBuilder<Record, Schema> readBuilder(InputFile inputFile) {
    return new Hdf5ReadBuilder(inputFile);
  }

  @Override
  public ModelWriteBuilder<Record, Schema> writeBuilder(EncryptedOutputFile outputFile) {
    throw new IllegalStateException(
        GURU_WRITE + " HDF5 DataWriteBuilder not implemented; analog read is live");
  }
}
