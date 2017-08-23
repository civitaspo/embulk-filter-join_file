package org.embulk.filter.join_file.file;

import org.embulk.spi.Column;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.msgpack.value.Value;

public interface FileReader
    extends AutoCloseable
{
    Schema getSchema();

    boolean nextRecord();

    boolean isNull(Column column);

    boolean getBoolean(Column column);

    String getString(Column column);

    long getLong(Column column);

    double getDouble(Column column);

    Timestamp getTimestamp(Column column);

    Value getJson(Column column);
}
