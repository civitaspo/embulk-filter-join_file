package org.embulk.filter.join_file.table;

import org.embulk.spi.Column;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.msgpack.value.Value;

public interface TableBuilder
{
    Schema getSchema();

    void addRecord();

    void setNull(Column column);

    void setBoolean(Column column, boolean v);

    void setString(Column column, String v);

    void setLong(Column column, long v);

    void setDouble(Column column, double v);

    void setTimestamp(Column column, Timestamp v);

    void setJson(Column column, Value v);

    Table build();
}
