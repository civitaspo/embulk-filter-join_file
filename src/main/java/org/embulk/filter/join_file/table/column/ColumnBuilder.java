package org.embulk.filter.join_file.table.column;

import org.embulk.spi.time.Timestamp;
import org.msgpack.value.Value;

public interface ColumnBuilder
{
    long getCurrentIndex();

    void addNull();

    void addBoolean(Boolean v);

    void addString(String v);

    void addLong(Long v);

    void addDouble(Double v);

    void addTimestamp(Timestamp v);

    void addJson(Value v);
}
