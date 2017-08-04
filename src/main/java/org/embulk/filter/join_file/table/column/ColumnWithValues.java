package org.embulk.filter.join_file.table.column;

import com.google.common.base.Optional;
import org.embulk.spi.Column;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Type;
import org.msgpack.value.Value;

public interface ColumnWithValues
{
    Column getColumn();

    String getName();

    Type getType();

    Optional<Boolean> findBoolean(long idx);

    Optional<String> findString(long idx);

    Optional<Long> findLong(long idx);

    Optional<Double> findDouble(long idx);

    Optional<Timestamp> findTimestamp(long idx);

    Optional<Value> findJson(long idx);

    Index toIndex();
}
