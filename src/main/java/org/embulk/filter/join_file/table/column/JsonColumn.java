package org.embulk.filter.join_file.table.column;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.embulk.spi.Column;
import org.msgpack.value.Value;

public class JsonColumn
        extends AbstractColumn<Value>
{
    JsonColumn(Column column, ImmutableList<ImmutableList<Optional<Value>>> values)
    {
        super(column, values);
    }

    @Override
    void validateType()
            throws IllegalStateException
    {
        if (!isJsonType()) {
            throw new IllegalStateException();
        }
    }

    @Override
    public Optional<Value> findJson(long idx)
    {
        return super.findValue(idx);
    }
}
