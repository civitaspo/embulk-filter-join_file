package org.embulk.filter.join_file.table.column;

import org.embulk.spi.Column;
import org.msgpack.value.Value;

public class JsonColumnBuilder
        extends AbstractColumnBuilder<Value>
{
    public JsonColumnBuilder(Column column)
    {
        super(column);
    }

    @Override
    public void addJson(Value v)
    {
        super.addValue(v);
    }
}
