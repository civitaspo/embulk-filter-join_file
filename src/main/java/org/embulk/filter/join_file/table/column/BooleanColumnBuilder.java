package org.embulk.filter.join_file.table.column;

import org.embulk.spi.Column;

public class BooleanColumnBuilder
        extends AbstractColumnBuilder<Boolean>
{
    public BooleanColumnBuilder(Column column)
    {
        super(column);
    }

    @Override
    public void addBoolean(Boolean v)
    {
        super.addValue(v);
    }
}
