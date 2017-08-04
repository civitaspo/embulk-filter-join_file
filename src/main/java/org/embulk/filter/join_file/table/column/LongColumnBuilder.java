package org.embulk.filter.join_file.table.column;

import org.embulk.spi.Column;

public class LongColumnBuilder
        extends AbstractColumnBuilder<Long>
{
    public LongColumnBuilder(Column column)
    {
        super(column);
    }

    @Override
    public void addLong(Long v)
    {
        super.addValue(v);
    }
}
