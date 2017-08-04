package org.embulk.filter.join_file.table.column;

import org.embulk.spi.Column;

public class StringColumnBuilder
        extends AbstractColumnBuilder<String>
{
    public StringColumnBuilder(Column column)
    {
        super(column);
    }

    @Override
    public void addString(String v)
    {
        super.addValue(v);
    }
}
