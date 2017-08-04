package org.embulk.filter.join_file.table.column;

import org.embulk.spi.Column;

public class DoubleColumnBuilder
        extends AbstractColumnBuilder<Double>
{
    public DoubleColumnBuilder(Column column)
    {
        super(column);
    }

    @Override
    public void addDouble(Double v)
    {
        super.addValue(v);
    }
}
