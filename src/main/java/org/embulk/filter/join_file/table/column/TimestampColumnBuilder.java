package org.embulk.filter.join_file.table.column;

import org.embulk.spi.Column;
import org.embulk.spi.time.Timestamp;

public class TimestampColumnBuilder
        extends AbstractColumnBuilder<Timestamp>
{
    public TimestampColumnBuilder(Column column)
    {
        super(column);
    }

    @Override
    public void addTimestamp(Timestamp v)
    {
        super.addValue(v);
    }
}
