package org.embulk.filter.join_file.table.column;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.embulk.spi.Column;
import org.embulk.spi.time.Timestamp;

public class TimestampColumn
        extends AbstractColumn<Timestamp>
{
    TimestampColumn(Column column, ImmutableList<ImmutableList<Optional<Timestamp>>> values)
    {
        super(column, values);
    }

    @Override
    void validateType()
            throws IllegalStateException
    {
        if (!isTimestampType()) {
            throw new IllegalStateException();
        }
    }

    @Override
    public Optional<Timestamp> findTimestamp(long idx)
    {
        return super.findValue(idx);
    }
}
