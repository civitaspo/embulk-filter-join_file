package org.embulk.filter.join_file.table.column;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.embulk.spi.Column;

public class LongColumn
        extends AbstractColumn<Long>
{
    LongColumn(Column column, ImmutableList<ImmutableList<Optional<Long>>> values)
    {
        super(column, values);
    }

    @Override
    void validateType()
            throws IllegalStateException
    {
        if (!isLongType()) {
            throw new IllegalStateException();
        }
    }

    @Override
    public Optional<Long> findLong(long idx)
    {
        return super.findValue(idx);
    }
}
