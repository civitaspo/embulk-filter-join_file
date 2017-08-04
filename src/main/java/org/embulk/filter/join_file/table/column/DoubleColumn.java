package org.embulk.filter.join_file.table.column;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.embulk.spi.Column;

public class DoubleColumn
        extends AbstractColumn<Double>
{
    DoubleColumn(Column column, ImmutableList<ImmutableList<Optional<Double>>> values)
    {
        super(column, values);
    }

    @Override
    void validateType()
            throws IllegalStateException
    {
        if (!isDoubleType()) {
            throw new IllegalStateException();
        }
    }

    @Override
    public Optional<Double> findDouble(long idx)
    {
        return super.findValue(idx);
    }
}
