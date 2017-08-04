package org.embulk.filter.join_file.table.column;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.embulk.spi.Column;

public class BooleanColumn
        extends AbstractColumn<Boolean>
{
    BooleanColumn(Column column, ImmutableList<ImmutableList<Optional<Boolean>>> values)
    {
        super(column, values);
    }

    @Override
    void validateType()
            throws IllegalStateException
    {
        if (!isBooleanType()) {
            throw new IllegalStateException();
        }
    }

    @Override
    public Optional<Boolean> findBoolean(long idx)
    {
        return super.findValue(idx);
    }
}
