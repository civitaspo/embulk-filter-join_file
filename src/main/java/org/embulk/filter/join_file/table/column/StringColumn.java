package org.embulk.filter.join_file.table.column;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.embulk.spi.Column;

public class StringColumn
        extends AbstractColumn<String>
{
    StringColumn(Column column, ImmutableList<ImmutableList<Optional<String>>> values)
    {
        super(column, values);
    }

    @Override
    void validateType()
            throws IllegalStateException
    {
        if (!isStringType()) {
            throw new IllegalStateException();
        }
    }

    @Override
    public Optional<String> findString(long idx)
    {
        return super.findValue(idx);
    }
}
