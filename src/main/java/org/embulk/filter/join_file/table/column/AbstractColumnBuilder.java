package org.embulk.filter.join_file.table.column;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.embulk.filter.join_file.exception.UnsupportedTypeException;
import org.embulk.spi.Column;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Type;
import org.msgpack.value.Value;
import static org.embulk.spi.type.Types.BOOLEAN;
import static org.embulk.spi.type.Types.DOUBLE;
import static org.embulk.spi.type.Types.JSON;
import static org.embulk.spi.type.Types.LONG;
import static org.embulk.spi.type.Types.STRING;
import static org.embulk.spi.type.Types.TIMESTAMP;

import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractColumnBuilder<T>
        implements ColumnBuilder
{
    public static ColumnBuilder builder(Column column)
    {
        if (column.getType().equals(BOOLEAN)) {
            return new BooleanColumnBuilder(column);
        }
        else if ((column.getType().equals(STRING))) {
            return new StringColumnBuilder(column);
        }
        else if (column.getType().equals(LONG)) {
            return new LongColumnBuilder(column);
        }
        else if (column.getType().equals(DOUBLE)) {
            return new DoubleColumnBuilder(column);
        }
        else if (column.getType().equals(TIMESTAMP)) {
            return new TimestampColumnBuilder(column);
        }
        else if (column.getType().equals(JSON)) {
            return new JsonColumnBuilder(column);
        }
        else {
            throw new UnsupportedTypeException(String.format("%s is not supported.", column.getType()));
        }
    }

    private static final int MAX_VALUES_INDEX = Integer.MAX_VALUE - 1; // Max List size is Integer.MAX_VALUE, so max index size is Integer.MAX_VALUE - 1
    private final Column column;
    private final AtomicLong index = new AtomicLong(0);
    private final AtomicLong currentValuesIndex = new AtomicLong(0);
    private final ImmutableList.Builder<ImmutableList<Optional<T>>> listBuilder = ImmutableList.builder();
    private ImmutableList.Builder<Optional<T>> valuesBuilder = ImmutableList.builder();

    public AbstractColumnBuilder(Column column)
    {
        this.column = column;
    }

    private Type getType()
    {
        return column.getType();
    }

    private boolean isNewBuilderRequired()
    {
        return (index.get() / MAX_VALUES_INDEX) > currentValuesIndex.get();
    }

    private void setNewValuesBuilder()
    {
        currentValuesIndex.incrementAndGet();
        listBuilder.add(valuesBuilder.build());
        valuesBuilder = ImmutableList.builder();
    }

    protected void addValue(T v)
    {
        if (isNewBuilderRequired()) {
            setNewValuesBuilder();
        }
        index.incrementAndGet();
        valuesBuilder.add(Optional.fromNullable(v));
    }

    @Override
    public long getCurrentIndex()
    {
        return index.get();
    }

    @Override
    public void addNull()
    {
        addValue(null);
    }

    @Override
    public void addBoolean(Boolean v)
    {
        throw new UnsupportedTypeException(String.format("%s is only supported.", getType()));
    }

    @Override
    public void addString(String v)
    {
        throw new UnsupportedTypeException(String.format("%s is only supported.", getType()));
    }

    @Override
    public void addLong(Long v)
    {
        throw new UnsupportedTypeException(String.format("%s is only supported.", getType()));
    }

    @Override
    public void addDouble(Double v)
    {
        throw new UnsupportedTypeException(String.format("%s is only supported.", getType()));
    }

    @Override
    public void addTimestamp(Timestamp v)
    {
        throw new UnsupportedTypeException(String.format("%s is only supported.", getType()));
    }

    @Override
    public void addJson(Value v)
    {
        throw new UnsupportedTypeException(String.format("%s is only supported.", getType()));
    }
}
