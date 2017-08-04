package org.embulk.filter.join_file.table.column;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
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

public abstract class AbstractColumn<T>
        implements ColumnWithValues
{
    private static final int MAX_VALUES_INDEX = Integer.MAX_VALUE - 1; // Max List size is Integer.MAX_VALUE, so max index size is Integer.MAX_VALUE - 1
    private final Column column;
    private final ImmutableList<ImmutableList<Optional<T>>> valuesList;

    AbstractColumn(Column column, ImmutableList<ImmutableList<Optional<T>>> valuesList)
    {
        this.column = column;
        this.valuesList = valuesList;

        validateType();
    }

    abstract void validateType() throws IllegalStateException;

    @Override
    public Column getColumn()
    {
        return column;
    }

    @Override
    public String getName()
    {
        return column.getName();
    }

    @Override
    public Type getType()
    {
        return column.getType();
    }

    public boolean isBooleanType()
    {
        return getType().equals(BOOLEAN);
    }

    public boolean isStringType()
    {
        return getType().equals(STRING);
    }

    public boolean isLongType()
    {
        return getType().equals(LONG);
    }

    public boolean isDoubleType()
    {
        return getType().equals(DOUBLE);
    }

    public boolean isTimestampType()
    {
        return getType().equals(TIMESTAMP);
    }

    public boolean isJsonType()
    {
        return getType().equals(JSON);
    }

    @Override
    public Optional<Boolean> findBoolean(long idx)
    {
        throw new UnsupportedTypeException(String.format("%s is only supported.", getType()));
    }

    @Override
    public Optional<String> findString(long idx)
    {
        throw new UnsupportedTypeException(String.format("%s is only supported.", getType()));
    }

    @Override
    public Optional<Long> findLong(long idx)
    {
        throw new UnsupportedTypeException(String.format("%s is only supported.", getType()));
    }

    @Override
    public Optional<Double> findDouble(long idx)
    {
        throw new UnsupportedTypeException(String.format("%s is only supported.", getType()));
    }

    @Override
    public Optional<Timestamp> findTimestamp(long idx)
    {
        throw new UnsupportedTypeException(String.format("%s is only supported.", getType()));
    }

    @Override
    public Optional<Value> findJson(long idx)
    {
        throw new UnsupportedTypeException(String.format("%s is only supported.", getType()));
    }

    @Override
    public Index toIndex()
    {
        Index.Builder builder = Index.builder();
        for (ImmutableList<Optional<T>> values : getValuesList()) {
            for (Optional<T> value : values) {
                builder.addIndexValue(value);
            }
        }
        return builder.build();
    }

    private ImmutableList<ImmutableList<Optional<T>>> getValuesList()
    {
        return valuesList;
    }

    private ImmutableList<Optional<T>> findValues(long idx)
    {
        // TODO: valuesIdx cannot be long?
        int valuesIdx = (int) (idx / MAX_VALUES_INDEX);

        ImmutableList<Optional<T>> values = getValuesList().get(valuesIdx);
        if (values == null) {
            throw new ArrayIndexOutOfBoundsException(String.format("Index: %d, ValuesIndex: %d", idx, valuesIdx));
        }
        return values;
    }

    protected Optional<T> findValue(long idx)
    {
        int valueIdx = (int) (idx % MAX_VALUES_INDEX);

        Optional<T> v = findValues(idx).get(valueIdx);
        if (v == null) {
            throw new ArrayIndexOutOfBoundsException(String.format("Index: %d, ValueIndex: %d", idx, valueIdx));
        }
        return v;
    }
}
