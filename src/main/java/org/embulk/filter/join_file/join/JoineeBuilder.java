package org.embulk.filter.join_file.join;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import org.embulk.config.ConfigException;
import org.embulk.spi.Column;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Type;
import org.msgpack.value.Value;
import pro.civitaspo.embulk.spi.DataBuilder;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.embulk.spi.type.Types.BOOLEAN;
import static org.embulk.spi.type.Types.DOUBLE;
import static org.embulk.spi.type.Types.JSON;
import static org.embulk.spi.type.Types.LONG;
import static org.embulk.spi.type.Types.STRING;
import static org.embulk.spi.type.Types.TIMESTAMP;

public class JoineeBuilder
        implements DataBuilder
{
    private final Schema schema;
    private final AtomicLong rowNumber = new AtomicLong(0);
    private final Map<Integer, Map<Long, Optional<Boolean>>> booleanData = Maps.newConcurrentMap();
    private final Map<Integer, Map<Long, Optional<String>>> stringData = Maps.newConcurrentMap();
    private final Map<Integer, Map<Long, Optional<Long>>> longData = Maps.newConcurrentMap();
    private final Map<Integer, Map<Long, Optional<Double>>> doubleData = Maps.newConcurrentMap();
    private final Map<Integer, Map<Long, Optional<Timestamp>>> timestampData = Maps.newConcurrentMap();
    private final Map<Integer, Map<Long, Optional<String>>> jsonData = Maps.newConcurrentMap(); // Embulk cannot deserialize org.msgpack.value.Value.

    public JoineeBuilder(Schema schema)
    {
        this.schema = schema;
    }

    private <T> void setValue(int columnIndex, Map<Integer, Map<Long, Optional<T>>> data, Optional<T> v)
    {
        if (!data.containsKey(columnIndex)) {
            data.put(columnIndex, Maps.newConcurrentMap());
        }
        data.get(columnIndex).put(rowNumber.get(), v);
    }

    public Joinee build()
    {
        return new Joinee(
                schema,
                booleanData,
                stringData,
                longData,
                doubleData,
                timestampData,
                jsonData,
                rowNumber.get());
    }

    @Override
    public Schema getSchema()
    {
        return schema;
    }

    @Override
    public void addRecord()
    {
        rowNumber.incrementAndGet();
    }

    @Override
    public void setNull(Column column)
    {
        setNull(column.getIndex());
    }

    @Override
    public void setNull(int columnIndex)
    {
        Column c = getSchema().getColumn(columnIndex);
        Type t = c.getType();

        if (t.equals(BOOLEAN)) {
            setValue(columnIndex, booleanData, Optional.absent());
        }
        else if (t.equals(STRING)) {
            setValue(columnIndex, stringData, Optional.absent());
        }
        else if (t.equals(LONG)) {
            setValue(columnIndex, longData, Optional.absent());
        }
        else if (t.equals(DOUBLE)) {
            setValue(columnIndex, doubleData, Optional.absent());
        }
        else if (t.equals(TIMESTAMP)) {
            setValue(columnIndex, timestampData, Optional.absent());
        }
        else if (t.equals(JSON)) {
            setValue(columnIndex, jsonData, Optional.absent());
        }
        else {
            throw new ConfigException(String.format("Unsupported t: %s", t.getName()));
        }
    }

    @Override
    public void setBoolean(Column column, boolean v)
    {
        setBoolean(column.getIndex(), v);
    }

    @Override
    public void setBoolean(int columnIndex, boolean v)
    {
        // TODO: check column type?
        setValue(columnIndex, booleanData, Optional.fromNullable(v));
    }

    @Override
    public void setString(Column column, String v)
    {
        setString(column.getIndex(), v);
    }

    @Override
    public void setString(int columnIndex, String v)
    {
        // TODO: check column type?
        setValue(columnIndex, stringData, Optional.fromNullable(v));
    }

    @Override
    public void setLong(Column column, long v)
    {
        setLong(column.getIndex(), v);
    }

    @Override
    public void setLong(int columnIndex, long v)
    {
        // TODO: check column type?
        setValue(columnIndex, longData, Optional.fromNullable(v));
    }

    @Override
    public void setDouble(Column column, double v)
    {
        setDouble(column.getIndex(), v);
    }

    @Override
    public void setDouble(int columnIndex, double v)
    {
        // TODO: check column type?
        setValue(columnIndex, doubleData, Optional.fromNullable(v));
    }

    @Override
    public void setTimestamp(Column column, Timestamp v)
    {
        setTimestamp(column.getIndex(), v);
    }

    @Override
    public void setTimestamp(int columnIndex, Timestamp v)
    {
        // TODO: check column type?
        setValue(columnIndex, timestampData, Optional.fromNullable(v));
    }

    @Override
    public void setJson(Column column, Value v)
    {
        setJson(column.getIndex(), v);
    }

    @Override
    public void setJson(int columnIndex, Value v)
    {
        // TODO: check column type?
        // TODO: use org.msgpack.value.Value if jackson-databind supports
        setValue(columnIndex, jsonData, Optional.fromNullable(v.toJson()));
    }
}
