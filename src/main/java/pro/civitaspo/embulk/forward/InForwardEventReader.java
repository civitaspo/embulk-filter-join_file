package pro.civitaspo.embulk.forward;

import com.google.common.collect.Lists;
import influent.EventStream;
import org.embulk.spi.Column;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.msgpack.value.Value;
import pro.civitaspo.embulk.spi.DataReader;

import java.util.List;
import java.util.Optional;

public class InForwardEventReader
        implements DataReader
{
    private final static String VALUES_KEY = "values";

    private final Schema schema;
    private EventStream event = null;
    private int eventMessageCount = 0;

    private int readCount = 0;
    private List<Value> values;

    public InForwardEventReader(Schema schema)
    {
        this.schema = schema;
    }

    public void setEvent(EventStream event)
    {
        this.event = event;
        this.eventMessageCount = event.getEntries().size();
    }

    @Override
    public Schema getSchema()
    {
        return schema;
    }

    @Override
    public boolean nextRecord()
    {
        if (eventMessageCount <= readCount) {
            return false;
        }

        Optional<List<Value>> values = event.getEntries().get(readCount++).getRecord().entrySet()
                .stream()
                .filter(valueValueEntry -> valueValueEntry.getKey().asStringValue().asString().contentEquals(VALUES_KEY))
                .map(valueValueEntry -> valueValueEntry.getValue().asArrayValue().list())
                .findFirst();

        this.values = values.orElse(Lists.newArrayListWithCapacity(schema.getColumnCount()));
        return true;
    }

    private Value getValue(int columnIndex)
    {
        return values.get(columnIndex);
    }

    @Override
    public boolean isNull(Column column)
    {
        return isNull(column.getIndex());
    }

    @Override
    public boolean isNull(int columnIndex)
    {
        return getValue(columnIndex).isNilValue();
    }

    @Override
    public boolean getBoolean(Column column)
    {
        return getBoolean(column.getIndex());
    }

    @Override
    public boolean getBoolean(int columnIndex)
    {
        return getValue(columnIndex).asBooleanValue().getBoolean();
    }

    @Override
    public String getString(Column column)
    {
        return getString(column.getIndex());
    }

    @Override
    public String getString(int columnIndex)
    {
        return getValue(columnIndex).asStringValue().asString();
    }

    @Override
    public long getLong(Column column)
    {
        return getLong(column.getIndex());
    }

    @Override
    public long getLong(int columnIndex)
    {
        return getValue(columnIndex).asIntegerValue().asLong();
    }

    @Override
    public double getDouble(Column column)
    {
        return getDouble(column.getIndex());
    }

    @Override
    public double getDouble(int columnIndex)
    {
        return getValue(columnIndex).asFloatValue().toDouble();
    }

    @Override
    public Timestamp getTimestamp(Column column)
    {
        return getTimestamp(column.getIndex());
    }

    @Override
    public Timestamp getTimestamp(int columnIndex)
    {
        List<Value> seed = getValue(columnIndex).asArrayValue().list();
        long epochSecond = seed.get(0).asIntegerValue().asLong();
        long nanoAdjustment = seed.get(1).asIntegerValue().asLong();
        return Timestamp.ofEpochSecond(epochSecond, nanoAdjustment);
    }

    @Override
    public Value getJson(Column column)
    {
        return getJson(column.getIndex());
    }

    @Override
    public Value getJson(int columnIndex)
    {
        return getValue(columnIndex);
    }

}
