package pro.civitaspo.embulk.forward;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.embulk.spi.Column;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.msgpack.value.Value;
import pro.civitaspo.embulk.spi.DataBuilder;

import java.util.List;
import java.util.Map;

public class OutForwardEventBuilder
        implements DataBuilder
{
    private final static String VALUES_KEY = "values";

    private final Schema schema;
    private final OutForwardService outForward;

    private List<Object> values;

    public OutForwardEventBuilder(
            Schema schema,
            OutForwardService outForward)
    {
        this.schema = schema;
        this.outForward = outForward;

        setNewMessage();
    }

    private void setNewMessage()
    {
        this.values = Lists.newArrayListWithCapacity(schema.getColumnCount());
    }

    @Override
    public Schema getSchema()
    {
        return schema;
    }

    @Override
    public void addRecord()
    {
        Map<String, Object> message = Maps.newHashMap();
        message.put(VALUES_KEY, values);
        outForward.emit(message);
        setNewMessage();
    }

    private void setValue(int columnIndex, Object v)
    {
        values.add(columnIndex, v);
    }

    @Override
    public void setNull(Column column)
    {
        setNull(column.getIndex());
    }

    @Override
    public void setNull(int columnIndex)
    {
        setValue(columnIndex, null);
    }

    @Override
    public void setBoolean(Column column, boolean v)
    {
        setBoolean(column.getIndex(), v);
    }

    @Override
    public void setBoolean(int columnIndex, boolean v)
    {
        setValue(columnIndex, v);
    }

    @Override
    public void setString(Column column, String v)
    {
        setString(column.getIndex(), v);
    }

    @Override
    public void setString(int columnIndex, String v)
    {
        setValue(columnIndex, v);
    }

    @Override
    public void setLong(Column column, long v)
    {
        setLong(column.getIndex(), v);
    }

    @Override
    public void setLong(int columnIndex, long v)
    {
        setValue(columnIndex, v);
    }

    @Override
    public void setDouble(Column column, double v)
    {
        setDouble(column.getIndex(), v);
    }

    @Override
    public void setDouble(int columnIndex, double v)
    {
        setValue(columnIndex, v);
    }

    @Override
    public void setTimestamp(Column column, Timestamp v)
    {

        setTimestamp(column.getIndex(), v);
    }

    @Override
    public void setTimestamp(int columnIndex, Timestamp v)
    {
        // TODO: confirm correct value is stored
        long epochSecond = v.getEpochSecond();
        long nanoAdjustment = v.getNano();
        setValue(columnIndex, new long[]{epochSecond, nanoAdjustment});
    }

    @Override
    public void setJson(Column column, Value v)
    {
        setJson(column.getIndex(), v);
    }

    @Override
    public void setJson(int columnIndex, Value v)
    {
        // TODO: confirm correct value is stored
        setValue(columnIndex, v);
    }
}
