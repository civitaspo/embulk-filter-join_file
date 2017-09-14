package pro.civitaspo.embulk.spi;

import org.embulk.spi.Column;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.msgpack.value.Value;

public interface DataBuilder
{
    Schema getSchema();

    void addRecord();

    void setNull(Column column);

    void setNull(int columnIndex);

    void setBoolean(Column column, boolean v);

    void setBoolean(int columnIndex, boolean v);

    void setString(Column column, String v);

    void setString(int columnIndex, String v);

    void setLong(Column column, long v);

    void setLong(int columnIndex, long v);

    void setDouble(Column column, double v);

    void setDouble(int columnIndex, double v);

    void setTimestamp(Column column, Timestamp v);

    void setTimestamp(int columnIndex, Timestamp v);

    void setJson(Column column, Value v);

    void setJson(int columnIndex, Value v);
}
