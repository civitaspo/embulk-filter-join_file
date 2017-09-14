package pro.civitaspo.embulk.spi;

import org.embulk.spi.Column;
import org.embulk.spi.Schema;
import org.embulk.spi.time.Timestamp;
import org.msgpack.value.Value;

public interface DataReader
{
    Schema getSchema();

    boolean nextRecord();

    boolean isNull(Column column);

    boolean isNull(int columnIndex);

    boolean getBoolean(Column column);

    boolean getBoolean(int columnIndex);

    String getString(Column column);

    String getString(int columnIndex);

    long getLong(Column column);

    long getLong(int columnIndex);

    double getDouble(Column column);

    double getDouble(int columnIndex);

    Timestamp getTimestamp(Column column);

    Timestamp getTimestamp(int columnIndex);

    Value getJson(Column column);

    Value getJson(int columnIndex);
}
