package org.embulk.filter.join_file.join;

import com.google.common.base.Optional;
import org.embulk.config.ConfigException;
import org.embulk.spi.Column;
import org.embulk.spi.Schema;
import org.embulk.spi.json.JsonParseException;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Type;
import org.msgpack.value.Value;
import pro.civitaspo.embulk.spi.DataReader;

import static org.embulk.spi.type.Types.BOOLEAN;
import static org.embulk.spi.type.Types.DOUBLE;
import static org.embulk.spi.type.Types.JSON;
import static org.embulk.spi.type.Types.LONG;
import static org.embulk.spi.type.Types.STRING;
import static org.embulk.spi.type.Types.TIMESTAMP;

public class JoineeReader
{
    private final Joinee joinee;
    private final JoineeOn on;
    private long currentRowNumber = 0L;

    private final JsonParser jsonParser = new JsonParser();

    public JoineeReader(Joinee joinee, JoineeOn joineeOn)
    {
        this.joinee = joinee;
        this.on = joineeOn;
    }

    public Schema getSchema()
    {
        return joinee.getSchema();
    }

    public void updateJoineeRowNumber(DataReader dataReader, Column onReaderColumn)
    {
        Type t = onReaderColumn.getType();

        if (t.equals(BOOLEAN)) {
            boolean v = dataReader.getBoolean(onReaderColumn);
            this.currentRowNumber = on.get(Optional.fromNullable(v));
        }
        else if (t.equals(STRING)) {
            String v = dataReader.getString(onReaderColumn);
            this.currentRowNumber = on.get(Optional.fromNullable(v));
        }
        else if (t.equals(LONG)) {
            long v = dataReader.getLong(onReaderColumn);
            this.currentRowNumber = on.get(Optional.fromNullable(v));
        }
        else if (t.equals(DOUBLE)) {
            double v = dataReader.getDouble(onReaderColumn);
            this.currentRowNumber = on.get(Optional.fromNullable(v));
        }
        else if (t.equals(TIMESTAMP)) {
            Timestamp v = dataReader.getTimestamp(onReaderColumn);
            this.currentRowNumber = on.get(Optional.fromNullable(v));
        }
        else if (t.equals(JSON)) {
            Value v = dataReader.getJson(onReaderColumn);
            this.currentRowNumber = on.get(Optional.fromNullable(v));
        }
        else {
            throw new ConfigException(String.format("Unsupported t: %s", t.getName()));
        }
    }

    public boolean isNull(int columnIndex)
    {
        Column c = getSchema().getColumn(columnIndex);
        Type t = c.getType();

        if (t.equals(BOOLEAN)) {
            return !joinee.getBooleanData().get(columnIndex).get(currentRowNumber).isPresent();
        }
        else if (t.equals(STRING)) {
            return !joinee.getStringData().get(columnIndex).get(currentRowNumber).isPresent();
        }
        else if (t.equals(LONG)) {
            return !joinee.getLongData().get(columnIndex).get(currentRowNumber).isPresent();
        }
        else if (t.equals(DOUBLE)) {
            return !joinee.getDoubleData().get(columnIndex).get(currentRowNumber).isPresent();
        }
        else if (t.equals(TIMESTAMP)) {
            return !joinee.getTimestampData().get(columnIndex).get(currentRowNumber).isPresent();
        }
        else if (t.equals(JSON)) {
            return !joinee.getJsonData().get(columnIndex).get(currentRowNumber).isPresent();
        }
        else {
            throw new ConfigException(String.format("Unsupported t: %s", t.getName()));
        }
    }

    public boolean getBoolean(int columnIndex)
    {
        // TODO: check column type?
        return joinee.getBooleanData().get(columnIndex).get(currentRowNumber).get();
    }

    public String getString(int columnIndex)
    {
        // TODO: check column type?
        return joinee.getStringData().get(columnIndex).get(currentRowNumber).get();
    }

    public long getLong(int columnIndex)
    {
        // TODO: check column type?
        return joinee.getLongData().get(columnIndex).get(currentRowNumber).get();
    }

    public double getDouble(int columnIndex)
    {
        // TODO: check column type?
        return joinee.getDoubleData().get(columnIndex).get(currentRowNumber).get();
    }

    public Timestamp getTimestamp(int columnIndex)
    {
        // TODO: check column type?
        return joinee.getTimestampData().get(columnIndex).get(currentRowNumber).get();
    }

    public Value getJson(int columnIndex)
    {
        // TODO: check column type?
        // TODO: use org.msgpack.value.Value if jackson-databind supports
        String jsonString = joinee.getJsonData().get(columnIndex).get(currentRowNumber).get();
        return jsonParser.parse(jsonString);
    }
}
