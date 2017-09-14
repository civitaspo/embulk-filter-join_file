package org.embulk.filter.join_file.join;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import org.embulk.config.ConfigException;
import org.embulk.spi.Column;
import org.embulk.spi.Schema;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.type.Type;
import org.msgpack.value.Value;

import java.util.Map;
import java.util.function.BiConsumer;

import static org.embulk.spi.type.Types.BOOLEAN;
import static org.embulk.spi.type.Types.DOUBLE;
import static org.embulk.spi.type.Types.JSON;
import static org.embulk.spi.type.Types.LONG;
import static org.embulk.spi.type.Types.STRING;
import static org.embulk.spi.type.Types.TIMESTAMP;

public class Joinee
{
    private final Schema schema;
    private final Map<Integer, Map<Long, Optional<Boolean>>> booleanData;
    private final Map<Integer, Map<Long, Optional<String>>> stringData;
    private final Map<Integer, Map<Long, Optional<Long>>> longData;
    private final Map<Integer, Map<Long, Optional<Double>>> doubleData;
    private final Map<Integer, Map<Long, Optional<Timestamp>>> timestampData;
    private final Map<Integer, Map<Long, Optional<String>>> jsonData; // Embulk cannot deserialize org.msgpack.value.Value.
    private final long maxRowNumber;

    @JsonCreator
    public Joinee(
            @JsonProperty("schema") Schema schema,
            @JsonProperty("boolean_data") Map<Integer, Map<Long, Optional<Boolean>>> booleanData,
            @JsonProperty("string_data") Map<Integer, Map<Long, Optional<String>>> stringData,
            @JsonProperty("long_data") Map<Integer, Map<Long, Optional<Long>>> longData,
            @JsonProperty("double_data") Map<Integer, Map<Long, Optional<Double>>> doubleData,
            @JsonProperty("timestamp_data") Map<Integer, Map<Long, Optional<Timestamp>>> timestampData,
            @JsonProperty("json_data") Map<Integer, Map<Long, Optional<String>>> jsonData,
            @JsonProperty("max_row_number") long maxRowNumber)
    {
        this.schema = schema;
        this.booleanData = booleanData;
        this.stringData = stringData;
        this.longData = longData;
        this.doubleData = doubleData;
        this.timestampData = timestampData;
        this.jsonData = jsonData;
        this.maxRowNumber = maxRowNumber;
    }

    @JsonProperty("schema")
    public Schema getSchema()
    {
        return schema;
    }

    @JsonIgnore
    public JoineeOn buildJoineeOn(String onColumnName)
    {
        int onColumnIndex = schema.lookupColumn(onColumnName).getIndex();
        return buildJoineeOn(onColumnIndex);
    }

    @JsonIgnore
    public JoineeOn buildJoineeOn(int onColumnIndex)
    {
        JoineeOn.Builder builder = JoineeOn.builder();

        Column c = schema.getColumn(onColumnIndex);
        int i = c.getIndex();
        Type t = c.getType();


        if (t.equals(BOOLEAN)) {
            booleanData.get(i).forEach((k, v) -> builder.put(v, k));
        }
        else if (t.equals(STRING)) {
            stringData.get(i).forEach((k, v) -> builder.put(v, k));
        }
        else if (t.equals(LONG)) {
            longData.get(i).forEach((k, v) -> builder.put(v, k));
        }
        else if (t.equals(DOUBLE)) {
            doubleData.get(i).forEach((k, v) -> builder.put(v, k));
        }
        else if (t.equals(TIMESTAMP)) {
            timestampData.get(i).forEach((k, v) -> builder.put(v, k));
        }
        else if (t.equals(JSON)) {
            JsonParser p = new JsonParser();
            jsonData.get(i).forEach((k, v) ->
            {
                String jsonString = v.or("null");
                Value json = p.parse(jsonString);
                builder.put(Optional.of(json), k);
            });
        }
        else {
            throw new ConfigException(String.format("Unsupported t: %s", t.getName()));
        }

        return builder.build();
    }

    @JsonProperty("boolean_data")
    public Map<Integer, Map<Long, Optional<Boolean>>> getBooleanData()
    {
        return booleanData;
    }

    @JsonProperty("string_data")
    public Map<Integer, Map<Long, Optional<String>>> getStringData()
    {
        return stringData;
    }

    @JsonProperty("long_data")
    public Map<Integer, Map<Long, Optional<Long>>> getLongData()
    {
        return longData;
    }

    @JsonProperty("double_data")
    public Map<Integer, Map<Long, Optional<Double>>> getDoubleData()
    {
        return doubleData;
    }

    @JsonProperty("timestamp_data")
    public Map<Integer, Map<Long, Optional<Timestamp>>> getTimestampData()
    {
        return timestampData;
    }

    @JsonProperty("json_data")
    public Map<Integer, Map<Long, Optional<String>>> getJsonData()
    {
        return jsonData;
    }

    @JsonProperty("max_row_number")
    public long getMaxRowNumber()
    {
        return maxRowNumber;
    }
}
