package org.embulk.filter.join_file;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.embulk.config.ConfigException;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.Exec;
import org.slf4j.Logger;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by takahiro.nakayama on 10/11/15.
 */
public class TableBuilder
{
    private final Logger logger = Exec.getLogger(TableBuilder.class);
    private final String filePath;
    private final String fileFormat;
    private final List<ColumnConfig> columns;
    private final String rowKeyName;
    private final String columnPrefix;

    public TableBuilder(String filePath, String fileFormat, List<ColumnConfig> columns, String rowKeyName, String columnPrefix)
    {
        this.filePath = filePath;
        this.fileFormat = fileFormat;
        this.columns = columns;
        this.rowKeyName = rowKeyName;
        this.columnPrefix = columnPrefix;
    }

    public HashMap<String, HashMap<String, String>> build()
            throws IOException
    {
        HashMap<String, HashMap<String, String>> table = Maps.newHashMap();

        for (HashMap<String, String> rawRecord: loadFile()) {

            HashMap<String, String> record = Maps.newHashMap();

            for (ColumnConfig column: columns) {
                String columnKey = columnPrefix + column.getName();
                String value = rawRecord.get(column.getName());

                record.put(columnKey, value);
            }

            String rowKey = rawRecord.get(rowKeyName);
            table.put(rowKey, record);
        }

        return table;
    }

    private List<HashMap<String, String>> loadFile()
            throws IOException
    {
        List<HashMap<String, String>> rawData;
        switch (fileFormat) {
            case "csv":
                logger.error("will support csv format, but not yet.");
                throw new NotImplementedException(); // TODO: will support csv format, but not yet.
            case "tsv":
                logger.error("will support tsv format, but not yet.");
                throw new NotImplementedException(); // TODO: will support tsv format, but not yet.
            case "yaml":
                logger.error("will support yaml format, but not yet.");
                throw new NotImplementedException(); // TODO: will support yaml format, but not yet.
            case "json":
                ObjectMapper mapper = new ObjectMapper();
                rawData = mapper.readValue(new File(filePath), new TypeReference<ArrayList<HashMap<String, String>>>(){});
                break;
            default:
                throw new ConfigException("Unsupported File Format: " + fileFormat);
        }

        return rawData;
    }
}
