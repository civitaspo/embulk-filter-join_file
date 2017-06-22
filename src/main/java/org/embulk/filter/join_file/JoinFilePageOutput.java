package org.embulk.filter.join_file;

import com.google.common.base.Throwables;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.type.Types;

import java.util.HashMap;
import java.util.List;

/**
 * Created by takahiro.nakayama on 10/11/15.
 */
public class JoinFilePageOutput
        implements PageOutput
{
    private final org.slf4j.Logger logger = Exec.getLogger(JoinFilePageOutput.class);
    private final PageReader pageReader;
    private final PageBuilder pageBuilder;
    private final Column joinBaseColumn;
    private final HashMap<String, HashMap<String, String>> table;
    private final List<Column> joinColumns;
    private final HashMap<String, TimestampParser> timestampParserMap;

    JoinFilePageOutput(
            Schema inputSchema,
            Schema outputSchema,
            Column joinBaseColumn,
            HashMap<String, HashMap<String, String>> table,
            List<Column> joinColumns,
            HashMap<String, TimestampParser> timestampParserMap,
            PageOutput pageOutput
            )
    {
        this.pageReader = new PageReader(inputSchema);
        this.pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, pageOutput);
        this.joinBaseColumn = joinBaseColumn;
        this.table = table;
        this.joinColumns = joinColumns;
        this.timestampParserMap = timestampParserMap;
    }


    @Override
    public void add(Page page)
    {
        pageReader.setPage(page);

        while (pageReader.nextRecord()) {
            setInputValues(pageBuilder);
            setJoinedValues(pageBuilder);
            pageBuilder.addRecord();
        }
    }

    @Override
    public void finish()
    {
        pageBuilder.finish();
    }

    @Override
    public void close()
    {
        pageReader.close();
        pageBuilder.close();
    }

    private void setInputValues(PageBuilder pageBuilder) {
        for (Column inputColumn: pageReader.getSchema().getColumns()) {
            if (pageReader.isNull(inputColumn)) {
                pageBuilder.setNull(inputColumn);
                continue;
            }

            if (Types.STRING.equals(inputColumn.getType())) {
                pageBuilder.setString(inputColumn, pageReader.getString(inputColumn));
            }
            else if (Types.BOOLEAN.equals(inputColumn.getType())) {
                pageBuilder.setBoolean(inputColumn, pageReader.getBoolean(inputColumn));
            }
            else if (Types.DOUBLE.equals(inputColumn.getType())) {
                pageBuilder.setDouble(inputColumn, pageReader.getDouble(inputColumn));
            }
            else if (Types.LONG.equals(inputColumn.getType())) {
                pageBuilder.setLong(inputColumn, pageReader.getLong(inputColumn));
            }
            else if (Types.TIMESTAMP.equals(inputColumn.getType())) {
                pageBuilder.setTimestamp(inputColumn, pageReader.getTimestamp(inputColumn));
            }
        }
    }

    private void setJoinedValues(PageBuilder pageBuilder) {
        for (Column column: joinColumns) {
            // get value from Table
            String rowKey = getCurrentJoinBaseColumnValue(pageReader, joinBaseColumn);
            if (!table.containsKey(rowKey) || !table.get(rowKey).containsKey(column.getName())) {
                pageBuilder.setNull(column);
                continue;
            }

            String value = table.get(rowKey).get(column.getName());
            if (value == null) {
                pageBuilder.setNull(column);
                continue;
            }

            if (Types.STRING.equals(column.getType())) {
                pageBuilder.setString(column, value);
            }
            else if (Types.BOOLEAN.equals(column.getType())) {
                pageBuilder.setBoolean(column, Boolean.parseBoolean(value));
            }
            else if (Types.DOUBLE.equals(column.getType())) {
                pageBuilder.setDouble(column, Double.parseDouble(value));
            }
            else if (Types.LONG.equals(column.getType())) {
                pageBuilder.setLong(column, Long.parseLong(value));
            }
            else if (Types.TIMESTAMP.equals(column.getType())) {
                TimestampParser parser = timestampParserMap.get(column.getName());
                pageBuilder.setTimestamp(column, parser.parse(value));
            }
        }
    }

    private static String getCurrentJoinBaseColumnValue(PageReader pageReader, Column joinBaseColumn)
    {
        if (pageReader.isNull(joinBaseColumn)) {
            return null;
        }

        if (Types.STRING.equals(joinBaseColumn.getType())) {
            return pageReader.getString(joinBaseColumn);
        }
        else if (Types.BOOLEAN.equals(joinBaseColumn.getType())) {
            return String.valueOf(pageReader.getBoolean(joinBaseColumn));
        }
        else if (Types.DOUBLE.equals(joinBaseColumn.getType())) {
            return String.valueOf(pageReader.getDouble(joinBaseColumn));
        }
        else if (Types.LONG.equals(joinBaseColumn.getType())) {
            return String.valueOf(pageReader.getLong(joinBaseColumn));
        }
        else if (Types.TIMESTAMP.equals(joinBaseColumn.getType())) {
            return String.valueOf(pageReader.getTimestamp(joinBaseColumn));
        }

        throw Throwables.propagate(new Throwable("Unsupported Column Type: " + joinBaseColumn.getType()));
    }
}
