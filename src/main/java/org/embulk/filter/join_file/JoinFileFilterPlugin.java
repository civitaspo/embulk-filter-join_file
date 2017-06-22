package org.embulk.filter.join_file;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.Exec;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.type.Types;
import org.joda.time.DateTimeZone;
import org.jruby.embed.ScriptingContainer;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JoinFileFilterPlugin
        implements FilterPlugin
{
    private static final Logger logger = Exec.getLogger(JoinFileFilterPlugin.class);

    public interface PluginTask
            extends Task, TimestampParser.Task
    {
        @Config("base_column")
        public ColumnConfig getBaseColumn();

        @Config("counter_column")
        @ConfigDefault("{name: id, type: long}")
        public ColumnConfig getCounterColumn();

        @Config("joined_column_prefix")
        @ConfigDefault("\"_joined_by_embulk_\"")
        public String getJoinedColumnPrefix();

        @Config("file_path")
        public String getFilePath();

        @Config("file_format")
        public String getFileFormat();

        @Config("columns")
        public List<ColumnConfig> getColumns();

        @Config("time_zone")
        @ConfigDefault("\"UTC\"")
        public String getTimeZone();

        public HashMap<String, HashMap<String, String>> getTable();
        public void setTable(HashMap<String, HashMap<String, String>> jsonTable);
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        try {
            TableBuilder tableBuilder = new TableBuilder(
                    task.getFilePath(),
                    task.getFileFormat(),
                    task.getColumns(),
                    task.getCounterColumn().getName(),
                    task.getJoinedColumnPrefix());
            
            task.setTable(tableBuilder.build());
        }
        catch (IOException e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }

        Schema outputSchema = buildOutputSchema(inputSchema, task.getColumns(), task.getJoinedColumnPrefix());
        logger.info("output schema: {}", outputSchema);
        
        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, Schema inputSchema,
            Schema outputSchema, PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        // create joinColumns/baseColumn
        final List<Column> outputColumns = outputSchema.getColumns();
        final List<Column> inputColumns = inputSchema.getColumns();

        Map<String, Column> inputColumnMap = Maps.newHashMap();
        final List<Column> joinColumns = new ArrayList<>();
        for (Column column : outputColumns) {
            if (!inputColumns.contains(column)) {
                joinColumns.add(column);
            } else {
                inputColumnMap.put(column.getName(), column);
            }
        }

        final Column baseColumn = inputColumnMap.get(task.getBaseColumn().getName());

        final HashMap<String, TimestampParser> timestampParserMap = buildTimestampParserMap(
                task.getJRuby(),
                task.getColumns(),
                task.getJoinedColumnPrefix(),
                task.getTimeZone());

        return new JoinFilePageOutput(inputSchema, outputSchema, baseColumn, task.getTable(), joinColumns, timestampParserMap, output);
    }

    private Schema buildOutputSchema(Schema inputSchema, List<ColumnConfig> columns, String joinedColumnPrefix)
    {
        ImmutableList.Builder<Column> builder = ImmutableList.builder();

        int i = 0; // columns index
        for (Column inputColumn: inputSchema.getColumns()) {
            Column outputColumn = new Column(i++, inputColumn.getName(), inputColumn.getType());
            builder.add(outputColumn);
        }
        for (ColumnConfig columnConfig: columns) {
            String columnName = joinedColumnPrefix + columnConfig.getName();
            builder.add(new Column(i++, columnName, columnConfig.getType()));
        }

        return new Schema(builder.build());
    }

    private HashMap<String, TimestampParser> buildTimestampParserMap(ScriptingContainer jruby, List<ColumnConfig> columns, String joinedColumnPrefix, String timeZone)
    {
        final HashMap<String, TimestampParser> timestampParserMap = Maps.newHashMap();
        for (ColumnConfig columnConfig: columns) {
            if (Types.TIMESTAMP.equals(columnConfig.getType())) {
                String format = columnConfig.getOption().get(String.class, "format");
                DateTimeZone timezone = DateTimeZone.forID(timeZone);
                TimestampParser parser = new TimestampParser(jruby, format, timezone);

                String columnName = joinedColumnPrefix + columnConfig.getName();

                timestampParserMap.put(columnName, parser);
            }
        }

        return timestampParserMap;
    }
}
