package org.embulk.filter.join_file;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.filter.join_file.table.JoinTableSchemaBuilder;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.time.TimestampParser;
import org.embulk.standards.LocalFileInputPlugin;

import java.util.List;

public class PluginTask
{
    public interface RootTask
            extends Task
    {
        @Config("on")
        OnTask getOn();

        @Config("file")
        FileTask getFile();
        void setFile(FileTask file);
    }

    public interface OnTask
            extends Task
    {
        @Config("input_column")
        String getInputColumn();

        @Config("file_column")
        String getFileColumn();
    }

    public interface FileTask
            extends Task, LocalFileInputPlugin.PluginTask, TimestampParser.Task
    {
        @Config("parser")
        ConfigSource getParser();
        void setParser(ConfigSource parser);

        @Config("decoders")
        @ConfigDefault("[]")
        List<ConfigSource> getDecorders();

        @Config("columns")
        SchemaConfig getColumns();

        @Config("column_prefix")
        @ConfigDefault("\"_joined_by_embulk_\"")
        String getColumnPrefix();

        @Config("parser_plugin_columns_option")
        @ConfigDefault("\"columns\"")
        String getParserPluginColumnsOption();
    }

    public static PluginTask loadConfig(ConfigSource configSource)
    {
        RootTask task = configSource.loadConfig(RootTask.class);
        return new PluginTask(task);
    }

    public static PluginTask loadTask(TaskSource taskSource)
    {
        RootTask task = taskSource.loadTask(RootTask.class);
        return new PluginTask(task);
    }

    private RootTask rootTask;

    private PluginTask(RootTask rootTask)
    {
        this.rootTask = rootTask;
        afterInitialize();
    }

    private void afterInitialize()
    {
        FileTask fileTask = getFileTask();
        ConfigSource parserTask = fileTask.getParser();
        List columnsOption = parserTask.get(List.class, fileTask.getParserPluginColumnsOption());
        if (columnsOption == null || columnsOption.isEmpty()) {
            parserTask.set(fileTask.getParserPluginColumnsOption(), getFileSchemaConfig());
            fileTask.setParser(parserTask);
            rootTask.setFile(fileTask);
        }
    }

    public RootTask getRootTask()
    {
        return rootTask;
    }

    public OnTask getOnTask()
    {
        return getRootTask().getOn();
    }

    public FileTask getFileTask()
    {
        return getRootTask().getFile();
    }

    public Schema getFileSchema()
    {
        return getFileSchemaConfig().toSchema();
    }

    public SchemaConfig getFileSchemaConfig()
    {
        return getFileTask().getColumns();
    }

    public List<ColumnConfig> getFileColumnConfigs()
    {
        return getFileSchemaConfig().getColumns();
    }

    public String getJoinTableColumnPrefix()
    {
        return getFileTask().getJoinTableColumnPrefix();
    }

    public Schema buildJoinTableSchema()
    {
        JoinTableSchemaBuilder builder = getJoinTableSchemaBuilder();
        getFileSchema()
                .getColumns()
                .forEach(c -> builder.addJoinTableColumn(c.getName(), c.getType()));
        return builder.build();
    }

    public TaskSource dump()
    {
        return getRootTask().dump();
    }

    public JoinTableSchemaBuilder getJoinTableSchemaBuilder()
    {
        return new JoinTableSchemaBuilder(getJoinTableColumnPrefix());
    }

    public Schema buildOutputSchema(Schema inputSchema)
    {
        JoinTableSchemaBuilder builder = getJoinTableSchemaBuilder();

        inputSchema
                .getColumns()
                .forEach(c -> builder.add(c.getName(), c.getType()));

        getFileSchema()
                .getColumns()
                .forEach(c -> builder.addJoinTableColumn(c.getName(), c.getType()));

        return builder.build();
    }
}
