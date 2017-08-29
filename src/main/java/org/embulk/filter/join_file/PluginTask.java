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
    }

    public interface OnTask
            extends Task
    {
        @Config("page_column")
        String getPageColumn();

        @Config("file_column")
        String getFileColumn();
    }

    public interface FileTask
            extends Task, LocalFileInputPlugin.PluginTask, TimestampParser.Task
    {
        @Config("parser")
        ConfigSource getParser();

        @Config("decoders")
        @ConfigDefault("[]")
        List<ConfigSource> getDecorders();

        @Config("columns")
        SchemaConfig getColumns();

        @Config("join_table_column_prefix")
        @ConfigDefault("\"_join_by_embulk_\"")
        String getJoinTableColumnPrefix();
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

    private final RootTask rootTask;

    private PluginTask(RootTask rootTask)
    {
        this.rootTask = rootTask;
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
