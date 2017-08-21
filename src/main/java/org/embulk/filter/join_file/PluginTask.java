package org.embulk.filter.join_file;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.Page;
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
        @Config("in_column")
        String getInColumn();

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

        @Config("joined_column_prefix")
        @ConfigDefault("\"_joined_by_embulk_\"")
        String getJoinedColumnPrefix();
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
        configure();
    }

    private void configure()
    {
        ToJoinedColumnName.configure(this);
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

    public SchemaConfig getFileSchemaConfig()
    {
        return getFileTask().getColumns();
    }

    public List<ColumnConfig> getFileColumnConfigs()
    {
        return getFileSchemaConfig().getColumns();
    }

    public String getJoinedColumnPrefix()
    {
        return getFileTask().getJoinedColumnPrefix();
    }

    public String convertToJoinedColumnName(String original)
    {
        return ToJoinedColumnName.convert(original);
    }

    public TaskSource dump()
    {
        return getRootTask().dump();
    }

    public Schema buildOutputSchema(Schema inputSchema)
    {
        final Schema.Builder builder = Schema.builder();
        inputSchema.getColumns().forEach(c ->
                builder.add(c.getName(), c.getType()));
        getFileColumnConfigs().forEach(cc ->
                builder.add(convertToJoinedColumnName(cc.getName()), cc.getType()));
        return builder.build();
    }
}
