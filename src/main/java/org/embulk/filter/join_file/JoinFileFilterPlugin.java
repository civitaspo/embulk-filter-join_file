package org.embulk.filter.join_file;

import com.google.common.collect.Lists;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.filter.join_file.join.Joinee;
import org.embulk.spi.Column;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;
import org.embulk.standards.LocalFileInputPlugin;
import pro.civitaspo.embulk.forward.InForwardService;
import pro.civitaspo.embulk.forward.OutForwardService;

import java.util.List;

public class JoinFileFilterPlugin
        implements FilterPlugin
{
    public interface PluginTask
            extends Task
    {
        @Config("on")
        OnTask getOnTask();

        @Config("file")
        FileTask getFileTask();

        Joinee getJoinee();
        void setJoinee(Joinee joinee);

        static Schema buildOutputSchema(PluginTask task, Schema inputSchema)
        {
            Schema.Builder builder = Schema.builder();

            for (Column c : inputSchema.getColumns()) {
                builder.add(c.getName(), c.getType());
            }

            FileTask fileTask = task.getFileTask();
            Schema joineeSchema = fileTask.getColumns().toSchema();
            for (Column c : joineeSchema.getColumns()) {
                String joinedColumn = new StringBuilder()
                        // NOTE: `FileTask#getColumnPrefix()` is only used here,
                        //       because this plugin manage columns only by index hereafter.
                        .append(fileTask.getColumnPrefix())
                        .append(c.getName())
                        .toString();
                builder.add(joinedColumn, c.getType());
            }

            return builder.build();
        }
    }

    public interface OnTask
            extends Task
    {
        @Config("input_column")
        String getInputColumnName();

        @Config("file_column")
        String getFileColumnName();
    }

    public interface FileTask
            extends Task, LocalFileInputPlugin.PluginTask, InForwardService.Task, OutForwardService.Task
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

        // NOTE: This plugin doesn't worry about format and timezone,
        //       because the responsibility is transferred to Embulk Parser Plugin.
        static void putParserColumnsIfAbsent(FileTask task)
        {
            ConfigSource parserTask = task.getParser();
            List columnsOption = parserTask.get(List.class, task.getParserPluginColumnsOption(), null);
            if (columnsOption == null || columnsOption.isEmpty()) {
                parserTask.set(task.getParserPluginColumnsOption(), task.getColumns());
                task.setParser(parserTask);
            }
        }
    }

    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        Schema outputSchema = PluginTask.buildOutputSchema(task, inputSchema);

        Joinee joinee = FileLoader.loadFile(task.getFileTask());
        task.setJoinee(joinee);

        task.getFileTask().setFiles(Lists.newArrayList()); // TODO: why is this required?

        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, Schema inputSchema,
            Schema outputSchema, PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        return new JoinFilePageOutput(task, inputSchema, outputSchema, output);
    }
}
