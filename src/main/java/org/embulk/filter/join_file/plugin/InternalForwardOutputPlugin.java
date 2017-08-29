package org.embulk.filter.join_file.plugin;

import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.slf4j.Logger;
import pro.civitaspo.embulk.forward.OutForwardEventBuilder;
import pro.civitaspo.embulk.forward.OutForwardService;
import pro.civitaspo.embulk.spi.PageReader;
import pro.civitaspo.embulk.spi.StandardColumnVisitor;

import java.util.List;

public class InternalForwardOutputPlugin
        implements OutputPlugin
{
    private static final Logger logger = Exec.getLogger(InternalForwardOutputPlugin.class);
    public static final String PLUGIN_NAME = "internal_forward";

    public interface PluginTask
            extends OutForwardService.Task
    {
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, Schema schema, int taskCount, Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        control.run(task.dump());
        OutForwardService.sendShutdownMessage(task);
        return Exec.newConfigDiff();
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, Schema schema, int taskCount, Control control)
    {
        throw new ConfigException("This plugin is not resumable.");
    }

    @Override
    public void cleanup(TaskSource taskSource, Schema schema, int taskCount, List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, Schema schema, int taskIndex)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        return new TransactionalPageOutput() {
            private PageReader reader = new PageReader(schema);
            private OutForwardService service = new OutForwardService(task);
            private OutForwardEventBuilder builder = new OutForwardEventBuilder(schema, service);
            private ColumnVisitor visitor = new StandardColumnVisitor(reader, builder);

            @Override
            public void add(Page page)
            {
                reader.setPage(page);

                while (reader.nextRecord()) {
                    schema.visitColumns(visitor);
                    builder.addRecord();
                }
            }

            @Override
            public void finish()
            {
                service.finish();
            }

            @Override
            public void close()
            {
                service.close();
            }

            @Override
            public void abort()
            {
                logger.warn("output plugin: {} does not support #abort", PLUGIN_NAME);
            }

            @Override
            public TaskReport commit()
            {
                return Exec.newTaskReport();
            }
        };
    }
}
