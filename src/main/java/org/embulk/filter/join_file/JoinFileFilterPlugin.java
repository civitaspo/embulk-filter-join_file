package org.embulk.filter.join_file;

import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.spi.FilterPlugin;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;

public class JoinFileFilterPlugin
        implements FilterPlugin
{
    @Override
    public void transaction(ConfigSource config, Schema inputSchema,
            FilterPlugin.Control control)
    {
        PluginTask task = PluginTask.loadConfig(config);

        Schema outputSchema = inputSchema;

        control.run(task.dump(), outputSchema);
    }

    @Override
    public PageOutput open(TaskSource taskSource, Schema inputSchema,
            Schema outputSchema, PageOutput output)
    {
        PluginTask task = PluginTask.loadTask(taskSource);

        // Write your code here :)
        throw new UnsupportedOperationException("JoinFileFilterPlugin.open method is not implemented yet");
    }
}
