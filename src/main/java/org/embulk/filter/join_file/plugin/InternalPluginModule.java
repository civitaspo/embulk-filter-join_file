package org.embulk.filter.join_file.plugin;

import com.google.common.base.Preconditions;
import com.google.inject.Binder;
import com.google.inject.Module;
import org.embulk.spi.OutputPlugin;

import static org.embulk.filter.join_file.plugin.InternalForwardOutputPlugin.PLUGIN_NAME;
import static org.embulk.plugin.InjectedPluginSource.registerPluginTo;

public class InternalPluginModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        Preconditions.checkNotNull(binder);
        registerPluginTo(binder, OutputPlugin.class, PLUGIN_NAME, InternalForwardOutputPlugin.class);
    }
}
