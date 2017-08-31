package org.embulk.filter.join_file.plugin;

import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Extension;

import java.util.List;

public class InternalPluginExtention
        implements Extension
{
    @Override
    public List<Module> getModules(ConfigSource systemConfig) // TODO: need META-INF?
    {
        return ImmutableList.of(new InternalPluginModule());
    }
}
