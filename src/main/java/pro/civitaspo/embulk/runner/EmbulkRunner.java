package pro.civitaspo.embulk.runner;

import com.google.common.collect.Lists;
import org.embulk.EmbulkEmbed;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.exec.ExecutionResult;
import org.embulk.guice.LifeCycleInjector;
import org.embulk.spi.Exec;
import org.slf4j.Logger;
import pro.civitaspo.embulk.spi.ElapsedTime;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Optional;

public class EmbulkRunner
{
    private static final Logger logger = Exec.getLogger(EmbulkRunner.class);

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private ConfigSource execConfig;
        private ConfigSource inputConfig;
        private List<ConfigSource> filtersConfig;
        private ConfigSource outputConfig;

        public Builder()
        {
        }

        public Builder execConfig(ConfigSource execConfig)
        {
            this.execConfig = execConfig;
            return this;
        }

        public Builder inputConfig(ConfigSource inputConfig)
        {
            this.inputConfig = inputConfig;
            return this;
        }

        public Builder filterConfig(List<ConfigSource> filtersConfig)
        {
            this.filtersConfig = filtersConfig;
            return this;
        }

        public Builder outputConfig(ConfigSource outputConfig)
        {
            this.outputConfig = outputConfig;
            return this;
        }

        public EmbulkRunner build()
        {
            return new EmbulkRunner(buildConfig());
        }

        protected ConfigSource buildConfig()
        {
            ConfigSource config = Exec.newConfigSource();

            config.set("exec", Optional.ofNullable(execConfig).orElse(Exec.newConfigSource()));
            config.set("in", Optional.ofNullable(inputConfig).orElseThrow(() -> new ConfigException("in: is null.")));
            config.set("filters", Optional.ofNullable(filtersConfig).orElse(Lists.newArrayList()));
            config.set("out", Optional.ofNullable(outputConfig).orElseThrow(() -> new ConfigException("out: is null.")));

            return config;
        }
    }

    private final ConfigSource config;

    EmbulkRunner(ConfigSource config)
    {
        this.config = config;
    }

    private EmbulkEmbed newEmbulkEmbed()
    {
        try {
            Constructor<EmbulkEmbed> constructor = EmbulkEmbed.class
                    .getDeclaredConstructor(ConfigSource.class, LifeCycleInjector.class);
            constructor.setAccessible(true);
            return constructor.newInstance(null, Exec.getInjector());
        }
        catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new ConfigException(e);
        }
    }

    public ExecutionResult run()
    {
        // TODO: expose config without secret configurations.
        
        return ElapsedTime.measure(new ElapsedTime.Measurable<ExecutionResult>()
        {
            @Override
            public void onStart()
            {
                logger.info("Start: Embulk Run");
            }

            @Override
            public void onFinished(long elapsedMillis)
            {
                logger.info("Finished: Embulk Run (Elapsed: {} ms)", elapsedMillis);
            }

            @Override
            public ExecutionResult run()
            {
                return newEmbulkEmbed().run(config);
            }
        });
    }
}
