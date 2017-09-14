package org.embulk.filter.join_file;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import org.embulk.config.Config;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.filter.join_file.JoinFileFilterPlugin.FileTask;
import org.embulk.filter.join_file.join.Joinee;
import org.embulk.filter.join_file.join.JoineeBuilder;
import org.embulk.filter.join_file.plugin.InternalForwardOutputPlugin;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.Schema;
import org.embulk.standards.LocalFileInputPlugin;
import org.slf4j.Logger;
import pro.civitaspo.embulk.forward.InForwardEventReader;
import pro.civitaspo.embulk.forward.InForwardService;
import pro.civitaspo.embulk.runner.AsyncEmbulkRunnerService;
import pro.civitaspo.embulk.runner.EmbulkRunner;
import pro.civitaspo.embulk.spi.ElapsedTime;
import pro.civitaspo.embulk.spi.StandardColumnVisitor;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import static org.embulk.filter.join_file.plugin.InternalForwardOutputPlugin.PLUGIN_NAME;

public class FileLoader
{
    private static final String LOCK = "ForInForwardServerCallback";
    private static final Logger logger = Exec.getLogger(FileLoader.class);


    public static Joinee loadFile(FileTask task)
    {
        FileTask.putParserColumnsIfAbsent(task);

        Schema fileSchema = task.getColumns().toSchema();
        JoineeBuilder builder = new JoineeBuilder(fileSchema);
        InForwardEventReader reader = new InForwardEventReader(fileSchema);
        StandardColumnVisitor visitor = new StandardColumnVisitor(reader, builder);

        Service inForwardService = InForwardService.builder()
                .task(task)
                .forEachEventCallback(es ->
                {
                    synchronized (LOCK) {
                        reader.setEvent(es);

                        while (reader.nextRecord()) {
                            fileSchema.visitColumns(visitor);
                            builder.addRecord();
                        }
                    }
                })
                .build();

        Service embulkRunnerService = buildEmbulkService(task);

        runServices(ImmutableList.of(inForwardService, embulkRunnerService));

        return builder.build();
    }

    private static Service buildEmbulkService(FileTask task)
    {
        EmbulkRunner.Builder builder = EmbulkRunner.builder();

        ConfigSource inputConfig = Exec.newConfigSource();
        inputConfig.set("type", "file"); // TODO: p-r to define LocalFileInputPlugin.PLUGIN_NAME = "file"
        for (Method m : LocalFileInputPlugin.PluginTask.class.getMethods()) {
            Config a = m.getAnnotation(Config.class);
            if (a == null) {
                continue;
            }
            try {
                inputConfig.set(a.value(), m.invoke(task));
            }
            catch (IllegalAccessException | InvocationTargetException e) {
                throw new ConfigException(e);
            }
        }
        inputConfig.set("parser", task.getParser());
        inputConfig.set("decoders", task.getDecorders());
        builder.inputConfig(inputConfig);

        ConfigSource outputConfig = Exec.newConfigSource();
        outputConfig.set("type", PLUGIN_NAME);
        for (Method m : InternalForwardOutputPlugin.PluginTask.class.getMethods()) {
            Config a = m.getAnnotation(Config.class);
            if (a == null) {
                continue;
            }
            try {
                outputConfig.set(a.value(), m.invoke(task));
            }
            catch (IllegalAccessException | InvocationTargetException e) {
                throw new ConfigException(e);
            }
        }
        builder.outputConfig(outputConfig);

        EmbulkRunner runner = builder.build();
        return new AsyncEmbulkRunnerService(runner);
    }

    private static void runServices(List<Service> services)
    {
        ServiceManager m = new ServiceManager(services);
        m.addListener(new ServiceManager.Listener() {
            @Override
            public void failure(Service service)
            {
                throw new DataException(service.failureCause());
            }
        });

        ElapsedTime.measureWithPolling(new ElapsedTime.Pollable<Void>() {
            @Override
            public boolean poll()
            {
                return m.isHealthy();
            }

            @Override
            public Void getResult()
            {
                return null;
            }

            @Override
            public void onStart()
            {
                logger.info("FileLoader: Start");
                m.startAsync().awaitHealthy();
            }

            @Override
            public void onWaiting(long elapsedMillis)
            {
                logger.info("FileLoader: Running (Elapsed: {}ms)", elapsedMillis);
            }

            @Override
            public void onFinished(long elapsedMillis)
            {
                logger.info("FileLoader: Finished (Elapsed: {}ms)", elapsedMillis);
                m.awaitStopped();
            }
        });
    }
}
