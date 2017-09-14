package pro.civitaspo.embulk.forward;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import influent.EventStream;
import influent.Tag;
import influent.forward.ForwardCallback;
import influent.forward.ForwardServer;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.slf4j.Logger;
import pro.civitaspo.embulk.spi.ElapsedTime;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class InForwardService
        extends AbstractExecutionThreadService
{
    private final static Logger logger = Exec.getLogger(InForwardService.class);

    public interface InForwardTask
            extends org.embulk.config.Task
    {
        @Config("port")
        @ConfigDefault("24224")
        int getPort();

        @Config("chunk_size_limit")
        @ConfigDefault("null")
        Optional<Long> getChunkSizeLimit();

        @Config("backlog")
        @ConfigDefault("null")
        Optional<Integer> getBacklog();

        @Config("send_buffer_size")
        @ConfigDefault("null")
        Optional<Integer> getSendBufferSize();

        @Config("receive_buffer_size")
        @ConfigDefault("null")
        Optional<Integer> getReceiveBufferSize();

        @Config("keep_alive_enabled")
        @ConfigDefault("null")
        Optional<Boolean> getKeepAliveEnabled();

        @Config("tcp_no_delay_enabled")
        @ConfigDefault("null")
        Optional<Boolean> getTcpNoDelayEnabled();
    }

    public interface Task
            extends ForwardParentTask
    {
        @Config("in_forward")
        @ConfigDefault("{}")
        InForwardTask getInForwardTask();
    }

    public static class Builder
    {
        private Task task;
        private Consumer<EventStream> eventConsumer;

        public Builder()
        {
        }

        public Builder task(Task task)
        {
            this.task = task;
            return this;
        }

        public Builder forEachEventCallback(Consumer<EventStream> eventConsumer)
        {
            this.eventConsumer = eventConsumer;
            return this;
        }

        public InForwardService build()
        {
            return new InForwardService(task, eventConsumer);
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    private final Task task;
    private final ForwardServer server;
    private final AtomicBoolean shouldShutdown = new AtomicBoolean(false);

    private InForwardService(Task task, Consumer<EventStream> eventConsumer)
    {
        this.task = task;
        this.server = buildServer(task.getInForwardTask(), eventConsumer);
    }

    private ForwardServer buildServer(InForwardTask t, Consumer<EventStream> eventConsumer)
    {
        ForwardServer.Builder builder = new ForwardServer.Builder(wrapEventConsumer(eventConsumer));

        builder.localAddress(t.getPort());

        if (t.getChunkSizeLimit().isPresent()) {
            builder.chunkSizeLimit(t.getChunkSizeLimit().get());
        }
        if (t.getBacklog().isPresent()) {
            builder.backlog(t.getBacklog().get());
        }
        if (t.getSendBufferSize().isPresent()) {
            builder.sendBufferSize(t.getSendBufferSize().get());
        }
        if (t.getReceiveBufferSize().isPresent()) {
            builder.receiveBufferSize(t.getReceiveBufferSize().get());
        }
        if (t.getKeepAliveEnabled().isPresent()) {
            builder.keepAliveEnabled(t.getKeepAliveEnabled().get());
        }
        if (t.getTcpNoDelayEnabled().isPresent()) {
            builder.tcpNoDelayEnabled(t.getTcpNoDelayEnabled().get());
        }
        // TODO: builder.workerPoolSize(1);

        return builder.build();
    }

    private ForwardCallback wrapEventConsumer(Consumer<EventStream> eventConsumer)
    {
        return ForwardCallback.of(es ->
        {
            if (isShutdownTag(es.getTag())) {
                logger.info("Receive shutdown tag: {}", es.getTag());
                shouldShutdown.set(true);
            }
            else if (isMessageTag(es.getTag())) {
                eventConsumer.accept(es);
            }
            else {
                throw new DataException(String.format("Unknown Tag received: %s", es.getTag().getName()));
            }
            return CompletableFuture.completedFuture(null);
        });
    }

    private boolean isShutdownTag(Tag tag)
    {
        return tag.getName().contentEquals(task.getShutdownTag());
    }

    private boolean isMessageTag(Tag tag)
    {
        return tag.getName().contentEquals(task.getMessageTag());
    }

    @Override
    protected void startUp()
            throws Exception
    {
        server.start();
    }

    @Override
    protected void shutDown()
            throws Exception
    {
        ElapsedTime.measureWithPolling(new ElapsedTime.Pollable<Void>() {
            private CompletableFuture<Void> future;

            @Override
            public boolean poll()
            {
                return future.isCancelled() || future.isCompletedExceptionally() || future.isDone();
            }

            @Override
            public Void getResult()
            {
                try {
                    return future.get();
                }
                catch (InterruptedException | ExecutionException e) {
                    logger.warn("InForwardService: Server Shutdown is failed.", e);
                    return null;
                }
            }

            @Override
            public void onStart()
            {
                logger.info("InForwardService: Server Shutdown Start.");
                this.future = server.shutdown();
            }

            @Override
            public void onWaiting(long elapsedMillis)
            {
                logger.info("InForwardService: Server Shutdown Running. (Elapsed: {} ms)", elapsedMillis);
            }

            @Override
            public void onFinished(long elapsedMillis)
            {
                logger.info("InForwardService: Server Shutdown Finish. (Elapsed: {} ms)", elapsedMillis);
            }
        });
    }

    @Override
    protected void run()
            throws Exception
    {
        ElapsedTime.measureWithPolling(new ElapsedTime.Pollable<Void>() {
            @Override
            public boolean poll()
            {
                return shouldShutdown.get();
            }

            @Override
            public Void getResult()
            {
                return null;
            }

            @Override
            public void onStart()
            {
                logger.info("InForwardService: Server Start.");
            }

            @Override
            public void onWaiting(long elapsedMillis)
            {
                logger.info("InForwardService: Server Running. (Elapsed: {} ms)", elapsedMillis);
            }

            @Override
            public void onFinished(long elapsedMillis)
            {
                logger.info("InForwardService: Server Shutdown. (Elapsed: {} ms)", elapsedMillis);
            }
        });
    }
}
