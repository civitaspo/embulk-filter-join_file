package pro.civitaspo.embulk.forward;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.spi.Exec;
import org.komamitsu.fluency.Fluency;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Map;

public class OutForwardService
{
    private final static Logger logger = Exec.getLogger(OutForwardService.class);

    public interface OutForwardTask
            extends org.embulk.config.Task
    {
        @Config("host")
        @ConfigDefault("\"localhost\"")
        String getHost();

        @Config("port")
        @ConfigDefault("24224")
        int getPort();

        @Config("max_buffer_size")
        @ConfigDefault("null")
        Optional<Long> getMaxBufferSize();

        @Config("buffer_chunk_initial_size")
        @ConfigDefault("null")
        Optional<Integer> getBufferChunkInitialSize();

        @Config("buffer_chunk_retention_size")
        @ConfigDefault("null")
        Optional<Integer> getBufferChunkRetentionSize();

        @Config("flush_interval_millis")
        @ConfigDefault("null")
        Optional<Integer> getFlushIntervalMillis();

        @Config("sender_max_retry_count")
        @ConfigDefault("null")
        Optional<Integer> getSenderMaxRetryCount();

        @Config("ack_response_mode")
        @ConfigDefault("null")
        Optional<Boolean> getAckResponseMode();

        @Config("file_backup_dir")
        @ConfigDefault("null")
        Optional<String> getFileBackupDir();

        @Config("wait_until_buffer_flushed")
        @ConfigDefault("null")
        Optional<Integer> getWaitUntilBufferFlushed();

        @Config("wait_until_flusher_terminated")
        @ConfigDefault("null")
        Optional<Integer> getWaitUntilFlusherTerminated();
    }

    public interface Task
            extends ForwardParentTask
    {
        @Config("out_forward")
        @ConfigDefault("{}")
        OutForwardTask getOutForwardTask();
    }

    public static void sendShutdownMessage(Task task)
    {
        logger.info("OutForwardService: Send a Shutdown Message.");
        OutForwardService outForward = new OutForwardService(task);
        outForward.emit(task.getShutdownTag(), Maps.newHashMap());
        outForward.finish();
        outForward.close();
    }

    private final Task task;
    private final Fluency client;

    public OutForwardService(Task task)
    {
        this.task = task;
        this.client = newFluency(task.getOutForwardTask());
    }

    private Fluency.Config configureFluencyConfig(OutForwardTask t)
    {
        Fluency.Config c = new Fluency.Config();
        if (t.getMaxBufferSize().isPresent()) {
            c.setMaxBufferSize(t.getMaxBufferSize().get());
        }
        if (t.getBufferChunkInitialSize().isPresent()) {
            c.setBufferChunkInitialSize(t.getBufferChunkInitialSize().get());
        }
        if (t.getBufferChunkRetentionSize().isPresent()) {
            c.setBufferChunkRetentionSize(t.getBufferChunkRetentionSize().get());
        }
        if (t.getFlushIntervalMillis().isPresent()) {
            c.setFlushIntervalMillis(t.getFlushIntervalMillis().get());
        }
        if (t.getSenderMaxRetryCount().isPresent()) {
            c.setSenderMaxRetryCount(t.getSenderMaxRetryCount().get());
        }
        if (t.getAckResponseMode().isPresent()) {
            c.setAckResponseMode(t.getAckResponseMode().get());
        }
        if (t.getFileBackupDir().isPresent()) {
            c.setFileBackupDir(t.getFileBackupDir().get());
        }
        if (t.getWaitUntilBufferFlushed().isPresent()) {
            c.setWaitUntilBufferFlushed(t.getWaitUntilBufferFlushed().get());
        }
        if (t.getWaitUntilFlusherTerminated().isPresent()) {
            c.setWaitUntilFlusherTerminated(t.getWaitUntilFlusherTerminated().get());
        }
        return c;
    }

    private Fluency newFluency(OutForwardTask t)
    {
        Fluency.Config c = configureFluencyConfig(t);
        try {
            return Fluency.defaultFluency(t.getHost(), t.getPort(), c);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void emit(String tag, Map<String, Object> message)
    {
        try {
            client.emit(tag, message);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void emit(Map<String, Object> message)
    {
        emit(task.getMessageTag(), message);
    }

    public void finish()
    {
        try {
            client.flush();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void close()
    {
        try {
            client.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
