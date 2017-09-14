package pro.civitaspo.embulk.runner;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.embulk.exec.ExecutionResult;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

public class AsyncEmbulkRunnerService
        extends AbstractExecutionThreadService
{
    private static final Logger logger = Exec.getLogger(AsyncEmbulkRunnerService.class);
    private final EmbulkRunner runner;

    public AsyncEmbulkRunnerService(EmbulkRunner runner)
    {
        this.runner = runner;
    }

    @Override
    protected void run()
            throws Exception
    {
        ExecutionResult result = runner.run();
        if (result.isSkipped()) {
            logger.warn("ExecutionResult: EmbulkRunner.run is skipped");
        }
        if (!result.getIgnoredExceptions().isEmpty()) {
            result.getIgnoredExceptions().forEach(e -> logger.warn("Ignored Error is found", e));
        }
        logger.debug("Execution Result: {}", result);
    }
}
