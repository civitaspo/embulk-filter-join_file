package pro.civitaspo.embulk.forward;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;

public interface ForwardParentTask
    extends Task
{
    @Config("shutdown_tag")
    @ConfigDefault("\"shutdown\"")
    String getShutdownTag();

    @Config("message_tag")
    @ConfigDefault("\"message\"")
    String getMessageTag();
}
