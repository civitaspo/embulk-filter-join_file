package pro.civitaspo.embulk.spi;

import org.embulk.spi.BufferAllocator;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;

public class PageBuilder
        extends org.embulk.spi.PageBuilder
        implements DataBuilder
{
    public PageBuilder(BufferAllocator allocator, Schema schema, PageOutput output)
    {
        super(allocator, schema, output);
    }
}
