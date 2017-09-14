package pro.civitaspo.embulk.spi;

import org.embulk.spi.Schema;

public class PageReader
        extends org.embulk.spi.PageReader
        implements DataReader
{
    public PageReader(Schema schema)
    {
        super(schema);
    }
}
