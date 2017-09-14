package org.embulk.filter.join_file;

import org.embulk.filter.join_file.JoinFileFilterPlugin.PluginTask;
import org.embulk.filter.join_file.join.JoinOnColumnVisitor;
import org.embulk.filter.join_file.join.Joinee;
import org.embulk.filter.join_file.join.JoineeOn;
import org.embulk.filter.join_file.join.JoineeReader;
import org.embulk.spi.Column;
import org.embulk.spi.Exec;
import org.embulk.spi.Page;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import pro.civitaspo.embulk.spi.PageBuilder;
import pro.civitaspo.embulk.spi.PageReader;

public class JoinFilePageOutput
        implements PageOutput
{
    private final PageBuilder pageBuilder;
    private final PageReader pageReader;
    private final Column onPageColumn;
    private final JoineeReader joineeReader;
    private final JoinOnColumnVisitor visitor;

    JoinFilePageOutput(PluginTask task, Schema inputSchema, Schema outputSchema, PageOutput output)
    {
        this.pageBuilder = new PageBuilder(Exec.getBufferAllocator(), outputSchema, output);
        this.pageReader = new PageReader(inputSchema);
        this.onPageColumn = inputSchema.lookupColumn(task.getOnTask().getInputColumnName());
        Joinee joinee = task.getJoinee();
        JoineeOn joineeOn = joinee.buildJoineeOn(task.getOnTask().getFileColumnName());
        this.joineeReader = new JoineeReader(joinee, joineeOn);
        this.visitor = new JoinOnColumnVisitor(pageBuilder, pageReader, joineeReader);
    }

    @Override
    public void add(Page page)
    {
        pageReader.setPage(page);
        while (pageReader.nextRecord()) {
            joineeReader.updateJoineeRowNumber(pageReader, onPageColumn);
            pageBuilder.getSchema().visitColumns(visitor);
            pageBuilder.addRecord();
        }
    }

    @Override
    public void finish()
    {
        pageBuilder.finish();
    }

    @Override
    public void close()
    {
        pageBuilder.close();
    }
}
