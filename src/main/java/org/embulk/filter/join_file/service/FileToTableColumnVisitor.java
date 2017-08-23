package org.embulk.filter.join_file.service;

import org.embulk.filter.join_file.file.FileReader;
import org.embulk.filter.join_file.table.TableBuilder;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;

public class FileToTableColumnVisitor
        implements ColumnVisitor
{
    private final FileReader reader;
    private final TableBuilder builder;

    public FileToTableColumnVisitor(FileReader reader, TableBuilder builder)
    {
        this.reader = reader;
        this.builder = builder;
    }

    private void nullOr(Column column, UseBuilderColumn func)
    {
        if (reader.isNull(column)) {
            setBuilderColumn(column, builder::setNull);
            return;
        }
        func.run(column);
    }

    @FunctionalInterface
    private interface UseBuilderColumn
    {
        void run(Column column);
    }

    private void setBuilderColumn(Column column, UseBuilderColumn func)
    {
        func.run(builder.getSchema().getColumn(column.getIndex()));
    }

    @Override
    public void booleanColumn(Column column)
    {
        nullOr(column, c -> builder.setBoolean(c, reader.getBoolean(column)));
    }

    @Override
    public void longColumn(Column column)
    {
        nullOr(column, c -> builder.setLong(c, reader.getLong(column)));
    }

    @Override
    public void doubleColumn(Column column)
    {
        nullOr(column, c -> builder.setDouble(c, reader.getDouble(column)));
    }

    @Override
    public void stringColumn(Column column)
    {
        nullOr(column, c -> builder.setString(c, reader.getString(column)));
    }

    @Override
    public void timestampColumn(Column column)
    {
        nullOr(column, c -> builder.setTimestamp(c, reader.getTimestamp(column)));
    }

    @Override
    public void jsonColumn(Column column)
    {
        nullOr(column, c -> builder.setJson(c, reader.getJson(column)));
    }
}
