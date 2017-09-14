package org.embulk.filter.copy.util;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageReader;
import pro.civitaspo.embulk.spi.DataBuilder;
import pro.civitaspo.embulk.spi.DataReader;

public class StandardColumnVisitor
    implements ColumnVisitor
{
    private final DataReader reader;
    private final DataBuilder builder;

    public StandardColumnVisitor(DataReader reader, DataBuilder builder)
    {
        this.reader = reader;
        this.builder = builder;
    }

    private void nullOr(Column column, Runnable runnable)
    {
        if (reader.isNull(column)) {
            builder.setNull(column);
            return;
        }
        runnable.run();
    }

    @Override
    public void booleanColumn(Column column)
    {
        nullOr(column, () -> builder.setBoolean(column, reader.getBoolean(column)));
    }

    @Override
    public void longColumn(Column column)
    {
        nullOr(column, () -> builder.setLong(column, reader.getLong(column)));
    }

    @Override
    public void doubleColumn(Column column)
    {
        nullOr(column, () -> builder.setDouble(column, reader.getDouble(column)));
    }

    @Override
    public void stringColumn(Column column)
    {
        nullOr(column, () -> builder.setString(column, reader.getString(column)));
    }

    @Override
    public void timestampColumn(Column column)
    {
        nullOr(column, () -> builder.setTimestamp(column, reader.getTimestamp(column)));
    }

    @Override
    public void jsonColumn(Column column)
    {
        nullOr(column, () -> builder.setJson(column, reader.getJson(column)));
    }
}
