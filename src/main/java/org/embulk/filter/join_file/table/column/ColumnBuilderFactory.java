package org.embulk.filter.join_file.table.column;

import com.google.common.collect.ImmutableMap;
import org.embulk.spi.Column;
import org.embulk.spi.Schema;

public class ColumnBuilderFactory
{
    private ColumnBuilderFactory()
    {
    }

    public static ImmutableMap<Column, ColumnBuilder> create(Schema schema)
    {
        ImmutableMap.Builder<Column, ColumnBuilder> builder = ImmutableMap.builder();
        for (Column column : schema.getColumns()) {
            builder.put(column, AbstractColumnBuilder.builder(column));
        }
        return builder.build();
    }
}
