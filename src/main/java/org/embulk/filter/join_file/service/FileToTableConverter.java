package org.embulk.filter.join_file.service;

import org.embulk.filter.join_file.file.FileReader;
import org.embulk.filter.join_file.table.Table;
import org.embulk.filter.join_file.table.TableBuilder;

public class FileToTableConverter
{
    private FileToTableConverter()
    {
    }

    public Table create(FileReader reader, TableBuilder builder)
    {
        FileToTableColumnVisitor visitor = new FileToTableColumnVisitor(reader, builder);

        while (reader.nextRecord()) {
            reader.getSchema().visitColumns(visitor);
            builder.addRecord();
        }
        return builder.build();
    }
}
