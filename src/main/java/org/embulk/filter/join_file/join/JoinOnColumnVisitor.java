package org.embulk.filter.join_file.join;

import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import pro.civitaspo.embulk.spi.DataBuilder;
import pro.civitaspo.embulk.spi.DataReader;

public class JoinOnColumnVisitor
        implements ColumnVisitor
{
    private final DataBuilder dataBuilder;
    private final DataReader dataReader;
    private final JoineeReader joineeReader;

    private final int joineeOffset;

    public JoinOnColumnVisitor(DataBuilder dataBuilder, DataReader dataReader, JoineeReader joineeReader)
    {
        this.dataBuilder = dataBuilder;
        this.dataReader = dataReader;
        this.joineeReader = joineeReader;
        this.joineeOffset = dataReader.getSchema().getColumnCount();
    }

    /*
    TODO: More accurate explanation.
    NOTE: This visitor do not mind column names.
          So, dataBuilder.getSchema() != dataReader.getSchema() + joineeReader.getSchema()
          Actually, dataBuilder.getSchema() = dataReader.getSchema() + joineeReader.getSchema() with column_prefix.
     */

    private boolean isJoinerColumn(Column column)
    {
        return column.getIndex() < joineeOffset;
    }

    private int toJoineeIndex(Column column)
    {
        return column.getIndex() - joineeOffset;
    }

    private void nullOr(Column column, Runnable runnable)
    {
        if (isJoinerColumn(column)) {
            if (dataReader.isNull(column)) {
                dataBuilder.setNull(column);
                return;
            }
        }
        else {
            int joineeIndex = toJoineeIndex(column);
            if (joineeReader.isNull(joineeIndex)) {
                dataBuilder.setNull(column);
                return;
            }
        }

        runnable.run();
    }

    @Override
    public void booleanColumn(Column column)
    {
        nullOr(column, () ->
        {
            if (isJoinerColumn(column)) {
                dataBuilder.setBoolean(column, dataReader.getBoolean(column));
            }
            else {
                int joineeIndex = toJoineeIndex(column);
                dataBuilder.setBoolean(column, joineeReader.getBoolean(joineeIndex));
            }
        });

    }

    @Override
    public void longColumn(Column column)
    {
        nullOr(column, () ->
        {
            if (isJoinerColumn(column)) {
                dataBuilder.setLong(column, dataReader.getLong(column));
            }
            else {
                int joineeIndex = toJoineeIndex(column);
                dataBuilder.setLong(column, joineeReader.getLong(joineeIndex));
            }
        });
    }

    @Override
    public void doubleColumn(Column column)
    {
        nullOr(column, () ->
        {
            if (isJoinerColumn(column)) {
                dataBuilder.setDouble(column, dataReader.getDouble(column));
            }
            else {
                int joineeIndex = toJoineeIndex(column);
                dataBuilder.setDouble(column, joineeReader.getDouble(joineeIndex));
            }
        });

    }

    @Override
    public void stringColumn(Column column)
    {
        nullOr(column, () ->
        {
            if (isJoinerColumn(column)) {
                dataBuilder.setString(column, dataReader.getString(column));
            }
            else {
                int joineeIndex = toJoineeIndex(column);
                dataBuilder.setString(column, joineeReader.getString(joineeIndex));
            }
        });
    }

    @Override
    public void timestampColumn(Column column)
    {
        nullOr(column, () ->
        {
            if (isJoinerColumn(column)) {
                dataBuilder.setTimestamp(column, dataReader.getTimestamp(column));
            }
            else {
                int joineeIndex = toJoineeIndex(column);
                dataBuilder.setTimestamp(column, joineeReader.getTimestamp(joineeIndex));
            }
        });
    }

    @Override
    public void jsonColumn(Column column)
    {
        nullOr(column, () ->
        {
            if (isJoinerColumn(column)) {
                dataBuilder.setJson(column, dataReader.getJson(column));
            }
            else {
                int joineeIndex = toJoineeIndex(column);
                dataBuilder.setJson(column, joineeReader.getJson(joineeIndex));
            }
        });
    }
}
