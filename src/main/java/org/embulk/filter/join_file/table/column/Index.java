package org.embulk.filter.join_file.table.column;

import com.google.common.collect.ImmutableMap;

import java.util.concurrent.atomic.AtomicLong;

public class Index
{
    static class Builder
    {
        private AtomicLong index = new AtomicLong(0);
        private ImmutableMap.Builder<Object, Long> indexMapBuilder = ImmutableMap.builder();

        Builder()
        {
        }

        public Builder addIndexValue(Object obj)
        {
            indexMapBuilder.put(obj, index.getAndIncrement());
            return this;
        }

        public Index build()
        {
            return new Index(indexMapBuilder.build());
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    private final ImmutableMap<Object, Long> indexMap;

    public Index(ImmutableMap<Object, Long> indexMap)
    {
        this.indexMap = indexMap;
    }

    public long getIndex(Object obj)
            throws IllegalAccessException
    {
        Long idx = indexMap.get(obj);
        if (idx == null) {
            throw new IllegalAccessException(String.format("%s is not found", obj));
        }
        return idx;
    }
}
