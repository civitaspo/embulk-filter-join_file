package org.embulk.filter.join_file.join;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class JoineeOn
{
    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private final ImmutableMap.Builder<Optional<?>, Long> builder = ImmutableMap.builder();

        public Builder()
        {
        }

        public Builder put(Optional<?> key, long value)
        {
            builder.put(key, value);
            return this;
        }

        public JoineeOn build()
        {
            return new JoineeOn(builder.build());
        }
    }

    private final Map<Optional<?>, Long> indexMap;

    private JoineeOn(Map<Optional<?>, Long> indexMap)
    {
        this.indexMap = indexMap;
    }

    public long get(Optional<?> joiner)
    {
        return indexMap.get(joiner);
    }
}
