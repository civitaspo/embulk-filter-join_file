package org.embulk.filter.join_file.table;

import com.google.common.collect.Maps;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Type;

import java.util.Map;

public class JoinTableSchemaBuilder
        extends Schema.Builder
{
    public JoinTableSchemaBuilder(String prefix)
    {
        ToJoinTableColumnName.configure(prefix);
    }

    public JoinTableSchemaBuilder addJoinTableColumn(String name, Type type)
    {
        return (JoinTableSchemaBuilder) add(ToJoinTableColumnName.convert(name), type);
    }

    private static class ToJoinTableColumnName
    {
        private static String joinTableColumnPrefix;
        private final static Map<String, String> cache = Maps.newHashMap();

        private ToJoinTableColumnName()
        {
        }

        static void configure(String prefix)
        {
            if (joinTableColumnPrefix != null) {
                return;
            }
            joinTableColumnPrefix = prefix;
        }

        static String convert(String original)
        {
            String name = cache.get(original);
            if (name != null) {
                return name;
            }
            String converted = String.format("%s%s", joinTableColumnPrefix, original);
            cache.put(original, converted);
            return converted;
        }
    }
}
