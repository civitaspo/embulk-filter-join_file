package org.embulk.filter.join_file;

import com.google.common.collect.Maps;

import java.util.Map;

public class ToJoinedColumnName
{
    private static String joinedColumnPrefix;
    private final static Map<String, String> cache = Maps.newHashMap();

    private ToJoinedColumnName()
    {
    }

    public static void configure(PluginTask task)
    {
        if (joinedColumnPrefix != null) {
            return;
        }
        joinedColumnPrefix = task.getJoinedColumnPrefix();
    }

    public static String convert(String original)
    {
        String name = cache.get(original);
        if (name != null) {
            return name;
        }
        String converted = String.format("%s%s", joinedColumnPrefix, original);
        cache.put(original, converted);
        return converted;
    }
}
