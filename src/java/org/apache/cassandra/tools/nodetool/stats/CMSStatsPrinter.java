/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.tools.nodetool.stats;

import java.io.PrintStream;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

public class CMSStatsPrinter
{
    /**
     * Factory method to get a printer based on the format.
     *
     * @param format The desired output format (e.g., json, yaml).
     * @return A StatsPrinter appropriate for the format.
     */
    public static StatsPrinter<CMSStatsHolder> withPrinter(String format, String title)
    {
        validate(format);

        switch (format)
        {
            case "json":
                return new StatsPrinter.JsonPrinter<>();
            case "yaml":
                return new StatsPrinter.YamlPrinter<>();
            default:
                return new CMSStatsPrinter.DefaultPrinter(title);
        }
    }

    private static void validate(String outputFormat)
    {
        if (!outputFormat.isEmpty() && !"json".equals(outputFormat) && !"yaml".equals(outputFormat))
            throw new IllegalArgumentException("arguments for -F are json, yaml");
    }

    private static class DefaultPrinter implements StatsPrinter<CMSStatsHolder>
    {
        private final String title;

        public DefaultPrinter(String title)
        {
            this.title = title;
        }

        @Override
        public void print(CMSStatsHolder data, PrintStream out)
        {
            Map<String, Object> map = data.convert2Map();

            if (map.isEmpty())
                return;
            int keywidth = keywidth(map);
            for (Long key : ImmutableList.sortedCopyOf(map.keySet().stream().map(Long::parseLong).collect(Collectors.toList())))
            {
                out.println(title + ": " + key);

                Object value = map.get(key.toString());
                assert value instanceof Map;
                Map<String, String> valueMap = (Map<String, String>) value;

                for (Map.Entry<String, String> nodeEntry : valueMap.entrySet())
                    out.printf("  %-" + keywidth + "s%s%n", nodeEntry.getKey(), nodeEntry.getValue());
            }
        }

        private static int keywidth(Map<?, Object> map)
        {
            assert !map.isEmpty();
            Object value = map.entrySet().iterator().next().getValue();
            assert value instanceof Map;
            Map<String, String> valueMap = (Map<String, String>) value;
            return valueMap.keySet().stream().max(Comparator.comparingInt(String::length)).get().length() + 1;
        }
    }

    public static class CMSStatsHolder implements StatsHolder
    {
        private final Supplier<Map<Long, Map<String, String>>> mapSupplier;

        public CMSStatsHolder(Supplier<Map<Long, Map<String, String>>> mapSupplier)
        {
            this.mapSupplier = mapSupplier;
        }

        @Override
        public Map<String, Object> convert2Map()
        {
            Map<Long, Map<String, String>> originalMap = new TreeMap<>(mapSupplier.get());
            Map<String, Object> output = new LinkedHashMap<>();

            for (Map.Entry<Long, Map<String, String>> entry : originalMap.entrySet())
                output.put(String.valueOf(entry.getKey()), entry.getValue());

            return output;
        }
    }
}
