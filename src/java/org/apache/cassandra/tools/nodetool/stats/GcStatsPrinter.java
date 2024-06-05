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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

/**
 * Printer for GC statistics.
 */
public class GcStatsPrinter
{
    /**
     * Factory method to get a printer based on the format.
     *
     * @param format The desired output format (e.g., json, yaml).
     * @return A StatsPrinter appropriate for the format.
     */
    public static StatsPrinter<GcStatsHolder> from(String format)
    {
        switch (format)
        {
            case "json":
                return new StatsPrinter.JsonPrinter<>();
            case "yaml":
                return new StatsPrinter.YamlPrinter<>();
            case "table":
                return new TablePrinter();
            default:
                return new LegacyPrinter();
        }
    }

    public static class TablePrinter implements StatsPrinter<GcStatsHolder>
    {
        /**
         * Prints GC statistics in a human-readable table format.
         *
         * @param data The GC statistics data holder.
         * @param out  The output stream to print to.
         */
        @Override
        public void print(GcStatsHolder data, PrintStream out)
        {
            Map<String, Object> stats = data.convert2Map();
            TableBuilder tableBuilder = new TableBuilder();

            for (Map.Entry<String, Object> entry : stats.entrySet())
                tableBuilder.add(GcStatsHolder.columnDescriptionMap.get(entry.getKey()), entry.getValue().toString());

            tableBuilder.printTo(out);
        }
    }

    /**
     * Default printer for GC statistics.
     */
    public static class LegacyPrinter implements StatsPrinter<GcStatsHolder>
    {
        @Override
        public void print(GcStatsHolder data, PrintStream out)
        {
            Map<String, Object> stats = data.convert2Map();
            TableBuilder tableBuilder = new TableBuilder();

            tableBuilder.add(new ArrayList<>(GcStatsHolder.columnDescriptionMap.values()));

            List<String> values = new ArrayList<>();
            for (String key : GcStatsHolder.columnDescriptionMap.keySet())
                values.add(stats.get(key).toString());

            tableBuilder.add(values);

            tableBuilder.printTo(out);
        }
    }
}