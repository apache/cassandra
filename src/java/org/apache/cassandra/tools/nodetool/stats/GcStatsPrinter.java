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
import java.util.Map;

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
            default:
                return new DefaultPrinter();
        }
    }

    /**
     * Default printer for GC statistics.
     */
    public static class DefaultPrinter implements StatsPrinter<GcStatsHolder>
    {
        /**
         * Prints GC statistics in a human-readable table format.
         *
         * @param data The GC statistics data holder.
         * @param out The output stream to print to.
         */
        @Override
        public void print(GcStatsHolder data, PrintStream out)
        {
            Map<String, Object> stats = data.convert2Map();

            out.printf("%20s%20s%20s%20s%20s%20s%25s%n", "Interval (ms)", "Max GC Elapsed (ms)", "Total GC Elapsed (ms)",
                    "Stdev GC Elapsed (ms)", "GC Reclaimed (MB)", "Collections", "Direct Memory Bytes");
            out.printf("%20.0f%20.0f%20.0f%20.0f%20.0f%20.0f%25d%n", stats.get("interval_ms"), stats.get("max_gc_elapsed_ms"),
                    stats.get("total_gc_elapsed_ms"), stats.get("stdev_gc_elapsed_ms"), stats.get("gc_reclaimed_mb"),
                    stats.get("collections"), (long) stats.get("direct_memory_bytes"));
        }
    }
}