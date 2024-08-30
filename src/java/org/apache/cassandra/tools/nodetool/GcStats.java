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
package org.apache.cassandra.tools.nodetool;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

import org.apache.cassandra.tools.nodetool.stats.StatsPrinter;
import org.apache.cassandra.tools.nodetool.stats.StatsHolder;

import io.airlift.airline.Option;
import io.airlift.airline.Command;

import java.io.PrintStream;
import java.util.Map;
import java.util.HashMap;

@Command(name = "gcstats", description = "Print GC Statistics")
public class GcStats extends NodeToolCmd {

    @Option(title = "format",
            name = {"-F", "--format"},
            description = "Output format (json)")
    private String outputFormat = "";

    @Override
    public void execute(NodeProbe probe) {
        if (!outputFormat.isEmpty() && !"json".equals(outputFormat)) {
            throw new IllegalArgumentException("arguments for -F are json only.");
        }

        double[] stats = probe.getAndResetGCStats();
        StatsHolder holder = new GcStatsHolder(stats);

        StatsPrinter<StatsHolder> printer = StatsPrinterFactory.createPrinter(outputFormat);
        printer.print(holder, probe.output().out);
    }

    static class GcStatsHolder implements StatsHolder {
        private final double[] stats;

        GcStatsHolder(double[] stats) {
            this.stats = stats;
        }

        @Override
        public Map<String, Object> convert2Map() {
            HashMap<String, Object> map = new HashMap<>();

            double mean = stats[2] / stats[5];
            double stdev = Math.sqrt((stats[3] / stats[5]) - (mean * mean));

            map.put("interval_ms", stats[0]);
            map.put("max_gc_elapsed_ms", stats[1]);
            map.put("total_gc_elapsed_ms", stats[2]);
            map.put("stdev_gc_elapsed_ms", stdev);
            map.put("gc_reclaimed_mb", stats[4]);
            map.put("collections", stats[5]);
            map.put("direct_memory_bytes", (long) stats[6]);

            return map;
        }
    }

    public static class StatsPrinterFactory {
        public static StatsPrinter<StatsHolder> createPrinter(String format) {
            if ("json".equalsIgnoreCase(format)) {
                return new StatsPrinter.JsonPrinter<>();
            } else {
                return new TextPrinter<>();
            }
        }
    }

    public static class TextPrinter<T extends StatsHolder> implements StatsPrinter<T> {
        @Override
        public void print(T data, PrintStream out) {
            double[] stats = ((GcStatsHolder) data).stats;
            double mean = stats[2] / stats[5];
            double stdev = Math.sqrt((stats[3] / stats[5]) - (mean * mean));
            out.printf("%20s%20s%20s%20s%20s%20s%25s%n", "Interval (ms)", "Max GC Elapsed (ms)", "Total GC Elapsed (ms)", "Stdev GC Elapsed (ms)", "GC Reclaimed (MB)", "Collections", "Direct Memory Bytes");
            out.printf("%20.0f%20.0f%20.0f%20.0f%20.0f%20.0f%25d%n", stats[0], stats[1], stats[2], stdev, stats[4], stats[5], (long) stats[6]);
        }
    }
}