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

import io.airlift.airline.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

import io.airlift.airline.Command;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

@Command(name = "gcstats", description = "Print GC Statistics")
public class GcStats extends NodeToolCmd
{

    @Option(title = "human_readable",
    name = {"-H", "--human-readable"},
    description = "Display gcstats in the table format for human-readable")
    private boolean humanReadable = false;

    public static final String INTERVAL = "Interval (ms)";
    public static final String MAX_GC = "Max GC Elapsed (ms)";
    public static final String TOTAL_GC = "Total GC Elapsed (ms)";
    public static final String STDEV_GC = "Stdev GC Elapsed (ms)";
    public static final String RECLAIMED_GC = "GC Reclaimed (MB)";
    public static final String COLLECTIONS = "Collections";
    public static final String TOTAL_DIRECT_MEMORY = "Total Direct Memory Bytes";
    public static final String MAX_DIRECT_MEMORY = "Max Direct Memory Bytes";
    public static final String RESERVED_DIRECT_MEMORY = "Reserved Direct Memory Bytes";

    @Override
    public void execute(NodeProbe probe)
    {
        double[] stats = probe.getAndResetGCStats();
        double mean = stats[2] / stats[5];
        double stdev = Math.sqrt((stats[3] / stats[5]) - (mean * mean));

        if (!humanReadable)
        {
            probe.output().out.printf("%20s%20s%20s%20s%20s%20s%25s%n", INTERVAL, MAX_GC, TOTAL_GC, STDEV_GC, RECLAIMED_GC, COLLECTIONS, TOTAL_DIRECT_MEMORY);
            probe.output().out.printf("%20.0f%20.0f%20.0f%20.0f%20.0f%20.0f%25d%n", stats[0], stats[1], stats[2], stdev, stats[4], stats[5], (long) stats[6]);
        }
        else
        {
            TableBuilder tableBuilder = new TableBuilder();
            tableBuilder.add(INTERVAL, String.valueOf(stats[0]));
            tableBuilder.add(MAX_GC, String.valueOf(stats[1]));
            tableBuilder.add(TOTAL_GC, String.valueOf(stats[2]));
            tableBuilder.add(STDEV_GC, String.valueOf(stats[3]));
            tableBuilder.add(RECLAIMED_GC, String.valueOf(stats[4]));
            tableBuilder.add(COLLECTIONS, String.valueOf(stats[5]));
            tableBuilder.add(TOTAL_DIRECT_MEMORY, String.valueOf((long) stats[6]));
            tableBuilder.add(MAX_DIRECT_MEMORY, String.valueOf((long) stats[7]));
            tableBuilder.add(RESERVED_DIRECT_MEMORY, String.valueOf((long) stats[8]));

            tableBuilder.printTo(probe.output().out);
        }
    }
}
