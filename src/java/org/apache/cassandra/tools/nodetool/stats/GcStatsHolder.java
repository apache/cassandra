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

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.cassandra.tools.NodeProbe;

import static org.apache.cassandra.io.util.FileUtils.stringifyFileSize;

/**
 * Holds and converts GC statistics to a map structure.
 */
public class GcStatsHolder implements StatsHolder
{
    public static final String INTERVAL = "interval_ms";
    public static final String MAX_GC = "max_gc_elapsed_ms";
    public static final String TOTAL_GC = "total_gc_elapsed_ms";
    public static final String STDEV_GC = "stdev_gc_elapsed_ms";
    public static final String RECLAIMED_GC = "gc_reclaimed_mb";
    public static final String GC_COUNT = "gc_count";
    public static final String ALLOCATED_DIRECT_MEMORY = "allocated_direct_memory_bytes";
    public static final String MAX_DIRECT_MEMORY = "max_direct_memory_bytes";
    public static final String RESERVED_DIRECT_MEMORY = "reserved_direct_memory_bytes";

    public static final Map<String, String> columnDescriptionMap = Collections.unmodifiableMap(new LinkedHashMap<>()
    {{
        put(INTERVAL, "Interval (ms)");
        put(MAX_GC, "Max GC Elapsed (ms)");
        put(TOTAL_GC, "Total GC Elapsed (ms)");
        put(STDEV_GC, "Stdev GC Elapsed (ms)");
        put(RECLAIMED_GC, "GC Reclaimed Bytes");
        put(GC_COUNT, "GC Count");
        put(ALLOCATED_DIRECT_MEMORY, "Allocated Direct Memory Bytes");
        put(MAX_DIRECT_MEMORY, "Max Direct Memory Bytes");
        put(RESERVED_DIRECT_MEMORY, "Reserved Direct Memory Bytes");
    }});

    private final NodeProbe probe;
    private final boolean humanReadable;

    public GcStatsHolder(NodeProbe probe, boolean humanReadable)
    {
        this.probe = probe;
        this.humanReadable = humanReadable;
    }

    /**
     * Converts the GC statistics gathered from the probe into a map.
     *
     * @return A map containing GC statistics with keys such as interval_ms, max_gc_elapsed_ms, etc.
     */
    @Override
    public Map<String, Object> convert2Map()
    {
        HashMap<String, Object> result = new LinkedHashMap<>();

        double[] stats = probe.getAndResetGCStats();
        double mean = stats[2] / stats[5];
        double stdev = Math.sqrt((stats[3] / stats[5]) - (mean * mean));

        long totalDirect = (long) stats[6];
        long maxDirect = (long) stats[7];
        long reservedDirect = (long) stats[8];

        result.put(INTERVAL, String.format("%.0f", stats[0]));
        result.put(MAX_GC, String.format("%.0f", stats[1]));
        result.put(TOTAL_GC, String.format("%.0f", stats[2]));
        result.put(STDEV_GC, String.format("%.0f", stdev));
        result.put(RECLAIMED_GC, stringifyFileSize((long) stats[4], humanReadable));
        result.put(GC_COUNT, String.valueOf((long) stats[5]));
        result.put(ALLOCATED_DIRECT_MEMORY, totalDirect == -1 ? Double.NaN : stringifyFileSize((long) stats[6], humanReadable));
        result.put(MAX_DIRECT_MEMORY, maxDirect == -1 ? Double.NaN : stringifyFileSize((long) stats[7], humanReadable));
        result.put(RESERVED_DIRECT_MEMORY, reservedDirect == -1 ? Double.NaN : stringifyFileSize((long) stats[8], humanReadable));

        return result;
    }
}