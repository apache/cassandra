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

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.tools.NodeProbe;

/**
 * Holds and converts GC statistics to a map structure.
 */
public class GcStatsHolder implements StatsHolder
{
    public final NodeProbe probe;

    public GcStatsHolder(NodeProbe probe)
    {
        this.probe = probe;
    }

    /**
     * Converts the GC statistics gathered from the probe into a map.
     *
     * @return A map containing GC statistics with keys such as interval_ms, max_gc_elapsed_ms, etc.
     */
    @Override
    public Map<String, Object> convert2Map()
    {
        HashMap<String, Object> result = new HashMap<>();

        double[] stats = probe.getAndResetGCStats();
        double mean = stats[2] / stats[5];
        double stdev = Math.sqrt((stats[3] / stats[5]) - (mean * mean));

        result.put("interval_ms", stats[0]);
        result.put("max_gc_elapsed_ms", stats[1]);
        result.put("total_gc_elapsed_ms", stats[2]);
        result.put("stdev_gc_elapsed_ms", stdev);
        result.put("gc_reclaimed_mb", stats[4]);
        result.put("collections", stats[5]);
        result.put("direct_memory_bytes", (long) stats[6]);

        return result;
    }
}