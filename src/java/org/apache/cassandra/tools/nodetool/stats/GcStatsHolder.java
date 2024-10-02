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

public class GcStatsHolder implements StatsHolder {
    public final NodeProbe probe;

    public GcStatsHolder(NodeProbe probe) {
        this.probe = probe;
    }

    public Map<String, Object> convert2Map() {
        HashMap<String, Object> result = new HashMap<>();
        double[] stats = probe.getAndResetGCStats();
        double mean = stats[2] / stats[5];
        double stdev = Math.sqrt((stats[3] / stats[5]) - (mean * mean));
        String out[] = new String[7];
        out[0] = String.format("%.0f", stats[0]);
        out[1] = String.format("%.0f", stats[1]);
        out[2] = String.format("%.0f", stats[2]);
        out[3] = String.format("%.0f", stdev);
        out[4] = String.format("%.0f", stats[4]);
        out[5] = String.format("%.0f", stats[5]);
        out[6] = String.format("%d", (long)stats[6]);
        result.put("Interval (ms)", out[0]);
        result.put("Max GC Elasped (ms)", out[1]);
        result.put("Total GC Elasped (ms)", out[2]);
        result.put("Stdev GC Elasped (ms)", out[3]);
        result.put("GC Reclaimed (MB)", out[4]);
        result.put("Collections", out[5]);
        result.put("Direct Memory Bytes", out[6]);
        return result;
    }
}