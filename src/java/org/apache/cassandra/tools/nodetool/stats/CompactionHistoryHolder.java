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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.management.openmbean.TabularData;

import org.apache.cassandra.tools.NodeProbe;

public class CompactionHistoryHolder implements StatsHolder
{
    public final NodeProbe probe;
    public List<String> indexNames;

    public CompactionHistoryHolder(NodeProbe probe)
    {
        this.probe = probe;
    }

    /**
     * Allows the Compaction History output to be ordered by 'compactedAt' - that is the
     * time at which compaction finished.
     */
    private static class CompactionHistoryRow implements Comparable<CompactionHistoryHolder.CompactionHistoryRow>
    {
        private final String id;
        private final String ksName;
        private final String cfName;
        private final long compactedAt;
        private final long bytesIn;
        private final long bytesOut;
        private final String rowMerged;
        private final String compactionProperties;

        CompactionHistoryRow(String id, String ksName, String cfName, long compactedAt, long bytesIn, long bytesOut, String rowMerged, String compactionProperties)
        {
            this.id = id;
            this.ksName = ksName;
            this.cfName = cfName;
            this.compactedAt = compactedAt;
            this.bytesIn = bytesIn;
            this.bytesOut = bytesOut;
            this.rowMerged = rowMerged;
            this.compactionProperties = compactionProperties;
        }

        public int compareTo(CompactionHistoryHolder.CompactionHistoryRow chr)
        {
            return Long.signum(chr.compactedAt - this.compactedAt);
        }

        private HashMap<String, Object> getAllAsMap()
        {
            HashMap<String, Object> compaction = new HashMap<>();
            compaction.put("id", this.id);
            compaction.put("keyspace_name", this.ksName);
            compaction.put("columnfamily_name", this.cfName);
            Instant instant = Instant.ofEpochMilli(this.compactedAt);
            LocalDateTime ldt = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
            compaction.put("compacted_at", ldt.toString());
            compaction.put("bytes_in", this.bytesIn);
            compaction.put("bytes_out", this.bytesOut);
            compaction.put("rows_merged", this.rowMerged);
            compaction.put("compaction_properties", this.compactionProperties);
            return compaction;
        }
    }

    @Override
    public Map<String, Object> convert2Map()
    {
        HashMap<String, Object> result = new HashMap<>();
        ArrayList<Map<String, Object>> compactions = new ArrayList<>();

        TabularData tabularData = probe.getCompactionHistory();
        this.indexNames = tabularData.getTabularType().getIndexNames();

        if (tabularData.isEmpty()) return result;

        List<CompactionHistoryHolder.CompactionHistoryRow> chrList = new ArrayList<>();
        Set<?> values = tabularData.keySet();
        for (Object eachValue : values)
        {
            List<?> value = (List<?>) eachValue;
            CompactionHistoryHolder.CompactionHistoryRow chr = new CompactionHistoryHolder.CompactionHistoryRow(
                (String)value.get(0),
                (String)value.get(1),
                (String)value.get(2),
                (Long)value.get(3),
                (Long)value.get(4),
                (Long)value.get(5),
                (String)value.get(6),
                (String)value.get(7)
            );
            chrList.add(chr);
        }

        Collections.sort(chrList);
        for (CompactionHistoryHolder.CompactionHistoryRow chr : chrList)
        {
            compactions.add(chr.getAllAsMap());
        }
        result.put("CompactionHistory", compactions);
        return result;
    }
}
