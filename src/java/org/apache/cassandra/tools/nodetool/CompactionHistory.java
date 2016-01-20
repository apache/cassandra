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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.management.openmbean.TabularData;

import io.airlift.command.Command;

import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

import static com.google.common.collect.Iterables.toArray;

@Command(name = "compactionhistory", description = "Print history of compaction")
public class CompactionHistory extends NodeToolCmd
{
    @Override
    public void execute(NodeProbe probe)
    {
        System.out.println("Compaction History: ");

        TabularData tabularData = probe.getCompactionHistory();
        if (tabularData.isEmpty())
        {
            System.out.printf("There is no compaction history");
            return;
        }

        TableBuilder table = new TableBuilder();
        List<String> indexNames = tabularData.getTabularType().getIndexNames();
        table.add(toArray(indexNames, String.class));

        Set<?> values = tabularData.keySet();
        List<CompactionHistoryRow> chr = new ArrayList<>();
        for (Object eachValue : values)
        {
            List<?> value = (List<?>) eachValue;
            CompactionHistoryRow chc = new CompactionHistoryRow((String)value.get(0),
                                                                (String)value.get(1),
                                                                (String)value.get(2),
                                                                (Long)value.get(3),
                                                                (Long)value.get(4),
                                                                (Long)value.get(5),
                                                                (String)value.get(6));
            chr.add(chc);
        }
        Collections.sort(chr);
        for (CompactionHistoryRow eachChc : chr)
        {
            table.add(eachChc.getAllAsArray());
        }
        table.printTo(System.out);
    }

    /**
     * Allows the Compaction History output to be ordered by 'compactedAt' - that is the
     * time at which compaction finished.
     */
    private static class CompactionHistoryRow implements Comparable<CompactionHistoryRow>
    {
        private final String id;
        private final String ksName;
        private final String cfName;
        private final long compactedAt;
        private final long bytesIn;
        private final long bytesOut;
        private final String rowMerged;

        CompactionHistoryRow(String id, String ksName, String cfName, long compactedAt, long bytesIn, long bytesOut, String rowMerged)
        {
            this.id = id;
            this.ksName = ksName;
            this.cfName = cfName;
            this.compactedAt = compactedAt;
            this.bytesIn = bytesIn;
            this.bytesOut = bytesOut;
            this.rowMerged = rowMerged;
        }

        public int compareTo(CompactionHistoryRow chc)
        {
            return Long.signum(chc.compactedAt - this.compactedAt);
        }

        public String[] getAllAsArray()
        {
            String[] obj = new String[7];
            obj[0] = this.id;
            obj[1] = this.ksName;
            obj[2] = this.cfName;
            Instant instant = Instant.ofEpochMilli(this.compactedAt);
            LocalDateTime ldt = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
            obj[3] = ldt.toString();
            obj[4] = Long.toString(this.bytesIn);
            obj[5] = Long.toString(this.bytesOut);
            obj[6] = this.rowMerged;
            return obj;
        }
    }
}
