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

import static java.lang.String.format;
import io.airlift.command.Command;
import io.airlift.command.Option;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

@Command(name = "compactionstats", description = "Print statistics on compactions")
public class CompactionStats extends NodeToolCmd
{
    @Option(title = "human_readable",
            name = {"-H", "--human-readable"},
            description = "Display bytes in human readable form, i.e. KB, MB, GB, TB")
    private boolean humanReadable = false;

    @Override
    public void execute(NodeProbe probe)
    {
        CompactionManagerMBean cm = probe.getCompactionManagerProxy();
        System.out.println("pending tasks: " + probe.getCompactionMetric("PendingTasks"));
        long remainingBytes = 0;
        List<Map<String, String>> compactions = cm.getCompactions();
        if (!compactions.isEmpty())
        {
            int compactionThroughput = probe.getCompactionThroughput();
            List<String[]> lines = new ArrayList<>();
            int[] columnSizes = new int[] { 0, 0, 0, 0, 0, 0, 0, 0 };

            addLine(lines, columnSizes, "id", "compaction type", "keyspace", "table", "completed", "total", "unit", "progress");
            for (Map<String, String> c : compactions)
            {
                long total = Long.parseLong(c.get("total"));
                long completed = Long.parseLong(c.get("completed"));
                String taskType = c.get("taskType");
                String keyspace = c.get("keyspace");
                String columnFamily = c.get("columnfamily");
                String completedStr = humanReadable ? FileUtils.stringifyFileSize(completed) : Long.toString(completed);
                String totalStr = humanReadable ? FileUtils.stringifyFileSize(total) : Long.toString(total);
                String unit = c.get("unit");
                String percentComplete = total == 0 ? "n/a" : new DecimalFormat("0.00").format((double) completed / total * 100) + "%";
                String id = c.get("compactionId");
                addLine(lines, columnSizes, id, taskType, keyspace, columnFamily, completedStr, totalStr, unit, percentComplete);
                if (taskType.equals(OperationType.COMPACTION.toString()))
                    remainingBytes += total - completed;
            }

            StringBuilder buffer = new StringBuilder();
            for (int columnSize : columnSizes) {
                buffer.append("%");
                buffer.append(columnSize + 3);
                buffer.append("s");
            }
            buffer.append("%n");
            String format = buffer.toString();

            for (String[] line : lines)
            {
                System.out.printf(format, line[0], line[1], line[2], line[3], line[4], line[5], line[6], line[7]);
            }

            String remainingTime = "n/a";
            if (compactionThroughput != 0)
            {
                long remainingTimeInSecs = remainingBytes / (1024L * 1024L * compactionThroughput);
                remainingTime = format("%dh%02dm%02ds", remainingTimeInSecs / 3600, (remainingTimeInSecs % 3600) / 60, (remainingTimeInSecs % 60));
            }
            System.out.printf("%25s%10s%n", "Active compaction remaining time : ", remainingTime);
        }
    }

    private void addLine(List<String[]> lines, int[] columnSizes, String... columns) {
        lines.add(columns);
        for (int i = 0; i < columns.length; i++) {
            columnSizes[i] = Math.max(columnSizes[i], columns[i].length());
        }
    }
}