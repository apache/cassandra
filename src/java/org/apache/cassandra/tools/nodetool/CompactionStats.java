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

import java.io.PrintStream;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import io.airlift.airline.Command;
import io.airlift.airline.Option;

import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionInfo.Unit;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

import static java.lang.String.format;

@Command(name = "compactionstats", description = "Print statistics on compactions")
public class CompactionStats extends NodeToolCmd
{
    @Option(title = "human_readable",
            name = {"-H", "--human-readable"},
            description = "Display bytes in human readable form, i.e. KiB, MiB, GiB, TiB")
    private boolean humanReadable = false;

    @Option(title = "vtable_output",
            name = {"-V", "--vtable"},
            description = "Display fields matching vtable output")
    private boolean vtableOutput = false;

    @Override
    public void execute(NodeProbe probe)
    {
        PrintStream out = probe.output().out;
        CompactionManagerMBean cm = probe.getCompactionManagerProxy();
        Map<String, Map<String, Integer>> pendingTaskNumberByTable =
            (Map<String, Map<String, Integer>>) probe.getCompactionMetric("PendingTasksByTableName");
        int numTotalPendingTask = 0;
        for (Entry<String, Map<String, Integer>> ksEntry : pendingTaskNumberByTable.entrySet())
        {
            for (Entry<String, Integer> tableEntry : ksEntry.getValue().entrySet())
                numTotalPendingTask += tableEntry.getValue();
        }
        out.println("pending tasks: " + numTotalPendingTask);
        for (Entry<String, Map<String, Integer>> ksEntry : pendingTaskNumberByTable.entrySet())
        {
            String ksName = ksEntry.getKey();
            for (Entry<String, Integer> tableEntry : ksEntry.getValue().entrySet())
            {
                String tableName = tableEntry.getKey();
                int pendingTaskCount = tableEntry.getValue();

                out.println("- " + ksName + '.' + tableName + ": " + pendingTaskCount);
            }
        }
        out.println();
        reportCompactionTable(cm.getCompactions(), probe.getCompactionThroughputBytes(), humanReadable, vtableOutput, out);
    }

    public static void reportCompactionTable(List<Map<String,String>> compactions, long compactionThroughputInBytes, boolean humanReadable, PrintStream out)
    {
        reportCompactionTable(compactions, compactionThroughputInBytes, humanReadable, false, out);
    }

    public static void reportCompactionTable(List<Map<String,String>> compactions, long compactionThroughputInBytes, boolean humanReadable, boolean vtableOutput, PrintStream out)
    {
        if (!compactions.isEmpty())
        {
            long remainingBytes = 0;
            TableBuilder table = new TableBuilder();

            if (vtableOutput)
                table.add("keyspace", "table", "task id", "completion ratio", "kind", "progress", "sstables", "total", "unit");
            else
                table.add("id", "compaction type", "keyspace", "table", "completed", "total", "unit", "progress");

            for (Map<String, String> c : compactions)
            {
                long total = Long.parseLong(c.get(CompactionInfo.TOTAL));
                long completed = Long.parseLong(c.get(CompactionInfo.COMPLETED));
                String taskType = c.get(CompactionInfo.TASK_TYPE);
                String keyspace = c.get(CompactionInfo.KEYSPACE);
                String columnFamily = c.get(CompactionInfo.COLUMNFAMILY);
                String unit = c.get(CompactionInfo.UNIT);
                boolean toFileSize = humanReadable && Unit.isFileSize(unit);
                String[] tables = c.get(CompactionInfo.SSTABLES).split(",");
                String progressStr = toFileSize ? FileUtils.stringifyFileSize(completed) : Long.toString(completed);
                String totalStr = toFileSize ? FileUtils.stringifyFileSize(total) : Long.toString(total);
                String percentComplete = total == 0 ? "n/a" : new DecimalFormat("0.00").format((double) completed / total * 100) + "%";
                String id = c.get(CompactionInfo.COMPACTION_ID);
                if (vtableOutput)
                    table.add(keyspace, columnFamily, id, percentComplete, taskType, progressStr, String.valueOf(tables.length), totalStr, unit);
                else
                    table.add(id, taskType, keyspace, columnFamily, progressStr, totalStr, unit, percentComplete);

                remainingBytes += total - completed;
            }
            table.printTo(out);

            String remainingTime = "n/a";
            if (compactionThroughputInBytes != 0)
            {
                long remainingTimeInSecs = remainingBytes / compactionThroughputInBytes;
                remainingTime = format("%dh%02dm%02ds", remainingTimeInSecs / 3600, (remainingTimeInSecs % 3600) / 60, (remainingTimeInSecs % 60));
            }
            out.printf("%25s%10s%n", "Active compaction remaining time : ", remainingTime);
        }
    }

}
