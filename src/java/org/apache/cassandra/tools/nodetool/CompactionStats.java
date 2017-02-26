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

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import io.airlift.airline.Command;
import io.airlift.airline.Option;

import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.CompactionInfo.Unit;
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

    @Option(title = "with_more_info",
            name = {"-v", "--with-more-info"},
            description = "Sort and display sstable paths for the compactions")
    private boolean withMoreInfo = false;

    @Override
    public void execute(NodeProbe probe)
    {
        CompactionManagerMBean cm = probe.getCompactionManagerProxy();
        Map<String, Map<String, Integer>> pendingTaskNumberByTable =
            (Map<String, Map<String, Integer>>) probe.getCompactionMetric("PendingTasksByTableName");
        int numTotalPendingTask = 0;
        for (Entry<String, Map<String, Integer>> ksEntry : pendingTaskNumberByTable.entrySet())
        {
            for (Entry<String, Integer> tableEntry : ksEntry.getValue().entrySet())
                numTotalPendingTask += tableEntry.getValue();
        }
        System.out.println("pending tasks: " + numTotalPendingTask);
        for (Entry<String, Map<String, Integer>> ksEntry : pendingTaskNumberByTable.entrySet())
        {
            String ksName = ksEntry.getKey();
            for (Entry<String, Integer> tableEntry : ksEntry.getValue().entrySet())
            {
                String tableName = tableEntry.getKey();
                int pendingTaskCount = tableEntry.getValue();

                System.out.println("- " + ksName + '.' + tableName + ": " + pendingTaskCount);
            }
        }
        System.out.println();
        reportCompactionTable(cm.getCompactions(), probe.getCompactionThroughput(), humanReadable, withMoreInfo);
    }

    public static void reportCompactionTable(List<Map<String,String>> compactions, int compactionThroughput, boolean humanReadable, boolean withMoreInfo)
    {

        if (!compactions.isEmpty())
        {
            long remainingBytes = 0;
            final TableBuilder table = new TableBuilder();
            final Map<String, List<Map<String, String>>> targetDirectoryRows;

            table.add("id", "compaction type", "keyspace", "table", "completed", "total", "unit", "progress");

            // Map target directories to the list of the corresponding compactions
            targetDirectoryRows = compactions.stream()
                    .collect(Collectors.groupingBy(c -> c.get("targetDirectory").substring(0,c.get("targetDirectory").lastIndexOf(c.get("keyspace")))));


            for (Entry<String, List<Map<String, String>>> entry : targetDirectoryRows.entrySet())
            {
                // if we require more info, add the directory name before the list of compactions
                if (withMoreInfo)
                {
                    try
                    {
                        // Use canonical path
                        final File compactionPath = new File(entry.getKey());
                        table.add(compactionPath.getCanonicalPath());
                    }
                    catch (IOException e)
                    {
                        // Couldn't parse entry into the canonical path, just use as is
                        table.add(entry.getKey());
                    }
                }

                // Add the compactions
                for (Map<String, String> c : entry.getValue())
                {
                    long total = Long.parseLong(c.get(CompactionInfo.TOTAL));
                    long completed = Long.parseLong(c.get(CompactionInfo.COMPLETED));
                    String taskType = c.get(CompactionInfo.TASK_TYPE);
                    String keyspace = c.get(CompactionInfo.KEYSPACE);
                    String columnFamily = c.get(CompactionInfo.COLUMNFAMILY);
                    String unit = c.get(CompactionInfo.UNIT);
                    boolean toFileSize = humanReadable && Unit.isFileSize(unit);
                    String completedStr = toFileSize ? FileUtils.stringifyFileSize(completed) : Long.toString(completed);
                    String totalStr = toFileSize ? FileUtils.stringifyFileSize(total) : Long.toString(total);
                    String percentComplete = total == 0 ? "n/a" : new DecimalFormat("0.00").format((double) completed / total * 100) + "%";
                    String id = c.get(CompactionInfo.COMPACTION_ID);
                    table.add(id, taskType, keyspace, columnFamily, completedStr, totalStr, unit, percentComplete);
                    if (taskType.equals(OperationType.COMPACTION.toString()))
                        remainingBytes += total - completed;
                }

                // If with more info, add an empty line between this and next path set
                if (withMoreInfo)
                    table.addEmptyLine();
            }

            // if we printing more info, we need to remove the extra newline before printing the totals
            if (withMoreInfo)
                table.removeLastLine();
            table.printTo(System.out);

            String remainingTime = "n/a";
            if (compactionThroughput != 0)
            {
                long remainingTimeInSecs = remainingBytes / (1024L * 1024L * compactionThroughput);
                remainingTime = format("%dh%02dm%02ds", remainingTimeInSecs / 3600, (remainingTimeInSecs % 3600) / 60, (remainingTimeInSecs % 60));
            }

            System.out.printf("%25s%10s%n", "Active compaction remaining time : ", remainingTime);
        }
    }

}