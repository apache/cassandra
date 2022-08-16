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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.airlift.airline.Command;
import io.airlift.airline.Option;

import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionInfo.Unit;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.util.File;
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

    @Option(title = "verbose",
            name = {"-v", "--verbose"},
            description = "Sort and display sstable paths for the compactions")
    private boolean verbose = false;

    @Override
    public void execute(NodeProbe probe)
    {
        if (verbose && vtableOutput)
        {
            throw new IllegalArgumentException("You can either specify verbose or vtable flag, not both.");
        }

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
        reportCompactionTable(cm.getCompactions(), probe.getCompactionThroughputBytes(), humanReadable, vtableOutput, out, verbose);
    }

    public static void reportCompactionTable(List<Map<String,String>> compactions, long compactionThroughputInBytes, boolean humanReadable, PrintStream out, boolean verbose)
    {
        reportCompactionTable(compactions, compactionThroughputInBytes, humanReadable, false, out, verbose);
    }

    public static void reportCompactionTable(List<Map<String,String>> compactions, long compactionThroughputInBytes, boolean humanReadable, boolean vtableOutput, PrintStream out, boolean verbose)
    {
        if (!compactions.isEmpty())
        {
            long remainingBytes = 0;
            TableBuilder table = new TableBuilder();

            if (vtableOutput)
                table.add("keyspace", "table", "task id", "completion ratio", "kind", "progress", "sstables", "total", "unit", "target directory");
            else
                table.add("id", "compaction type", "keyspace", "table", "completed", "total", "unit", "progress");

            // Map target directories to the list of the corresponding compactions

            Map<String, List<Map<String, String>>> targetDirectoryRows;

            if (vtableOutput)
            {
                targetDirectoryRows = compactions.stream().collect(Collectors.groupingBy(c -> {
                    String targetDirectory = c.get("targetDirectory");
                    if (targetDirectory == null || targetDirectory.isEmpty())
                        return CompactionInfo.NON_COMPACTION_OPERATION;
                    else
                        return targetDirectory;
                }, LinkedHashMap::new, Collectors.mapping(Function.identity(), Collectors.toList())));
            }
            else
            {
                targetDirectoryRows = compactions.stream().collect(Collectors.groupingBy(c -> {
                    String targetDirectory = c.get("targetDirectory");
                    String keyspace = c.get("keyspace");

                    if (keyspace != null && !keyspace.isEmpty())
                    {
                        int i = targetDirectory.lastIndexOf(keyspace);
                        if (i != -1)
                            return targetDirectory.substring(0, i);
                        else
                            return CompactionInfo.NON_COMPACTION_OPERATION;
                    }
                    else
                        return CompactionInfo.NON_COMPACTION_OPERATION;
                }, LinkedHashMap::new, Collectors.mapping(Function.identity(), Collectors.toList())));
            }

            for (Entry<String, List<Map<String, String>>> entry : targetDirectoryRows.entrySet())
            {
                String targetDirectory;
                String key = entry.getKey();
                if (key.equals(CompactionInfo.NON_COMPACTION_OPERATION))
                {
                    targetDirectory = key;
                }
                else
                {
                    try
                    {
                        targetDirectory = new File(key).canonicalPath();
                    }
                    catch (Throwable e)
                    {
                        // Couldn't parse entry into the canonical path, just use as is
                        targetDirectory = key;
                    }
                }

                if (verbose)
                    table.add(targetDirectory);

                for (Map<String, String> c : entry.getValue())
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
                    String percentComplete = total == 0 ? "n/a" : new DecimalFormat("0.00").format((double) completed / total * 100) + '%';
                    String id = c.get(CompactionInfo.COMPACTION_ID);
                    if (vtableOutput)
                    {
                        String targetDirectoryForOutput = targetDirectory.equals(CompactionInfo.NON_COMPACTION_OPERATION) ? "" : targetDirectory;
                        table.add(keyspace, columnFamily, id, percentComplete, taskType, progressStr, String.valueOf(tables.length), totalStr, unit, targetDirectoryForOutput);
                    }
                    else
                        table.add(id, taskType, keyspace, columnFamily, progressStr, totalStr, unit, percentComplete);

                    if (taskType.equals(OperationType.COMPACTION.toString()))
                        remainingBytes += total - completed;
                }

                // If with more info, add an empty line between this and next path set
                if (verbose)
                    table.addEmptyLine();
            }

            // if we are printing more info, we need to remove the extra newline before printing the totals
            if (verbose)
                table.removeLastLine();

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