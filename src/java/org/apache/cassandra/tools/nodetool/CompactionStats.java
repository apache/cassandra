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
import java.text.NumberFormat;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import io.airlift.airline.Command;
import io.airlift.airline.Option;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionInfo.Unit;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

import static java.lang.String.format;

@Command(name = "compactionstats", description = "Print statistics on compactions")
public class CompactionStats extends NodeToolCmd
{
    private static final String LINE_SEPARATOR = CassandraRelevantProperties.LINE_SEPARATOR.getString();

    @Option(title = "human_readable",
            name = {"-H", "--human-readable"},
            description = "Display bytes in human readable form, i.e. KiB, MiB, GiB, TiB")
    private boolean humanReadable = false;

    @Override
    public void execute(NodeProbe probe)
    {
        PrintStream out = probe.output().out;
        out.print(pendingTasksAndConcurrentCompactorsStats(probe));
        out.print(compactionsCompletedStats(probe));
        out.print(compactionThroughputStats(probe));
        out.println();
        CompactionManagerMBean cm = probe.getCompactionManagerProxy();
        reportCompactionTable(cm.getCompactions(), probe.getCompactionThroughput(), humanReadable, out);
    }

    private static String pendingTasksAndConcurrentCompactorsStats(NodeProbe probe)
    {
        Map<String, Map<String, Integer>> pendingTaskNumberByTable =
                (Map<String, Map<String, Integer>>) probe.getCompactionMetric("PendingTasksByTableName");
        StringBuffer toPrint = new StringBuffer();
        toPrint.append(String.format("%s concurrent compactors, %s pending tasks", probe.getConcurrentCompactors()
                , numPendingTasks(pendingTaskNumberByTable)));
        toPrint.append(LINE_SEPARATOR);
        for (Entry<String, Map<String, Integer>> ksEntry : pendingTaskNumberByTable.entrySet())
        {
            String ksName = ksEntry.getKey();
            for (Entry<String, Integer> tableEntry : ksEntry.getValue().entrySet())
            {
                toPrint.append("- " + ksName + '.' + tableEntry.getKey() + ": " + tableEntry.getValue());
                toPrint.append(LINE_SEPARATOR);
            }
        }

        return toPrint.toString();
    }

    private static int numPendingTasks(Map<String, Map<String, Integer>> pendingTaskNumberByTable)
    {
        int numTotalPendingTasks = 0;
        for (Entry<String, Map<String, Integer>> ksEntry : pendingTaskNumberByTable.entrySet())
        {
            for (Entry<String, Integer> tableEntry : ksEntry.getValue().entrySet())
                numTotalPendingTasks += tableEntry.getValue();
        }

        return numTotalPendingTasks;
    }

    private static String compactionsCompletedStats(NodeProbe probe)
    {
        Long completedTasks = (Long)probe.getCompactionMetric("CompletedTasks");
        CassandraMetricsRegistry.JmxMeterMBean totalCompactionsCompletedMetrics =
                (CassandraMetricsRegistry.JmxMeterMBean)probe.getCompactionMetric("TotalCompactionsCompleted");
        NumberFormat formatter = new DecimalFormat("##.00");
        StringBuffer toPrint = new StringBuffer();
        toPrint.append(String.format("compactions completed: %s", completedTasks));
        toPrint.append(LINE_SEPARATOR);
        toPrint.append(String.format("\tminute rate:    %s/second", formatter.format(totalCompactionsCompletedMetrics.getOneMinuteRate())));
        toPrint.append(LINE_SEPARATOR);
        toPrint.append(String.format("\t5 minute rate:    %s/second", formatter.format(totalCompactionsCompletedMetrics.getFiveMinuteRate())));
        toPrint.append(LINE_SEPARATOR);
        toPrint.append(String.format("\t15 minute rate:    %s/second", formatter.format(totalCompactionsCompletedMetrics.getFifteenMinuteRate())));
        toPrint.append(LINE_SEPARATOR);
        toPrint.append(String.format("\tMean rate:    %s/second", formatter.format(totalCompactionsCompletedMetrics.getMeanRate())));
        toPrint.append(LINE_SEPARATOR);

        return toPrint.toString();
    }

    private static String compactionThroughputStats(NodeProbe probe)
    {
        double configured = probe.getCompactionThroughput();
        double actual = probe.getCompactionRate() / (1024 * 1024);
        if(configured == 0)
        {
            return String.format("compaction throughput absolute: %s MBps", actual);
        }
        else
        {
            double percentage = (actual / configured) * 100;
            return String.format("compaction throughput ratio: %s MBps / %s MBps (%s%s)", actual, configured, percentage, "%");
        }

    }

    public static void reportCompactionTable(List<Map<String,String>> compactions, int compactionThroughput, boolean humanReadable, PrintStream out)
    {
        if (!compactions.isEmpty())
        {
            long remainingBytes = 0;
            TableBuilder table = new TableBuilder();

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
                String completedStr = toFileSize ? FileUtils.stringifyFileSize(completed) : Long.toString(completed);
                String totalStr = toFileSize ? FileUtils.stringifyFileSize(total) : Long.toString(total);
                String percentComplete = total == 0 ? "n/a" : new DecimalFormat("0.00").format((double) completed / total * 100) + "%";
                String id = c.get(CompactionInfo.COMPACTION_ID);
                table.add(id, taskType, keyspace, columnFamily, completedStr, totalStr, unit, percentComplete);
                remainingBytes += total - completed;
            }
            table.printTo(out);

            String remainingTime = "n/a";
            if (compactionThroughput != 0)
            {
                long remainingTimeInSecs = remainingBytes / (1024L * 1024L * compactionThroughput);
                remainingTime = format("%dh%02dm%02ds", remainingTimeInSecs / 3600, (remainingTimeInSecs % 3600) / 60, (remainingTimeInSecs % 60));
            }
            out.printf("%25s%10s%n", "Active compaction remaining time : ", remainingTime);
        }
    }

}
