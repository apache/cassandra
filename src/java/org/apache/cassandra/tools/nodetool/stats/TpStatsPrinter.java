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

import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.google.common.collect.Multimap;

import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;

import static java.util.stream.Collectors.toList;

public class TpStatsPrinter
{
    public static StatsPrinter from(String format)
    {
        switch (format)
        {
            case "json":
                return new StatsPrinter.JsonPrinter();
            case "yaml":
                return new StatsPrinter.YamlPrinter();
            default:
                return new DefaultPrinter();
        }
    }

    public static class DefaultPrinter implements StatsPrinter<TpStatsHolder>
    {
        @Override
        public void print(TpStatsHolder data, PrintStream out)
        {
            final TableBuilder poolBuilder = new TableBuilder();
            poolBuilder.add("Pool Name", "Active", "Pending", "Completed", "Blocked", "All time blocked");

            final Multimap<String, String> threadPools = data.probe.getThreadPools();

            for (final Map.Entry<String, String> tpool : threadPools.entries())
            {
                poolBuilder.add(tpool.getValue(),
                                 data.probe.getThreadPoolMetric(tpool.getKey(), tpool.getValue(), "ActiveTasks").toString(),
                                 data.probe.getThreadPoolMetric(tpool.getKey(), tpool.getValue(), "PendingTasks").toString(),
                                 data.probe.getThreadPoolMetric(tpool.getKey(), tpool.getValue(), "CompletedTasks").toString(),
                                 data.probe.getThreadPoolMetric(tpool.getKey(), tpool.getValue(), "CurrentlyBlockedTasks").toString(),
                                 data.probe.getThreadPoolMetric(tpool.getKey(), tpool.getValue(), "TotalBlockedTasks").toString());
            }

            poolBuilder.printTo(out);

            out.println("\nLatencies waiting in queue (micros) per dropped message types");

            final TableBuilder droppedBuilder = new TableBuilder();
            droppedBuilder.add("Message type", "Dropped    ", "50%     ", "95%     ", "99%     ", "Max");

            final HashMap<String, Object> droppedMessage = new HashMap<>();
            final HashMap<String, Double[]> waitLatencies = new HashMap<>();

            for (final Map.Entry<String, Integer> entry : data.probe.getDroppedMessages().entrySet())
            {
                droppedMessage.put(entry.getKey(), entry.getValue());
                try
                {
                    waitLatencies.put(entry.getKey(), data.probe.metricPercentilesAsArray(data.probe.getMessagingQueueWaitMetrics(entry.getKey())));
                }
                catch (RuntimeException e)
                {
                    // ignore the exceptions when fetching metrics
                }
            }

            for (final Map.Entry<String, Object> entry : droppedMessage.entrySet())
            {
                final List<String> columns = Stream.of(entry.getKey(), entry.getValue().toString()).collect(toList());

                if (waitLatencies.containsKey(entry.getKey()))
                {
                    final Double[] latencies = waitLatencies.get(entry.getKey());
                    columns.addAll(Arrays.asList(latencies[0].toString(),
                                                 latencies[2].toString(),
                                                 latencies[4].toString(),
                                                 latencies[6].toString()));
                }
                else
                {
                    columns.addAll(Arrays.asList("N/A", "N/A", "N/A", "N/A"));
                }

                droppedBuilder.add(columns.toArray(new String[0]));
            }

            droppedBuilder.printTo(out);
        }
    }
}
