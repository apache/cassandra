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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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
            Map<String, Object> convertData = data.convert2Map();

            out.printf("%-30s%10s%10s%15s%10s%18s%n", "Pool Name", "Active", "Pending", "Completed", "Blocked", "All time blocked");

            Map<Object, Object> threadPools = convertData.get("ThreadPools") instanceof Map<?, ?> ? (Map)convertData.get("ThreadPools") : Collections.emptyMap();
            for (Map.Entry<Object, Object> entry : threadPools.entrySet())
            {
                Map values = entry.getValue() instanceof Map<?, ?> ? (Map)entry.getValue() : Collections.emptyMap();
                out.printf("%-30s%10s%10s%15s%10s%18s%n",
                           entry.getKey(),
                           values.get("ActiveTasks"),
                           values.get("PendingTasks"),
                           values.get("CompletedTasks"),
                           values.get("CurrentlyBlockedTasks"),
                           values.get("TotalBlockedTasks"));
            }

            out.printf("%n%-20s%10s%n", "Message type", "Dropped");

            Map<Object, Object> droppedMessages = convertData.get("DroppedMessage") instanceof Map<?, ?> ? (Map)convertData.get("DroppedMessage") : Collections.emptyMap();
            for (Map.Entry<Object, Object> entry : droppedMessages.entrySet())
            {
                out.printf("%-20s%10s%n", entry.getKey(), entry.getValue());
            }
        }
    }
}
