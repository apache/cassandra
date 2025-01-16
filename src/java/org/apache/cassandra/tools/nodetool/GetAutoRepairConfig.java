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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import io.airlift.airline.Command;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;

import java.io.PrintStream;
import java.util.Map;
import java.util.Set;

@Command(name = "getautorepairconfig", description = "Print autorepair configurations")
public class GetAutoRepairConfig extends NodeToolCmd
{
    @VisibleForTesting
    protected static PrintStream out = System.out;

    @Override
    public void execute(NodeProbe probe)
    {
        AutoRepairConfig config = probe.getAutoRepairConfig();
        if (config == null || !config.isAutoRepairSchedulingEnabled())
        {
            out.println("Auto-repair is not enabled");
            return;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("repair scheduler configuration:");
        appendConfig(sb, "repair_check_interval", config.getRepairCheckInterval());
        appendConfig(sb, "repair_max_retries", config.getRepairMaxRetries());
        appendConfig(sb, "repair_retry_backoff", config.getRepairRetryBackoff());
        appendConfig(sb, "repair_task_min_duration", config.getRepairTaskMinDuration());
        appendConfig(sb, "history_clear_delete_hosts_buffer_interval", config.getAutoRepairHistoryClearDeleteHostsBufferInterval());
        for (RepairType repairType : RepairType.values())
        {
            sb.append(formatRepairTypeConfig(probe, repairType, config));
        }

        out.println(sb);
    }

    private String formatRepairTypeConfig(NodeProbe probe, RepairType repairType, AutoRepairConfig config)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("\nconfiguration for repair_type: ").append(repairType.getConfigName());
        sb.append("\n\tenabled: ").append(config.isAutoRepairEnabled(repairType));
        // Only show configuration if enabled
        if (config.isAutoRepairEnabled(repairType))
        {
            Set<InetAddressAndPort> priorityHosts = probe.getRepairPriorityForHosts(repairType);
            if (!priorityHosts.isEmpty())
            {
                appendConfig(sb, "priority_hosts", Joiner.on(',').skipNulls().join(priorityHosts));
            }

            appendConfig(sb , "min_repair_interval", config.getRepairMinInterval(repairType));
            appendConfig(sb , "repair_by_keyspace", config.getRepairByKeyspace(repairType));
            appendConfig(sb , "number_of_repair_threads", config.getRepairThreads(repairType));
            appendConfig(sb , "sstable_upper_threshold", config.getRepairSSTableCountHigherThreshold(repairType));
            appendConfig(sb , "table_max_repair_time", config.getAutoRepairTableMaxRepairTime(repairType));
            appendConfig(sb , "ignore_dcs", config.getIgnoreDCs(repairType));
            appendConfig(sb , "repair_primary_token_range_only", config.getRepairPrimaryTokenRangeOnly(repairType));
            appendConfig(sb , "parallel_repair_count", config.getParallelRepairCount(repairType));
            appendConfig(sb , "parallel_repair_percentage", config.getParallelRepairPercentage(repairType));
            appendConfig(sb , "materialized_view_repair_enabled", config.getMaterializedViewRepairEnabled(repairType));
            appendConfig(sb , "initial_scheduler_delay", config.getInitialSchedulerDelay(repairType));
            appendConfig(sb , "repair_session_timeout", config.getRepairSessionTimeout(repairType));
            appendConfig(sb , "force_repair_new_node", config.getForceRepairNewNode(repairType));

            final ParameterizedClass splitterClass = config.getTokenRangeSplitter(repairType);
            final String splitterClassName =  splitterClass.class_name != null ? splitterClass.class_name : AutoRepairConfig.DEFAULT_SPLITTER.getName();
            appendConfig(sb, "token_range_splitter", splitterClassName);
            Map<String, String> tokenRangeSplitterParameters = probe.getAutoRepairTokenRangeSplitterParameters(repairType);
            if (!tokenRangeSplitterParameters.isEmpty())
            {
                for (Map.Entry<String, String> param : tokenRangeSplitterParameters.entrySet())
                {
                    appendConfig(sb, String.format("token_range_splitter.%s", param.getKey()), param.getValue());
                }
            }
        }

        return sb.toString();
    }

    private <T> void appendConfig(StringBuilder sb, String config, T value)
    {
        sb.append(String.format("%s%s: %s", "\n\t", config, value));
    }
}
