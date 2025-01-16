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
import com.google.common.base.Splitter;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool.NodeToolCmd;
import org.apache.cassandra.utils.LocalizeString;

import java.io.PrintStream;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

@Command(name = "setautorepairconfig", description = "sets the autorepair configuration")
public class SetAutoRepairConfig extends NodeToolCmd
{
    @VisibleForTesting
    @Arguments(title = "<autorepairparam> <value>", usage = "<autorepairparam> <value>",
    description = "autorepair param and value.\nPossible autorepair parameters are as following: " +
                  "[start_scheduler|number_of_repair_threads|min_repair_interval|sstable_upper_threshold" +
                  "|enabled|table_max_repair_time|priority_hosts|forcerepair_hosts|ignore_dcs" +
                  "|history_clear_delete_hosts_buffer_interval|repair_primary_token_range_only" +
                  "|parallel_repair_count|parallel_repair_percentage|materialized_view_repair_enabled|repair_max_retries" +
                  "|repair_retry_backoff|repair_session_timeout|min_repair_task_duration|token_range_splitter.<property>]",
    required = true)
    protected List<String> args = new ArrayList<>();

    @VisibleForTesting
    @Option(title = "repair type", name = { "-t", "--repair-type" }, description = "Repair type")
    protected String repairTypeStr;

    @VisibleForTesting
    protected PrintStream out = System.out;

    private static final String TOKEN_RANGE_SPLITTER_PROPERTY_PREFIX = "token_range_splitter.";

    @Override
    public void execute(NodeProbe probe)
    {
        checkArgument(args.size() == 2, "setautorepairconfig requires param-type, and value args.");
        String paramType = args.get(0);
        String paramVal = args.get(1);

        if (!probe.getAutoRepairConfig().isAutoRepairSchedulingEnabled() && !paramType.equalsIgnoreCase("start_scheduler"))
        {
            out.println("Auto-repair is not enabled");
            return;
        }

        // options that do not require --repair-type option
        switch (paramType)
        {
            case "start_scheduler":
                if (Boolean.parseBoolean(paramVal))
                {
                    probe.startScheduler();
                }
                return;
            case "history_clear_delete_hosts_buffer_interval":
                probe.setAutoRepairHistoryClearDeleteHostsBufferDuration(paramVal);
                return;
            case "repair_max_retries":
                probe.setAutoRepairMaxRetriesCount(Integer.parseInt(paramVal));
                return;
            case "repair_retry_backoff":
                probe.setAutoRepairRetryBackoff(paramVal);
                return;
            case "min_repair_task_duration":
                probe.setAutoRepairMinRepairTaskDuration(paramVal);
                return;
            default:
                // proceed to options that require --repair-type option
                break;
        }

        // options below require --repair-type option
        checkArgument(repairTypeStr != null, "--repair-type is required for this parameter.");
        final RepairType repairType = RepairType.valueOf(LocalizeString.toUpperCaseLocalized(repairTypeStr));

        if(paramType.startsWith(TOKEN_RANGE_SPLITTER_PROPERTY_PREFIX))
        {
            final String key = paramType.replace(TOKEN_RANGE_SPLITTER_PROPERTY_PREFIX, "");
            probe.setAutoRepairTokenRangeSplitterParameter(repairType, key, paramVal);
            return;
        }

        Set<InetAddressAndPort> hosts;
        switch (paramType)
        {
            case "enabled":
                probe.setAutoRepairEnabled(repairType, Boolean.parseBoolean(paramVal));
                break;
            case "number_of_repair_threads":
                probe.setRepairThreads(repairType, Integer.parseInt(paramVal));
                break;
            case "min_repair_interval":
                probe.setRepairMinInterval(repairType, paramVal);
                break;
            case "sstable_upper_threshold":
                probe.setRepairSSTableCountHigherThreshold(repairType, Integer.parseInt(paramVal));
                break;
            case "table_max_repair_time":
                probe.setAutoRepairTableMaxRepairTime(repairType, paramVal);
                break;
            case "priority_hosts":
                hosts = retrieveHosts(paramVal);
                if (!hosts.isEmpty())
                {
                    probe.setRepairPriorityForHosts(repairType, hosts);
                }
                break;
            case "forcerepair_hosts":
                hosts = retrieveHosts(paramVal);
                if (!hosts.isEmpty())
                {
                    probe.setForceRepairForHosts(repairType, hosts);
                }
                break;
            case "ignore_dcs":
                Set<String> ignoreDCs = new HashSet<>();
                for (String dc : Splitter.on(',').split(paramVal))
                {
                    ignoreDCs.add(dc);
                }
                probe.setAutoRepairIgnoreDCs(repairType, ignoreDCs);
                break;
            case "repair_primary_token_range_only":
                probe.setPrimaryTokenRangeOnly(repairType, Boolean.parseBoolean(paramVal));
                break;
            case "parallel_repair_count":
                probe.setParallelRepairCount(repairType, Integer.parseInt(paramVal));
                break;
            case "parallel_repair_percentage":
                probe.setParallelRepairPercentage(repairType, Integer.parseInt(paramVal));
                break;
            case "materialized_view_repair_enabled":
                probe.setMaterializedViewRepairEnabled(repairType, Boolean.parseBoolean(paramVal));
                break;
            case "repair_session_timeout":
                probe.setRepairSessionTimeout(repairType, paramVal);
                break;
            default:
                throw new IllegalArgumentException("Unknown parameter: " + paramType);
        }
    }

    private Set<InetAddressAndPort> retrieveHosts(String paramVal)
    {
        Set<InetAddressAndPort> hosts = new HashSet<>();
        for (String host : Splitter.on(',').split(paramVal))
        {
            try
            {
                hosts.add(InetAddressAndPort.getByName(host));
            }
            catch (UnknownHostException e)
            {
                out.println("invalid ip address: " + host);
            }
        }

        return hosts;
    }
}
