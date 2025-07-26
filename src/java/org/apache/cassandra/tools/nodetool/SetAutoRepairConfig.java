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


import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.cassandra.tools.NodeProbe;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Allows to set AutoRepair configuration through nodetool.
 */
@Command(name = "setautorepairconfig", description = "sets the autorepair configuration")
public class SetAutoRepairConfig extends AbstractCommand
{
    @VisibleForTesting
    protected List<String> args = new ArrayList<>();

    @Parameters(index = "0", arity = "0..1", description = { "Autorepair param type.",
                                                          "Possible autorepair parameters are as following: " +
                                                          "[start_scheduler|number_of_repair_threads|min_repair_interval|sstable_upper_threshold" +
                                                          "|enabled|table_max_repair_time|priority_hosts|forcerepair_hosts|ignore_dcs" +
                                                          "|history_clear_delete_hosts_buffer_interval|repair_primary_token_range_only" +
                                                          "|parallel_repair_count|parallel_repair_percentage" +
                                                          "|allow_parallel_replica_repair|allow_parallel_repair_across_schedules" +
                                                          "|materialized_view_repair_enabled|repair_max_retries" +
                                                          "|repair_retry_backoff|repair_session_timeout|min_repair_task_duration" +
                                                          "|repair_by_keyspace|token_range_splitter.<property>]" })
    public String autorepairParamType;

    @Parameters(index = "1", description = "Autorepair param value", arity = "0..1")
    public String autorepairParamValue;

    @VisibleForTesting
    @Option(paramLabel = "repairType", names = { "-t", "--repair-type" }, description = "Repair type")
    public String repairTypeStr;

    @VisibleForTesting
    protected PrintStream out = System.out;

    private static final String TOKEN_RANGE_SPLITTER_PROPERTY_PREFIX = "token_range_splitter.";

    @Override
    public void execute(NodeProbe probe)
    {
        args = args.isEmpty() ? CommandUtils.concatArgs(autorepairParamType, autorepairParamValue) : args;
        checkArgument(args.size() == 2, "setautorepairconfig requires param-type, and value args.");
        String paramType = args.get(0);
        String paramVal = args.get(1);

        if (probe.isAutoRepairDisabled() && !paramType.equalsIgnoreCase("start_scheduler"))
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
                    probe.startAutoRepairScheduler();
                }
                return;
            case "history_clear_delete_hosts_buffer_interval":
                probe.setAutoRepairHistoryClearDeleteHostsBufferDuration(paramVal);
                return;
            case "min_repair_task_duration":
                probe.setAutoRepairMinRepairTaskDuration(paramVal);
                return;
            default:
                // proceed to options that require --repair-type option
                break;
        }

        // options below require --repair-type option
        Objects.requireNonNull(repairTypeStr, "--repair-type is required for this parameter.");

        if(paramType.startsWith(TOKEN_RANGE_SPLITTER_PROPERTY_PREFIX))
        {
            final String key = paramType.replace(TOKEN_RANGE_SPLITTER_PROPERTY_PREFIX, "");
            probe.setAutoRepairTokenRangeSplitterParameter(repairTypeStr, key, paramVal);
            return;
        }

        switch (paramType)
        {
            case "enabled":
                probe.setAutoRepairEnabled(repairTypeStr, Boolean.parseBoolean(paramVal));
                break;
            case "number_of_repair_threads":
                probe.setAutoRepairThreads(repairTypeStr, Integer.parseInt(paramVal));
                break;
            case "min_repair_interval":
                probe.setAutoRepairMinInterval(repairTypeStr, paramVal);
                break;
            case "sstable_upper_threshold":
                probe.setAutoRepairSSTableCountHigherThreshold(repairTypeStr, Integer.parseInt(paramVal));
                break;
            case "table_max_repair_time":
                probe.setAutoRepairTableMaxRepairTime(repairTypeStr, paramVal);
                break;
            case "priority_hosts":
                if (paramVal!= null && !paramVal.isEmpty())
                {
                    probe.setAutoRepairPriorityForHosts(repairTypeStr, paramVal);
                }
                break;
            case "forcerepair_hosts":
                probe.setAutoRepairForceRepairForHosts(repairTypeStr, paramVal);
                break;
            case "ignore_dcs":
                Set<String> ignoreDCs = new HashSet<>();
                for (String dc : Splitter.on(',').split(paramVal))
                {
                    ignoreDCs.add(dc);
                }
                probe.setAutoRepairIgnoreDCs(repairTypeStr, ignoreDCs);
                break;
            case "repair_primary_token_range_only":
                probe.setAutoRepairPrimaryTokenRangeOnly(repairTypeStr, Boolean.parseBoolean(paramVal));
                break;
            case "parallel_repair_count":
                probe.setAutoRepairParallelRepairCount(repairTypeStr, Integer.parseInt(paramVal));
                break;
            case "parallel_repair_percentage":
                probe.setAutoRepairParallelRepairPercentage(repairTypeStr, Integer.parseInt(paramVal));
                break;
            case "allow_parallel_replica_repair":
                probe.setAutoRepairAllowParallelReplicaRepair(repairTypeStr, Boolean.parseBoolean(paramVal));
                break;
            case "allow_parallel_replica_repair_across_schedules":
                probe.setAutoRepairAllowParallelReplicaRepairAcrossSchedules(repairTypeStr, Boolean.parseBoolean(paramVal));
                break;
            case "materialized_view_repair_enabled":
                probe.setAutoRepairMaterializedViewRepairEnabled(repairTypeStr, Boolean.parseBoolean(paramVal));
                break;
            case "repair_session_timeout":
                probe.setAutoRepairSessionTimeout(repairTypeStr, paramVal);
                break;
            case "repair_by_keyspace":
                probe.setAutoRepairRepairByKeyspace(repairTypeStr, Boolean.parseBoolean(paramVal));
                break;
            case "repair_max_retries":
                probe.setAutoRepairMaxRetriesCount(repairTypeStr, Integer.parseInt(paramVal));
                break;
            case "repair_retry_backoff":
                probe.setAutoRepairRetryBackoff(repairTypeStr, paramVal);
                break;
            default:
                throw new IllegalArgumentException("Unknown parameter: " + paramType);
        }
    }
}
