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
package org.apache.cassandra.service;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;
import org.apache.cassandra.repair.autorepair.AutoRepairUtils;
import org.apache.cassandra.utils.MBeanWrapper;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;

/**
 * Implement all the MBeans for AutoRepair.
 */
public class AutoRepairService implements AutoRepairServiceMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=AutoRepairService";

    @VisibleForTesting
    protected AutoRepairConfig config;

    public static final AutoRepairService instance = new AutoRepairService();

    @VisibleForTesting
    protected AutoRepairService()
    {
    }

    public static void setup()
    {
        instance.config = DatabaseDescriptor.getAutoRepairConfig();
    }

    static
    {
        MBeanWrapper.instance.registerMBean(instance, MBEAN_NAME);
    }

    public void checkCanRun(String repairType)
    {
        checkCanRun(RepairType.parse(repairType));
    }

    public void checkCanRun(RepairType repairType)
    {
        if (!config.isAutoRepairSchedulingEnabled())
            throw new ConfigurationException("Auto-repair scheduler is disabled.");

        if (repairType != RepairType.INCREMENTAL)
            return;

        if (config.getMaterializedViewRepairEnabled(repairType) && DatabaseDescriptor.isMaterializedViewsOnRepairEnabled())
            throw new ConfigurationException("Cannot run incremental repair while materialized view replay is enabled. Set materialized_views_on_repair_enabled to false.");

        if (DatabaseDescriptor.isCDCEnabled() && DatabaseDescriptor.isCDCOnRepairEnabled())
            throw new ConfigurationException("Cannot run incremental repair while CDC replay is enabled. Set cdc_on_repair_enabled to false.");
    }

    public AutoRepairConfig getAutoRepairConfig()
    {
        return config;
    }

    @Override
    public boolean isAutoRepairDisabled()
    {
        return config == null || !config.isAutoRepairSchedulingEnabled();
    }

    @Override
    public String getAutoRepairConfiguration()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("repair scheduler configuration:");
        appendConfig(sb, "repair_check_interval", config.getRepairCheckInterval());
        appendConfig(sb, "repair_task_min_duration", config.getRepairTaskMinDuration());
        appendConfig(sb, "history_clear_delete_hosts_buffer_interval", config.getAutoRepairHistoryClearDeleteHostsBufferInterval());
        for (RepairType repairType : RepairType.values())
        {
            sb.append(formatRepairTypeConfig(repairType, config));
        }
        return sb.toString();
    }

    @Override
    public void setAutoRepairEnabled(String repairType, boolean enabled)
    {
        checkCanRun(repairType);
        config.setAutoRepairEnabled(RepairType.parse(repairType), enabled);
    }

    @Override
    public void setRepairThreads(String repairType, int repairThreads)
    {
        config.setRepairThreads(RepairType.parse(repairType), repairThreads);
    }

    @Override
    public void setRepairPriorityForHosts(String repairType, String commaSeparatedHostSet)
    {
        Set<InetAddressAndPort> hosts = InetAddressAndPort.parseHosts(commaSeparatedHostSet, false);
        if (!hosts.isEmpty())
        {
            AutoRepairUtils.addPriorityHosts(RepairType.parse(repairType), hosts);
        }
    }

    @Override
    public void setForceRepairForHosts(String repairType, String commaSeparatedHostSet)
    {
        Set<InetAddressAndPort> hosts = InetAddressAndPort.parseHosts(commaSeparatedHostSet, false);
        if (!hosts.isEmpty())
        {
            AutoRepairUtils.setForceRepair(RepairType.parse(repairType), hosts);
        }
    }

    @Override
    public void setRepairMinInterval(String repairType, String minRepairInterval)
    {
        config.setRepairMinInterval(RepairType.parse(repairType), minRepairInterval);
    }

    @Override
    public void startScheduler()
    {
        config.startScheduler();
    }

    @Override
    public void setAutoRepairHistoryClearDeleteHostsBufferDuration(String duration)
    {
        config.setAutoRepairHistoryClearDeleteHostsBufferInterval(duration);
    }

    @Override
    public void setAutoRepairMinRepairTaskDuration(String duration)
    {
        config.setRepairTaskMinDuration(duration);
    }

    @Override
    public void setRepairSSTableCountHigherThreshold(String repairType, int sstableHigherThreshold)
    {
        config.setRepairSSTableCountHigherThreshold(RepairType.parse(repairType), sstableHigherThreshold);
    }

    @Override
    public void setAutoRepairTableMaxRepairTime(String repairType, String autoRepairTableMaxRepairTime)
    {
        config.setAutoRepairTableMaxRepairTime(RepairType.parse(repairType), autoRepairTableMaxRepairTime);
    }

    @Override
    public void setIgnoreDCs(String repairType, Set<String> ignoreDCs)
    {
        config.setIgnoreDCs(RepairType.parse(repairType), ignoreDCs);
    }

    @Override
    public void setPrimaryTokenRangeOnly(String repairType, boolean primaryTokenRangeOnly)
    {
        config.setRepairPrimaryTokenRangeOnly(RepairType.parse(repairType), primaryTokenRangeOnly);
    }

    @Override
    public void setParallelRepairPercentage(String repairType, int percentage)
    {
        config.setParallelRepairPercentage(RepairType.parse(repairType), percentage);
    }

    @Override
    public void setParallelRepairCount(String repairType, int count)
    {
        config.setParallelRepairCount(RepairType.parse(repairType), count);
    }

    @Override
    public void setAllowParallelReplicaRepair(String repairType, boolean enabled)
    {
        config.setAllowParallelReplicaRepair(RepairType.parse(repairType), enabled);
    }

    @Override
    public void setAllowParallelReplicaRepairAcrossSchedules(String repairType, boolean enabled)
    {
        config.setAllowParallelReplicaRepairAcrossSchedules(RepairType.parse(repairType), enabled);
    }

    @Override
    public void setMVRepairEnabled(String repairType, boolean enabled)
    {
        config.setMaterializedViewRepairEnabled(RepairType.parse(repairType), enabled);
    }

    @Override
    public void setRepairSessionTimeout(String repairType, String timeout)
    {
        config.setRepairSessionTimeout(RepairType.parse(repairType), timeout);
    }

    @Override
    public Set<String> getOnGoingRepairHostIds(String repairType)
    {
        List<AutoRepairUtils.AutoRepairHistory> histories = AutoRepairUtils.getAutoRepairHistory(RepairType.parse(repairType));
        if (histories == null)
        {
            return Collections.emptySet();
        }
        Set<String> hostIds = new HashSet<>();
        AutoRepairUtils.CurrentRepairStatus currentRepairStatus = new AutoRepairUtils.CurrentRepairStatus(histories, AutoRepairUtils.getPriorityHostIds(RepairType.parse(repairType)), null);
        for (UUID id : currentRepairStatus.hostIdsWithOnGoingRepair)
        {
            hostIds.add(id.toString());
        }
        for (UUID id : currentRepairStatus.hostIdsWithOnGoingForceRepair)
        {
            hostIds.add(id.toString());
        }
        return Collections.unmodifiableSet(hostIds);
    }

    @Override
    public void setAutoRepairTokenRangeSplitterParameter(String repairType, String key, String value)
    {
        config.getTokenRangeSplitterInstance(RepairType.parse(repairType)).setParameter(key, value);
    }

    @Override
    public void setRepairByKeyspace(String repairType, boolean repairByKeyspace)
    {
        config.setRepairByKeyspace(RepairType.parse(repairType), repairByKeyspace);
    }

    @Override
    public void setAutoRepairMaxRetriesCount(String repairType, int retries)
    {
        config.setRepairMaxRetries(RepairType.parse(repairType), retries);
    }

    @Override
    public void setAutoRepairRetryBackoff(String repairType, String interval)
    {
        config.setRepairRetryBackoff(RepairType.parse(repairType), interval);
    }

    private String formatRepairTypeConfig(RepairType repairType, AutoRepairConfig config)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("\nconfiguration for repair_type: ").append(repairType.getConfigName());
        sb.append("\n\tenabled: ").append(config.isAutoRepairEnabled(repairType));
        // Only show configuration if enabled
        if (config.isAutoRepairEnabled(repairType))
        {
            Set<InetAddressAndPort> priorityHosts = AutoRepairUtils.getPriorityHosts(repairType);
            if (!priorityHosts.isEmpty())
            {
                appendConfig(sb, "priority_hosts", Joiner.on(',').skipNulls().join(priorityHosts));
            }

            appendConfig(sb, "min_repair_interval", config.getRepairMinInterval(repairType));
            appendConfig(sb, "repair_by_keyspace", config.getRepairByKeyspace(repairType));
            appendConfig(sb, "number_of_repair_threads", config.getRepairThreads(repairType));
            appendConfig(sb, "sstable_upper_threshold", config.getRepairSSTableCountHigherThreshold(repairType));
            appendConfig(sb, "table_max_repair_time", config.getAutoRepairTableMaxRepairTime(repairType));
            appendConfig(sb, "ignore_dcs", config.getIgnoreDCs(repairType));
            appendConfig(sb, "repair_primary_token_range_only", config.getRepairPrimaryTokenRangeOnly(repairType));
            appendConfig(sb, "parallel_repair_count", config.getParallelRepairCount(repairType));
            appendConfig(sb, "parallel_repair_percentage", config.getParallelRepairPercentage(repairType));
            appendConfig(sb, "allow_parallel_replica_repair", config.getAllowParallelReplicaRepair(repairType));
            appendConfig(sb, "allow_parallel_replica_repair_across_schedules", config.getAllowParallelReplicaRepairAcrossSchedules(repairType));
            appendConfig(sb, "materialized_view_repair_enabled", config.getMaterializedViewRepairEnabled(repairType));
            appendConfig(sb, "initial_scheduler_delay", config.getInitialSchedulerDelay(repairType));
            appendConfig(sb, "repair_session_timeout", config.getRepairSessionTimeout(repairType));
            appendConfig(sb, "force_repair_new_node", config.getForceRepairNewNode(repairType));
            appendConfig(sb, "repair_max_retries", config.getRepairMaxRetries(repairType));
            appendConfig(sb, "repair_retry_backoff", config.getRepairRetryBackoff(repairType));

            final ParameterizedClass splitterClass = config.getTokenRangeSplitter(repairType);
            final String splitterClassName =  splitterClass.class_name != null ? splitterClass.class_name : AutoRepairConfig.DEFAULT_SPLITTER.getName();
            appendConfig(sb, "token_range_splitter", splitterClassName);
            Map<String, String> tokenRangeSplitterParameters = config.getTokenRangeSplitterInstance(repairType).getParameters();
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
