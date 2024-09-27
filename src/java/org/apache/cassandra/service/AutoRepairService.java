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

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;
import org.apache.cassandra.repair.autorepair.AutoRepairUtils;
import org.apache.cassandra.utils.MBeanWrapper;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;

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

    public void checkCanRun(RepairType repairType)
    {
        if (!config.isAutoRepairSchedulingEnabled())
            throw new ConfigurationException("Auto-repair scheduller is disabled.");

        if (repairType != RepairType.incremental)
            return;

        if (CassandraRelevantProperties.STREAMING_REQUIRES_VIEW_BUILD_DURING_REPAIR.getBoolean())
            throw new ConfigurationException("Cannot run incremental repair while materialized view replay is enabled.");

        if (CassandraRelevantProperties.STREAMING_REQUIRES_CDC_REPLAY.getBoolean())
            throw new ConfigurationException("Cannot run incremental repair while CDC replay is enabled.");
    }

    @Override
    public AutoRepairConfig getAutoRepairConfig()
    {
        return config;
    }

    @Override
    public void setAutoRepairEnabled(RepairType repairType, boolean enabled)
    {
        checkCanRun(repairType);
        config.setAutoRepairEnabled(repairType, enabled);
    }

    @Override
    public void setRepairThreads(RepairType repairType, int repairThreads)
    {
        config.setRepairThreads(repairType, repairThreads);
    }

    @Override
    public void setRepairPriorityForHosts(RepairType repairType, Set<InetAddressAndPort> hosts)
    {
        AutoRepairUtils.addPriorityHosts(repairType, hosts);
    }

    @Override
    public Set<InetAddressAndPort> getRepairHostPriority(RepairType repairType) {
        return AutoRepairUtils.getPriorityHosts(repairType);
    }

    @Override
    public void setForceRepairForHosts(RepairType repairType, Set<InetAddressAndPort> hosts)
    {
        AutoRepairUtils.setForceRepair(repairType, hosts);
    }

    @Override
    public void setRepairSubRangeNum(RepairType repairType, int repairSubRanges)
    {
        config.setRepairSubRangeNum(repairType, repairSubRanges);
    }

    @Override
    public void setRepairMinInterval(RepairType repairType, String minRepairInterval)
    {
        config.setRepairMinInterval(repairType, minRepairInterval);
    }

    public void setAutoRepairHistoryClearDeleteHostsBufferDuration(String duration)
    {
        config.setAutoRepairHistoryClearDeleteHostsBufferInterval(duration);
    }

    @Override
    public void setAutoRepairMaxRetriesCount(int retries)
    {
        config.setRepairMaxRetries(retries);
    }

    @Override
    public void setAutoRepairRetryBackoff(String interval)
    {
        config.setRepairRetryBackoff(interval);
    }

    @Override
    public void setRepairSSTableCountHigherThreshold(RepairType repairType, int sstableHigherThreshold)
    {
        config.setRepairSSTableCountHigherThreshold(repairType, sstableHigherThreshold);
    }

    @Override
    public void setAutoRepairTableMaxRepairTime(RepairType repairType, String autoRepairTableMaxRepairTime)
    {
        config.setAutoRepairTableMaxRepairTime(repairType, autoRepairTableMaxRepairTime);
    }

    @Override
    public void setIgnoreDCs(RepairType repairType, Set<String> ignoreDCs)
    {
        config.setIgnoreDCs(repairType, ignoreDCs);
    }

    @Override
    public void setPrimaryTokenRangeOnly(RepairType repairType, boolean primaryTokenRangeOnly)
    {
        config.setRepairPrimaryTokenRangeOnly(repairType, primaryTokenRangeOnly);
    }

    @Override
    public void setParallelRepairPercentageInGroup(RepairType repairType, int percentageInGroup)
    {
        config.setParallelRepairPercentage(repairType, percentageInGroup);
    }

    @Override
    public void setParallelRepairCountInGroup(RepairType repairType, int countInGroup)
    {
        config.setParallelRepairCount(repairType, countInGroup);
    }

    public void setMVRepairEnabled(RepairType repairType, boolean enabled)
    {
        config.setMVRepairEnabled(repairType, enabled);
    }

    public void setRepairSessionTimeout(RepairType repairType, String timeout)
    {
        config.setRepairSessionTimeout(repairType, timeout);
    }

    @Override
    public Set<String> getOnGoingRepairHostIds(RepairType rType)
    {
        Set<String> hostIds = new HashSet<>();
        List<AutoRepairUtils.AutoRepairHistory> histories = AutoRepairUtils.getAutoRepairHistory(rType);
        if (histories == null)
        {
            return null;
        }
        AutoRepairUtils.CurrentRepairStatus currentRepairStatus = new AutoRepairUtils.CurrentRepairStatus(histories, AutoRepairUtils.getPriorityHostIds(rType));
        for (UUID id : currentRepairStatus.hostIdsWithOnGoingRepair)
        {
            hostIds.add(id.toString());
        }
        for (UUID id : currentRepairStatus.hostIdsWithOnGoingForceRepair)
        {
            hostIds.add(id.toString());
        }
        return hostIds;
    }
}
