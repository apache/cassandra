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
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.unifiedrepair.UnifiedRepairConfig;
import org.apache.cassandra.repair.unifiedrepair.UnifiedRepairConfig.RepairType;
import org.apache.cassandra.repair.unifiedrepair.UnifiedRepairUtils;
import org.apache.cassandra.utils.MBeanWrapper;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;

public class UnifiedRepairService implements UnifiedRepairServiceMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=UnifiedRepairService";

    @VisibleForTesting
    protected UnifiedRepairConfig config;

    public static final UnifiedRepairService instance = new UnifiedRepairService();

    @VisibleForTesting
    protected UnifiedRepairService()
    {
    }

    public static void setup()
    {
        instance.config = DatabaseDescriptor.getUnifiedRepairConfig();
    }

    static
    {
        MBeanWrapper.instance.registerMBean(instance, MBEAN_NAME);
    }

    public void checkCanRun(RepairType repairType)
    {
        if (!config.isUnifiedRepairSchedulingEnabled())
            throw new ConfigurationException("Unified-repair scheduller is disabled.");

        if (repairType != RepairType.incremental)
            return;

        if (DatabaseDescriptor.isMaterializedViewsOnRepairEnabled())
            throw new ConfigurationException("Cannot run incremental repair while materialized view replay is enabled. Set materialized_views_on_repair_enabled to false.");

        if (DatabaseDescriptor.isCDCOnRepairEnabled())
            throw new ConfigurationException("Cannot run incremental repair while CDC replay is enabled. Set cdc_on_repair_enabled to false.");
    }

    @Override
    public UnifiedRepairConfig getUnifiedRepairConfig()
    {
        return config;
    }

    @Override
    public void setUnifiedRepairEnabled(RepairType repairType, boolean enabled)
    {
        checkCanRun(repairType);
        config.setUnifiedRepairEnabled(repairType, enabled);
    }

    @Override
    public void setRepairThreads(RepairType repairType, int repairThreads)
    {
        config.setRepairThreads(repairType, repairThreads);
    }

    @Override
    public void setRepairPriorityForHosts(RepairType repairType, Set<InetAddressAndPort> hosts)
    {
        UnifiedRepairUtils.addPriorityHosts(repairType, hosts);
    }

    @Override
    public Set<InetAddressAndPort> getRepairHostPriority(RepairType repairType) {
        return UnifiedRepairUtils.getPriorityHosts(repairType);
    }

    @Override
    public void setForceRepairForHosts(RepairType repairType, Set<InetAddressAndPort> hosts)
    {
        UnifiedRepairUtils.setForceRepair(repairType, hosts);
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

    @Override
    public void startScheduler()
    {
        config.startScheduler();
    }

    public void setUnifiedRepairHistoryClearDeleteHostsBufferDuration(String duration)
    {
        config.setUnifiedRepairHistoryClearDeleteHostsBufferInterval(duration);
    }

    @Override
    public void setUnifiedRepairMaxRetriesCount(int retries)
    {
        config.setRepairMaxRetries(retries);
    }

    @Override
    public void setUnifiedRepairRetryBackoff(String interval)
    {
        config.setRepairRetryBackoff(interval);
    }

    @Override
    public void setRepairSSTableCountHigherThreshold(RepairType repairType, int sstableHigherThreshold)
    {
        config.setRepairSSTableCountHigherThreshold(repairType, sstableHigherThreshold);
    }

    @Override
    public void setUnifiedRepairTableMaxRepairTime(RepairType repairType, String unifiedRepairTableMaxRepairTime)
    {
        config.setUnifiedRepairTableMaxRepairTime(repairType, unifiedRepairTableMaxRepairTime);
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
    public void setParallelRepairPercentage(RepairType repairType, int percentage)
    {
        config.setParallelRepairPercentage(repairType, percentage);
    }

    @Override
    public void setParallelRepairCount(RepairType repairType, int count)
    {
        config.setParallelRepairCount(repairType, count);
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
        List<UnifiedRepairUtils.UnifiedRepairHistory> histories = UnifiedRepairUtils.getUnifiedRepairHistory(rType);
        if (histories == null)
        {
            return hostIds;
        }
        UnifiedRepairUtils.CurrentRepairStatus currentRepairStatus = new UnifiedRepairUtils.CurrentRepairStatus(histories, UnifiedRepairUtils.getPriorityHostIds(rType));
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
