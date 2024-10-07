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
import org.apache.cassandra.repair.AutoRepairConfig;
import org.apache.cassandra.repair.AutoRepairConfig.RepairType;
import org.apache.cassandra.repair.AutoRepairUtilsV2;
import org.apache.cassandra.repair.AutoRepairV2;
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

    @Override
    public AutoRepairConfig getAutoRepairConfig()
    {
        return config;
    }

    @Override
    public Set<InetAddressAndPort> filterHostsInLocalGroup(RepairType repairType, Set<InetAddressAndPort> hostsToFilter)
    {
        return AutoRepairUtilsV2.processNodesByGroup(repairType, hostsToFilter);
    }

    public void runAutoRepairOnce(RepairType repairType, long millisToWait)
    {
        AutoRepairV2.instance.repairAsync(repairType, millisToWait);
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
    public Set<String> getOnGoingRepairHostIdsByGroupHash(RepairType rType, int groupHash)
    {
        Set<String> hostIds = new HashSet<>();
        List<AutoRepairUtilsV2.AutoRepairHistory> histories = AutoRepairUtilsV2.getAutoRepairHistoryByGroupID(rType, groupHash);
        if (histories == null)
        {
            return null;
        }
        AutoRepairUtilsV2.CurrentRepairStatus currentRepairStatus = new AutoRepairUtilsV2.CurrentRepairStatus(histories, AutoRepairUtilsV2.getPriorityHostIds(rType, groupHash));
        for (UUID id : currentRepairStatus.hostIdsWithOnGoingRepair)
        {
            hostIds.add(id.toString());
        }
        return hostIds;
    }

    @Override
    public Set<String> getOnGoingForceRepairHostIdsByGroupHash(RepairType rType, int groupHash)
    {
        Set<String> hostIds = new HashSet<>();
        List<AutoRepairUtilsV2.AutoRepairHistory> histories = AutoRepairUtilsV2.getAutoRepairHistoryByGroupID(rType, groupHash);
        if (histories == null)
        {
            return null;
        }
        AutoRepairUtilsV2.CurrentRepairStatus currentRepairStatus = new AutoRepairUtilsV2.CurrentRepairStatus(histories, AutoRepairUtilsV2.getPriorityHostIds(rType, groupHash));
        for (UUID id : currentRepairStatus.hostIdsWithOnGoingForceRepair)
        {
            hostIds.add(id.toString());
        }
        return hostIds;
    }

    @Override
    public void setRepairPriorityForHosts(RepairType repairType, Set<InetAddressAndPort> hosts)
    {
        AutoRepairUtilsV2.addPriorityHosts(repairType, hosts);
    }

    @Override
    public Set<InetAddressAndPort> getRepairHostPriority(RepairType repairType) {
        return AutoRepairUtilsV2.getPriorityHosts(repairType);
    }

    @Override
    public void setForceRepairForHosts(RepairType repairType, Set<InetAddressAndPort> hosts)
    {
        AutoRepairUtilsV2.setForceRepair(repairType, hosts);
    }

    @Override
    public void setRepairSubRangeNum(RepairType repairType, int repairSubRanges)
    {
        config.setRepairSubRangeNum(repairType, repairSubRanges);
    }

    @Override
    public void setRepairMinIntervalInHours(RepairType repairType, int repairMinFrequencyInHours)
    {
        config.setRepairMinIntervalInHours(repairType, repairMinFrequencyInHours);
    }

    @Override
    public void setAutoRepairHistoryClearDeleteHostsBufferInSecV2(int seconds)
    {
        config.setAutoRepairHistoryClearDeleteHostsBufferInSec(seconds);
    }

    @Override
    public void setAutoRepairMaxRetriesCount(int retries)
    {
        config.setRepairMaxRetries(retries);
    }

    @Override
    public void setAutoRepairRetryBackoffInSec(long seconds)
    {
        config.setRepairRetryBackoffInSec(seconds);
    }

    @Override
    public void setRepairSSTableCountHigherThreshold(RepairType repairType, int sstableHigherThreshold)
    {
        config.setRepairSSTableCountHigherThreshold(repairType, sstableHigherThreshold);
    }

    @Override
    public void setRepairIgnoreKeyspaces(RepairType repairType, String ignoreKeyspaceRegex)
    {
        config.setRepairIgnoreKeyspaces(repairType, ignoreKeyspaceRegex);
    }

    @Override
    public void setRepairOnlyKeyspaces(RepairType repairType, String repairOnlyKeyspacesRegex)
    {
        config.setRepairOnlyKeyspaces(repairType, repairOnlyKeyspacesRegex);
    }

    @Override
    public void setAutoRepairTableMaxRepairTimeInSec(RepairType repairType, long autoRepairTableMaxRepairTimeInSec)
    {
        config.setAutoRepairTableMaxRepairTimeInSec(repairType, autoRepairTableMaxRepairTimeInSec);
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
        config.setParallelRepairPercentageInGroup(repairType, percentageInGroup);
    }

    @Override
    public void setParallelRepairCountInGroup(RepairType repairType, int countInGroup)
    {
        config.setParallelRepairCountInGroup(repairType, countInGroup);
    }

    public void setMVRepairEnabled(RepairType repairType, boolean enabled)
    {
        config.setMVRepairEnabled(repairType, enabled);
    }
}
