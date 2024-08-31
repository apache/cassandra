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
import org.apache.cassandra.repair.AutoRepair;
import org.apache.cassandra.repair.AutoRepairConfig;
import org.apache.cassandra.repair.AutoRepairConfig.RepairType;
import org.apache.cassandra.repair.AutoRepairUtils;
import org.apache.cassandra.repair.AutoRepairUtilsV2;
import org.apache.cassandra.utils.MBeanWrapper;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.cassandra.utils.MBeanWrapper;

import com.google.common.annotations.VisibleForTesting;

// TODO: deprecate methods accessing legacy auto-repair config once migration to new auto-repair framework is complete (SO-28867)
public class AutoRepairService implements AutoRepairServiceMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=AutoRepairService";

    private boolean autoRepairEnabled;
    private boolean autoRepairStarted;
    private int repairThreads;
    private int repairSubRangeNum;
    private int repairMinFrequencyInHours;
    private int sstableCountHigherThreshold;
    private Pattern ignoreKeyspaces;
    private Pattern repairOnlyKeyspaces;
    private long autoRepairTableMaxRepairTimeInSec;
    private Set<String> autoRepairIgnoreDCs;
    private Set<Set<String>> autoRepairDCGroups = new HashSet<>();
    private int autoRepairHistoryClearDeleteHostsBufferInSec;
    private boolean primaryTokenRangeOnly;
    private int parallelRepairPercentageInGroup;
    private int parallelRepairCountInGroup;
    private boolean mvRepairEnabled;
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

    @Override
    public boolean isAutoRepairEnabled()
    {
        return autoRepairEnabled;
    }

    @Override
    public void setAutoRepairStatus(boolean autoRepairStatus)
    {
        autoRepairStarted = autoRepairEnabled = autoRepairStatus;
    }

    @Override
    public void startAutoRepair()
    {
        autoRepairStarted = true;
    }

    public void runAutoRepairOnce(long millisToWait)
    {
        AutoRepair.runRepair(millisToWait);
    }

    @Override
    public boolean isAutoRepairStarted()
    {
        return autoRepairStarted;
    }

    @Override
    public void stopAutoRepair()
    {
        autoRepairStarted = false;
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
    public void setRepairThreads(int repairThreads)
    {
        this.repairThreads = repairThreads;
    }

    @Override
    public void setRepairThreads(RepairType repairType, int repairThreads)
    {
        config.setRepairThreads(repairType, repairThreads);
    }

    @Override
    public int getRepairThreads()
    {
        return repairThreads;
    }

    @Override
    public Set<String> getOnGoingRepairHostIdsByGroupHash(int groupHash)
    {
        Set<String> hostIds = new HashSet<>();
        List<AutoRepairUtils.AutoRepairHistory> histories = AutoRepairUtils.getAutoRepairHistoryByGroupID(groupHash);
        if (histories == null)
        {
            return null;
        }
        AutoRepairUtils.CurrentRepairStatus currentRepairStatus = new AutoRepairUtils.CurrentRepairStatus(histories, AutoRepairUtils.getPriorityHostIds(groupHash));
        for (UUID id : currentRepairStatus.hostIdsWithOnGoingRepair)
        {
            hostIds.add(id.toString());
        }
        return hostIds;
    }

    @Override
    public Set<String> getOnGoingForceRepairHostIdsByGroupHash(int groupHash)
    {
        Set<String> hostIds = new HashSet<>();
        List<AutoRepairUtils.AutoRepairHistory> histories = AutoRepairUtils.getAutoRepairHistoryByGroupID(groupHash);
        if (histories == null)
        {
            return null;
        }
        AutoRepairUtils.CurrentRepairStatus currentRepairStatus = new AutoRepairUtils.CurrentRepairStatus(histories, AutoRepairUtils.getPriorityHostIds(groupHash));
        for (UUID id : currentRepairStatus.hostIdsWithOnGoingForceRepair)
        {
            hostIds.add(id.toString());
        }
        return hostIds;
    }

    @Override
    public void setRepairPriorityForHosts(Set<InetAddressAndPort> host)
    {
        AutoRepairUtils.addPriorityHost(host);
    }

    @Override
    public void setRepairPriorityForHosts(RepairType repairType, Set<InetAddressAndPort> hosts)
    {
        AutoRepairUtilsV2.addPriorityHosts(repairType, hosts);
    }

    @Override
    public Set<InetAddressAndPort> getRepairHostPriority()
    {
        return AutoRepairUtils.getPriorityHosts();
    }

    @Override
    public Set<InetAddressAndPort> getRepairHostPriority(RepairType repairType) {
        return AutoRepairUtilsV2.getPriorityHosts(repairType);
    }

    public void setForceRepairForHosts(Set<InetAddressAndPort> hosts)
    {
        AutoRepairUtils.setForceRepair(hosts);
    }

    @Override
    public void setForceRepairForHosts(RepairType repairType, Set<InetAddressAndPort> hosts)
    {
        AutoRepairUtilsV2.setForceRepair(repairType, hosts);
    }

    @Override
    public int getRepairSubRangeNum()
    {
        return repairSubRangeNum;
    }

    @Override
    public void setRepairSubRangeNum(int repairSubRanges)
    {
        this.repairSubRangeNum = repairSubRanges;
    }

    @Override
    public void setRepairSubRangeNum(RepairType repairType, int repairSubRanges)
    {
        config.setRepairSubRangeNum(repairType, repairSubRanges);
    }

    @Override
    public int getRepairMinFrequencyInHours()
    {
        return repairMinFrequencyInHours;
    }

    @Override
    public void setRepairMinFrequencyInHours(int repairMinFrequencyInHours)
    {
        this.repairMinFrequencyInHours = repairMinFrequencyInHours;
    }

    @Override
    public void setRepairMinIntervalInHours(RepairType repairType, int repairMinFrequencyInHours)
    {
        config.setRepairMinIntervalInHours(repairType, repairMinFrequencyInHours);
    }

    @Override
    public int getAutoRepairHistoryClearDeleteHostsBufferInSec()
    {
        return this.autoRepairHistoryClearDeleteHostsBufferInSec;
    }

    @Override
    public void setAutoRepairHistoryClearDeleteHostsBufferInSec(int seconds)
    {
        this.autoRepairHistoryClearDeleteHostsBufferInSec = seconds;
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
    public int getRepairSSTableCountHigherThreshold()
    {
        return sstableCountHigherThreshold;
    }

    @Override
    public void setRepairSSTableCountHigherThreshold(int sstableHigherThreshold)
    {
        this.sstableCountHigherThreshold = sstableHigherThreshold;
    }

    @Override
    public void setRepairSSTableCountHigherThreshold(RepairType repairType, int sstableHigherThreshold)
    {
        config.setRepairSSTableCountHigherThreshold(repairType, sstableHigherThreshold);
    }

    @Override
    public Pattern getRepairIgnoreKeyspaces()
    {
        return ignoreKeyspaces;
    }

    @Override
    public void setRepairIgnoreKeyspaces(Pattern ignoreKeyspaceRegex)
    {
        ignoreKeyspaces = ignoreKeyspaceRegex;
    }

    @Override
    public void setRepairIgnoreKeyspaces(RepairType repairType, String ignoreKeyspaceRegex)
    {
        config.setRepairIgnoreKeyspaces(repairType, ignoreKeyspaceRegex);
    }

    @Override
    public Pattern getRepairOnlyKeyspaces()
    {
        return repairOnlyKeyspaces;
    }

    @Override
    public void setRepairOnlyKeyspaces(Pattern repairOnlyKeyspacesRegex)
    {
        this.repairOnlyKeyspaces = repairOnlyKeyspacesRegex;
    }

    @Override
    public void setRepairOnlyKeyspaces(RepairType repairType, String repairOnlyKeyspacesRegex)
    {
        config.setRepairOnlyKeyspaces(repairType, repairOnlyKeyspacesRegex);
    }

    @Override
    public long getAutoRepairTableMaxRepairTimeInSec()
    {
        return autoRepairTableMaxRepairTimeInSec;
    }

    @Override
    public void setAutoRepairTableMaxRepairTimeInSec(long autoRepairTableMaxRepairTimeInSec)
    {
        this.autoRepairTableMaxRepairTimeInSec = autoRepairTableMaxRepairTimeInSec;
    }

    @Override
    public void setAutoRepairTableMaxRepairTimeInSec(RepairType repairType, long autoRepairTableMaxRepairTimeInSec)
    {
        config.setAutoRepairTableMaxRepairTimeInSec(repairType, autoRepairTableMaxRepairTimeInSec);
    }

    @Override
    public Set<String> getIgnoreDCs()
    {
        return autoRepairIgnoreDCs;
    }

    @Override
    public void setIgnoreDCs(Set<String> ignorDCs)
    {
        this.autoRepairIgnoreDCs = ignorDCs;
    }

    @Override
    public void setIgnoreDCs(RepairType repairType, Set<String> ignoreDCs)
    {
        config.setIgnoreDCs(repairType, ignoreDCs);
    }

    public void setDCGourps(Set<Set<String>> dcGourps)
    {
        autoRepairDCGroups = dcGourps;
    }

    public Set<Set<String>> getDCGroups()
    {
        return autoRepairDCGroups;
    }

    public TreeSet<UUID> getCurrentRingHostIds()
    {
        return AutoRepairUtils.getHostIdsInCurrentRing();
    }

    public boolean getRepairPrimaryTokenRangeOnly()
    {
        return primaryTokenRangeOnly;
    }

    public void setPrimaryTokenRangeOnly(boolean primaryTokenRangeOnly)
    {
        this.primaryTokenRangeOnly = primaryTokenRangeOnly;
    }

    @Override
    public void setPrimaryTokenRangeOnly(RepairType repairType, boolean primaryTokenRangeOnly)
    {
        config.setRepairPrimaryTokenRangeOnly(repairType, primaryTokenRangeOnly);
    }

    public int getParallelRepairPercentageInGroup()
    {
        return parallelRepairPercentageInGroup;
    }

    public void setParallelRepairPercentageInGroup(int percentageInGroup)
    {
        this.parallelRepairPercentageInGroup = percentageInGroup;
    }

    @Override
    public void setParallelRepairPercentageInGroup(RepairType repairType, int percentageInGroup)
    {
        config.setParallelRepairPercentageInGroup(repairType, percentageInGroup);
    }

    public int getParallelRepairCountInGroup()
    {
        return parallelRepairCountInGroup;
    }

    public void setParallelRepairCountInGroup(int countInGroup)
    {
        this.parallelRepairCountInGroup = countInGroup;
    }

    @Override
    public void setParallelRepairCountInGroup(RepairType repairType, int countInGroup)
    {
        config.setParallelRepairCountInGroup(repairType, countInGroup);
    }

    public boolean getMVRepairEnabled()
    {
        return mvRepairEnabled;
    }

    public void setMVRepairEnabled(boolean enabled)
    {
        this.mvRepairEnabled = enabled;
    }

    public void setMVRepairEnabled(RepairType repairType, boolean enabled)
    {
        config.setMVRepairEnabled(repairType, enabled);
    }
}
