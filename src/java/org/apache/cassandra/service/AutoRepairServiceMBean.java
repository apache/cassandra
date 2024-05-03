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

import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Pattern;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.AutoRepairConfig;
import org.apache.cassandra.repair.AutoRepairConfig.RepairType;

public interface AutoRepairServiceMBean
{
    /**
     * Check if AutoRepair is enabled for this node
     */
    public boolean isAutoRepairEnabled();

    /**
     * Start AutoRepair status
     */
    public void setAutoRepairStatus(boolean autoRepairStatus);

    /**
     * Start AutoRepair
     */
    public void startAutoRepair();

    /**
     * run Auto repair once on this node
     */
    public void runAutoRepairOnce(long millisToWait);


    /**
     * Check if AutoRepair is started for this node
     */
    public boolean isAutoRepairStarted();

    /**
     * Stop ongoing AutoRepair
     */
    public void stopAutoRepair();

    /**
     * Enable or disable auto-repair for a given repair type
     */
    public void setAutoRepairEnabled(RepairType repairType, boolean enabled);

    /**
     * Set repair threads
     */
    public void setRepairThreads(int repairThreads);
    public void setRepairThreads(RepairType repairType, int repairThreads);

    /**
     * Get repair threads
     */
    public int getRepairThreads();

    /**
     * Get current ongoing repair host ids by group hash
     */
    public Set<String> getOnGoingRepairHostIdsByGroupHash(int groupHash);

    /**
     * Get current force repair host ids by group hash
     */
    public Set<String> getOnGoingForceRepairHostIdsByGroupHash(int groupHash);

    /**
     * Set repair priority for hosts
     */
    public void setRepairPriorityForHosts(Set<InetAddressAndPort> host);
    public void setRepairPriorityForHosts(RepairType repairType, Set<InetAddressAndPort> host);

    /**
     * Set force repair for hosts
     */
    public void setForceRepairForHosts(Set<InetAddressAndPort> host);
    public void setForceRepairForHosts(RepairType repairType, Set<InetAddressAndPort> host);

    /**
     * Get repair priority
     */
    public Set<InetAddressAndPort> getRepairHostPriority();
    public Set<InetAddressAndPort> getRepairHostPriority(RepairType repairType);

    /**
     * Get repair subrange numbers
     */
    public int getRepairSubRangeNum();

    /**
     * Set repair subrange numbers
     */
    public void setRepairSubRangeNum(int repairSubRangeNum);
    public void setRepairSubRangeNum(RepairType repairType, int repairSubRangeNum);

    /**
     * Get repair subranges
     */
    public int getRepairMinFrequencyInHours();

    /**
     * Set repair interval in hours
     */
    public void setRepairMinFrequencyInHours(int repairMinFrequencyInHours);
    public void setRepairMinIntervalInHours(RepairType repairType, int repairMinIntervalInHours);

    /**
     * Get auto repair history clear
     */
    public int getAutoRepairHistoryClearDeleteHostsBufferInSec();


    /**
     * Set repair subranges
     */
    public void setAutoRepairHistoryClearDeleteHostsBufferInSec(int seconds);
    public void setAutoRepairHistoryClearDeleteHostsBufferInSecV2(int seconds);

    /**
     * Get repair sstable count higher threshold
     */
    public int getRepairSSTableCountHigherThreshold();

    /**
     * Set repair sstable count higher threshold
     */
    public void setRepairSSTableCountHigherThreshold(int ssTableHigherThreshold);
    public void setRepairSSTableCountHigherThreshold(RepairType repairType, int ssTableHigherThreshold);

    /**
     * Get repair ignore keyspaces regex
     */
    public Pattern getRepairIgnoreKeyspaces();

    /**
     * Set repair ignore keyspaces regex
     */
    public void setRepairIgnoreKeyspaces(Pattern ignoreKeyspaceRegex);
    public void setRepairIgnoreKeyspaces(RepairType repairType, String ignoreKeyspaceRegex);

    /**
     * Get repair only keyspaces regex to repair only specified keyspace
     */
    public Pattern getRepairOnlyKeyspaces();

    /**
     * Set repair only keyspaces regex
     */
    public void setRepairOnlyKeyspaces(Pattern repairOnlyKeyspacesRegex);
    public void setRepairOnlyKeyspaces(RepairType repairType, String repairOnlyKeyspacesRegex);

    /**
     * Get table max repair time in sec
     */
    public long getAutoRepairTableMaxRepairTimeInSec();

    /**
     * Set table max repair time in sec
     */
    public void setAutoRepairTableMaxRepairTimeInSec(long autoRepairTableMaxRepairTimeInSec);
    public void setAutoRepairTableMaxRepairTimeInSec(RepairType repairType, long autoRepairTableMaxRepairTimeInSec);

    /**
     * Get ignore dcs list
     */
    public Set<String> getIgnoreDCs();

    /**
     * Set ignore dcs list
     */
    public void setIgnoreDCs(Set<String> ignorDCs);
    public void setIgnoreDCs(RepairType repairType, Set<String> ignorDCs);

    /**
     * Get data center groups
     */
    public Set<Set<String>> getDCGroups();

    /**
     * Set data center groups
     */
    public void setDCGourps(Set<Set<String>> dcGourps);

    /**
     * Get the current ring this node within
     */
    public TreeSet<UUID> getCurrentRingHostIds();

    /**
     * Return 'true' if AutoRepair should repair the primary ranges only; else, 'false'
     */
    public boolean getRepairPrimaryTokenRangeOnly();

    /**
     * Set this 'true' if AutoRepair should repair only the primary ranges owned by this node; else, 'false'
     */
    public void setPrimaryTokenRangeOnly(boolean primaryTokenRangeOnly);
    public void setPrimaryTokenRangeOnly(RepairType repairType, boolean primaryTokenRangeOnly);

    /**
     * Return percentage of the nodes in one group should run repair parallelly
     */
    public int getParallelRepairPercentageInGroup();

    /**
     * Set percentage of the nodes in one group should run repair parallelly
     */
    public void setParallelRepairPercentageInGroup(int percentageInGroup);
    public void setParallelRepairPercentageInGroup(RepairType repairType, int percentageInGroup);

    /**
     * Return number of the nodes in one group should run repair parallelly
     */
    public int getParallelRepairCountInGroup();

    /**
     * Return number of the nodes in one group should run repair parallelly
     */
    public void setParallelRepairCountInGroup(int countInGroup);
    public void setParallelRepairCountInGroup(RepairType repairType, int countInGroup);

    /**
     * Return if MVs should be included in the AutoRepair
     */
    public boolean getMVRepairEnabled();

    /**
     * Set if MVs should be included in the AutoRepair or not
     */
    public void setMVRepairEnabled(boolean enabled);
    public void setMVRepairEnabled(RepairType repairType, boolean enabled);

    public AutoRepairConfig getAutoRepairConfig();

    /**
     * Returns hosts that are in the same group as this node
     */
    public Set<InetAddressAndPort> filterHostsInLocalGroup(RepairType repairType, Set<InetAddressAndPort> hostsToFilter);
}
