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

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.AutoRepairConfig;
import org.apache.cassandra.repair.AutoRepairConfig.RepairType;

public interface AutoRepairServiceMBean
{
    /**
     * run Auto repair once on this node
     */
    public void runAutoRepairOnce(RepairType type, long millisToWait);

    /**
     * Enable or disable auto-repair for a given repair type
     */
    public void setAutoRepairEnabled(RepairType repairType, boolean enabled);

    /**
     * Set repair threads
     */
    public void setRepairThreads(RepairType repairType, int repairThreads);

    /**
     * Get current ongoing repair host ids by group hash
     */
    public Set<String> getOnGoingRepairHostIdsByGroupHash(RepairType type, int groupHash);

    /**
     * Get current force repair host ids by group hash
     */
    public Set<String> getOnGoingForceRepairHostIdsByGroupHash(RepairType type, int groupHash);

    /**
     * Set repair priority for hosts
     */
    public void setRepairPriorityForHosts(RepairType repairType, Set<InetAddressAndPort> host);

    /**
     * Set force repair for hosts
     */
    public void setForceRepairForHosts(RepairType repairType, Set<InetAddressAndPort> host);

    /**
     * Get repair priority
     */
    public Set<InetAddressAndPort> getRepairHostPriority(RepairType repairType);

    /**
     * Set repair subrange numbers
     */
    public void setRepairSubRangeNum(RepairType repairType, int repairSubRangeNum);

    /**
     * Set repair interval in hours
     */
    public void setRepairMinIntervalInHours(RepairType repairType, int repairMinIntervalInHours);

    /**
     * Set auto repair history clear
     */
    public void setAutoRepairHistoryClearDeleteHostsBufferInSecV2(int seconds);

    /**
     * Set auto repair max retries count
     */
    public void setAutoRepairMaxRetriesCount(int retries);

    /**
     * Set auto repair retry backoff in seconds
     */
    public void setAutoRepairRetryBackoffInSec(long seconds);

    /**
     * Set minimum duration for a single repair job
     */
    public void setAutoRepairMinRepairTaskDurationInSec(long duration);

    /**
     * Set repair sstable count higher threshold
     */
    public void setRepairSSTableCountHigherThreshold(RepairType repairType, int ssTableHigherThreshold);

    /**
     * Set repair ignore keyspaces regex
     */
    public void setRepairIgnoreKeyspaces(RepairType repairType, String ignoreKeyspaceRegex);

    /**
     * Set repair only keyspaces regex
     */
    public void setRepairOnlyKeyspaces(RepairType repairType, String repairOnlyKeyspacesRegex);

    /**
     * Set table max repair time in sec
     */
    public void setAutoRepairTableMaxRepairTimeInSec(RepairType repairType, long autoRepairTableMaxRepairTimeInSec);

    /**
     * Set ignore dcs list
     */
    public void setIgnoreDCs(RepairType repairType, Set<String> ignorDCs);

    /**
     * Set this 'true' if AutoRepair should repair only the primary ranges owned by this node; else, 'false'
     */
    public void setPrimaryTokenRangeOnly(RepairType repairType, boolean primaryTokenRangeOnly);

    /**
     * Set percentage of the nodes in one group should run repair parallelly
     */
    public void setParallelRepairPercentageInGroup(RepairType repairType, int percentageInGroup);

    /**
     * Return number of the nodes in one group should run repair parallelly
     */
    public void setParallelRepairCountInGroup(RepairType repairType, int countInGroup);

    /**
     * Set if MVs should be included in the AutoRepair or not
     */
    public void setMVRepairEnabled(RepairType repairType, boolean enabled);

    public AutoRepairConfig getAutoRepairConfig();

    /**
     * Returns hosts that are in the same group as this node
     */
    public Set<InetAddressAndPort> filterHostsInLocalGroup(RepairType repairType, Set<InetAddressAndPort> hostsToFilter);
}
