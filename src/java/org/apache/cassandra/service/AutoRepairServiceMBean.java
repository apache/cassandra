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

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;

import java.util.Set;

public interface AutoRepairServiceMBean
{
    /**
     * Enable or disable auto-repair for a given repair type
     */
    public void setAutoRepairEnabled(RepairType repairType, boolean enabled);

    public void setRepairThreads(RepairType repairType, int repairThreads);

    public void setRepairPriorityForHosts(RepairType repairType, Set<InetAddressAndPort> host);

    public void setForceRepairForHosts(RepairType repairType, Set<InetAddressAndPort> host);

    public Set<InetAddressAndPort> getRepairHostPriority(RepairType repairType);

    public void setRepairSubRangeNum(RepairType repairType, int repairSubRangeNum);

    public void setRepairMinInterval(RepairType repairType, String minRepairInterval);

    public void setAutoRepairHistoryClearDeleteHostsBufferDuration(String duration);

    public void setAutoRepairMaxRetriesCount(int retries);
    public void setAutoRepairRetryBackoff(String interval);
    public void setRepairSSTableCountHigherThreshold(RepairType repairType, int ssTableHigherThreshold);

    public void setAutoRepairTableMaxRepairTime(RepairType repairType, String autoRepairTableMaxRepairTime);
    public void setIgnoreDCs(RepairType repairType, Set<String> ignorDCs);

    public void setPrimaryTokenRangeOnly(RepairType repairType, boolean primaryTokenRangeOnly);

    public void setParallelRepairPercentageInGroup(RepairType repairType, int percentageInGroup);
    public void setParallelRepairCountInGroup(RepairType repairType, int countInGroup);

    public void setMVRepairEnabled(RepairType repairType, boolean enabled);

    public AutoRepairConfig getAutoRepairConfig();
}
