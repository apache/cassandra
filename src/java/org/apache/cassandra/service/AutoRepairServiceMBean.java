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

/**
 * Defines all the MBeans exposed for AutoRepair.
 */
public interface AutoRepairServiceMBean
{
    public void setAutoRepairEnabled(String repairType, boolean enabled);

    public void setRepairThreads(String repairType, int repairThreads);

    public void setRepairPriorityForHosts(String repairType, String commaSeparatedHostSet);

    public void setForceRepairForHosts(String repairType, String commaSeparatedHostSet);

    public void setRepairMinInterval(String repairType, String minRepairInterval);

    void startScheduler();

    public void setAutoRepairHistoryClearDeleteHostsBufferDuration(String duration);

    public void setAutoRepairMinRepairTaskDuration(String duration);

    public void setRepairSSTableCountHigherThreshold(String repairType, int ssTableHigherThreshold);

    public void setAutoRepairTableMaxRepairTime(String repairType, String autoRepairTableMaxRepairTime);

    public void setIgnoreDCs(String repairType, Set<String> ignorDCs);

    public void setPrimaryTokenRangeOnly(String repairType, boolean primaryTokenRangeOnly);

    public void setParallelRepairPercentage(String repairType, int percentage);

    public void setParallelRepairCount(String repairType, int count);

    public void setAllowParallelReplicaRepair(String repairType, boolean enabled);

    public void setAllowParallelReplicaRepairAcrossSchedules(String repairType, boolean enabled);

    public void setMVRepairEnabled(String repairType, boolean enabled);

    public boolean isAutoRepairDisabled();

    public String getAutoRepairConfiguration();

    public void setRepairSessionTimeout(String repairType, String timeout);

    public Set<String> getOnGoingRepairHostIds(String repairType);

    public void setAutoRepairTokenRangeSplitterParameter(String repairType, String key, String value);

    public void setRepairByKeyspace(String repairType, boolean repairByKeyspace);

    public void setAutoRepairMaxRetriesCount(String repairType, int retries);

    public void setAutoRepairRetryBackoff(String repairType, String interval);
}
