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
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;
import org.apache.cassandra.repair.autorepair.AutoRepairUtils;
import org.apache.cassandra.utils.MBeanWrapper;

import java.util.Set;

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
    public void setAutoRepairEnabled(RepairType repairType, boolean enabled)
    {
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
