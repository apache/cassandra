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

import org.apache.cassandra.repair.AutoRepairUtils;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Set;
import org.apache.cassandra.utils.MBeanWrapper;

public class AutoRepairService implements AutoRepairServiceMBean
{
    public static final String MBEAN_NAME = "org.apache.cassandra.db:type=AutoRepairService";

    private boolean autoRepairEnabled;
    private boolean autoRepairStarted;
    private int repairThreads;
    private int repairSubRangeNum;
    private int repairMinFrequencyInHours;
    private int sstableCountHigherThreshold;
    private Set<String> ignoreKeyspaces = Collections.emptySet();
    private Set<String> repairOnlyKeyspaces = Collections.emptySet();

    public static final AutoRepairService instance = new AutoRepairService();

    private AutoRepairService()
    {
    }

    static {
        MBeanWrapper.instance.registerMBean(instance, MBEAN_NAME);
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

    @Override
    public void setRepairThreads(int repairThreads)
    {
        this.repairThreads = repairThreads;
    }

    @Override
    public int getRepairThreads()
    {
        return repairThreads;
    }

    @Override
    public void setRepairPriorityForHosts(Set<InetAddress> host)
    {
        AutoRepairUtils.addPriorityHost(host);
    }

    @Override
    public Set<InetAddress> getRepairHostPriority()
    {
        return AutoRepairUtils.getPriorityHosts();
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
    public Set<String> getRepairIgnoreKeyspaces()
    {
        return ignoreKeyspaces;
    }

    @Override
    public void setRepairIgnoreKeyspaces(Set<String> keyspaces)
    {
        ignoreKeyspaces = keyspaces;
    }

    @Override
    public Set<String> getRepairOnlyKeyspaces()
    {
        return repairOnlyKeyspaces;
    }

    @Override
    public void setRepairOnlyKeyspaces(Set<String> repairOnlyKeyspaces)
    {
        this.repairOnlyKeyspaces = repairOnlyKeyspaces;
    }
}
