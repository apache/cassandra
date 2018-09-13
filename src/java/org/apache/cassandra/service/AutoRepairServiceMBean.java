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

import java.net.InetAddress;
import java.util.Set;

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
     * Check if AutoRepair is started for this node
     */
    public boolean isAutoRepairStarted();

    /**
     * Stop ongoing AutoRepair
     */
    public void stopAutoRepair();

    /**
     * Set repair threads
     */
    public void setRepairThreads(int repairThreads);

    /**
     * Get repair threads
     */
    public int getRepairThreads();

    /**
     * Set repair priority for hosts
     */
    public void setRepairPriorityForHosts(Set<InetAddress> host);

    /**
     * Get repair priority
     */
    public Set<InetAddress> getRepairHostPriority();

    /**
     * Get repair subrange numbers
     */
    public int getRepairSubRangeNum();

    /**
     * Set repair subrange numbers
     */
    public void setRepairSubRangeNum(int repairSubRangeNum);

    /**
     * Get repair subranges
     */
    public int getRepairMinFrequencyInHours();

    /**
     * Set repair subranges
     */
    public void setRepairMinFrequencyInHours(int repairMinFrequencyInHours);

    /**
     * Get repair sstable count higher threshold
     */
    public int getRepairSSTableCountHigherThreshold();

    /**
     * Set repair sstable count higher threshold
     */
    public void setRepairSSTableCountHigherThreshold(int ssTableHigherThreshold);

    /**
     * Get repair ignore keyspaces list
     */
    public Set<String> getRepairIgnoreKeyspaces();

    /**
     * Set repair ignore keyspaces list
     */
    public void setRepairIgnoreKeyspaces(Set<String> ignoreKeyspace);

    /**
     * Get repair only keyspaces list to repair only specified keyspace
     */
    public Set<String> getRepairOnlyKeyspaces();

    /**
     * Set repair only keyspaces list
     */
    public void setRepairOnlyKeyspaces(Set<String> repairOnlyKeyspaces);
}
