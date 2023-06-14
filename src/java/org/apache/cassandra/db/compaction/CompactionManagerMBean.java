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
package org.apache.cassandra.db.compaction;

import java.util.List;
import java.util.Map;
import javax.management.openmbean.TabularData;

public interface CompactionManagerMBean
{
    /** List of running compaction objects. */
    public List<Map<String, String>> getCompactions();

    /** List of running compaction summary strings. */
    public List<String> getCompactionSummary();

    /** compaction history **/
    public TabularData getCompactionHistory();

    /**
     * Triggers the compaction of user specified sstables.
     * You can specify files from various keyspaces and columnfamilies.
     * If you do so, user defined compaction is performed several times to the groups of files
     * in the same keyspace/columnfamily.
     *
     * @param dataFiles a comma separated list of sstable file to compact.
     *                  must contain keyspace and columnfamily name in path(for 2.1+) or file name itself.
     */
    public void forceUserDefinedCompaction(String dataFiles);

    /**
     * Triggers the cleanup of user specified sstables.
     * You can specify files from various keyspaces and columnfamilies.
     * If you do so, cleanup is performed each file individually
     *
     * @param dataFiles a comma separated list of sstable file to cleanup.
     *                  must contain keyspace and columnfamily name in path(for 2.1+) or file name itself.
     */
    public void forceUserDefinedCleanup(String dataFiles);


    /**
     * Stop all running compaction-like tasks having the provided {@code type}.
     * @param type the type of compaction to stop. Can be one of:
     *   - COMPACTION
     *   - VALIDATION
     *   - CLEANUP
     *   - SCRUB
     *   - INDEX_BUILD
     */
    public void stopCompaction(String type);

    /**
     * Stop an individual running compaction using the compactionId.
     * @param compactionId Compaction ID of compaction to stop. Such IDs can be found in
     *                     the transaction log files whose name starts with compaction_,
     *                     located in the table transactions folder.
     */
    public void stopCompactionById(String compactionId);

    /**
     * Returns core size of compaction thread pool
     */
    public int getCoreCompactorThreads();

    /**
     * Allows user to resize maximum size of the compaction thread pool.
     * @param number New maximum of compaction threads
     */
    public void setCoreCompactorThreads(int number);

    /**
     * Returns maximum size of compaction thread pool
     */
    public int getMaximumCompactorThreads();

    /**
     * Allows user to resize maximum size of the compaction thread pool.
     * @param number New maximum of compaction threads
     */
    public void setMaximumCompactorThreads(int number);

    /**
     * Returns core size of validation thread pool
     */
    public int getCoreValidationThreads();

    /**
     * Allows user to resize maximum size of the compaction thread pool.
     * @param number New maximum of compaction threads
     */
    public void setCoreValidationThreads(int number);

    /**
     * Returns size of validator thread pool
     */
    public int getMaximumValidatorThreads();

    /**
     * Allows user to resize maximum size of the validator thread pool.
     * @param number New maximum of validator threads
     */
    public void setMaximumValidatorThreads(int number);

    /**
     * Returns core size of view build thread pool
     */
    public int getCoreViewBuildThreads();

    /**
     * Enable / disable STCS in L0
     */
    public boolean getDisableSTCSInL0();
    public void setDisableSTCSInL0(boolean disabled);

    /**
     * Allows user to resize maximum size of the view build thread pool.
     * @param number New maximum of view build threads
     */
    public void setCoreViewBuildThreads(int number);

    /**
     * Returns size of view build thread pool
     */
    public int getMaximumViewBuildThreads();

    /**
     * Allows user to resize maximum size of the view build thread pool.
     * @param number New maximum of view build threads
     */
    public void setMaximumViewBuildThreads(int number);

    /**
     * Get automatic sstable upgrade enabled
     */
    public boolean getAutomaticSSTableUpgradeEnabled();
    /**
     * Set if automatic sstable upgrade should be enabled
     */
    public void setAutomaticSSTableUpgradeEnabled(boolean enabled);

    /**
     * Get the number of concurrent sstable upgrade tasks we should run
     * when automatic sstable upgrades are enabled
     */
    public int getMaxConcurrentAutoUpgradeTasks();

    /**
     * Set the number of concurrent sstable upgrade tasks we should run
     * when automatic sstable upgrades are enabled
     */
    public void setMaxConcurrentAutoUpgradeTasks(int value);
}
