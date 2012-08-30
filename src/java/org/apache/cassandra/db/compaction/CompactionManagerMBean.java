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

public interface CompactionManagerMBean
{
    /** List of running compaction objects. */
    public List<Map<String, String>> getCompactions();

    /** List of running compaction summary strings. */
    public List<String> getCompactionSummary();

    /**
     * @see org.apache.cassandra.metrics.CompactionMetrics#pendingTasks
     * @return estimated number of compactions remaining to perform
     */
    @Deprecated
    public int getPendingTasks();

    /**
     * @see org.apache.cassandra.metrics.CompactionMetrics#completedTasks
     * @return number of completed compactions since server [re]start
     */
    @Deprecated
    public long getCompletedTasks();

    /**
     * @see org.apache.cassandra.metrics.CompactionMetrics#totalBytesCompacted
     * @return total number of bytes compacted since server [re]start
     */
    @Deprecated
    public long getTotalBytesCompacted();

    /**
     * @see org.apache.cassandra.metrics.CompactionMetrics#totalCompactionsCompleted
     * @return total number of compactions since server [re]start
     */
    @Deprecated
    public long getTotalCompactionsCompleted();

    /**
     * Triggers the compaction of user specified sstables.
     *
     * @param ksname the keyspace for the sstables to compact
     * @param dataFiles a comma separated list of sstable filename to compact
     */
    public void forceUserDefinedCompaction(String ksname, String dataFiles);

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
}
