/**
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

public interface CompactionManagerMBean
{
    /** List of running compaction objects. */
    public List<CompactionInfo> getCompactions();

    /** List of running compaction summary strings. */
    public List<String> getCompactionSummary();

    /**
     * @return estimated number of compactions remaining to perform
     */
    public int getPendingTasks();

    /**
     * @return number of completed compactions since server [re]start
     */
    public long getCompletedTasks();

    /**
     * Triggers the compaction of user specified sstables.
     *
     * @param ksname the keyspace for the sstables to compact
     * @param dataFiles a comma separated list of sstable filename to compact
     */
    public void forceUserDefinedCompaction(String ksname, String dataFiles);
}
