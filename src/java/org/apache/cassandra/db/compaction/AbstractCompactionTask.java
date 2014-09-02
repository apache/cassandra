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

import java.util.Set;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.CompactionManager.CompactionExecutorStatsCollector;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.DiskAwareRunnable;

public abstract class AbstractCompactionTask extends DiskAwareRunnable
{
    protected final ColumnFamilyStore cfs;
    protected Set<SSTableReader> sstables;
    protected boolean isUserDefined;
    protected OperationType compactionType;

    /**
     * @param cfs
     * @param sstables must be marked compacting
     */
    public AbstractCompactionTask(ColumnFamilyStore cfs, Set<SSTableReader> sstables)
    {
        this.cfs = cfs;
        this.sstables = sstables;
        this.isUserDefined = false;
        this.compactionType = OperationType.COMPACTION;

        // enforce contract that caller should mark sstables compacting
        Set<SSTableReader> compacting = cfs.getDataTracker().getCompacting();
        for (SSTableReader sstable : sstables)
            assert compacting.contains(sstable) : sstable.getFilename() + " is not correctly marked compacting";
    }

    /**
     * executes the task and unmarks sstables compacting
     */
    public int execute(CompactionExecutorStatsCollector collector)
    {
        try
        {
            return executeInternal(collector);
        }
        finally
        {
            cfs.getDataTracker().unmarkCompacting(sstables);
        }
    }

    protected abstract int executeInternal(CompactionExecutorStatsCollector collector);

    protected Directories getDirectories()
    {
        return cfs.directories;
    }

    public AbstractCompactionTask setUserDefined(boolean isUserDefined)
    {
        this.isUserDefined = isUserDefined;
        return this;
    }

    public AbstractCompactionTask setCompactionType(OperationType compactionType)
    {
        this.compactionType = compactionType;
        return this;
    }

    public String toString()
    {
        return "CompactionTask(" + sstables + ")";
    }
}
