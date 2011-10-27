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

import java.util.Collection;
import java.util.Set;
import java.io.IOException;

import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager.CompactionExecutorStatsCollector;

public abstract class AbstractCompactionTask
{
    protected ColumnFamilyStore cfs;
    protected Collection<SSTableReader> sstables;

    public AbstractCompactionTask(ColumnFamilyStore cfs, Collection<SSTableReader> sstables)
    {
        this.cfs = cfs;
        this.sstables = sstables;
    }

    public abstract int execute(CompactionExecutorStatsCollector collector) throws IOException;

    public ColumnFamilyStore getColumnFamilyStore()
    {
        return cfs;
    }

    public Collection<SSTableReader> getSSTables()
    {
        return sstables;
    }

    /**
     * Try to mark the sstable to compact as compacting.
     * It returns true if some sstables have been marked for compaction, false
     * otherwise.
     * This *must* be called before calling execute(). Moreover,
     * unmarkSSTables *must* always be called after execute() if this
     * method returns true.
     */
    public boolean markSSTablesForCompaction()
    {
        return markSSTablesForCompaction(cfs.getMinimumCompactionThreshold(), cfs.getMaximumCompactionThreshold());
    }

    public boolean markSSTablesForCompaction(int min, int max)
    {
        Set<SSTableReader> marked = cfs.getDataTracker().markCompacting(sstables, min, max);

        if (marked == null || marked.isEmpty())
        {
            cancel();
            return false;
        }

        this.sstables = marked;
        return true;
    }

    public void unmarkSSTables()
    {
        cfs.getDataTracker().unmarkCompacting(sstables);
    }

    // Can be overriden for action that need to be performed if the task won't
    // execute (if sstable can't be marked successfully)
    protected void cancel() {}
}
