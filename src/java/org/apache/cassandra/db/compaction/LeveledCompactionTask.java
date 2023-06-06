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
import java.util.stream.Collectors;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.MajorLeveledCompactionWriter;
import org.apache.cassandra.db.compaction.writers.MaxSSTableSizeWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;

public class LeveledCompactionTask extends CompactionTask
{
    private final int level;
    private final long maxSSTableBytes;
    private final boolean majorCompaction;

    public LeveledCompactionTask(ColumnFamilyStore cfs, LifecycleTransaction txn, int level, long gcBefore, long maxSSTableBytes, boolean majorCompaction)
    {
        super(cfs, txn, gcBefore);
        this.level = level;
        this.maxSSTableBytes = maxSSTableBytes;
        this.majorCompaction = majorCompaction;
    }

    @Override
    public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                          Directories directories,
                                                          LifecycleTransaction txn,
                                                          Set<SSTableReader> nonExpiredSSTables)
    {
        if (majorCompaction)
            return new MajorLeveledCompactionWriter(cfs, directories, txn, nonExpiredSSTables, maxSSTableBytes, false);
        return new MaxSSTableSizeWriter(cfs, directories, txn, nonExpiredSSTables, maxSSTableBytes, getLevel(), false);
    }

    @Override
    protected boolean partialCompactionsAcceptable()
    {
        throw new UnsupportedOperationException("This is now handled in reduceScopeForLimitedSpace");
    }

    protected int getLevel()
    {
        return level;
    }

    @Override
    public boolean reduceScopeForLimitedSpace(Set<SSTableReader> nonExpiredSSTables, long expectedSize)
    {
        if (transaction.originals().size() > 1 && level <= 1)
        {
            // Try again w/o the largest one.
            logger.warn("insufficient space to do L0 -> L{} compaction. {}MiB required, {} for compaction {}",
                        level,
                        (float) expectedSize / 1024 / 1024,
                        transaction.originals()
                                   .stream()
                                   .map(sstable -> String.format("%s (level=%s, size=%s)", sstable, sstable.getSSTableLevel(), sstable.onDiskLength()))
                                   .collect(Collectors.joining(",")),
                        transaction.opId());
            // Note that we have removed files that are still marked as compacting.
            // This suboptimal but ok since the caller will unmark all the sstables at the end.
            int l0SSTableCount = 0;
            SSTableReader largestL0SSTable = null;
            for (SSTableReader sstable : nonExpiredSSTables)
            {
                if (sstable.getSSTableLevel() == 0)
                {
                    l0SSTableCount++;
                    if (largestL0SSTable == null || sstable.onDiskLength() > largestL0SSTable.onDiskLength())
                        largestL0SSTable = sstable;
                }
            }
            // no point doing a L0 -> L{0,1} compaction if we have cancelled all L0 sstables
            if (largestL0SSTable != null && l0SSTableCount > 1)
            {
                logger.info("Removing {} (level={}, size={}) from compaction {}",
                            largestL0SSTable,
                            largestL0SSTable.getSSTableLevel(),
                            largestL0SSTable.onDiskLength(),
                            transaction.opId());
                transaction.cancel(largestL0SSTable);
                return true;
            }
        }
        return false;
    }
}
