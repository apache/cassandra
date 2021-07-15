/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.compaction.unified;

import java.util.List;
import java.util.Set;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * The sole purpose of this class is to currently create a {@link ShardedCompactionWriter}.
 */
public class UnifiedCompactionTask extends CompactionTask
{
    private final long minSstableSizeInBytes;
    private final List<PartitionPosition> boundaries;

    public UnifiedCompactionTask(ColumnFamilyStore cfs,
                                 UnifiedCompactionStrategy strategy,
                                 LifecycleTransaction txn,
                                 int gcBefore,
                                 long minSstableSizeInBytes,
                                 List<PartitionPosition> boundaries)
    {
        super(cfs, txn, gcBefore, strategy.getController().getIgnoreOverlapsInExpirationCheck(), strategy);
        this.minSstableSizeInBytes = minSstableSizeInBytes;
        this.boundaries = boundaries;
    }

    @Override
    public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                          Directories directories,
                                                          LifecycleTransaction txn,
                                                          Set<SSTableReader> nonExpiredSSTables)
    {
        return new ShardedCompactionWriter(cfs, directories, txn, nonExpiredSSTables, keepOriginals, minSstableSizeInBytes, boundaries);
    }
}