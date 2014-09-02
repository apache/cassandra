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

import java.util.*;

import org.apache.cassandra.db.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;

public class SSTableSplitter {

    private final SplittingCompactionTask task;

    private CompactionInfo.Holder info;

    public SSTableSplitter(ColumnFamilyStore cfs, SSTableReader sstable, int sstableSizeInMB)
    {
        this.task = new SplittingCompactionTask(cfs, sstable, sstableSizeInMB);
    }

    public void split()
    {
        task.execute(new StatsCollector());
    }

    public class StatsCollector implements CompactionManager.CompactionExecutorStatsCollector
    {
        public void beginCompaction(CompactionInfo.Holder ci)
        {
            SSTableSplitter.this.info = ci;
        }

        public void finishCompaction(CompactionInfo.Holder ci)
        {
            // no-op
        }
    }

    public static class SplittingCompactionTask extends CompactionTask
    {
        private final int sstableSizeInMB;

        public SplittingCompactionTask(ColumnFamilyStore cfs, SSTableReader sstable, int sstableSizeInMB)
        {
            super(cfs, Collections.singletonList(sstable), CompactionManager.NO_GC, true);
            this.sstableSizeInMB = sstableSizeInMB;

            if (sstableSizeInMB <= 0)
                throw new IllegalArgumentException("Invalid target size for SSTables, must be > 0 (got: " + sstableSizeInMB + ")");
        }

        @Override
        protected CompactionController getCompactionController(Set<SSTableReader> toCompact)
        {
            return new SplitController(cfs);
        }

        @Override
        protected boolean newSSTableSegmentThresholdReached(SSTableWriter writer)
        {
            return writer.getOnDiskFilePointer() > sstableSizeInMB * 1024L * 1024L;
        }

        @Override
        protected boolean partialCompactionsAcceptable()
        {
            return true;
        }
    }

    public static class SplitController extends CompactionController
    {
        public SplitController(ColumnFamilyStore cfs)
        {
            super(cfs, CompactionManager.NO_GC);
        }

        @Override
        public long maxPurgeableTimestamp(DecoratedKey key)
        {
            return Long.MIN_VALUE;
        }
    }
}
