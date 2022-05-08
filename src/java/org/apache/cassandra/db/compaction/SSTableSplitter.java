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
import java.util.function.LongPredicate;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.compaction.writers.MaxSSTableSizeWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;

public class SSTableSplitter 
{
    private final SplittingCompactionTask task;

    public SSTableSplitter(ColumnFamilyStore cfs, LifecycleTransaction transaction, int sstableSizeInMB)
    {
        this.task = new SplittingCompactionTask(cfs, transaction, sstableSizeInMB);
    }

    public void split()
    {
        task.execute(ActiveCompactionsTracker.NOOP);
    }

    public static class SplittingCompactionTask extends CompactionTask
    {
        private final int sstableSizeInMiB;

        public SplittingCompactionTask(ColumnFamilyStore cfs, LifecycleTransaction transaction, int sstableSizeInMB)
        {
            super(cfs, transaction, CompactionManager.NO_GC, false);
            this.sstableSizeInMiB = sstableSizeInMB;

            if (sstableSizeInMB <= 0)
                throw new IllegalArgumentException("Invalid target size for SSTables, must be > 0 (got: " + sstableSizeInMB + ")");
        }

        @Override
        protected CompactionController getCompactionController(Set<SSTableReader> toCompact)
        {
            return new SplitController(cfs);
        }

        @Override
        public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                              Directories directories,
                                                              LifecycleTransaction txn,
                                                              Set<SSTableReader> nonExpiredSSTables)
        {
            return new MaxSSTableSizeWriter(cfs, directories, txn, nonExpiredSSTables, sstableSizeInMiB * 1024L * 1024L, 0, false);
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
        public LongPredicate getPurgeEvaluator(DecoratedKey key)
        {
            return time -> false;
        }
    }
}
