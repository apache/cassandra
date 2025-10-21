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

package org.apache.cassandra.db.compaction.unified;

import java.util.Collection;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.CompactionTask;
import org.apache.cassandra.db.compaction.ShardManager;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * The sole purpose of this class is to currently create a {@link ShardedCompactionWriter}.
 */
public class UnifiedCompactionTask extends CompactionTask
{
    private final ShardManager shardManager;
    private final Controller controller;
    private final Range<Token> operationRange;
    private final Set<SSTableReader> actuallyCompact;

    public UnifiedCompactionTask(ColumnFamilyStore cfs,
                                 UnifiedCompactionStrategy strategy,
                                 ILifecycleTransaction txn,
                                 long gcBefore,
                                 ShardManager shardManager)
    {
        this(cfs, strategy, txn, gcBefore, shardManager, null, null);
    }

    public UnifiedCompactionTask(ColumnFamilyStore cfs,
                                 UnifiedCompactionStrategy strategy,
                                 ILifecycleTransaction txn,
                                 long gcBefore,
                                 ShardManager shardManager,
                                 Range<Token> operationRange,
                                 Collection<SSTableReader> actuallyCompact)
    {
        super(cfs, txn, gcBefore);
        this.controller = strategy.getController();
        this.shardManager = shardManager;

        if (operationRange != null)
        {
            assert actuallyCompact != null : "Ranged tasks should use a set of sstables to compact";
        }
        this.operationRange = operationRange;
        // To make sure actuallyCompact tracks any removals from txn.originals(), we intersect the given set with it.
        // This should not be entirely necessary (as shouldReduceScopeForSpace() is false for ranged tasks), but it
        // is cleaner to enforce inputSSTables()'s requirements.
        this.actuallyCompact = actuallyCompact != null ? Sets.intersection(ImmutableSet.copyOf(actuallyCompact),
                                                                           txn.originals())
                                                       : txn.originals();
    }

    @Override
    public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                          Directories directories,
                                                          ILifecycleTransaction txn,
                                                          Set<SSTableReader> nonExpiredSSTables)
    {
        double density = shardManager.calculateCombinedDensity(nonExpiredSSTables);
        int numShards = controller.getNumShards(density * shardManager.shardSetCoverage());
        // In multi-task operations we need to expire many ranges in a source sstable for early open. Not doable yet.
        boolean earlyOpenAllowed = tokenRange() == null;
        return new ShardedCompactionWriter(cfs,
                                           directories,
                                           txn,
                                           nonExpiredSSTables,
                                           keepOriginals,
                                           earlyOpenAllowed,
                                           shardManager.boundaries(numShards));
    }

    @Override
    protected Range<Token> tokenRange()
    {
        return operationRange;
    }

    @Override
    protected boolean shouldReduceScopeForSpace()
    {
        // Because parallelized tasks share input sstables, we can't reduce the scope of individual tasks
        // (as doing that will leave some part of an sstable out of the compaction but still drop the whole sstable
        // when the task set completes).
        return tokenRange() == null;
    }

    @Override
    protected Set<SSTableReader> inputSSTables()
    {
        return actuallyCompact;
    }
}