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

package org.apache.cassandra.db.compaction.differential;

import java.util.List;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.AbstractCompactionStrategy;
import org.apache.cassandra.db.compaction.ShardManager;
import org.apache.cassandra.db.compaction.ShardManagerNoDisks;
import org.apache.cassandra.db.compaction.ShardTracker;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.db.compaction.unified.ShardedCompactionWriter;
import org.apache.cassandra.db.compaction.unified.UnifiedCompactionTask;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.apache.cassandra.db.ColumnFamilyStore.RING_VERSION_IRRELEVANT;
import static org.junit.Assert.assertTrue;

/**
 * UCS on the cursor path, through the real {@link UnifiedCompactionTask} and
 * {@link ShardedCompactionWriter}, matches the iterator path.
 */
public class UnifiedCompactionDifferentialTest extends DifferentialCompactionTester
{
    /** The real UCS task building a {@link ShardedCompactionWriter} over the given shard count. */
    private static TaskFactory sharded(ColumnFamilyStore cfs, int numShards, boolean retainOriginals)
    {
        UnifiedCompactionStrategy strategy = unifiedStrategy(cfs);
        ShardManager shardManager = new ShardManagerNoDisks(ColumnFamilyStore.fullWeightedRange(RING_VERSION_IRRELEVANT,
                                                                                               cfs.getPartitioner()));
        return (c, txn, gcBefore) -> new UnifiedCompactionTask(c, strategy, txn, gcBefore, shardManager)
        {
            @Override
            public CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs,
                                                                  Directories directories,
                                                                  ILifecycleTransaction transaction,
                                                                  Set<SSTableReader> nonExpiredSSTables)
            {
                // A fresh tracker per writer, since it is stateful.
                return new ShardedCompactionWriter(cfs, directories, transaction, nonExpiredSSTables,
                                                   retainOriginals, true /* earlyOpenAllowed */,
                                                   shardManager.boundaries(numShards));
            }
        };
    }

    private static UnifiedCompactionStrategy unifiedStrategy(ColumnFamilyStore cfs)
    {
        for (List<AbstractCompactionStrategy> perRepairState : cfs.getCompactionStrategyManager().getStrategies())
            for (AbstractCompactionStrategy strategy : perRepairState)
                if (strategy instanceof UnifiedCompactionStrategy)
                    return (UnifiedCompactionStrategy) strategy;
        throw new AssertionError("the table is not on UnifiedCompactionStrategy");
    }

    private ColumnFamilyStore ucsTable() throws Throwable
    {
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v text, PRIMARY KEY (pk, ck)) " +
                    "WITH compaction = {'class': 'UnifiedCompactionStrategy'} " +
                    "AND compression = {'enabled': 'false'}");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        return cfs;
    }

    @Test
    public void shardedWriterMatchesIterator() throws Throwable
    {
        ColumnFamilyStore cfs = ucsTable();

        String padding = "x".repeat(120);
        for (int round = 0; round < 2; round++)
        {
            for (long pk = 0; pk < 200; pk++)
                for (long ck = 0; ck < 4; ck++)
                    execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, ck, padding + round + "-" + ck);
            flush();
        }

        CapturedOutput out = assertCursorMatchesIterator(cfs, cfs.getLiveSSTables(), sharded(cfs, 4, true));
        assertTrue("sharding must produce several outputs to test anything, got " + out.sstables.size(),
                   out.sstables.size() >= 2);
    }

    /** Every output stays inside one shard on the cursor path. */
    @Test
    public void everyOutputStaysInsideOneShardOnCursorPath() throws Throwable
    {
        assertEveryOutputStaysInsideOneShard(true);
    }

    /** The same expectation on the iterator path. */
    @Test
    public void everyOutputStaysInsideOneShardOnIteratorPath() throws Throwable
    {
        assertEveryOutputStaysInsideOneShard(false);
    }

    private void assertEveryOutputStaysInsideOneShard(boolean cursor) throws Throwable
    {
        ColumnFamilyStore cfs = ucsTable();

        String padding = "y".repeat(200);
        for (long pk = 0; pk < 120; pk++)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, 0L, padding);
        flush();
        for (long pk = 0; pk < 120; pk += 2)
            execute("INSERT INTO %s (pk, ck, v) VALUES (?, ?, ?)", pk, 1L, padding);
        flush();

        int numShards = 8;
        ShardManager shardManager = new ShardManagerNoDisks(ColumnFamilyStore.fullWeightedRange(RING_VERSION_IRRELEVANT,
                                                                                               cfs.getPartitioner()));
        // Commit, so the live set is the sharded output.
        commitThroughFactory(cfs, cursor, sharded(cfs, numShards, false));

        Set<SSTableReader> outputs = cfs.getLiveSSTables();
        assertTrue("sharding must produce several outputs to test anything, got " + outputs.size(),
                   outputs.size() >= 2);
        for (SSTableReader sstable : outputs)
            assertInsideOneShard(shardManager.boundaries(numShards), sstable);
    }

    /** Asserts the sstable's last key has not crossed the end of the shard its first key falls in. */
    private static void assertInsideOneShard(ShardTracker tracker, SSTableReader sstable)
    {
        DecoratedKey first = sstable.getFirst();
        DecoratedKey last = sstable.getLast();
        tracker.advanceTo(first.getToken());
        Token shardEnd = tracker.shardEnd();
        assertTrue(sstable + " spans shard boundary " + shardEnd + " (" + first + " to " + last + ')',
                   shardEnd == null || last.getToken().compareTo(shardEnd) <= 0);
    }
}
