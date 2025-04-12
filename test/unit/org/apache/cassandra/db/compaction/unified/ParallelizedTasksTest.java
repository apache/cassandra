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
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.AbstractCompactionTask;
import org.apache.cassandra.db.compaction.ActiveCompactionsTracker;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.compaction.ShardManager;
import org.apache.cassandra.db.compaction.ShardManagerNoDisks;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.db.lifecycle.CompositeLifecycleTransaction;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.PartialLifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.Transactional;
import org.mockito.Mockito;

import static org.apache.cassandra.db.ColumnFamilyStore.RING_VERSION_IRRELEVANT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ParallelizedTasksTest extends ShardingTestBase
{
    @Before
    public void before()
    {
        // Disabling durable write since we don't care
        schemaChange("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND durable_writes=false");
        schemaChange(String.format("CREATE TABLE IF NOT EXISTS %s.%s (k int, t int, v blob, PRIMARY KEY (k, t))", KEYSPACE, TABLE));
    }

    @AfterClass
    public static void tearDownClass()
    {
        QueryProcessor.executeInternal("DROP KEYSPACE IF EXISTS " + KEYSPACE);
    }

    private ColumnFamilyStore getColumnFamilyStore()
    {
        return Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
    }

    @Test
    public void testOneSSTablePerShard() throws Throwable
    {
        int numShards = 5;
        testParallelized(numShards, PARTITIONS, numShards, true);
    }

    @Test
    public void testMultipleInputSSTables() throws Throwable
    {
        int numShards = 3;
        testParallelized(numShards, PARTITIONS, numShards, false);
    }

    private void testParallelized(int numShards, int rowCount, int numOutputSSTables, boolean compact) throws Throwable
    {
        ColumnFamilyStore cfs = getColumnFamilyStore();
        cfs.disableAutoCompaction();

        populate(rowCount, compact);

        LifecycleTransaction transaction = cfs.getTracker().tryModify(cfs.getLiveSSTables(), OperationType.COMPACTION);

        ShardManager shardManager = new ShardManagerNoDisks(ColumnFamilyStore.fullWeightedRange(RING_VERSION_IRRELEVANT, cfs.getPartitioner()));

        Controller mockController = Mockito.mock(Controller.class);
        UnifiedCompactionStrategy mockStrategy = Mockito.mock(UnifiedCompactionStrategy.class, Mockito.CALLS_REAL_METHODS);
        Mockito.when(mockStrategy.getController()).thenReturn(mockController);
        Mockito.when(mockController.getNumShards(Mockito.anyDouble())).thenReturn(numShards);

        Collection<SSTableReader> sstables = transaction.originals();
        CompositeLifecycleTransaction compositeTransaction = new CompositeLifecycleTransaction(transaction);

        List<AbstractCompactionTask> tasks = shardManager.splitSSTablesInShards(
        sstables,
        numShards,
        (rangeSSTables, range) ->
        new UnifiedCompactionTask(cfs,
                                  mockStrategy,
                                  new PartialLifecycleTransaction(compositeTransaction),
                                  0,
                                  shardManager,
                                  range,
                                  rangeSSTables)
        );
        compositeTransaction.completeInitialization();
        assertEquals(numOutputSSTables, tasks.size());

        List<Future<?>> futures = tasks.stream().map(t -> ForkJoinPool.commonPool().submit(() -> t.execute(ActiveCompactionsTracker.NOOP))).collect(Collectors.toList());
        FBUtilities.waitOnFutures(futures);
        assertTrue(transaction.state() == Transactional.AbstractTransactional.State.COMMITTED);

        verifySharding(numShards, rowCount, numOutputSSTables, cfs);
        cfs.truncateBlocking();
    }
}