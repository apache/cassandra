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

package org.apache.cassandra.io.sstable;

import java.util.Arrays;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.harry.ColumnSpec;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.execution.CQLTesterVisitExecutor;
import org.apache.cassandra.harry.execution.CQLVisitExecutor;
import org.apache.cassandra.harry.execution.DataTracker;
import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.model.QuiescentChecker;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.Child;
import org.apache.cassandra.io.sstable.ZeroCopySSTableSplitter.Result;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.big.BigFormat;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_RANDOM_SEED;
import static org.apache.cassandra.harry.checker.TestHelper.withRandom;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Harry model checking across a published zero-copy split of a compressed BIG SSTable. */
public class ZeroCopySSTableSplitterHarryTest extends CQLTester
{
    private static final AtomicInteger idGen = new AtomicInteger();

    private static final int PARTITIONS = 40;
    private static final int ROWS = 24;
    private static final int ROUNDS = 3;
    private static final int OPS_PER_ROUND = 120;

    @Test
    public void harryHistorySurvivesPublishedSplit()
    {
        Assume.assumeTrue(BigFormat.isSelected());

        long seed = TEST_RANDOM_SEED.getLong(new Random().nextLong());
        withRandom(seed, rng -> runHistory(rng));
    }

    private void runHistory(EntropySource rng) throws Throwable
    {
        String keyspace = "harry_split_ks" + idGen.incrementAndGet();
        String table = "tbl" + idGen.incrementAndGet();
        SchemaSpec schema = new SchemaSpec(rng.next(),
                                           1000,
                                           keyspace,
                                           table,
                                           Arrays.asList(ColumnSpec.pk("pk1", ColumnSpec.asciiType),
                                                         ColumnSpec.pk("pk2", ColumnSpec.int64Type)),
                                           Arrays.asList(ColumnSpec.ck("ck1", ColumnSpec.asciiType, false),
                                                         ColumnSpec.ck("ck2", ColumnSpec.int64Type, true)),
                                           Arrays.asList(ColumnSpec.regularColumn("r1", ColumnSpec.asciiType),
                                                         ColumnSpec.regularColumn("r2", ColumnSpec.int64Type),
                                                         ColumnSpec.regularColumn("r3", ColumnSpec.textType)),
                                           Arrays.asList(ColumnSpec.staticColumn("s1", ColumnSpec.asciiType),
                                                         ColumnSpec.staticColumn("s2", ColumnSpec.int64Type)));

        schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = " +
                                   "{'class': 'SimpleStrategy', 'replication_factor': '1'}", keyspace));
        createTable(schema.compile());
        schemaChange(String.format("ALTER TABLE %s.%s WITH compression = " +
                                   "{'class': 'LZ4Compressor', 'chunk_length_in_kb': '4'}",
                                   keyspace, table));

        ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);
        for (int pd = 0; pd < PARTITIONS; pd++)
        {
            history.insert(pd, 0);
            history.insert(pd, 1);
        }

        for (int round = 0; round < ROUNDS; round++)
        {
            for (int op = 0; op < OPS_PER_ROUND; op++)
                addMutation(history, schema, rng);
            history.customThrowing(() -> Util.flush(cfs), "flush round " + round);
        }

        history.customThrowing(() -> {
            cfs.forceMajorCompaction();
            splitAndPublish(cfs);
        }, "compact and publish zero-copy split");

        for (int pd = 0; pd < PARTITIONS; pd++)
        {
            history.selectPartition(pd);
            if ((pd & 3) == 0)
                history.selectRow(pd, rng.nextInt(0, ROWS));
            if ((pd & 3) == 1)
            {
                int lower = rng.nextInt(0, ROWS);
                int upper = rng.nextInt(lower, 2 * ROWS);
                history.selectRowRange(pd, lower, upper,
                                       rng.nextInt(schema.clusteringKeys.size()),
                                       rng.nextBoolean(), rng.nextBoolean());
            }
        }

        DataTracker tracker = new DataTracker.SequentialDataTracker();
        CQLVisitExecutor executor = new CQLTesterVisitExecutor(schema,
                                                               tracker,
                                                               new QuiescentChecker(schema.valueGenerators,
                                                                                    tracker,
                                                                                    history),
                                                               statement -> execute(statement.cql(),
                                                                                    statement.bindings()));
        CQLVisitExecutor.replay(executor, history);
    }

    private static void addMutation(HistoryBuilder history, SchemaSpec schema, EntropySource rng)
    {
        int pd = rng.nextInt(0, PARTITIONS);
        int row = rng.nextInt(0, ROWS);
        int kind = rng.nextInt(0, 100);
        if (kind < 55)
            history.insert(pd, row);
        else if (kind < 70)
            history.update(pd, row);
        else if (kind < 82)
            history.deleteRow(pd, row);
        else if (kind < 95)
        {
            int lower = rng.nextInt(0, ROWS);
            int upper = rng.nextInt(lower, 2 * ROWS);
            history.deleteRowRange(pd, lower, upper,
                                   rng.nextInt(schema.clusteringKeys.size()),
                                   rng.nextBoolean(), rng.nextBoolean());
        }
        else
            history.deletePartition(pd);
    }

    private static void splitAndPublish(ColumnFamilyStore cfs) throws Throwable
    {
        Set<SSTableReader> live = cfs.getLiveSSTables();
        assertEquals("major compaction must produce one split parent", 1, live.size());
        SSTableReader parent = live.iterator().next();
        assertTrue("Harry parent is unsupported by the zero-copy splitter",
                   ZeroCopySSTableSplitter.isSupported(parent));

        LifecycleTransaction transaction = cfs.getTracker().tryModify(parent, OperationType.COMPACTION);
        assertNotNull("could not mark Harry parent compacting", transaction);
        boolean committed = false;
        int childCount = 0;
        try
        {
            long targetSize = Math.max(1, parent.descriptor.fileFor(Components.DATA).length() / 4);
            Result result = ZeroCopySSTableSplitter.splitBySize(parent, targetSize, transaction);
            assertTrue("size-based Harry split produced only one child", result.children.size() > 1);
            childCount = result.children.size();
            for (Child child : result.children)
                transaction.update(child.reader, false);
            transaction.obsoleteOriginals();
            transaction.prepareToCommit();
            transaction.commit();
            committed = true;
        }
        finally
        {
            if (!committed)
                transaction.abort();
        }

        assertEquals("published split children must replace the Harry parent",
                     childCount, cfs.getLiveSSTables().size());
    }
}
