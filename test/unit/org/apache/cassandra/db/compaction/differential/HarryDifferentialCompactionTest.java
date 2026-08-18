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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.harry.ColumnSpec;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.dsl.HistoryBuilderHelper;
import org.apache.cassandra.harry.execution.CQLTesterVisitExecutor;
import org.apache.cassandra.harry.execution.CQLVisitExecutor;
import org.apache.cassandra.harry.execution.DataTracker;
import org.apache.cassandra.harry.model.QuiescentChecker;
import org.apache.cassandra.harry.op.Visit;

import static org.apache.cassandra.harry.checker.TestHelper.withRandom;

/**
 * Harry-driven differential soak: deep probabilistic tombstone/overwrite histories (the
 * workload shapes Harry's DSL is strongest at — interleaved row/column/range/partition
 * deletes against overlapping inserts) executed through plain CQL in this JVM, flushed into
 * several sstables, then both compaction paths compared byte-for-byte. The differential
 * harness is the oracle, so no Harry read-validation visits are issued.
 *
 * Schema uses simple types only (the currently supported cursor surface; Harry cannot
 * generate multi-cell columns yet — ColumnSpec TODOs). Reversed clustering included.
 */
public class HarryDifferentialCompactionTest extends DifferentialCompactionTester
{
    private static final AtomicInteger idGen = new AtomicInteger(0);

    private static final int PARTITIONS = 30;
    private static final int ROWS = 20;
    private static final int REG_COLS = 5;
    private static final int ROUNDS = 3;
    private static final int OPS_PER_ROUND = 120;

    @Test
    public void harryTombstoneHistories() throws Throwable
    {
        // Raised out here, not inside the callback: withRandom catches Throwable and rewraps it,
        // so an assumption violated inside would reach JUnit as a failure instead of a skip.
        assumeBigFormatSelected();

        long seed = System.currentTimeMillis();
        logger.info("harryTombstoneHistories seed={}", seed);
        withRandom(seed, rng -> {
            String ks = "harry_diff_ks" + idGen.incrementAndGet();
            String table = "tbl" + idGen.incrementAndGet();
            SchemaSpec schema = new SchemaSpec(rng.next(),
                                               1000,
                                               ks,
                                               table,
                                               Arrays.asList(ColumnSpec.pk("pk1", ColumnSpec.asciiType),
                                                             ColumnSpec.pk("pk2", ColumnSpec.int64Type)),
                                               Arrays.asList(ColumnSpec.ck("ck1", ColumnSpec.asciiType, false),
                                                             ColumnSpec.ck("ck2", ColumnSpec.int64Type, true)),
                                               Arrays.asList(ColumnSpec.regularColumn("r1", ColumnSpec.asciiType),
                                                             ColumnSpec.regularColumn("r2", ColumnSpec.int64Type),
                                                             ColumnSpec.regularColumn("r3", ColumnSpec.doubleType),
                                                             ColumnSpec.regularColumn("r4", ColumnSpec.int32Type),
                                                             ColumnSpec.regularColumn("r5", ColumnSpec.textType)),
                                               Arrays.asList(ColumnSpec.staticColumn("s1", ColumnSpec.asciiType),
                                                             ColumnSpec.staticColumn("s2", ColumnSpec.int64Type)));

            schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = " +
                                       "{'class': 'SimpleStrategy', 'replication_factor': '1'}", ks));
            createTable(schema.compile());
            ColumnFamilyStore cfs = Keyspace.open(ks).getColumnFamilyStore(table);
            cfs.disableAutoCompaction();

            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);

            for (int round = 0; round < ROUNDS; round++)
            {
                for (int op = 0; op < OPS_PER_ROUND; op++)
                {
                    int pd = rng.nextInt(0, PARTITIONS);
                    int row = rng.nextInt(0, ROWS);
                    int kind = rng.nextInt(0, 100);
                    if (kind < 70)
                        history.insert(pd, row);
                    else if (kind < 80)
                        history.deleteRow(pd, row);
                    else if (kind < 88)
                    {
                        int lower = rng.nextInt(0, ROWS);
                        int upper = rng.nextInt(lower, 2 * ROWS);
                        history.deleteRowRange(pd, lower, upper,
                                               rng.nextInt(schema.clusteringKeys.size()),
                                               rng.nextBoolean(),
                                               rng.nextBoolean());
                    }
                    else if (kind < 96)
                        HistoryBuilderHelper.deleteRandomColumns(schema, pd, row, rng, history);
                    else
                        history.deletePartition(pd);
                }
                history.customThrowing(() -> flush(ks, table), "flush round");
            }

            replay(schema, history);

            assertCursorMatchesIterator(cfs);
        });
    }

    private void replay(SchemaSpec schema, HistoryBuilder history)
    {
        DataTracker tracker = new DataTracker.SequentialDataTracker();
        CQLVisitExecutor executor =
            new CQLTesterVisitExecutor(schema, tracker,
                                       new QuiescentChecker(schema.valueGenerators, tracker, history),
                                       statement -> execute(statement.cql(), statement.bindings()));
        for (Visit visit : history)
            executor.execute(visit);
    }
}
