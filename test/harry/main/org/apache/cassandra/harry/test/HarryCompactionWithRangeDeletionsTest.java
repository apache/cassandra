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

package org.apache.cassandra.harry.test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import accord.utils.Invariants;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.harry.ColumnSpec;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.execution.CQLTesterVisitExecutor;
import org.apache.cassandra.harry.execution.CQLVisitExecutor;
import org.apache.cassandra.harry.execution.CompiledStatement;
import org.apache.cassandra.harry.execution.DataTracker;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.model.QuiescentChecker;
import org.apache.cassandra.harry.op.Operations;
import org.apache.cassandra.harry.op.Visit;
import org.apache.cassandra.harry.util.BitSet;
import org.apache.cassandra.harry.util.ThrowingRunnable;
import org.apache.cassandra.io.sstable.HarrySSTableWriter;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.harry.checker.TestHelper.withRandom;

public class HarryCompactionWithRangeDeletionsTest extends CQLTester
{
    private static final AtomicInteger idGen = new AtomicInteger(0);
    public static final int TEST_REPS = 100;
    public static final int PARTITIONS_RANGE = 100;
    public static final int ROWS_RANGE = 10;
    public static final int STATIC_COLS = 3;
    public static final int REG_COLS = 9;
    private final Generator<BitSet> staticColumnsGenerator = BitSet.generator(STATIC_COLS);
    private final Generator<BitSet> regularColumnsGenerator = BitSet.generator(REG_COLS);

    private String keyspace;
    private String table;
    private String qualifiedTable;
    private File dataDir;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    public void perTestSetup() throws IOException
    {
        keyspace = "cql_keyspace" + idGen.incrementAndGet();
        table = "table" + idGen.incrementAndGet();
        qualifiedTable = keyspace + '.' + table;
        dataDir = new File(tempFolder.newFolder().getAbsolutePath() + File.pathSeparator() + keyspace + File.pathSeparator() + table);
        assert dataDir.tryCreateDirectories();

        ServerTestUtils.prepareServerNoRegister();
        StorageService.instance.initServer();
        requireNetwork();
    }

    private final Generator<SchemaSpec> schemaSpecGenerator = rng -> {
        return new SchemaSpec(rng.next(),
                              1000,
                              keyspace,
                              table,
                              Arrays.asList(ColumnSpec.pk("pk1", ColumnSpec.asciiType),
                                            ColumnSpec.pk("pk2", ColumnSpec.int64Type)),
                              Arrays.asList(ColumnSpec.ck("ck1", ColumnSpec.asciiType, true),
                                            ColumnSpec.ck("ck2", ColumnSpec.int64Type, true)),
                              Arrays.asList(ColumnSpec.regularColumn("r1", ColumnSpec.asciiType),
                                            ColumnSpec.regularColumn("r2", ColumnSpec.int64Type),
                                            ColumnSpec.regularColumn("r3", ColumnSpec.int8Type),
                                            ColumnSpec.regularColumn("r4", ColumnSpec.doubleType),
                                            ColumnSpec.regularColumn("r5", ColumnSpec.floatType),
                                            ColumnSpec.regularColumn("r6", ColumnSpec.int32Type),
                                            ColumnSpec.regularColumn("r7", ColumnSpec.booleanType),
                                            ColumnSpec.regularColumn("r8", ColumnSpec.int16Type),
                                            ColumnSpec.regularColumn("r9", ColumnSpec.textType)),
                              Arrays.asList(ColumnSpec.staticColumn("s1", ColumnSpec.asciiType),
                                            ColumnSpec.staticColumn("s2", ColumnSpec.int64Type),
                                            ColumnSpec.staticColumn("s3", ColumnSpec.asciiType)));
    };

    @Test
    public void testFlushAndCompact1() throws IOException {
        testFlushAndCompact(1);
    }

    @Test
    public void testFlushAndCompact2() throws IOException {
        testFlushAndCompact(2);
    }

    @Test
    public void testFlushAndCompact3() throws IOException {
        testFlushAndCompact(3);
    }

    @Test
    public void testFlushAndCompact4() throws IOException {
        testFlushAndCompact(4);
    }

    @Test
    public void testFlushAndCompact5() throws IOException
    {
        testFlushAndCompact(5);
    }

    public void testFlushAndCompact(int flushcount) throws IOException
    {
        for (int i = 0; i < TEST_REPS; i++)
            testFlushAndCompactOnce(flushcount);
    }

    public void testFlushAndCompactOnce(int flushcount) throws IOException
    {
        perTestSetup();
        withRandom(205413964293041L, rng -> {

            SchemaSpec schema = schemaSpecGenerator.generate(rng);
            schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", schema.keyspace));
            createTable(schema.compile());

            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);
            history.customThrowing(() -> {
                ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
                cfs.disableAutoCompaction();
            }, "disable compaction");

            AtomicReference<HarrySSTableWriter> sstableWriter = new AtomicReference<>();
            ThrowingRunnable flushAndChangeWriter = () -> {
                HarrySSTableWriter prev = sstableWriter.get();
                if (prev != null)
                {
                    prev.close();
                    StorageService.instance.bulkLoad(dataDir.absolutePath());
                    dataDir.forEach(file -> file.delete());
                }

                Invariants.require(sstableWriter.getAndSet(HarrySSTableWriter.builder()
                                                                                   .forTable(schema.compile())
                                                                                   .inDirectory(dataDir)
                                                                                   .build()) == prev);
            };
            flushAndChangeWriter.run();

            for (int sstablesFlushed = 0; sstablesFlushed < flushcount; sstablesFlushed++)
            {
                for (int i = 0; i < PARTITIONS_RANGE; i++)
                {
                    for (int j = 0; j < ROWS_RANGE; j++)
                    {
                        history.insert(rng.nextInt(0, 2 * PARTITIONS_RANGE), rng.nextInt(0, 2 * ROWS_RANGE)); // some overlap, but not all
                    }
                }
                int lowerBoundRowIdx = rng.nextInt(ROWS_RANGE);
                int upperBoundRowIdx = rng.nextInt(lowerBoundRowIdx, 2 * ROWS_RANGE);
                history.deleteRowRange(rng.nextInt(0, 2 * PARTITIONS_RANGE),
                                       lowerBoundRowIdx,
                                       upperBoundRowIdx,
                                       rng.nextInt(REG_COLS),
                                       rng.nextBoolean(),
                                       rng.nextBoolean());

                history.customThrowing(flushAndChangeWriter, "flush sstable" + sstablesFlushed);
            }

            history.customThrowing(() -> {
                ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
                cfs.forceMajorCompaction();
            }, "major compaction");

            for (int i = 0; i < 2 * PARTITIONS_RANGE; i++)
                history.selectPartition(i);

            replay(schema, history, sstableWriter::get);
        });
    }

    public void replay(SchemaSpec schema, HistoryBuilder historyBuilder, Supplier<HarrySSTableWriter> writer)
    {
        CQLVisitExecutor executor = create(schema, historyBuilder, writer);
        for (Visit visit : historyBuilder)
            executor.execute(visit);
    }

    public CQLVisitExecutor create(SchemaSpec schema, HistoryBuilder historyBuilder, Supplier<HarrySSTableWriter> writer)
    {
        DataTracker tracker = new DataTracker.SequentialDataTracker();
        return new CQLTesterVisitExecutor(schema, tracker,
                                          new QuiescentChecker(schema.valueGenerators, tracker, historyBuilder),
                                          statement -> {
                                              if (logger.isTraceEnabled())
                                                  logger.trace(statement.toString());
                                              return execute(statement.cql(), statement.bindings());
                                          })
        {
            @Override
            protected void executeMutatingVisit(Visit visit, CompiledStatement statement)
            {
                try
                {
                    writer.get().addRow(statement.cql(), statement.bindings());
                }
                catch (IOException e)
                {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            protected void executeValidatingVisit(Visit visit, List<Operations.SelectStatement> selects, CompiledStatement compiledStatement)
            {
                super.executeValidatingVisit(visit, selects, compiledStatement);
            }

            @Override
            public void execute(Visit visit)
            {
                if (visit.visitedPartitions.size() > 1)
                    throw new IllegalStateException("SSTable Generator does not support batch statements and transactions");

                super.execute(visit);
            }
        };
    }
}