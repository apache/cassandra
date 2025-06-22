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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import accord.utils.Invariants;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.cql3.CQLTester;
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
import org.apache.cassandra.harry.util.ThrowingRunnable;
import org.apache.cassandra.io.sstable.HarrySSTableWriter;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.harry.checker.TestHelper.withRandom;

public class HarrySSTableWriterTest extends CQLTester
{
    private static final AtomicInteger idGen = new AtomicInteger(0);
    private static final int NUMBER_WRITES_IN_RUNNABLE = 10;

    private String keyspace;
    private String table;
    private String qualifiedTable;
    private File dataDir;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
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

    private final Generator<SchemaSpec> simple_schema = rng -> {
        return new SchemaSpec(rng.next(),
                              1000,
                              keyspace,
                              table,
                              Arrays.asList(ColumnSpec.pk("pk1", ColumnSpec.asciiType),
                                            ColumnSpec.pk("pk2", ColumnSpec.int64Type)),
                              Arrays.asList(ColumnSpec.ck("ck1", ColumnSpec.asciiType, false),
                                            ColumnSpec.ck("ck2", ColumnSpec.int64Type, false)),
                              Arrays.asList(ColumnSpec.regularColumn("r1", ColumnSpec.asciiType),
                                            ColumnSpec.regularColumn("r2", ColumnSpec.int64Type),
                                            ColumnSpec.regularColumn("r3", ColumnSpec.asciiType)),
                              Arrays.asList(ColumnSpec.staticColumn("s1", ColumnSpec.asciiType),
                                            ColumnSpec.staticColumn("s2", ColumnSpec.int64Type),
                                            ColumnSpec.staticColumn("s3", ColumnSpec.asciiType)));
    };

    @Test
    public void generateSSTableTest()
    {
        withRandom(rng -> {

            SchemaSpec schema = simple_schema.generate(rng);
            schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", schema.keyspace));
            createTable(schema.compile());

            HistoryBuilder history = new HistoryBuilder(schema.valueGenerators);
            for (int i = 0; i < 100; i++)
                history.insert(1);

            AtomicReference<HarrySSTableWriter> sstableWriter = new AtomicReference<>();
            ThrowingRunnable resetWriter = () -> {
                HarrySSTableWriter prev = sstableWriter.get();
                if (prev != null)
                {
                    prev.close();
                    StorageService.instance.bulkLoad(dataDir.absolutePath());
                }

                Invariants.require(sstableWriter.getAndSet(HarrySSTableWriter.builder()
                                                                                   .forTable(schema.compile())
                                                                                   .inDirectory(dataDir)
                                                                                   .build()) == prev);
            };
            resetWriter.run();

            for (int i = 0; i < 100; i++)
            {
                for (int j = 0; j < 10; j++)
                    history.insert(i, j);
            }

            history.customThrowing(resetWriter, "flush sstable");

            for (int i = 0; i < 100; i++)
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