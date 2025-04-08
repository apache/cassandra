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
package org.apache.cassandra.harry.stress;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Consumer;

import com.google.common.io.Files;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.execution.CQLVisitExecutor;
import org.apache.cassandra.harry.execution.CompiledStatement;
import org.apache.cassandra.harry.execution.DataTracker;
import org.apache.cassandra.harry.execution.QueryBuildingVisitExecutor;
import org.apache.cassandra.harry.execution.ResultSetRow;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.ValueGenerators;
import org.apache.cassandra.harry.model.Model;
import org.apache.cassandra.harry.op.Operations;
import org.apache.cassandra.harry.op.Visit;
import org.apache.cassandra.harry.stress.distribution.Distribution;
import org.apache.cassandra.io.sstable.HarrySSTableWriter;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Generates SSTables using the same visit generation machinery as {@link HarryStress}.
 * Visits are compiled into CQL statements and fed into {@link HarrySSTableWriter} to produce
 * SSTables on disk. The writer automatically flushes to a new SSTable when the configured
 * size threshold is reached.
 */
public class SSTableGenerator
{
    public static final Logger LOGGER = LoggerFactory.getLogger(SSTableGenerator.class);

    private final SchemaSpec schema;
    private final VisitGenerator visitGenerator;
    private final ActivePartition.Partitions partitionFactory;
    private final boolean disableCompression;
    private final int sstableSizeMiB;
    private final int sstableLevel;
    private final long repairedAtMillis;

    public SSTableGenerator(SchemaSpec schema,
                            Distribution rowPopulation,
                            VisitGenerator.ColumnPopulation columnPopulation,
                            Generator<VisitGenerator.VisitType> visitTypeGen,
                            Distribution visitSizeDistribution,
                            VisitGenerator.OpKindGenFactory operationKindGen,
                            RotationStrategy rotationStrategy,
                            boolean disableCompression,
                            int sstableSizeMiB,
                            long minPartitionIdx,
                            long maxPartitionIdx,
                            long initialLts)
    {
        this(schema, rowPopulation, columnPopulation, visitTypeGen, visitSizeDistribution,
             operationKindGen, rotationStrategy, disableCompression, sstableSizeMiB,
             minPartitionIdx, maxPartitionIdx, initialLts, 0, ActiveRepairService.UNREPAIRED_SSTABLE);
    }

    public SSTableGenerator(SchemaSpec schema,
                            Distribution rowPopulation,
                            VisitGenerator.ColumnPopulation columnPopulation,
                            Generator<VisitGenerator.VisitType> visitTypeGen,
                            Distribution visitSizeDistribution,
                            VisitGenerator.OpKindGenFactory operationKindGen,
                            RotationStrategy rotationStrategy,
                            boolean disableCompression,
                            int sstableSizeMiB,
                            long minPartitionIdx,
                            long maxPartitionIdx,
                            long initialLts,
                            int sstableLevel)
    {
        this(schema, rowPopulation, columnPopulation, visitTypeGen, visitSizeDistribution,
             operationKindGen, rotationStrategy, disableCompression, sstableSizeMiB,
             minPartitionIdx, maxPartitionIdx, initialLts, sstableLevel, ActiveRepairService.UNREPAIRED_SSTABLE);
    }

    public SSTableGenerator(SchemaSpec schema,
                            Distribution rowPopulation,
                            VisitGenerator.ColumnPopulation columnPopulation,
                            Generator<VisitGenerator.VisitType> visitTypeGen,
                            Distribution visitSizeDistribution,
                            VisitGenerator.OpKindGenFactory operationKindGen,
                            RotationStrategy rotationStrategy,
                            boolean disableCompression,
                            int sstableSizeMiB,
                            long minPartitionIdx,
                            long maxPartitionIdx,
                            long initialLts,
                            int sstableLevel,
                            long repairedAtMillis)
    {
        this.schema = schema;
        this.disableCompression = disableCompression;
        this.sstableSizeMiB = sstableSizeMiB;
        this.sstableLevel = sstableLevel;
        this.repairedAtMillis = repairedAtMillis;
        this.partitionFactory = new ActivePartition.Partitions(schema, rowPopulation, columnPopulation, rotationStrategy, minPartitionIdx, maxPartitionIdx, initialLts);
        this.visitGenerator = new VisitGenerator(partitionFactory,
                                                 visitTypeGen,
                                                 visitSizeDistribution,
                                                 operationKindGen,
                                                 initialLts);
        partitionFactory.populate();
    }

    public void generate(File directory, long totalVisits, java.io.File progressFile)
    {
        generate(directory, totalVisits, visit -> {}, progressFile);
    }

    public void generate(File directory, long totalVisits, Consumer<Visit> onVisit, java.io.File progressFile)
    {
        HarrySSTableWriter writer = newWriter(directory);
        CQLVisitExecutor executor = createExecutor(writer, progressFile);

        for (long i = 0; i < totalVisits; i++)
        {
            Visit visit = visitGenerator.get();

            for (long pd : visit.visitedPartitions)
                partitionFactory.forPd(pd).ref();

            try
            {
                if (!visit.validating())
                    executor.execute(visit);

                onVisit.accept(visit);

                partitionFactory.maybeSwitchPartition(visit.lts, action -> {});
            }
            finally
            {
                for (long pd : visit.visitedPartitions)
                    partitionFactory.forPd(pd).deref();
            }
        }

        closeWriter(writer);
    }

    private HarrySSTableWriter newWriter(File directory)
    {
        return newWriter(directory, sstableLevel, repairedAtMillis);
    }

    private HarrySSTableWriter newWriter(File directory, int level)
    {
        return newWriter(schema, disableCompression, sstableSizeMiB, directory, level, ActiveRepairService.UNREPAIRED_SSTABLE);
    }

    private HarrySSTableWriter newWriter(File directory, int level, long repairedAtMillis)
    {
        return newWriter(schema, disableCompression, sstableSizeMiB, directory, level, repairedAtMillis);
    }

    public static HarrySSTableWriter newWriter(SchemaSpec schema, boolean disableCompression, int sstableSizeMiB, File directory, int level)
    {
        return newWriter(schema, disableCompression, sstableSizeMiB, directory, level, ActiveRepairService.UNREPAIRED_SSTABLE);
    }

    public static HarrySSTableWriter newWriter(SchemaSpec schema, boolean disableCompression, int sstableSizeMiB, File directory, int level, long repairedAtMillis)
    {
        try
        {
            String tableCql = schema.compile();
            if (disableCompression)
            {
                String noSemicolon = tableCql.endsWith(";") ? tableCql.substring(0, tableCql.length() - 1) : tableCql;
                if (noSemicolon.contains(" WITH "))
                    tableCql = noSemicolon + " AND compression = {'enabled': 'false'};";
                else
                    tableCql = noSemicolon + " WITH compression = {'enabled': 'false'};";
            }
            return HarrySSTableWriter.builder()
                                     .forTable(tableCql)
                                     .inDirectory(directory)
                                     .withMaxSSTableSizeInMiB(sstableSizeMiB)
                                     .withSSTableLevel(level)
                                     .withRepairedAtMillis(repairedAtMillis)
                                     .build();
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to create SSTable writer", e);
        }
    }

    private CQLVisitExecutor createExecutor(HarrySSTableWriter writer, java.io.File progressFile)
    {
        return createExecutor(schema, partitionFactory, writer, progressFile);
    }

    public static CQLVisitExecutor createExecutor(SchemaSpec schema, ValueGenerators<Object[], Object[]> valueGenerators, HarrySSTableWriter writer, java.io.File progressFile)
    {
        DataTracker tracker = new DataTracker.NoOpDataTracker();
        QueryBuildingVisitExecutor queryBuilder = new QueryBuildingVisitExecutor(schema,
                                                                                 QueryBuildingVisitExecutor.WrapQueries.EMPTY,
                                                                                 valueGenerators);
        return new CQLVisitExecutor(schema, tracker, Model.NO_OP, queryBuilder)
        {
            long lts;
            {
                writer.setListener(file -> {
                    System.out.println(String.format("Written to %d: %s", lts, file));
                    if (progressFile != null)
                    {
                        try { Files.write(ByteBufferUtil.getArray(ByteBufferUtil.bytes(lts)), progressFile); }
                        catch (IOException e) { throw new RuntimeException(e); }
                    }
                });
            }
            @Override
            protected void executeMutatingVisit(Visit visit, CompiledStatement statement)
            {
                lts = visit.lts;
                try
                {
                    writer.addRow(statement.cql(), statement.bindings());
                }
                catch (IOException e)
                {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            protected void executeValidatingVisit(Visit visit, List<Operations.SelectStatement> selects, CompiledStatement compiledStatement)
            {
            }

            @Override
            protected List<ResultSetRow> executeWithResult(Visit visit, CompiledStatement statement)
            {
                throw new UnsupportedOperationException("SSTable generation does not support reads");
            }

            @Override
            protected void executeWithoutResult(Visit visit, CompiledStatement statement)
            {
                executeMutatingVisit(visit, statement);
            }

            @Override
            public void execute(Visit visit)
            {
                if (visit.visitedPartitions.length > 1)
                    throw new IllegalStateException("SSTable generator does not support batch statements across multiple partitions");

                super.execute(visit);
            }
        };
    }

    private static void closeWriter(HarrySSTableWriter writer)
    {
        try
        {
            writer.close();
        }
        catch (IOException e)
        {
            throw new UncheckedIOException("Failed to close SSTable writer", e);
        }
    }
}
