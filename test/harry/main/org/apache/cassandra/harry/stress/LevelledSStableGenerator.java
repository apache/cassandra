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

import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.execution.CQLVisitExecutor;
import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.ValueGenerators;
import org.apache.cassandra.harry.gen.rng.SeedableEntropySource;
import org.apache.cassandra.harry.op.Visit;
import org.apache.cassandra.harry.stress.distribution.Distribution;
import org.apache.cassandra.io.sstable.HarrySSTableWriter;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.service.ActiveRepairService;

import static org.apache.cassandra.harry.stress.ActivePartition.createActivePartition;

public class LevelledSStableGenerator
{
    private final SchemaSpec schema;
    private final boolean disableCompression;
    private final int sstableSizeMiB;
    private final Distribution rowPopulation;
    private final Distribution visitSizeDistribution;
    private final VisitGenerator.OpKindGenFactory opKindGen;
    private final VisitGenerator.ColumnPopulation columnPopulation;
    private final ActivePartition.PartitionKeyGen pkGen;
    private final TokenIndex tokenIndex;
    private final SSTableLevelPicker levelPicker;
    private final File directory;
    private final long maxPartitions;
    private final long repairedAtMillis;

    public LevelledSStableGenerator(SchemaSpec schema,
                                    Distribution rowPopulation,
                                    VisitGenerator.ColumnPopulation columnPopulation,
                                    Distribution visitSizeDistribution,
                                    VisitGenerator.OpKindGenFactory operationKindGen,
                                    boolean disableCompression,
                                    int sstableSizeMiB,
                                    SSTableLevelPicker levelPicker,
                                    TokenIndex tokenIndex,
                                    File directory)
    {
        this(schema, rowPopulation, columnPopulation, visitSizeDistribution, operationKindGen,
             disableCompression, sstableSizeMiB, levelPicker, tokenIndex, directory, Long.MAX_VALUE, ActiveRepairService.UNREPAIRED_SSTABLE);
    }

    public LevelledSStableGenerator(SchemaSpec schema,
                                    Distribution rowPopulation,
                                    VisitGenerator.ColumnPopulation columnPopulation,
                                    Distribution visitSizeDistribution,
                                    VisitGenerator.OpKindGenFactory operationKindGen,
                                    boolean disableCompression,
                                    int sstableSizeMiB,
                                    SSTableLevelPicker levelPicker,
                                    TokenIndex tokenIndex,
                                    File directory,
                                    long maxPartitions)
    {
        this(schema, rowPopulation, columnPopulation, visitSizeDistribution, operationKindGen,
             disableCompression, sstableSizeMiB, levelPicker, tokenIndex, directory, maxPartitions, ActiveRepairService.UNREPAIRED_SSTABLE);
    }

    public LevelledSStableGenerator(SchemaSpec schema,
                                    Distribution rowPopulation,
                                    VisitGenerator.ColumnPopulation columnPopulation,
                                    Distribution visitSizeDistribution,
                                    VisitGenerator.OpKindGenFactory operationKindGen,
                                    boolean disableCompression,
                                    int sstableSizeMiB,
                                    SSTableLevelPicker levelPicker,
                                    TokenIndex tokenIndex,
                                    File directory,
                                    long maxPartitions,
                                    long repairedAtMillis)
    {
        this.schema = schema;
        this.rowPopulation = rowPopulation;
        this.columnPopulation = columnPopulation;
        this.visitSizeDistribution = visitSizeDistribution;
        this.opKindGen = operationKindGen;
        this.disableCompression = disableCompression;
        this.sstableSizeMiB = sstableSizeMiB;
        this.levelPicker = levelPicker;
        this.tokenIndex = tokenIndex;
        this.directory = directory;
        this.maxPartitions = maxPartitions;
        this.repairedAtMillis = repairedAtMillis;
        this.pkGen = new ActivePartition.PartitionKeyGen(schema);
    }

    public void generate(long minToken, long maxToken)
    {
        TokenIndex.EntryIterator iter = tokenIndex.range(minToken, maxToken);
        HarrySSTableWriter[] writers = new HarrySSTableWriter[levelPicker.size()];
        CQLVisitExecutor[] executors = new CQLVisitExecutor[levelPicker.size()];
        // This is a bit of a hack: we do not create full blown Partitions, since we don't
        // pick partitions in the same way we were picking them during "normal" generation.
        CurrentPartition currentPartition = new CurrentPartition(pkGen);
        for (int i = 0; i < writers.length; i++)
        {
            writers[i] = SSTableGenerator.newWriter(schema, disableCompression, sstableSizeMiB, directory, i, repairedAtMillis);
            executors[i] = SSTableGenerator.createExecutor(schema, currentPartition, writers[i], null);
        }
        long counter = 0;
        long lastChecking = System.currentTimeMillis();
        while (iter.hasNext() && counter < maxPartitions)
        {
            counter++;
            if (counter % 1000 == 0)
            {
                long now = System.currentTimeMillis();
                System.out.println("Processed " + counter + " partitions " + (now - lastChecking) + "ms elapsed");
                lastChecking = now;
            }
            long pd = iter.pd();
            long[] ltss = iter.readLts();
            ActivePartition partition = byPd(pd);
            currentPartition.current = partition;
            for (long lts : ltss)
            {
                Visit visit = VisitGenerator.mutatingVisit(lts, visitSizeDistribution, opKindGen, rng -> partition);
                CQLVisitExecutor executor = executors[levelPicker.pick(lts)];
                executor.execute(visit);
            }
            currentPartition.current = null;
            pkGen.cleanup(pd);
            iter.advance();
        }

        for (int i = 0; i < writers.length; i++)
        {
            try
            {
                writers[i].close();
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }
    }

    public static class SSTableLevelPicker
    {
        final long[] cdf;
        final long total;

        public SSTableLevelPicker(int... weights)
        {
            cdf = new long[weights.length];
            cdf[0] = weights[0];
            for (int i = 1; i < weights.length; i++)
                cdf[i] = cdf[i - 1] + weights[i];
            total = cdf[cdf.length - 1];
        }

        public int size()
        {
            return cdf.length;
        }

        public int pick(long lts)
        {
            EntropySource rng = SeedableEntropySource.entropySource(lts, 1);
            long val = rng.nextLong(0, total);
            for (int i = 0; i < cdf.length; i++)
            {
                if (val < cdf[i])
                    return i;
            }
            return cdf.length - 1;
        }
    }

    private ActivePartition byPd(long pd)
    {
        long idx = ActivePartition.DescriptorIndexBijection.INSTANCE.toIdx(pd);
        ActivePartition partition = createActivePartition(idx, pd, schema, rowPopulation, columnPopulation, pkGen, (pd_) -> {});
        pkGen.ensure(pd, d -> SeedableEntropySource.computeWithSeed(d, SchemaSpec.forKeys(schema.partitionKeys)::generate));
        return partition;
    }

    static class CurrentPartition extends ValueGenerators<Object[], Object[]>
    {
        ActivePartition current;

        CurrentPartition(HistoryBuilder.IndexedBijection<Object[]> pkGen)
        {
            super(pkGen);
        }

        @Override
        public ActivePartition forPd(long pd)
        {
            if (current == null || current.pd != pd)
                throw new IllegalStateException("No ActivePartition for pd=" + pd + "; expected pd=" + (current != null ? current.pd : "null"));
            return current;
        }
    }
}