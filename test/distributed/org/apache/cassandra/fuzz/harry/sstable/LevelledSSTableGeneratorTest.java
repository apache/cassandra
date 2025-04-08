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

package org.apache.cassandra.fuzz.harry.sstable;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.execution.DataTracker;
import org.apache.cassandra.harry.execution.InJvmDTestVisitExecutor;
import org.apache.cassandra.harry.execution.QueryBuildingVisitExecutor;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.harry.model.QuiescentChecker;
import org.apache.cassandra.harry.op.ClusteringOrderBy;
import org.apache.cassandra.harry.op.Operations;
import org.apache.cassandra.harry.op.Visit;
import org.apache.cassandra.harry.stress.ActivePartition;
import org.apache.cassandra.harry.stress.LevelledSStableGenerator;
import org.apache.cassandra.harry.stress.RotationStrategy;
import org.apache.cassandra.harry.stress.config.StressSchemaConfig;
import org.apache.cassandra.harry.stress.TokenIndex;
import org.apache.cassandra.harry.stress.TokenIndexGenerator;
import org.apache.cassandra.harry.stress.VisitGenerator;
import org.apache.cassandra.harry.stress.distribution.Distribution;
import org.apache.cassandra.harry.stress.distribution.Distributions;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.tcm.ClusterMetadataService;

public class LevelledSSTableGeneratorTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(LevelledSSTableGeneratorTest.class);

    private static final String SCHEMA_CONFIG = "test/resources/harry/stress/levelled-sstable-schema.yaml";

    private static final long INITIAL_LTS = 1;
    private static final long VISITS = 100_000;                // total writes; with visitSize=1, one op per LTS
    private static final long END_LTS = INITIAL_LTS + VISITS; // first LTS past the written history; reads use it
    private static final int SSTABLE_SIZE_MIB = 1;
    private static final int[] LEVEL_WEIGHTS = { 1, 2, 4, 8, 16 }; // index == LCS level; weights spread writes across levels

    // Initialize static state for offline tool usage, since we are generating SSTables on the main class loader rather
    // than in-jvm dtest nodes
    public static void initForOfflineTool()
    {
        DatabaseDescriptor.toolInitialization(false);
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ClusterMetadataService.initializeForClients();
    }

    @Test
    public void generateAndValidateLevelledSSTables() throws Throwable
    {
        initForOfflineTool();

        StressSchemaConfig config = StressSchemaConfig.load(Paths.get(SCHEMA_CONFIG));
        SchemaSpec schema = config.schema();

        Distribution visitSize = Distributions.fixed(1);
        VisitGenerator.OpKindGenFactory opKindGen = new VisitGenerator.RandomOpKindGenFactory();

        try (Cluster cluster = init(Cluster.build(1).start(), 1))
        {
            cluster.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + schema.keyspace +
                                 " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            cluster.schemaChange(schema.compile());
            cluster.get(1).nodetool("disableautocompaction", schema.keyspace, schema.table);


            // Build TokenIndex
            File tokenDir = new File(Files.createTempDirectory("harry-token-index"));
            TokenIndexGenerator.generate(tokenDir.toJavaIOFile(), schema, config.rotationStrategy(),
                                         config.rowPopulation(), Generators.constant(VisitGenerator.VisitType.MUTATE),
                                         visitSize, INITIAL_LTS, VISITS);
            TokenIndex tokenIndex = new TokenIndex(new File(tokenDir, "merged_tokens"),
                                                   new File(tokenDir, "merged_tokens.idx"));

            // Generate levelled SSTables
            File sstableDir = new File(Files.createDirectories(Files.createTempDirectory("harry-levelled-sstables")
                                                                    .resolve(schema.keyspace)
                                                                    .resolve(schema.table + '-' + "0".repeat(32))));
            LevelledSStableGenerator generator =
            new LevelledSStableGenerator(schema, config.rowPopulation(), config.columnPopulation(), visitSize, opKindGen,
                                         false, SSTABLE_SIZE_MIB,
                                         new LevelledSStableGenerator.SSTableLevelPicker(LEVEL_WEIGHTS),
                                         tokenIndex, sstableDir);
            generator.generate(Long.MIN_VALUE, Long.MAX_VALUE);

            // Import SSTables; -l to keeps generated levels
            cluster.get(1).nodetoolResult("import", "-l", schema.keyspace, schema.table, sstableDir.absolutePath())
                   .asserts().success();

            TestOracle model = replay(schema, config, tokenIndex, visitSize, opKindGen);
            tokenIndex.close();

            // Validate
            InJvmDTestVisitExecutor validator = new InJvmDTestVisitExecutor(schema, model.partitions, new DataTracker.NoOpDataTracker(),
                                                                            new QuiescentChecker(model.partitions, model.tracker), cluster,
                                                                            lts -> 1,
                                                                            v -> InJvmDTestVisitExecutor.PageSizeSelector.NO_PAGING,
                                                                            InJvmDTestVisitExecutor.RetryPolicy.NO_RETRY,
                                                                            v -> ConsistencyLevel.NODE_LOCAL,
                                                                            QueryBuildingVisitExecutor.WrapQueries.EMPTY);
            for (long pd : model.generatedPds)
                validator.execute(new Visit(END_LTS, new Operations.Operation[]{ new Operations.SelectPartition(END_LTS, pd, ClusteringOrderBy.ASC) }));
            logger.info("Validated {} partitions", model.generatedPds.size());
        }
    }

    private static TestOracle replay(SchemaSpec schema, StressSchemaConfig config, TokenIndex tokenIndex,
                                     Distribution visitSize, VisitGenerator.OpKindGenFactory opKindGen)
    {
        RotationStrategy rotation = config.rotationStrategy();
        ActivePartition.Partitions partitions = new ActivePartition.Partitions(schema, config.rowPopulation(), config.columnPopulation(),
                                                                               rotation, 0, rotation.targetSize(), INITIAL_LTS);
        partitions.populate();
        DataTracker.SimpleDataTracker modelTracker = new DataTracker.SimpleDataTracker();
        Set<Long> generatedPds = new TreeSet<>();
        TokenIndex.EntryIterator iter = tokenIndex.range(Long.MIN_VALUE, Long.MAX_VALUE);
        while (iter.hasNext())
        {
            long pd = iter.pd();
            generatedPds.add(pd);
            for (long lts : iter.readLts())
            {
                Visit visit = VisitGenerator.mutatingVisit(lts, visitSize, opKindGen, rng -> partitions.forPd(pd));
                modelTracker.begin(visit);
                modelTracker.end(visit);
            }
            iter.advance();
        }
        return new TestOracle(partitions, modelTracker, generatedPds);
    }

    private static final class TestOracle
    {
        final ActivePartition.Partitions partitions;
        final DataTracker.SimpleDataTracker tracker;
        final Set<Long> generatedPds;

        TestOracle(ActivePartition.Partitions partitions, DataTracker.SimpleDataTracker tracker, Set<Long> generatedPds)
        {
            this.partitions = partitions;
            this.tracker = tracker;
            this.generatedPds = generatedPds;
        }
    }
}
