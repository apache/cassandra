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

package org.apache.cassandra.fuzz.sai;


import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.collect.Streams;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.dsl.HistoryBuilderHelper;
import org.apache.cassandra.harry.dsl.ReplayingHistoryBuilder;
import org.apache.cassandra.harry.execution.InJvmDTestVisitExecutor;
import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.harry.gen.SchemaGenerators;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.service.consensus.TransactionalMode;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.harry.checker.TestHelper.withRandom;
import static org.apache.cassandra.harry.dsl.SingleOperationBuilder.IdxRelation;

// TODO: "WITH OPTIONS = {'case_sensitive': 'false', 'normalize': 'true', 'ascii': 'true'};",
public abstract class SingleNodeSAITestBase extends TestBaseImpl
{
    private static final int VALIDATION_SKIP = 739;
    private static final int QUERIES_PER_VALIDATION = 8;

    private static final int FLUSH_SKIP = 2217;
    private static final int COMPACTION_SKIP = 4435;
    private static final int DEFAULT_REPAIR_SKIP = 8869;

    private static final int OPERATIONS_PER_RUN = 30_000;

    private static final int NUM_PARTITIONS = 64;
    private static final int NUM_VISITED_PARTITIONS = 16;
    protected static final int MAX_PARTITION_SIZE = 2000;

    private static final int UNIQUE_CELL_VALUES = 5;

    protected static final Logger logger = LoggerFactory.getLogger(SingleNodeSAITest.class);
    protected static Cluster cluster;

    protected TransactionalMode transactionalMode;

    protected SingleNodeSAITestBase(TransactionalMode transactionalMode)
    {
        this.transactionalMode = transactionalMode;
    }

    protected int iterations()
    {
        return 5;
    }

    @BeforeClass
    public static void before() throws Throwable
    {
        init(1,
             // At lower fetch sizes, queries w/ hundreds or thousands of matches can take a very long time.
             defaultConfig().andThen(c -> c.set("range_request_timeout", "180s")
                                           .set("read_request_timeout", "180s")
                                           .set("write_request_timeout", "180s")
                                           .set("native_transport_timeout", "180s")
                                           .set("slow_query_log_timeout", "180s")
                                           .with(GOSSIP).with(NETWORK))
        );
    }

    protected static void init(int nodes, Consumer<IInstanceConfig> cfg) throws Throwable
    {
        cluster = Cluster.build()
                         .withNodes(nodes)
                         .withConfig(cfg)
                         .createWithoutStarting();
        cluster.startup();
        cluster = init(cluster);
    }
    @AfterClass
    public static void afterClass()
    {
        cluster.close();
    }

    @Before
    public void beforeEach()
    {
        cluster.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
        cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " + rf() + "};");
    }

    protected int rf()
    {
        return 1;
    }

    @Test
    public void simplifiedSaiTest()
    {
        withRandom(rng -> saiTest(rng,
                                  SchemaGenerators.trivialSchema(KEYSPACE, "simplified", 1000).generate(rng),
                                  () -> true,
                                  DEFAULT_REPAIR_SKIP));
    }

    @Test
    public void indexOnlySaiTest()
    {
        for (int i = 0; i < iterations(); i++)
        {
            logger.info("Starting iteration {}...", i);
            withRandom(rng -> saiTest(rng,
                                      schemaGenerator(rng.nextBoolean()).generate(rng),
                                      () -> true,
                                      rng.nextBoolean() ? DEFAULT_REPAIR_SKIP : Integer.MAX_VALUE));
        }
    }

    @Test
    public void mixedFilteringSaiTest()
    {
        for (int i = 0; i < iterations(); i++)
        {
            logger.info("Starting iteration {}...", i);
            withRandom(rng -> saiTest(rng,
                                      schemaGenerator(rng.nextBoolean()).generate(rng),
                                      () -> rng.nextFloat() < 0.7f,
                                      rng.nextBoolean() ? DEFAULT_REPAIR_SKIP : Integer.MAX_VALUE));
        }
    }

    private void saiTest(EntropySource rng, SchemaSpec schema, Supplier<Boolean> createIndex, int repairSkip)
    {
        logger.info(schema.compile());

        Generator<Integer> globalPkGen = Generators.int32(0, Math.min(NUM_PARTITIONS, schema.valueGenerators.pkPopulation()));
        Generator<Integer> ckGen = Generators.int32(0, schema.valueGenerators.ckPopulation());

        beforeEach();
        cluster.forEach(i -> i.nodetool("disableautocompaction"));

        cluster.schemaChange(schema.compile());
        cluster.schemaChange(schema.compile().replace(schema.keyspace + '.' + schema.table, schema.keyspace + ".debug_table"));

        try
        {
            AtomicInteger indexCount = new AtomicInteger();

            Streams.concat(schema.clusteringKeys.stream(), schema.regularColumns.stream(), schema.staticColumns.stream())
                   .forEach(column -> {
                       if (createIndex.get())
                       {
                           logger.info("Adding index to column {}...", column.name);
                           cluster.schemaChange(String.format("CREATE INDEX %s_sai_idx ON %s.%s (%s) USING 'sai' ",
                                                              column.name, schema.keyspace, schema.table, column.name));
                           indexCount.incrementAndGet();
                       }
                       else
                       {
                           logger.info("Leaving column {} unindexed...", column.name);
                       }
                   });

            CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.setInt(indexCount.get());
            waitForIndexesQueryable(schema);

            HistoryBuilder history = new ReplayingHistoryBuilder(schema.valueGenerators,
                                                                 (hb) -> InJvmDTestVisitExecutor.builder()
                                                                                                .pageSizeSelector(pageSizeSelector(rng))
                                                                                                .consistencyLevel(consistencyLevelSelector())
                                                                                                .doubleWriting(schema, hb, cluster, "debug_table"));
            Set<Integer> partitions = new HashSet<>();
            int attempts = 0;
            while (partitions.size() < NUM_VISITED_PARTITIONS && attempts < NUM_VISITED_PARTITIONS * 10)
            {
                partitions.add(globalPkGen.generate(rng));
                attempts++;
            }

            if (partitions.size() < NUM_VISITED_PARTITIONS)
                logger.warn("Unable to generate {} partitions to visit. Continuing with {}...", NUM_VISITED_PARTITIONS, partitions.size());

            Generator<Integer> pkGen = Generators.pick(List.copyOf(partitions));

            // Ensure that we don't attempt to use range queries against SAI indexes that don't support them:
            Set<Integer> eqOnlyRegularColumns = new HashSet<>();
            for (int i = 0; i < schema.regularColumns.size(); i++)
                if (IndexTermType.isEqOnlyType(schema.regularColumns.get(i).type.asServerType()))
                    eqOnlyRegularColumns.add(i);

            Set<Integer> eqOnlyStaticColumns = new HashSet<>();
            for (int i = 0; i < schema.staticColumns.size(); i++)
                if (IndexTermType.isEqOnlyType(schema.staticColumns.get(i).type.asServerType()))
                    eqOnlyStaticColumns.add(i);

            Set<Integer> eqOnlyClusteringColumns = new HashSet<>();
            for (int i = 0; i < schema.clusteringKeys.size(); i++)
                if (IndexTermType.isEqOnlyType(schema.clusteringKeys.get(i).type.asServerType()))
                    eqOnlyClusteringColumns.add(i);

            for (int i = 0; i < OPERATIONS_PER_RUN; i++)
            {
                int partitionIndex = pkGen.generate(rng);
                HistoryBuilderHelper.insertRandomData(schema, partitionIndex, ckGen.generate(rng), rng, 0.5d, history);

                if (rng.nextFloat() > 0.99f)
                {
                    int row1 = ckGen.generate(rng);
                    int row2 = ckGen.generate(rng);
                    history.deleteRowRange(partitionIndex,
                                           Math.min(row1, row2),
                                           Math.max(row1, row2),
                                           rng.nextInt(schema.clusteringKeys.size()),
                                           rng.nextBoolean(),
                                           rng.nextBoolean());
                }

                if (rng.nextFloat() > 0.995f)
                    HistoryBuilderHelper.deleteRandomColumns(schema, partitionIndex, ckGen.generate(rng), rng, history);

                if (rng.nextFloat() > 0.9995f)
                    history.deletePartition(partitionIndex);

                if (i % FLUSH_SKIP == 0)
                    history.custom(() -> flush(schema), "Flush");
                else if (i % COMPACTION_SKIP == 0)
                    history.custom(() -> compact(schema), "Compact");
                else if (i % repairSkip == 0)
                    history.custom(() -> repair(schema), "Repair");

                if (i > 0 && i % VALIDATION_SKIP == 0)
                {
                    for (int j = 0; j < QUERIES_PER_VALIDATION; j++)
                    {
                        List<IdxRelation> regularRelations =
                        HistoryBuilderHelper.generateValueRelations(rng,
                                                                    schema.regularColumns.size(),
                                                                    column -> Math.min(schema.valueGenerators.regularPopulation(column), UNIQUE_CELL_VALUES),
                                                                    eqOnlyRegularColumns::contains);

                        List<IdxRelation> staticRelations =
                        HistoryBuilderHelper.generateValueRelations(rng,
                                                                    schema.staticColumns.size(),
                                                                    column -> Math.min(schema.valueGenerators.staticPopulation(column), UNIQUE_CELL_VALUES),
                                                                    eqOnlyStaticColumns::contains);

                        Integer pk = pkGen.generate(rng);

                        IdxRelation[] ckRelations =
                        HistoryBuilderHelper.generateClusteringRelations(rng,
                                                                         schema.clusteringKeys.size(),
                                                                         ckGen,
                                                                         eqOnlyClusteringColumns).toArray(new IdxRelation[0]);

                        IdxRelation[] regularRelationsArray = regularRelations.toArray(new IdxRelation[regularRelations.size()]);
                        IdxRelation[] staticRelationsArray = staticRelations.toArray(new IdxRelation[staticRelations.size()]);

                        history.select(pk, ckRelations, regularRelationsArray, staticRelationsArray);
                    }
                }
            }
        }
        finally
        {
            // Any Accord tables must be dropped before the keyspace is dropped:
            cluster.schemaChange("DROP TABLE IF EXISTS " + KEYSPACE + '.' + schema.table);
            cluster.schemaChange("DROP TABLE IF EXISTS " + KEYSPACE + '.' + "debug_table");
        }
    }

    protected Generator<SchemaSpec> schemaGenerator(boolean disableReadRepair)
    {
        SchemaSpec.OptionsBuilder builder = SchemaSpec.optionsBuilder().disableReadRepair(disableReadRepair)
                                                                       .compactionStrategy("LeveledCompactionStrategy");
        return SchemaGenerators.schemaSpecGen(KEYSPACE, "basic_sai", MAX_PARTITION_SIZE, builder);
    }

    protected void flush(SchemaSpec schema)
    {
        cluster.get(1).nodetool("flush", schema.keyspace, schema.table);
    }

    protected void compact(SchemaSpec schema)
    {
        cluster.get(1).nodetool("compact", schema.keyspace);
    }

    protected void repair(SchemaSpec schema)
    {
        // Repair is nonsensical for a single node, but a repair does flush first, so do that at least.
        cluster.get(1).nodetool("flush", schema.keyspace, schema.table);
    }

    protected void waitForIndexesQueryable(SchemaSpec schema) {}

    public static Consumer<IInstanceConfig> defaultConfig()
    {
        return (cfg) -> {
            cfg.set("row_cache_size", "50MiB")
               .set("index_summary_capacity", "50MiB")
               .set("counter_cache_size", "50MiB")
               .set("key_cache_size", "50MiB")
               .set("file_cache_size", "50MiB")
               .set("index_summary_capacity", "50MiB")
               .set("memtable_heap_space", "128MiB")
               .set("memtable_offheap_space", "128MiB")
               .set("memtable_flush_writers", 1)
               .set("concurrent_compactors", 1)
               .set("concurrent_reads", 5)
               .set("concurrent_writes", 5)
               .set("compaction_throughput_mb_per_sec", 10)
               .set("hinted_handoff_enabled", false);
        };
    }

    protected InJvmDTestVisitExecutor.ConsistencyLevelSelector consistencyLevelSelector()
    {
        return visit -> {
            if (visit.selectOnly)
                return ConsistencyLevel.ALL;

            // The goal here is to make replicas as out of date as possible, modulo the efforts of repair
            // and read-repair in the test itself. node_local bypasses Accord which breaks any attempt at testing Accord
            // so if we are running with Accord use QUORUM (which Accord will ignore since it runs with transactional
            // mode full).
            if (transactionalMode != null && transactionalMode.nonSerialWritesThroughAccord)
                return ConsistencyLevel.QUORUM;
            else
                return ConsistencyLevel.NODE_LOCAL;

        };
    }

    protected InJvmDTestVisitExecutor.PageSizeSelector pageSizeSelector(EntropySource rng)
    {
        // Chosing a fetch size has implications for how well this test will excercise paging, short-read protection, and
        // other important parts of the distributed query apparatus. This should be set low enough to ensure a significant
        // number of queries during validation page, but not too low that more expesive queries time out and fail the test.
        return lts -> rng.nextInt(1, 20);
    }
}