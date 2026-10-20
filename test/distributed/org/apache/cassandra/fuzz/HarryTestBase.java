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

package org.apache.cassandra.fuzz;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.harry.ColumnSpec;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.dsl.HistoryBuilderHelper;
import org.apache.cassandra.harry.dsl.ReplayingHistoryBuilder;
import org.apache.cassandra.harry.execution.InJvmDTestVisitExecutor;
import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.harry.gen.SchemaGenerators;
import org.apache.cassandra.service.consensus.TransactionalMode;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.harry.dsl.SingleOperationBuilder.IdxRelation;

/**
 * Generates mutation histories and validates paged reads against Harry's model.
 */
public abstract class HarryTestBase extends TestBaseImpl
{
    private static final int VALIDATION_SKIP = 739;
    private static final int QUERIES_PER_VALIDATION = 8;

    private static final int FLUSH_SKIP = 2217;
    private static final int COMPACTION_SKIP = 4435;
    protected static final int DEFAULT_REPAIR_SKIP = 8869;

    private static final int OPERATIONS_PER_RUN = 30_000;

    private static final int NUM_PARTITIONS = 64;
    private static final int NUM_VISITED_PARTITIONS = 16;
    protected static final int MAX_PARTITION_SIZE = 2000;

    private static final int UNIQUE_CELL_VALUES = 5;

    protected static final Logger logger = LoggerFactory.getLogger(HarryTestBase.class);
    protected static Cluster cluster;

    protected TransactionalMode transactionalMode;

    protected HarryTestBase(TransactionalMode transactionalMode)
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
        init(1);
    }

    protected static void init(int nodes) throws Throwable
    {
        // Small pages can make queries with thousands of matches expensive.
        init(nodes, defaultConfig().andThen(c -> c.set("range_request_timeout", "180s")
                                                 .set("read_request_timeout", "180s")
                                                 .set("write_request_timeout", "180s")
                                                 .set("native_transport_timeout", "180s")
                                                 .set("slow_query_log_timeout", "180s")
                                                 .with(GOSSIP).with(NETWORK)));
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
        return cluster.size();
    }

    protected void runHistory(EntropySource rng, SchemaSpec schema, Consumer<SchemaSpec> prepareSchema, int repairSkip)
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
            prepareSchema.accept(schema);

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

            // Restrict generated relations when a subclass cannot execute range queries for a type.
            Set<Integer> eqOnlyRegularColumns = new HashSet<>();
            for (int i = 0; i < schema.regularColumns.size(); i++)
                if (isEqualityOnly(schema.regularColumns.get(i)))
                    eqOnlyRegularColumns.add(i);

            Set<Integer> eqOnlyStaticColumns = new HashSet<>();
            for (int i = 0; i < schema.staticColumns.size(); i++)
                if (isEqualityOnly(schema.staticColumns.get(i)))
                    eqOnlyStaticColumns.add(i);

            Set<Integer> eqOnlyClusteringColumns = new HashSet<>();
            for (int i = 0; i < schema.clusteringKeys.size(); i++)
                if (isEqualityOnly(schema.clusteringKeys.get(i)))
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

                        if (!supportsFilteringIntersections())
                        {
                            // CASSANDRA-19007: non-indexed intersections can lose matches split across replicas.
                            staticRelations.clear();
                            if (!regularRelations.isEmpty())
                            {
                                int column = regularRelations.get(rng.nextInt(regularRelations.size())).column;
                                regularRelations.removeIf(relation -> relation.column != column);
                            }
                        }

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
        return SchemaGenerators.schemaSpecGen(KEYSPACE, "basic_harry", MAX_PARTITION_SIZE, builder);
    }

    protected void flush(SchemaSpec schema)
    {
        if (cluster.size() == 1)
            cluster.get(1).nodetool("flush", schema.keyspace, schema.table);
        else
            cluster.forEach(i -> i.nodetool("flush", schema.keyspace));
    }

    protected void compact(SchemaSpec schema)
    {
        cluster.forEach(i -> i.nodetool("compact", schema.keyspace));
    }

    protected void repair(SchemaSpec schema)
    {
        if (cluster.size() == 1)
            flush(schema);
        else
            cluster.get(1).nodetool("repair", schema.keyspace);
    }

    protected boolean isEqualityOnly(ColumnSpec<?> column)
    {
        return false;
    }

    protected boolean supportsFilteringIntersections()
    {
        return false;
    }

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
