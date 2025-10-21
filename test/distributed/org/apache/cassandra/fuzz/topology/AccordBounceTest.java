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

package org.apache.cassandra.fuzz.topology;

import accord.primitives.Range;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.log.FuzzTestBase;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.dsl.ReplayingHistoryBuilder;
import org.apache.cassandra.harry.execution.InJvmDTestVisitExecutor;
import org.apache.cassandra.harry.execution.QueryBuildingVisitExecutor;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.SchemaGenerators;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.junit.Test;

import java.util.*;
import java.util.function.Supplier;

import static org.apache.cassandra.harry.checker.TestHelper.withRandom;

public class AccordBounceTest extends FuzzTestBase
{
    private static final int WRITES = 10;
    private static final int POPULATION = 1000;

    // Test bounce in presence of unwritten allocation.
    @Test
    public void emptyJournalAllocationBounceTest() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(1).start()))
        {
            withRandom(rng -> {
                Generator<SchemaSpec> schemaGen = SchemaGenerators.trivialSchema(KEYSPACE, new Supplier<String>()
                                                                                 {
                                                                                     int i = 0;

                                                                                     @Override
                                                                                     public String get()
                                                                                     {
                                                                                         return "bootstrap_fuzz" + (i++);
                                                                                     }
                                                                                 }, POPULATION,
                                                                                 SchemaSpec.optionsBuilder()
                                                                                         .addWriteTimestamps(false)
                                                                                         .withTransactionalMode(TransactionalMode.full)
                );

                List<HistoryBuilder> historyBuilders = new ArrayList<>();
                for (int i = 0; i < 10; i++)
                {
                    SchemaSpec schema = schemaGen.generate(rng);
                    cluster.schemaChange(schema.compile());
                    historyBuilders.add(new ReplayingHistoryBuilder(schema.valueGenerators,
                                                                    hb -> InJvmDTestVisitExecutor.builder()
                                                                            .consistencyLevel(ConsistencyLevel.QUORUM)
                                                                            .wrapQueries(QueryBuildingVisitExecutor.WrapQueries.TRANSACTION)
                                                                            .pageSizeSelector(p -> InJvmDTestVisitExecutor.PageSizeSelector.NO_PAGING)
                                                                            .build(schema, hb, cluster)));
                }

                for (HistoryBuilder hb : historyBuilders)
                    for (int pk = 0; pk < 5; pk++)
                    {
                        for (int i = 0; i < 5; i++)
                            hb.insert(pk);
                        cluster.get(1).runOnInstance(() -> {
                            AccordService accordService = (AccordService) AccordService.instance();
                            accordService.journal().unsafeGetJournal().unsafeConsumeBytesForTesting(200, bb -> {});
                        });
                    }

                for (HistoryBuilder hb : historyBuilders)
                    for (int pk = 0; pk < 5; pk++)
                        hb.selectPartition(pk);

                ClusterUtils.stopUnchecked(cluster.get(1));
                cluster.get(1).startup();

                for (HistoryBuilder hb : historyBuilders)
                    for (int pk = 0; pk < 5; pk++)
                        hb.selectPartition(pk);
            });
        }
    }


    @Test
    public void commandStoresBounceTest() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(1).start()))
        {
            withRandom(rng -> {
                Generator<SchemaSpec> schemaGen = SchemaGenerators.trivialSchema(KEYSPACE, new Supplier<String>() {
                                                                                     int i = 0;
                                                                                     @Override
                                                                                     public String get()
                                                                                     {
                                                                                         return  "bootstrap_fuzz" + (i++);
                                                                                     }
                                                                                 }, POPULATION,
                                                                                 SchemaSpec.optionsBuilder()
                                                                                         .addWriteTimestamps(false)
                                                                                         .withTransactionalMode(TransactionalMode.full)
                );

                List<HistoryBuilder> historyBuilders = new ArrayList<>();
                for (int i = 0; i < 10; i++)
                {
                    SchemaSpec schema = schemaGen.generate(rng);
                    cluster.schemaChange(schema.compile());
                    historyBuilders.add(new ReplayingHistoryBuilder(schema.valueGenerators,
                                                                    hb -> InJvmDTestVisitExecutor.builder()
                                                                            .consistencyLevel(ConsistencyLevel.QUORUM)
                                                                            .wrapQueries(QueryBuildingVisitExecutor.WrapQueries.TRANSACTION)
                                                                            .pageSizeSelector(p -> InJvmDTestVisitExecutor.PageSizeSelector.NO_PAGING)
                                                                            .build(schema, hb, cluster)));
                }

                Runnable writeAndValidate = () -> {
                    for (HistoryBuilder hb : historyBuilders)
                        for (int pk = 0; pk < 10; pk++)
                            for (int i = 0; i < 10; i++)
                                hb.insert(pk);
                    for (HistoryBuilder hb : historyBuilders)
                        for (int pk = 0; pk < 10; pk++)
                            hb.selectPartition(pk);
                };

                // Command Stores should not be lost on bounce
                Map<Integer, Set<String>> before = cluster.get(1).callOnInstance(() -> {
                    Map<Integer, Set<String>> m = new HashMap<>();
                    AccordService.instance().node().commandStores().forEach((store, ranges) -> {
                        Set<String> set = new HashSet<>();
                        for (Range range : ranges.all())
                            set.add(range.toString());
                        m.put(store.id(), set);
                    });
                    return m;
                });
                for (int i = 0; i < 5; i++)
                {
                    writeAndValidate.run();
                    ClusterUtils.stopUnchecked(cluster.get(1));
                    cluster.get(1).startup();

                    SchemaSpec schema = schemaGen.generate(rng);
                    cluster.schemaChange(schema.compile());

                    Map<Integer, Set<String>> after = cluster.get(1).callOnInstance(() -> {
                        Map<Integer, Set<String>> m = new HashMap<>();
                        AccordService.instance().node().commandStores().forEach((store, ranges) -> {
                            Set<String> set = new HashSet<>();
                            for (Range range : ranges.all())
                                set.add(range.toString());
                            m.put(store.id(), set);
                        });
                        return m;
                    });
                    if (!before.equals(after))
                    {
                        for (Integer k : before.keySet())
                        {
                            if (!after.containsKey(k))
                                throw new AssertionError(String.format("%d is contained only in before set with %s", k, before.get(k)));

                            for (String s : before.get(k))
                            {
                                if (!after.get(k).contains(s))
                                    throw new AssertionError(String.format("%d is contained in before set with %s but in after set with %s", k, before.get(k), after.get(k)));
                            }
                        }
                    }
                    before = after;
                }
            });
        }
    }
}

