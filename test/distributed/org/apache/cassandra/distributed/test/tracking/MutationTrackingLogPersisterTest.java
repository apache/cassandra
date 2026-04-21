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

package org.apache.cassandra.distributed.test.tracking;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.log.FuzzTestBase;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.dsl.ReplayingHistoryBuilder;
import org.apache.cassandra.harry.execution.InJvmDTestVisitExecutor;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.SchemaGenerators;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.replication.MutationTrackingService;

import static org.apache.cassandra.harry.checker.TestHelper.withRandom;

public class MutationTrackingLogPersisterTest extends FuzzTestBase
{
    private static final int POPULATION = 1000;

    @Test
    public void testLogPersisterClearsStaticSegments() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(3)
                                        .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                              .with(Feature.GOSSIP))
                                        .start())
        {
            int tables = 3;
            int writesPerKey = 10;
            int pks = 100;

            withRandom(rng -> {
                cluster.schemaChange(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                                   "AND replication_type='tracked'",
                                                   KEYSPACE));

                List<HistoryBuilder> builders = new ArrayList<>();
                for (int i = 0; i < tables; i++)
                {
                    Generator<SchemaSpec> schemaGen = SchemaGenerators.trivialSchema(KEYSPACE, () -> "log_persister_test_" + (builders.size() + 1), POPULATION,
                                                                                     SchemaSpec.optionsBuilder());

                    SchemaSpec schema = schemaGen.generate(rng);
                    cluster.schemaChange(schema.compile());
                    builders.add(new ReplayingHistoryBuilder(schema.valueGenerators,
                                                             hb -> InJvmDTestVisitExecutor.builder()
                                                                                          .consistencyLevel(ConsistencyLevel.QUORUM)
                                                                                          .build(schema, hb, cluster)));
                }

                int counter = 0;
                for (int pk = 0; pk < pks; pk++)
                {
                    for (HistoryBuilder history : builders)
                        for (int i = 0; i < writesPerKey; i++)
                            history.insert(pk);

                    if (++counter % 10 == 0)
                        cluster.get(1).runOnInstance(() -> MutationJournal.instance().closeCurrentSegmentForTestingIfNonEmpty());
                }

                cluster.forEach(i -> i.nodetoolResult("flush", KEYSPACE).asserts().success());
                cluster.forEach(i -> i.runOnInstance(() -> MutationTrackingService.instance().persistLogStateForTesting()));
                cluster.forEach(i -> i.runOnInstance(() -> MutationTrackingService.instance().broadcastOffsetsForTesting()));
                cluster.forEach(i -> i.runOnInstance(() -> MutationTrackingService.instance().persistLogStateForTesting()));

                cluster.forEach(i -> i.runOnInstance(() -> {
                    int staticSegments = MutationJournal.instance().countStaticSegmentsForTesting();
                    Assert.assertEquals("Expected no static segments after log persister runs", 0, staticSegments);
                }));
            });
        }
    }
}
