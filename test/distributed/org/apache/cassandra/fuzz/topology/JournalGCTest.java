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

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.log.FuzzTestBase;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.dsl.ReplayingHistoryBuilder;
import org.apache.cassandra.harry.execution.InJvmDTestVisitExecutor;
import org.apache.cassandra.harry.execution.QueryBuildingVisitExecutor;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.SchemaGenerators;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.JournalKey;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.cassandra.harry.checker.TestHelper.withRandom;

public class JournalGCTest extends FuzzTestBase
{
    private static final int POPULATION = 1000;

    @Test
    public void journalGCTest() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(1)
                                            .withConfig(cfg -> cfg.set("accord.gc_delay", "1s")
                                                    .set("accord.shard_durability_target_splits", "1")
                                                    .set("accord.shard_durability_cycle", "1s")
                                                    .set("accord.global_durability_cycle", "1s"))
                                            .start()))
        {
            withRandom(rng -> {
                cluster.get(1).runOnInstance(() -> {
                    Keyspace.open(SchemaConstants.ACCORD_KEYSPACE_NAME).getColumnFamilyStore(AccordKeyspace.JOURNAL).disableAutoCompaction();
                });

                Generator<SchemaSpec> schemaGen = SchemaGenerators.trivialSchema(KEYSPACE, () -> "bootstrap_fuzz", POPULATION,
                                                                                 SchemaSpec.optionsBuilder()
                                                                                         .addWriteTimestamps(false)
                                                                                         .withTransactionalMode(TransactionalMode.full));

                SchemaSpec schema = schemaGen.generate(rng);
                cluster.schemaChange(schema.compile());
                HistoryBuilder history = new ReplayingHistoryBuilder(schema.valueGenerators,
                                                                     hb -> InJvmDTestVisitExecutor.builder()
                                                                             .consistencyLevel(ConsistencyLevel.QUORUM)
                                                                             .wrapQueries(QueryBuildingVisitExecutor.WrapQueries.TRANSACTION)
                                                                             .pageSizeSelector(p -> InJvmDTestVisitExecutor.PageSizeSelector.NO_PAGING)
                                                                             .build(schema, hb, cluster));

                for (int pk = 0; pk < 500; pk++) {
                    for (int i = 0; i < 500; i++)
                        history.insert(pk);
                }

                cluster.get(1).runOnInstance(() -> {
                    ((AccordService) AccordService.instance()).journal().closeCurrentSegmentForTestingIfNonEmpty();
                    ((AccordService) AccordService.instance()).journal().compactor().run();
                });

                int before = cluster.get(1).callOnInstance(() -> {
                    AtomicInteger a = new AtomicInteger();
                    ((AccordService) AccordService.instance()).journal().forEach((v) -> {
                        if (v.type == JournalKey.Type.COMMAND_DIFF)
                            a.incrementAndGet();
                    });
                    return a.get();
                });

                Thread.sleep(10_000);
                cluster.get(1).runOnInstance(() -> {
                    Keyspace.open(SchemaConstants.ACCORD_KEYSPACE_NAME).getColumnFamilyStore(AccordKeyspace.JOURNAL).forceMajorCompaction();
                });

                cluster.get(1).forceCompact("system_accord", "journal");

                int after = cluster.get(1).callOnInstance(() -> {
                    AtomicInteger a = new AtomicInteger();
                    ((AccordService) AccordService.instance()).journal().forEach((v) -> {
                        if (v.type == JournalKey.Type.COMMAND_DIFF)
                            a.incrementAndGet();
                    });
                    return a.get();
                });
                Assert.assertTrue(String.format("%s should have been strictly smaller than %s", after, before), before > after);
                Assert.assertEquals(0, after);
            });
        }
    }
}

