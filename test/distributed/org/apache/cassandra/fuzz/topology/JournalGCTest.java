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

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;

import accord.primitives.TxnId;
import org.apache.cassandra.db.Keyspace;
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
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.JournalKey;
import org.apache.cassandra.service.consensus.TransactionalMode;

import static org.apache.cassandra.db.ColumnFamilyStore.FlushReason.UNIT_TESTS;
import static org.apache.cassandra.harry.checker.TestHelper.withRandom;

public class JournalGCTest extends FuzzTestBase
{
    private static final int POPULATION = 1000;

    @Test
    public void journalGCTest() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(1)
                                            .withConfig(cfg -> cfg.set("write_request_timeout", "2s")
                                                                  .set("accord.expire_syncpoint", "1s*attempts<=300s")
                                                                  .set("accord.retry_syncpoint", "1s*attempts")
                                                                  .set("accord.shard_durability_target_splits", "5")
                                                                  .set("accord.shard_durability_max_splits", "10")
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

                for (int pk = 0; pk <= 500; pk++) {
                    for (int i = 0; i < 100; i++)
                        history.insert(pk);

                    if (pk > 0 && pk % 100 == 0)
                    {
                        cluster.get(1).runOnInstance(() -> {
                            ((AccordService) AccordService.instance()).journal().closeCurrentSegmentForTestingIfNonEmpty();
                            ((AccordService) AccordService.instance()).journal().compactor().run();
                        });
                    }

                    if (pk > 0 && pk % 200 == 0)
                    {
                        ClusterUtils.stopUnchecked(cluster.get(1));
                        cluster.get(1).startup();
                    }
                }

                cluster.get(1).runOnInstance(() -> {
                    ((AccordService) AccordService.instance()).journal().closeCurrentSegmentForTestingIfNonEmpty();
                    ((AccordService) AccordService.instance()).journal().compactor().run();
                });

                String maximumId = cluster.get(1).callOnInstance(() -> {
                    AtomicReference<TxnId> a = new AtomicReference<>();
                    ((AccordService) AccordService.instance()).journal().forEach((v) -> {
                        if (v.type == JournalKey.Type.COMMAND_DIFF && (a.get() == null || v.id.compareTo(a.get()) > 0))
                            a.set(v.id);
                    });
                    return a.get() == null ? "" : a.get().toString();
                });

                Callable<Integer> countDiffs = () -> cluster.get(1).applyOnInstance(maxIdStr -> {
                    AtomicInteger a = new AtomicInteger();
                    TxnId maxId = TxnId.parse(maxIdStr);
                    ((AccordService) AccordService.instance()).journal().forEach((v) -> {
                        if (v.type == JournalKey.Type.COMMAND_DIFF && v.id.compareTo(maxId) <= 0)
                            a.incrementAndGet();
                    });
                    return a.get();
                }, maximumId);

                int after =-1;
                int maxCycles = 10;
                for (int i = 0; i < maxCycles; i++)
                {
                    cluster.get(1).acceptOnInstance((ks, tbl) -> {
                        Keyspace.open(ks).getColumnFamilyStore(tbl).forceBlockingFlush(UNIT_TESTS);
                        Keyspace.open(SchemaConstants.ACCORD_KEYSPACE_NAME).getColumnFamilyStore(AccordKeyspace.COMMANDS_FOR_KEY).forceBlockingFlush(UNIT_TESTS);
                        Keyspace.open(SchemaConstants.ACCORD_KEYSPACE_NAME).getColumnFamilyStore(AccordKeyspace.JOURNAL).forceMajorCompaction();
                    }, schema.keyspace, schema.table);
                    after = countDiffs.call();
                    if (after == 0)
                        return;
                    Thread.sleep(10000);
                }
                Assert.fail("Should have GC'd all in (way under) " + maxCycles + " cycles. Remaining: " + after);
            });
        }
    }
}