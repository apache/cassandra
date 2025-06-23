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

import org.junit.Test;

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
import org.apache.cassandra.service.consensus.TransactionalMode;

import static org.apache.cassandra.harry.checker.TestHelper.withRandom;

public class IdenticalTopologyTest extends FuzzTestBase
{
    private static final int POPULATION = 1000;

    @Test
    public void identicalTopologyTest() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(1)
                                             .withConfig(cfg -> cfg.set("accord.journal.compaction_period", "600s") // deliberately high
                                                                   .set("max_value_size", "1MiB")) // deliberately low
                                             .start(),
                                    1))
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

                for (int i = 0; i <= 100; i++)
                {
                    cluster.schemaChange(String.format("ALTER TABLE %s.%s WITH comment = '%d';", schema.keyspace, schema.table, i));

                    for (int j = 0; j < 5; j++)
                        history.insert(j);
                    if (i % 20 == 0)
                    {
                        cluster.get(1).runOnInstance(() -> {
                            ((AccordService) AccordService.instance()).journal().closeCurrentSegmentForTestingIfNonEmpty();
                        });
                    }
                }
                cluster.get(1).runOnInstance(() -> {
                    ((AccordService) AccordService.instance()).journal().compactor().run();
                });
                cluster.get(1).forceCompact("system_accord", "journal");
                ClusterUtils.stopUnchecked(cluster.get(1));
                cluster.get(1).startup();
            });
        }
    }
}

