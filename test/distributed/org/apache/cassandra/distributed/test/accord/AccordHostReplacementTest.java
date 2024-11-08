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

package org.apache.cassandra.distributed.test.accord;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.dsl.HistoryBuilder;
import org.apache.cassandra.harry.dsl.ReplayingHistoryBuilder;
import org.apache.cassandra.harry.execution.RingAwareInJvmDTestVisitExecutor;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.harry.gen.SchemaGenerators;
import org.apache.cassandra.harry.model.TokenPlacementModel;
import org.apache.cassandra.service.consensus.TransactionalMode;

import static org.apache.cassandra.distributed.shared.ClusterUtils.stopUnchecked;
import static org.apache.cassandra.distributed.shared.ClusterUtils.waitForCMSToQuiesce;
import static org.apache.cassandra.harry.checker.TestHelper.withRandom;

public class AccordHostReplacementTest extends TestBaseImpl
{
    private static final Generator<TransactionalMode> transactionalModeGen = Generators.pick(Stream.of(TransactionalMode.values()).filter(t -> t.accordIsEnabled).collect(Collectors.toList()));

    @Test
    public void hostReplace() throws IOException
    {
        // start 3 node cluster, then do a host replacement of one of the nodes
        Cluster.Builder clusterBuilder = Cluster.build(3)
                                                .withConfig(c -> c.with(Feature.values())
                                                                  .set("accord.command_store_shard_count", "1")
                                                                  .set("accord.queue_shard_count", "1")
                                                );
        TokenSupplier tokenRing = TokenSupplier.evenlyDistributedTokens(3, clusterBuilder.getTokenCount());
        int nodeToReplace = 2;
        clusterBuilder = clusterBuilder.withTokenSupplier((TokenSupplier) node -> tokenRing.tokens(node == 4 ? nodeToReplace : node));
        try (Cluster cluster = clusterBuilder.start())
        {
            fixDistributedSchemas(cluster);
            init(cluster);

            withRandom(rng -> {
                Generator<SchemaSpec> schemaGen = SchemaGenerators.schemaSpecGen(KEYSPACE, "host_replace", 1000,
                                                                                 SchemaSpec.optionsBuilder().withTransactionalMode(transactionalModeGen.generate(rng)));
                SchemaSpec schema = schemaGen.generate(rng);
                Generators.TrackingGenerator<Integer> pkGen = Generators.tracking(Generators.int32(0, Math.min(schema.valueGenerators.pkPopulation(), 1000)));

                HistoryBuilder history = historyBuilder(schema, cluster);
                waitForCMSToQuiesce(cluster, cluster.get(1));

                for (int i = 0; i < 1000; i++)
                    history.insert(pkGen.generate(rng));
                for (int pk : pkGen.generated())
                    history.selectPartition(pk);

                history.custom(() -> {
                    stopUnchecked(cluster.get(nodeToReplace));
                    ClusterUtils.replaceHostAndStart(cluster, cluster.get(nodeToReplace));
                }, "Replace");

                for (int pk : pkGen.generated())
                    history.selectPartition(pk);
            });
        }
    }

    private static HistoryBuilder historyBuilder(SchemaSpec schema, Cluster cluster)
    {
        HistoryBuilder history = new ReplayingHistoryBuilder(schema.valueGenerators,
                                                             hb -> RingAwareInJvmDTestVisitExecutor.builder()
                                                                                                   .replicationFactor(new TokenPlacementModel.SimpleReplicationFactor(3))
                                                                                                   .consistencyLevel(ConsistencyLevel.ALL)
                                                                                                   .build(schema, hb, cluster));
        history.customThrowing(() -> cluster.schemaChange(schema.compile()), "Setup");
        return history;
    }
}