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
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import accord.utils.Gens;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.harry.HarryHelper;
import org.apache.cassandra.harry.ddl.SchemaSpec;
import org.apache.cassandra.harry.dsl.ReplayingHistoryBuilder;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.harry.sut.SystemUnderTest;
import org.apache.cassandra.harry.sut.TokenPlacementModel;
import org.apache.cassandra.harry.sut.injvm.InJvmSut;
import org.apache.cassandra.harry.tracker.DefaultDataTracker;
import org.apache.cassandra.service.consensus.TransactionalMode;

import static accord.utils.Property.qt;
import static java.lang.String.format;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopUnchecked;
import static org.apache.cassandra.distributed.shared.ClusterUtils.waitForCMSToQuiesce;

public class AccordHostReplacementTest extends TestBaseImpl
{
    private static final List<TransactionalMode> TRANSACTIONAL_MODES = Stream.of(TransactionalMode.values()).filter(t -> t.accordIsEnabled).collect(Collectors.toList());

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

            qt().withExamples(1).check(rs -> {
                SchemaSpec schema = HarryHelper.schemaSpecBuilder(HarryHelper.KEYSPACE, "tbl")
                                               .transactionalMode(Generators.toHarry(Gens.pick(TRANSACTIONAL_MODES).map(Optional::of)))
                                               .surjection()
                                               .inflate(rs.nextLong());
                ReplayingHistoryBuilder harry = new ReplayingHistoryBuilder(rs.nextLong(), 100, 1, new DefaultDataTracker(), new InJvmSut(cluster), schema, new TokenPlacementModel.SimpleReplicationFactor(3), SystemUnderTest.ConsistencyLevel.ALL);
                cluster.schemaChange(format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};", HarryHelper.KEYSPACE));
                cluster.schemaChange(schema.compile().cql());
                waitForCMSToQuiesce(cluster, cluster.get(1));

                for (int i = 0; i < 1000; i++)
                    harry.insert();
                harry.validateAll(harry.quiescentLocalChecker());

                stopUnchecked(cluster.get(nodeToReplace));
                ClusterUtils.replaceHostAndStart(cluster, cluster.get(nodeToReplace));

                harry.validateAll(harry.quiescentLocalChecker());
            });
        }
    }
}
