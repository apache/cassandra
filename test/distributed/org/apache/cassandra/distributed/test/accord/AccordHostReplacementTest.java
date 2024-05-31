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
import java.nio.ByteBuffer;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.utils.Gen;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.utils.CassandraGenerators;
import org.apache.cassandra.utils.CassandraGenerators.TableMetadataBuilder;

import static accord.utils.Property.qt;
import static org.apache.cassandra.distributed.shared.ClusterUtils.stopUnchecked;
import static org.apache.cassandra.utils.AccordGenerators.fromQT;
import static org.apache.cassandra.utils.CassandraGenerators.insertCqlAllColumns;

public class AccordHostReplacementTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(AccordHostReplacementTest.class);

    @Test
    public void hostReplace() throws IOException
    {
        // start 2 node cluster and bootstrap 3rd node
        // do a host replacement of one of the nodes
        Cluster.Builder clusterBuilder = Cluster.build(3)
                                                .withConfig(c -> c.with(Feature.values())
                                                                  .set("accord.shard_count", "1"));
        TokenSupplier tokenRing = TokenSupplier.evenlyDistributedTokens(3, clusterBuilder.getTokenCount());
        int nodeToReplace = 2;
        clusterBuilder = clusterBuilder.withTokenSupplier((TokenSupplier) node -> tokenRing.tokens(node == 4 ? nodeToReplace : node));
        try (Cluster cluster = clusterBuilder.start())
        {
            fixDistributedSchemas(cluster);
            init(cluster);

            qt().withSeed(5215087322357346205L).withExamples(1).check(rs -> {
                TableMetadata metadata = fromQT(new TableMetadataBuilder()
                                                .withKeyspaceName(KEYSPACE)
                                                .withTableKinds(TableMetadata.Kind.REGULAR)
                                                .withKnownMemtables()
                                                //TODO (coverage): include "fast_path = 'keyspace'" override
                                                .withTransactionalMode(TransactionalMode.full)
                                                .build())
                                         .next(rs);
                maybeCreateUDTs(cluster, metadata);
                cluster.schemaChange(metadata.toCqlString(false, false));

                Gen<ByteBuffer[]> dataGen = fromQT(fromQT(new CassandraGenerators.DataGeneratorBuilder(metadata).build(ignore -> 10)).next(rs));
                String insertStmt = wrapInTxn(insertCqlAllColumns(metadata));

                for (int i = 0; i < 10; i++)
                    cluster.coordinator(1).execute(insertStmt, ConsistencyLevel.ANY, (Object[]) dataGen.next(rs));

//                cluster.forEach(i -> i.nodetoolResult("repair", "--full", "--accord-only", KEYSPACE).asserts().success());

                stopUnchecked(cluster.get(nodeToReplace));
                ClusterUtils.replaceHostAndStart(cluster, cluster.get(nodeToReplace));

//                cluster.forEach(i -> {
//                    if (i.isShutdown()) return;
//                    i.nodetoolResult("repair", "--full", "--accord-only", KEYSPACE).asserts().success();
//                });
            });
        }
    }

    private static void maybeCreateUDTs(Cluster cluster, TableMetadata metadata)
    {
        CassandraGenerators.visitUDTs(metadata, next -> {
            String cql = next.toCqlString(false, false);
            logger.warn("Creating UDT {}", cql);
            cluster.schemaChange(cql);
        });
    }
}
