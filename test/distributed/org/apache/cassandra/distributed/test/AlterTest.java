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

package org.apache.cassandra.distributed.test;

import java.util.List;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.config.CassandraRelevantProperties.JOIN_RING;
import static org.apache.cassandra.distributed.action.GossipHelper.withProperty;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.api.TokenSupplier.evenlyDistributedTokens;
import static org.apache.cassandra.distributed.shared.NetworkTopology.singleDcNetworkTopology;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class AlterTest extends TestBaseImpl
{
    @Test
    public void getAndSetCompressionParametersTest() throws Throwable
    {
        try (ICluster<IInvokableInstance> cluster = init(builder().withNodes(2)
                                                                  .start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck));");
            cluster.stream().forEach((i) -> {
                i.acceptsOnInstance((IIsolatedExecutor.SerializableConsumer<String>) (ks) -> {
                    Keyspace.open(ks)
                            .getColumnFamilyStore("tbl")
                            .setCompressionParametersJson("{\"chunk_length_in_kb\": \"128\"," +
                                                          "  \"class\": \"org.apache.cassandra.io.compress.LZ4Compressor\"}");
                    Assert.assertTrue(Keyspace.open(ks)
                                              .getColumnFamilyStore("tbl")
                                              .getCompressionParametersJson().contains("128"));
                }).accept(KEYSPACE);
            });
            cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".tbl ADD v2 int");

            cluster.stream().forEach((i) -> {
                i.acceptsOnInstance((IIsolatedExecutor.SerializableConsumer<String>) (ks) -> {
                    Assert.assertFalse(Keyspace.open(ks)
                                               .getColumnFamilyStore("tbl")
                                               .getCompressionParametersJson().contains("128"));
                }).accept(KEYSPACE);
            });
        }
    }

    @Test
    public void alteringKeyspaceOnInsufficientNumberOfReplicasFiltersOutGossppingOnlyMembersTest() throws Throwable
    {
        int originalNodeCount = 1;
        int expandedNodeCount = originalNodeCount + 1;

        try (Cluster cluster = builder().withNodes(originalNodeCount)
                                        .withTokenSupplier(evenlyDistributedTokens(expandedNodeCount, 1))
                                        .withNodeIdTopology(singleDcNetworkTopology(expandedNodeCount, "dc0", "rack0"))
                                        .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL))
                                        .start())
        {
            IInstanceConfig config = cluster.newInstanceConfig();
            IInvokableInstance gossippingOnlyMember = cluster.bootstrap(config);
            withProperty(JOIN_RING, false, () -> gossippingOnlyMember.startup(cluster));

            int attempts = 0;
            // it takes some time the underlying structure is populated otherwise the test is flaky
            while (((IInvokableInstance) (cluster.get(2))).callOnInstance(() -> StorageService.instance.getTokenMetadata().getAllMembers().isEmpty()))
            {
                if (attempts++ > 30)
                    throw new RuntimeException("timeouted on waiting for a member");
                Thread.sleep(1000);
            }

            for (String operation : new String[] { "CREATE", "ALTER" })
            {
                SimpleQueryResult result = cluster.coordinator(2)
                                                  .executeWithResult(operation + " KEYSPACE abc WITH replication = {'class' : 'NetworkTopologyStrategy', 'dc0' : 2 }",
                                                                     ONE);
                assertFalse(result.warnings().isEmpty());
                assertThat(result.warnings().get(0)).contains("Your replication factor 2 for keyspace abc is higher than the number of nodes 1 for datacenter dc0");
            }
        }
    }

    @Test
    public void unknownMemtableConfigurationTest() throws Throwable
    {
        Logger logger = LoggerFactory.getLogger(getClass());
        try (Cluster cluster = Cluster.build(1)
                                      .withTokenSupplier(TokenSupplier.evenlyDistributedTokens(3, 1))
                                      .withConfig(c -> c.with(Feature.values())
                                                        .set("memtable", ImmutableMap.of(
                                                        "configurations", ImmutableMap.of(
                                                            "testconfig", ImmutableMap.of(
                                                                "class_name", "SkipListMemtable")))))
                                      .start())
        {
            init(cluster);
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int PRIMARY KEY)");

            // Start Node 2 without the memtable configuration definition.
            IInvokableInstance node1 = cluster.get(1);
            IInvokableInstance node2 = ClusterUtils.addInstance(cluster, node1.config(), c -> c.set("memtable", ImmutableMap.of()));
            node2.startup(cluster);

            try
            {
                cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".tbl WITH memtable = 'testconfig'", false, node2);
                fail("Expected ALTER to fail with unknown memtable configuration.");
            }
            catch (Throwable t)
            {
                // expected
                logger.info("Expected: {}", t.getMessage());
                Assert.assertTrue(Throwables.isCausedBy(t, x -> x.getMessage().matches("Memtable configuration.*not found.*")));
            }
            long mark = node2.logs().mark();

            cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".tbl WITH memtable = 'testconfig'", false, node1);
            // the above should succeed, the configuration is acceptable to node1

            ClusterUtils.awaitGossipSchemaMatch(cluster);
            List<String> errorInLog = node2.logs().grep(mark,"ERROR.*Invalid memtable configuration.*").getResult();
            Assert.assertTrue(errorInLog.size() > 0);
            logger.info(Lists.listToString(errorInLog));

            // Add a new node that has an invalid definition but should accept the already defined table schema.
            IInvokableInstance node3 = ClusterUtils.addInstance(cluster,
                                                                node2.config(),
                                                                c -> c.set("memtable", ImmutableMap.of(
                                                                "configurations", ImmutableMap.of(
                                                                    "testconfig", ImmutableMap.of(
                                                                        "class_name", "NotExistingMemtable")))));
            node3.startup(cluster);
            ClusterUtils.awaitGossipSchemaMatch(cluster);

            errorInLog = node3.logs().grep("ERROR.*Invalid memtable configuration.*").getResult();
            Assert.assertTrue(errorInLog.size() > 0);
            logger.info(Lists.listToString(errorInLog));

            // verify that all nodes can write to the table
            node1.executeInternalWithResult("INSERT INTO " + KEYSPACE + ".tbl (pk) VALUES (?)", 1);
            node2.executeInternalWithResult("INSERT INTO " + KEYSPACE + ".tbl (pk) VALUES (?)", 2);
            node3.executeInternalWithResult("INSERT INTO " + KEYSPACE + ".tbl (pk) VALUES (?)", 3);
        }
    }
}
