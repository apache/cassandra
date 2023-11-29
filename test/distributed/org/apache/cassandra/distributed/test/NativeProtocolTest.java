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

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.service.StorageService;
import org.junit.Assert;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.impl.RowUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.config.CassandraRelevantProperties.JOIN_RING;
import static org.apache.cassandra.distributed.action.GossipHelper.withProperty;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.api.TokenSupplier.evenlyDistributedTokens;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.apache.cassandra.distributed.shared.NetworkTopology.singleDcNetworkTopology;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

// TODO: this test should be removed after running in-jvm dtests is set up via the shared API repository
public class NativeProtocolTest extends TestBaseImpl
{
    @Test
    public void withClientRequests() throws Throwable
    {
        try (ICluster ignored = init(builder().withNodes(3)
                                              .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
                                              .start()))
        {

            try (com.datastax.driver.core.Cluster cluster = com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1").build();
                 Session session = cluster.connect())
            {
                session.execute("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck));");
                session.execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) values (1,1,1);");
                Statement select = new SimpleStatement("select * from " + KEYSPACE + ".tbl;").setConsistencyLevel(ConsistencyLevel.ALL);
                final ResultSet resultSet = session.execute(select);
                assertRows(RowUtil.toObjects(resultSet), row(1, 1, 1));
                Assert.assertEquals(3, cluster.getMetadata().getAllHosts().size());
            }
        }
    }

    @Test
    public void withCounters() throws Throwable
    {
        try (ICluster ignored = init(builder().withNodes(3)
                                              .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
                                              .start()))
        {
            final com.datastax.driver.core.Cluster cluster = com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1").build();
            Session session = cluster.connect();
            session.execute("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck counter, PRIMARY KEY (pk));");
            session.execute("UPDATE " + KEYSPACE + ".tbl set ck = ck + 10 where pk = 1;");
            Statement select = new SimpleStatement("select * from " + KEYSPACE + ".tbl;").setConsistencyLevel(ConsistencyLevel.ALL);
            final ResultSet resultSet = session.execute(select);
            assertRows(RowUtil.toObjects(resultSet), row(1, 10L));
            Assert.assertEquals(3, cluster.getMetadata().getAllHosts().size());
            session.close();
            cluster.close();
        }
    }

    @Test
    public void restartTransportOnGossippingOnlyMember() throws Throwable
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

            assertTrue(gossippingOnlyMember.callOnInstance((IIsolatedExecutor.SerializableCallable<Boolean>)
                                                           () -> StorageService.instance.isNativeTransportRunning()));

            gossippingOnlyMember.runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> StorageService.instance.stopNativeTransport());

            assertFalse(gossippingOnlyMember.callOnInstance((IIsolatedExecutor.SerializableCallable<Boolean>)
                                                            () -> StorageService.instance.isNativeTransportRunning()));

            gossippingOnlyMember.runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> StorageService.instance.startNativeTransport());

            assertTrue(gossippingOnlyMember.callOnInstance((IIsolatedExecutor.SerializableCallable<Boolean>)
                                                           () -> StorageService.instance.isNativeTransportRunning()));
        }
    }

    @Test
    public void testBinaryReflectsRpcReadiness() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL)
                                                                    .set("start_native_transport", "false"))
                                        .start())
        {
            IInvokableInstance i = cluster.get(1);

            // rpc is false when native transport is not enabled
            assertFalse(i.callOnInstance((IIsolatedExecutor.SerializableCallable<Boolean>) () -> StorageService.instance.isNativeTransportRunning()));
            assertFalse(i.callOnInstance((IIsolatedExecutor.SerializableCallable<Boolean>) () -> StorageService.instance.isRpcReady(FBUtilities.getBroadcastAddressAndPort())));

            // but if we enable it, e.g. by nodetool enablebinary, rpc will be enabled
            i.runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> StorageService.instance.startNativeTransport());
            assertTrue(i.callOnInstance((IIsolatedExecutor.SerializableCallable<Boolean>) () -> StorageService.instance.isRpcReady(FBUtilities.getBroadcastAddressAndPort())));

            // by calling e.g. nodetool disablebinary, rpc will NOT be set to false again
            // please read CASSANDRA-18935 for in-depth explanation why it is so
            i.runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> StorageService.instance.stopNativeTransport());
            assertTrue(i.callOnInstance((IIsolatedExecutor.SerializableCallable<Boolean>) () -> StorageService.instance.isRpcReady(FBUtilities.getBroadcastAddressAndPort())));
        }
    }
}

