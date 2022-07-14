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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.api.QueryResults;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;

import static org.apache.cassandra.distributed.action.GossipHelper.withProperty;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.api.TokenSupplier.evenlyDistributedTokens;
import static org.apache.cassandra.distributed.shared.NetworkTopology.singleDcNetworkTopology;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ThriftTest extends TestBaseImpl
{
    @Test
    public void writeThenReadCQL() throws IOException, TException
    {
        try (Cluster cluster = init(Cluster.build(1).withConfig(c -> c.with(Feature.NATIVE_PROTOCOL)).start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, value int, PRIMARY KEY (pk))");

            ThriftClientUtils.thriftClient(cluster.get(1), thrift -> {
                thrift.set_keyspace(KEYSPACE);
                Mutation mutation = new Mutation();
                ColumnOrSuperColumn csoc = new ColumnOrSuperColumn();
                Column column = new Column();
                column.setName(CompositeType.build(ByteBufferUtil.bytes("value")));
                column.setValue(ByteBufferUtil.bytes(0));
                column.setTimestamp(System.currentTimeMillis());
                csoc.setColumn(column);
                mutation.setColumn_or_supercolumn(csoc);

                thrift.batch_mutate(Collections.singletonMap(ByteBufferUtil.bytes(0),
                                                             Collections.singletonMap("tbl", Arrays.asList(mutation))),
                                    org.apache.cassandra.thrift.ConsistencyLevel.ALL);
            });

            SimpleQueryResult qr = cluster.coordinator(1).executeWithResult("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL);
            AssertUtils.assertRows(qr, QueryResults.builder().row(0, 0).build());
        }
    }

    @Test
    public void restartThriftOnGossippingOnlyMember() throws Throwable
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
            withProperty("cassandra.join_ring", Boolean.toString(false), () -> gossippingOnlyMember.startup(cluster));

            assertTrue(gossippingOnlyMember.callOnInstance((IIsolatedExecutor.SerializableCallable<Boolean>)
                                                           () -> StorageService.instance.isRPCServerRunning()));

            gossippingOnlyMember.runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> StorageService.instance.stopRPCServer());

            assertFalse(gossippingOnlyMember.callOnInstance((IIsolatedExecutor.SerializableCallable<Boolean>)
                                                            () -> StorageService.instance.isRPCServerRunning()));

            gossippingOnlyMember.runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> StorageService.instance.startRPCServer());

            assertTrue(gossippingOnlyMember.callOnInstance((IIsolatedExecutor.SerializableCallable<Boolean>)
                                                           () -> StorageService.instance.isRPCServerRunning()));
        }
    }
}
