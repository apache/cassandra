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

package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.exceptions.InvalidRoutingException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper.*;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CounterMutationVerbHandlerOutOfRangeTest
{
    private static final String KEYSPACE = "CounterCacheTest";
    private static final String TABLE = "Counter1";

    private CounterMutationVerbHandler handler;
    private ColumnFamilyStore cfs;
    private long startingTotalMetricCount;
    private long startingKeyspaceMetricCount;

    @BeforeClass
    public static void init() throws Exception
    {
        ServerTestUtils.prepareServerNoRegister();
        SchemaLoader.createKeyspace(KEYSPACE,
                                    KeyspaceParams.simple(1),
                                    SchemaLoader.counterCFMD(KEYSPACE, TABLE));
        ServerTestUtils.markCMS();
        StorageService.instance.unsafeSetInitialized();
    }

    @Before
    public void setup() throws Exception
    {
        ServerTestUtils.resetCMS();
        ClusterMetadataTestHelper.addEndpoint(broadcastAddress, bytesToken(100));
        ClusterMetadataTestHelper.addEndpoint(node1, bytesToken(0));

        MessagingService.instance().inboundSink.clear();
        MessagingService.instance().outboundSink.clear();

        handler = new CounterMutationVerbHandler();
        cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        cfs.truncateBlocking();
        startingKeyspaceMetricCount = keyspaceMetricValue();
        startingTotalMetricCount = StorageMetrics.totalOpsForInvalidToken.getCount();
    }

    @Test
    public void acceptMutationForNaturalEndpoint() throws Exception
    {
        int messageId = randomInt();
        int value = randomInt();
        int key = 30;
        CounterMutation mutation = mutation(key, value);
        handler.doVerb(Message.builder(Verb.MUTATION_REQ, mutation).from(node1).withId(messageId).build());

        // unlike non-counter mutations, we can't verify the response message for a successful write.
        // acting as the leader for the mutation, we'll try to forward the writes to the other replicas
        // but this will fail as the other node isn't really there. The response message is written
        // by the callback to these messages so it will never get sent. So the best we can do is to check
        // that the mutation was actually applied locally. When a counter mutation is rejected by the verb
        // handler we *can* verify the failure response message is sent.
        verifyWrite(key, value);
        assertEquals(startingTotalMetricCount, StorageMetrics.totalOpsForInvalidToken.getCount());
        assertEquals(startingKeyspaceMetricCount, keyspaceMetricValue());
    }

    @Test
    public void acceptMutationForPendingEndpoint() throws Exception
    {
        // reset ClusterMetadata then join the remote node and partially
        // join the localhost one to emulate pending ranges
        ServerTestUtils.resetCMS();
        ClusterMetadataTestHelper.addEndpoint(node1, bytesToken(0));
        ClusterMetadataTestHelper.register(broadcastAddress);
        ClusterMetadataTestHelper.joinPartially(broadcastAddress, bytesToken(100));
        assertEquals(NodeState.BOOTSTRAPPING, ClusterMetadata.current().directory.peerState(broadcastAddress));

        int messageId = randomInt();
        int value = randomInt();
        int key = 50;
        CounterMutation mutation = mutation(key, value);
        handler.doVerb(Message.builder(Verb.MUTATION_REQ, mutation).from(node1).withId(messageId).build());
        verifyWrite(key, value);
        assertEquals(startingTotalMetricCount, StorageMetrics.totalOpsForInvalidToken.getCount());
        assertEquals(startingKeyspaceMetricCount, keyspaceMetricValue());
    }

    @Test
    public void rejectMutationForTokenOutOfRange() throws Exception
    {
        // reject a mutation for a token the node neither owns nor is pending
        ListenableFuture<MessageDelivery> messageSink = registerOutgoingMessageSink();
        int messageId = randomInt();
        int value = randomInt();
        int key = 200;
        CounterMutation mutation = mutation(key, value);
        try
        {
            handler.doVerb(Message.builder(Verb.MUTATION_REQ, mutation).from(node1).withId(messageId).build());
            fail("this should now throw exception");
        }
        catch (InvalidRoutingException ignore) {}
        assertEquals(startingTotalMetricCount + 1, StorageMetrics.totalOpsForInvalidToken.getCount());
        assertEquals(startingKeyspaceMetricCount + 1, keyspaceMetricValue());
    }

    private void verifyWrite(int key, int value)
    {
        ReadCommand read = Util.cmd(cfs, bytes(key)).build();
        ColumnMetadata col = cfs.metadata().getColumn(bytes("val"));
        assertEquals((long)value, CounterContext.instance().total(Util.getOnlyRow(read).getCell(col)));
    }


    private static void verifyFailureResponse(ListenableFuture<MessageDelivery> messageSink, int messageId ) throws Exception
    {
        MessageDelivery response = messageSink.get(100, TimeUnit.MILLISECONDS);
        assertEquals(Verb.FAILURE_RSP, response.message.verb());
        assertEquals(broadcastAddress, response.message.from());
        assertTrue(response.message.payload instanceof RequestFailureReason);
        assertEquals(messageId, response.message.id());
        assertEquals(node1, response.to);
    }

    private CounterMutation mutation(int key, int columnValue)
    {
        TableMetadata cfm = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        DecoratedKey dk = cfs.decorateKey(bytes(key));
        ColumnMetadata col = cfs.metadata().getColumn(bytes("val"));
        ByteBuffer val = CounterContext.instance().createLocal(columnValue);
        Cell counterCell = BufferCell.live(col, FBUtilities.timestampMicros(), val);
        Row row = BTreeRow.singleCellRow(cfs.metadata().comparator.make("clustering_1"), counterCell);
        PartitionUpdate update = PartitionUpdate.singleRowUpdate(cfm, dk, row);
        return new CounterMutation(new Mutation(update), ConsistencyLevel.ANY);
    }

    private long keyspaceMetricValue()
    {
        return cfs.keyspace.metric.outOfRangeTokenWrites.getCount();
    }
}
