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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.apache.cassandra.utils.TokenRangeTestUtil.MessageDelivery;
import static org.apache.cassandra.utils.TokenRangeTestUtil.broadcastAddress;
import static org.apache.cassandra.utils.TokenRangeTestUtil.bytesToken;
import static org.apache.cassandra.utils.TokenRangeTestUtil.node1;
import static org.apache.cassandra.utils.TokenRangeTestUtil.randomInt;
import static org.apache.cassandra.utils.TokenRangeTestUtil.registerOutgoingMessageSink;

public class MutationVerbHandlerOutOfRangeTest
{
    private static final String TEST_NAME = "mutation_vh_test_";
    private static final String KEYSPACE = TEST_NAME + "cql_keyspace";
    private static final String TABLE = "table1";

    private ColumnFamilyStore cfs;
    private long startingTotalMetricCount;
    private long startingKeyspaceMetricCount;

    @BeforeClass
    public static void init() throws Exception
    {
        SchemaLoader.loadSchema();
        SchemaLoader.schemaDefinition(TEST_NAME);
        StorageService.instance.initServer(0);

    }

    @Before
    public void setup() throws Exception
    {
        DatabaseDescriptor.setLogOutOfTokenRangeRequests(true);
        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(true);

        StorageService.instance.getTokenMetadata().clearUnsafe();
        StorageService.instance.getTokenMetadata().updateNormalToken(bytesToken(0), node1);
        StorageService.instance.getTokenMetadata().updateNormalToken(bytesToken(100), broadcastAddress);

        MessagingService.instance().inboundSink.clear();
        MessagingService.instance().outboundSink.clear();

        cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        startingKeyspaceMetricCount = keyspaceMetricValue(cfs);
        startingTotalMetricCount = StorageMetrics.totalOpsForInvalidToken.getCount();
    }

    @Test
    public void acceptMutationForNaturalEndpoint() throws Exception
    {
       acceptMutationForNaturalEndpoint(new MutationVerbHandler());
    }

    @Test
    public void acceptReadRepairForNaturalEndpoint() throws Exception
    {
        acceptMutationForNaturalEndpoint(new ReadRepairVerbHandler());
    }

    private void acceptMutationForNaturalEndpoint(IVerbHandler<Mutation> handler) throws Exception
    {
        ListenableFuture<MessageDelivery> messageSink = registerOutgoingMessageSink();
        int messageId = randomInt();
        int value = randomInt();
        int key = 50;
        Mutation mutation = mutation(key, value);
        handler.doVerb(Message.builder(Verb.MUTATION_REQ, mutation).from(node1).withId(messageId).build());
        getAndVerifyResponse(messageSink, messageId, key, value, false, false);
    }

    @Test
    public void acceptMutationForPendingEndpoint() throws Exception
    {
        acceptMutationForPendingEndpoint(new MutationVerbHandler());
    }

    @Test
    public void acceptReadRepairForPendingEndpoint() throws Exception
    {
        acceptMutationForPendingEndpoint(new ReadRepairVerbHandler());
    }

    private void acceptMutationForPendingEndpoint(IVerbHandler<Mutation> handler ) throws Exception
    {
        // remove localhost from TM and add it back as pending
        StorageService.instance.getTokenMetadata().removeEndpoint(broadcastAddress);
        Multimap<Range<Token>, Replica> pending = HashMultimap.create();
        Range<Token> range = new Range<>(bytesToken(0), bytesToken(100));
        pending.put(range, new Replica(broadcastAddress, range, true));
        StorageService.instance.getTokenMetadata().setPendingRangesUnsafe(KEYSPACE, pending);

        ListenableFuture<MessageDelivery> messageSink = registerOutgoingMessageSink();
        int messageId = randomInt();
        int value = randomInt();
        int key = 50;
        Mutation mutation = mutation(key, value);
        handler.doVerb(Message.builder(Verb.MUTATION_REQ, mutation).from(node1).withId(messageId).build());
        getAndVerifyResponse(messageSink, messageId, key, value, false, false);
    }

    @Test
    public void rejectMutationForTokenOutOfRange() throws Exception
    {
        rejectMutationForTokenOutOfRange(new MutationVerbHandler());
    }

    @Test
    public void rejectReadRepairForTokenOutOfRange() throws Exception
    {
        rejectMutationForTokenOutOfRange(new ReadRepairVerbHandler());
    }

    private void rejectMutationForTokenOutOfRange(IVerbHandler<Mutation> handler) throws Exception
    {
        // reject a mutation for a token the node neither owns nor is pending
        ListenableFuture<MessageDelivery> messageSink = registerOutgoingMessageSink();
        int messageId = randomInt();
        int value = randomInt();
        int key = 200;
        Mutation mutation = mutation(key, value);
        handler.doVerb(Message.builder(Verb.MUTATION_REQ, mutation).from(node1).withId(messageId).build());
        getAndVerifyResponse(messageSink, messageId, key, value, true, true);
    }

    @Test
    public void acceptMutationIfRejectionNotEnabled() throws Exception
    {
        acceptMutationIfRejectionNotEnabled(new MutationVerbHandler());
    }

    @Test
    public void acceptReadRepairIfRejectionNotEnabled() throws Exception
    {
        acceptMutationIfRejectionNotEnabled(new ReadRepairVerbHandler());
    }

    private void acceptMutationIfRejectionNotEnabled(IVerbHandler<Mutation> handler) throws Exception
    {
        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        ListenableFuture<MessageDelivery> messageSink = registerOutgoingMessageSink();
        int messageId = randomInt();
        int value = randomInt();
        int key = 200;
        Mutation mutation = mutation(key, value);
        handler.doVerb(Message.builder(Verb.MUTATION_REQ, mutation).from(node1).withId(messageId).build());
        getAndVerifyResponse(messageSink, messageId, key, value, true, false);
    }

    static <V> int toInt(Cell<V> cell)
    {
        return cell.accessor().toInt(cell.value());
    }

    private void getAndVerifyResponse(ListenableFuture<MessageDelivery> messageSink,
                                      int messageId,
                                      int key,
                                      int value,
                                      boolean isOutOfRange,
                                      boolean expectFailure) throws InterruptedException, ExecutionException, TimeoutException
    {
        MessageDelivery response = messageSink.get(100, TimeUnit.MILLISECONDS);
        assertEquals(expectFailure ? Verb.FAILURE_RSP : Verb.MUTATION_RSP, response.message.verb());
        assertEquals(broadcastAddress, response.message.from());
        assertEquals(expectFailure, response.message.payload instanceof RequestFailureReason);
        assertEquals(messageId, response.message.id());
        assertEquals(node1, response.to);
        assertEquals(startingTotalMetricCount + (isOutOfRange ? 1 : 0), StorageMetrics.totalOpsForInvalidToken.getCount());
        assertEquals(startingKeyspaceMetricCount + (isOutOfRange ? 1 : 0), keyspaceMetricValue(cfs));
        if (!expectFailure)
        {
            ReadCommand read = Util.cmd(cfs, bytes(key)).build();
            ColumnMetadata col = cfs.metadata().getColumn(bytes("v1"));
            assertEquals(value, toInt(Util.getOnlyRow(read).getCell(col)));
        }
    }

    private static long keyspaceMetricValue(ColumnFamilyStore cfs)
    {
        return cfs.keyspace.metric.outOfRangeTokenWrites.getCount();
    }

    private Mutation mutation(int key, int columnValue)
    {
        TableMetadata cfm = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        DecoratedKey dk = cfs.decorateKey(bytes(key));
        ColumnMetadata col = cfs.metadata().getColumn(bytes("v1"));
        Cell<?> cell = BufferCell.live(col, FBUtilities.timestampMicros(), bytes(columnValue));
        Row row = BTreeRow.singleCellRow(Clustering.EMPTY, cell);
        PartitionUpdate update = PartitionUpdate.singleRowUpdate(cfm, dk, row);
        return new Mutation(update);
    }
}
