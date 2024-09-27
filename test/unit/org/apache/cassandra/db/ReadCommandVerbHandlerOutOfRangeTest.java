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

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.exceptions.InvalidRoutingException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper.*;
import static org.apache.cassandra.net.Verb.READ_REQ;
import static org.junit.Assert.assertEquals;

public class ReadCommandVerbHandlerOutOfRangeTest
{
    private static ReadCommandVerbHandler handler;
    private static TableMetadata metadata_nonreplicated;
    private ColumnFamilyStore cfs;
    private long startingTotalMetricCount;
    private long startingKeyspaceMetricCount;

    private static final String TEST_NAME = "read_command_vh_test_";
    private static final String KEYSPACE_NONREPLICATED = TEST_NAME + "cql_keyspace";
    private static final String TABLE = "table1";

    @BeforeClass
    public static void init() throws Throwable
    {
        ServerTestUtils.prepareServerNoRegister();
        SchemaLoader.schemaDefinition(TEST_NAME);
        ServerTestUtils.markCMS();
        metadata_nonreplicated = Schema.instance.getTableMetadata(KEYSPACE_NONREPLICATED, TABLE);
        StorageService.instance.unsafeSetInitialized();
    }

    @Before
    public void setup()
    {
        ServerTestUtils.resetCMS();
        ClusterMetadataTestHelper.addEndpoint(broadcastAddress, bytesToken(100));
        ClusterMetadataTestHelper.addEndpoint(node1, bytesToken(0));

        MessagingService.instance().inboundSink.clear();
        MessagingService.instance().outboundSink.clear();
        MessagingService.instance().outboundSink.add((message, to) -> false);
        MessagingService.instance().inboundSink.add((message) -> false);

        cfs = Keyspace.open(KEYSPACE_NONREPLICATED).getColumnFamilyStore(TABLE);
        startingKeyspaceMetricCount = keyspaceMetricValue(cfs);
        startingTotalMetricCount = StorageMetrics.totalOpsForInvalidToken.getCount();
        handler = new ReadCommandVerbHandler();
    }

    private static DecoratedKey key(TableMetadata metadata, int key)
    {
        return metadata.partitioner.decorateKey(ByteBufferUtil.bytes(key));
    }

    @Test
    public void acceptPartitionReadForNaturalEndpoint() throws Exception
    {
        verifyPartitionReadAccepted();
    }

    @Test
    public void acceptPartitionReadForPendingEndpoint() throws Exception
    {
        // reset ClusterMetadata then join the remote node and partially
        // join the localhost one to emulate pending ranges
        ServerTestUtils.resetCMS();
        ClusterMetadataTestHelper.addEndpoint(node1, bytesToken(0));
        ClusterMetadataTestHelper.register(broadcastAddress);
        ClusterMetadataTestHelper.joinPartially(broadcastAddress, bytesToken(100));
        assertEquals(NodeState.BOOTSTRAPPING, ClusterMetadata.current().directory.peerState(broadcastAddress));

        verifyPartitionReadAccepted();
    }

    private void verifyPartitionReadAccepted() throws Exception
    {
        ListenableFuture<MessageDelivery> messageSink = registerOutgoingMessageSink();
        int messageId = randomInt();
        int key = 50;
        ReadCommand command = partitionRead(key);
        handler.doVerb(Message.builder(READ_REQ, command).from(node1).withId(messageId).build());
        getAndVerifyResponse(messageSink, messageId, false);
    }

    @Test(expected = InvalidRoutingException.class)
    public void rejectPartitionReadForTokenOutOfRange() 
    {
        // reject a read for a key who's token the node doesn't own the range for
        int messageId = randomInt();
        int key = 200;
        ReadCommand command = partitionRead(key);
        handler.doVerb(Message.builder(READ_REQ, command).from(node1).withId(messageId).build());
        // we automatically send a failure response if doVerb above throws
    }

    @Test
    public void acceptRangeReadForNaturalEndpoint() throws Exception
    {
        // reset ClusterMetadata then join the remote node and partially
        // join the localhost one to emulate pending ranges
        ServerTestUtils.resetCMS();
        ClusterMetadataTestHelper.addEndpoint(node1, bytesToken(0));
        ClusterMetadataTestHelper.register(broadcastAddress);
        ClusterMetadataTestHelper.joinPartially(broadcastAddress, bytesToken(100));
        assertEquals(NodeState.BOOTSTRAPPING, ClusterMetadata.current().directory.peerState(broadcastAddress));

        verifyRangeReadAccepted();
    }

    @Test
    public void acceptRangeReadForPendingEndpoint() throws Exception
    {
        verifyRangeReadAccepted();
    }

    private void verifyRangeReadAccepted() throws Exception
    {
        ListenableFuture<MessageDelivery> messageSink = registerOutgoingMessageSink();
        int messageId = randomInt();
        ReadCommand command = rangeRead(50, 60);
        handler.doVerb(Message.builder(READ_REQ, command).from(node1).withId(messageId).build());
        getAndVerifyResponse(messageSink, messageId, false);
    }

    @Test(expected = InvalidRoutingException.class)
    public void rejectRangeReadForUnownedRange() throws Exception
    {
        int messageId = randomInt();
        ReadCommand command = rangeRead(150, 160);
        handler.doVerb(Message.builder(READ_REQ, command).from(node1).withId(messageId).build());
    }

    private void getAndVerifyResponse(ListenableFuture<MessageDelivery> messageSink,
                                      int messageId,
                                      boolean isOutOfRange) throws InterruptedException, ExecutionException, TimeoutException
    {
        MessageDelivery response = messageSink.get(100, TimeUnit.MILLISECONDS);
        assertEquals(isOutOfRange ? Verb.FAILURE_RSP : Verb.READ_RSP, response.message.verb());
        assertEquals(broadcastAddress, response.message.from());
        assertEquals(isOutOfRange, response.message.payload instanceof RequestFailureReason);
        assertEquals(messageId, response.message.id());
        assertEquals(node1, response.to);
        assertEquals(startingTotalMetricCount + (isOutOfRange ? 1 : 0), StorageMetrics.totalOpsForInvalidToken.getCount());
        assertEquals(startingKeyspaceMetricCount + (isOutOfRange ? 1 : 0), keyspaceMetricValue(cfs));
    }

    private ReadCommand partitionRead(int key)
    {
        return new StubReadCommand(key, metadata_nonreplicated);
    }

    private ReadCommand rangeRead(int start, int end)
    {
        Range<Token> range = new Range<>(key(metadata_nonreplicated, start).getToken(),
                                         key(metadata_nonreplicated, end).getToken());
        return new StubRangeReadCommand(range, metadata_nonreplicated);
    }

    private static class StubReadCommand extends SinglePartitionReadCommand
    {
        private final TableMetadata tmd;

        StubReadCommand(int key, TableMetadata tmd)
        {
            super(tmd.epoch,
                  false,
                  0,
                  false,
                  tmd,
                  FBUtilities.nowInSeconds(),
                  ColumnFilter.all(tmd),
                  RowFilter.none(),
                  DataLimits.NONE,
                  key(tmd, key),
                  null,
                  null,
                  false,
                  null);

            this.tmd = tmd;
        }

        public UnfilteredPartitionIterator executeLocally(ReadExecutionController executionController)
        {
            return EmptyIterators.unfilteredPartition(tmd);
        }

        public String toString()
        {
            return "<<StubReadCommand>>";
        }
    }

    private static class StubRangeReadCommand extends PartitionRangeReadCommand
    {
        private final TableMetadata cfm;

        StubRangeReadCommand(Range<Token> range, TableMetadata tmd)
        {
            super(tmd.epoch,
                  false,
                  0,
                  false,
                  tmd,
                  FBUtilities.nowInSeconds(),
                  ColumnFilter.all(tmd),
                  RowFilter.none(),
                  DataLimits.NONE,
                  DataRange.forTokenRange(range),
                  null,
                  false);

            this.cfm = tmd;
        }

        public UnfilteredPartitionIterator executeLocally(ReadExecutionController executionController)
        {
            return EmptyIterators.unfilteredPartition(cfm);
        }
    }

    private static long keyspaceMetricValue(ColumnFamilyStore cfs)
    {
        return cfs.keyspace.metric.outOfRangeTokenReads.getCount();
    }
}
