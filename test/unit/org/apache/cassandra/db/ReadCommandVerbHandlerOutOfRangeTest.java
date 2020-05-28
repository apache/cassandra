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
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TokenRangeTestUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import static org.apache.cassandra.net.Verb.READ_REQ;
import static org.apache.cassandra.utils.TokenRangeTestUtil.broadcastAddress;
import static org.apache.cassandra.utils.TokenRangeTestUtil.bytesToken;
import static org.apache.cassandra.utils.TokenRangeTestUtil.node1;
import static org.apache.cassandra.utils.TokenRangeTestUtil.randomInt;
import static org.apache.cassandra.utils.TokenRangeTestUtil.registerOutgoingMessageSink;

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
        SchemaLoader.loadSchema();
        SchemaLoader.schemaDefinition(TEST_NAME);
        StorageService.instance.initServer(0);
        metadata_nonreplicated = Schema.instance.getTableMetadata(KEYSPACE_NONREPLICATED, TABLE);
    }

    @Before
    public void setup()
    {
        DatabaseDescriptor.setLogOutOfTokenRangeRequests(true);
        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(true);

        StorageService.instance.getTokenMetadata().clearUnsafe();
        StorageService.instance.getTokenMetadata().updateNormalToken(bytesToken(0), node1);
        StorageService.instance.getTokenMetadata().updateNormalToken(bytesToken(100), broadcastAddress);

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
    public void acceptReadForNaturalEndpoint() throws Exception
    {
        ListenableFuture<TokenRangeTestUtil.MessageDelivery> messageSink = registerOutgoingMessageSink();
        int messageId = randomInt();
        int key = 50;
        ReadCommand command = command(key);
        handler.doVerb(Message.builder(READ_REQ, command).from(node1).withId(messageId).build());
        getAndVerifyResponse(messageSink, messageId, false, false);
    }

    @Test
    public void rejectReadForTokenOutOfRange() throws Exception
    {
        // reject a read for a key who's token the node doesn't own the range for
        ListenableFuture<TokenRangeTestUtil.MessageDelivery> messageSink = registerOutgoingMessageSink();
        int messageId = randomInt();
        int key = 200;
        ReadCommand command = command(key);
        handler.doVerb(Message.builder(READ_REQ, command).from(node1).withId(messageId).build());
        getAndVerifyResponse(messageSink, messageId, true, true);
    }

    @Test
    public void acceptReadIfRejectionNotEnabled() throws Exception
    {
        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        ListenableFuture<TokenRangeTestUtil.MessageDelivery> messageSink = registerOutgoingMessageSink();
        int messageId = randomInt();
        int key = 200;
        ReadCommand command = command(key);
        handler.doVerb(Message.builder(READ_REQ, command).from(node1).withId(messageId).build());
        getAndVerifyResponse(messageSink, messageId, true, false);
    }

    @Test
    public void rangeReadCommandBoundsAreNotChecked() throws Exception
    {
        // checking is only currently done for single partition reads, range reads will continue to
        // accept any range they are given. So for a range wholly outside the node's ownership we
        // expect the metric to remain unchanged and read command to be executed.
        // This test is added for 3.0 because the single partition & range  commands are now processed
        // by the same verb handler.
        // rdar://problem/33535104 is to extend checking to range reads
        ListenableFuture<TokenRangeTestUtil.MessageDelivery> messageSink = registerOutgoingMessageSink();
        int messageId = randomInt();
        Range<Token> range = new Range<>(key(metadata_nonreplicated, 150).getToken(),
                                         key(metadata_nonreplicated, 160).getToken());
        ReadCommand command = new StubRangeReadCommand(range, metadata_nonreplicated);
        handler.doVerb(Message.builder(READ_REQ, command).from(node1).withId(messageId).build());
        getAndVerifyResponse(messageSink, messageId, false, false);
    }

    private void getAndVerifyResponse(ListenableFuture<TokenRangeTestUtil.MessageDelivery> messageSink,
                                      int messageId,
                                      boolean isOutOfRange,
                                      boolean expectFailure) throws InterruptedException, ExecutionException, TimeoutException
    {
        assertEquals(startingTotalMetricCount + (isOutOfRange ? 1 : 0), StorageMetrics.totalOpsForInvalidToken.getCount());
        assertEquals(startingKeyspaceMetricCount + (isOutOfRange ? 1 : 0), keyspaceMetricValue(cfs));
        if (expectFailure)
        {
            try
            {
                TokenRangeTestUtil.MessageDelivery response = messageSink.get(10, TimeUnit.MILLISECONDS);
                fail(String.format("Didn't expect any message to be sent, but sent %s to %s in response to %s",
                                   response.message.toString(),
                                   response.to,
                                   response.message.id()));
            }
            catch (TimeoutException e)
            {
                // expected
            }
        }
        else
        {
            TokenRangeTestUtil.MessageDelivery response = messageSink.get(10, TimeUnit.MILLISECONDS);
            assertEquals(Verb.READ_RSP, response.message.verb());
            assertEquals(broadcastAddress, response.message.from());
            assertEquals(messageId, response.message.id());
            assertEquals(node1, response.to);
        }
    }

    private ReadCommand command(int key)
    {
        return new StubReadCommand(key, metadata_nonreplicated);
    }

    private static class StubReadCommand extends SinglePartitionReadCommand
    {
        private final TableMetadata tmd;

        StubReadCommand(int key, TableMetadata tmd)
        {
            super(false,
                  0,
                  false,
                  tmd,
                  FBUtilities.nowInSeconds(),
                  ColumnFilter.all(tmd),
                  RowFilter.NONE,
                  DataLimits.NONE,
                  key(tmd, key),
                  null,
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
            super(false,
                  0,
                  false,
                  tmd,
                  FBUtilities.nowInSeconds(),
                  ColumnFilter.all(tmd),
                  RowFilter.NONE,
                  DataLimits.NONE,
                  DataRange.forTokenRange(range),
                  null);

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
