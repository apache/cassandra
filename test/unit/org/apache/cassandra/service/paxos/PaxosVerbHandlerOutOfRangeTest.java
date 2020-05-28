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

package org.apache.cassandra.service.paxos;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import static org.junit.Assert.assertEquals;

import static org.apache.cassandra.utils.TokenRangeTestUtil.MessageDelivery;
import static org.apache.cassandra.utils.TokenRangeTestUtil.broadcastAddress;
import static org.apache.cassandra.utils.TokenRangeTestUtil.bytesToken;
import static org.apache.cassandra.utils.TokenRangeTestUtil.node1;
import static org.apache.cassandra.utils.TokenRangeTestUtil.randomInt;
import static org.apache.cassandra.utils.TokenRangeTestUtil.registerOutgoingMessageSink;

public class PaxosVerbHandlerOutOfRangeTest
{
    // For the purposes of this testing, the details of the Commit don't really matter
    // as we're just testing the rejection (or lack of) and not the result of doing
    // whatever the specific verb handlers are supposed to do when they don't reject
    // a given Commit

    private static final String TEST_NAME = "paxos_vh_test_";
    private static final String KEYSPACE = TEST_NAME + "cql_keyspace";
    private static final String TABLE = "table1";

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
        startingTotalMetricCount = StorageMetrics.totalOpsForInvalidToken.getCount();
        startingKeyspaceMetricCount = keyspaceMetricValue();
    }
    private static DecoratedKey key(TableMetadata metadata, int key)
    {
        return metadata.partitioner.decorateKey(ByteBufferUtil.bytes(key));
    }

    @Test
    public void acceptPrepareForNaturalEndpoint() throws Exception
    {
        acceptRequestForNaturalEndpoint(Verb.PAXOS_PREPARE_REQ, Verb.PAXOS_PREPARE_RSP, new PrepareVerbHandler());
    }

    @Test
    public void acceptProposeForNaturalEndpoint() throws Exception
    {
        acceptRequestForNaturalEndpoint(Verb.PAXOS_PROPOSE_REQ, Verb.PAXOS_PROPOSE_RSP, new ProposeVerbHandler());
    }

    @Test
    public void acceptCommitForNaturalEndpoint() throws Exception
    {
        acceptRequestForNaturalEndpoint(Verb.PAXOS_COMMIT_REQ, Verb.PAXOS_COMMIT_RSP, new CommitVerbHandler());
    }

    private void acceptRequestForNaturalEndpoint(Verb requestVerb, Verb responseVerb, AbstractPaxosVerbHandler handler) throws Exception
    {
        ListenableFuture<MessageDelivery> messageSink = registerOutgoingMessageSink();
        int messageId = randomInt();
        int key = 50;
        Commit commit = commit(key);
        handler.doVerb(Message.builder(requestVerb, commit).from(node1).withId(messageId).build());
        getAndVerifyResponse(messageSink, responseVerb, messageId, false, false);
    }

    @Test
    public void acceptPrepareForPendingEndpoint() throws Exception
    {
        acceptRequestForPendingEndpoint(Verb.PAXOS_PREPARE_REQ, Verb.PAXOS_PREPARE_RSP, new PrepareVerbHandler());
    }

    @Test
    public void acceptProposeForPendingEndpoint() throws Exception
    {
        acceptRequestForPendingEndpoint(Verb.PAXOS_PROPOSE_REQ, Verb.PAXOS_PROPOSE_RSP, new ProposeVerbHandler());
    }

    @Test
    public void acceptCommitForPendingEndpoint() throws Exception
    {
        acceptRequestForPendingEndpoint(Verb.PAXOS_COMMIT_REQ, Verb.PAXOS_COMMIT_RSP, new CommitVerbHandler());
    }

    private void acceptRequestForPendingEndpoint(Verb requestVerb, Verb responseVerb, AbstractPaxosVerbHandler handler) throws Exception
    {
        // remove localhost from TM and add it back as pending
        StorageService.instance.getTokenMetadata().removeEndpoint(broadcastAddress);
        Multimap<Range<Token>, Replica> pending = HashMultimap.create();
        Range<Token> range = new Range<>(bytesToken(0), bytesToken(100));
        pending.put(range, new Replica(broadcastAddress, range, true));
        StorageService.instance.getTokenMetadata().setPendingRangesUnsafe(KEYSPACE, pending);

        ListenableFuture<MessageDelivery> messageSink = registerOutgoingMessageSink();
        int messageId = randomInt();
        int key = 50;
        Commit commit = commit(key);
        handler.doVerb(Message.builder(requestVerb, commit).from(node1).withId(messageId).build());
        getAndVerifyResponse(messageSink, responseVerb, messageId, false, false);
    }

    @Test
    public void rejectPrepareForTokenOutOfRange() throws Exception
    {
        rejectRequestForTokenOutOfRange(Verb.PAXOS_PREPARE_REQ, Verb.FAILURE_RSP, new PrepareVerbHandler());
    }

    @Test
    public void rejectProposeForTokenOutOfRange() throws Exception
    {
        rejectRequestForTokenOutOfRange(Verb.PAXOS_PROPOSE_REQ, Verb.FAILURE_RSP, new ProposeVerbHandler());
    }

    @Test
    public void rejectCommitForTokenOutOfRange() throws Exception
    {
        rejectRequestForTokenOutOfRange(Verb.PAXOS_COMMIT_REQ, Verb.FAILURE_RSP, new CommitVerbHandler());
    }

    private void rejectRequestForTokenOutOfRange(Verb requestVerb, Verb responseVerb, AbstractPaxosVerbHandler handler) throws Exception
    {
        // reject a commit for a token the node neither owns nor is pending
        ListenableFuture<MessageDelivery> messageSink = registerOutgoingMessageSink();
        int messageId = randomInt();
        int key = 200;
        Commit commit = commit(key);
        handler.doVerb(Message.builder(requestVerb, commit).from(node1).withId(messageId).build());
        getAndVerifyResponse(messageSink, responseVerb, messageId, true, true);
    }

    @Test
    public void acceptPrepareIfRejectionNotEnabled() throws Exception
    {
        acceptRequestIfRejectionNotEnabled(Verb.PAXOS_PREPARE_REQ, Verb.PAXOS_PREPARE_RSP, new PrepareVerbHandler());
    }

    @Test
    public void acceptProposeIfRejectionNotEnabled() throws Exception
    {
        acceptRequestIfRejectionNotEnabled(Verb.PAXOS_PROPOSE_REQ, Verb.PAXOS_PROPOSE_RSP, new ProposeVerbHandler());
    }

    @Test
    public void acceptCommitIfRejectionNotEnabled() throws Exception
    {
        acceptRequestIfRejectionNotEnabled(Verb.PAXOS_COMMIT_REQ, Verb.PAXOS_COMMIT_RSP, new CommitVerbHandler());
    }

    private void acceptRequestIfRejectionNotEnabled(Verb requestVerb, Verb responseVerb, AbstractPaxosVerbHandler handler) throws Exception
    {
        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        ListenableFuture<MessageDelivery> messageSink = registerOutgoingMessageSink();
        int messageId = randomInt();
        int key = 200;
        Commit commit = commit(key);
        handler.doVerb(Message.builder(requestVerb, commit).from(node1).withId(messageId).build());
        getAndVerifyResponse(messageSink, responseVerb, messageId, true, false);
    }

    private void getAndVerifyResponse(ListenableFuture<MessageDelivery> messageSink,
                                      Verb verb,
                                      int messageId,
                                      boolean isOutOfRange,
                                      boolean expectFailure) throws InterruptedException, ExecutionException, TimeoutException
    {
        MessageDelivery response = messageSink.get(100, TimeUnit.MILLISECONDS);
        assertEquals(verb, response.message.verb());
        Assert.assertEquals(broadcastAddress, response.message.from());
        assertEquals(expectFailure, response.message.payload instanceof RequestFailureReason);
        assertEquals(messageId, response.message.id());
        Assert.assertEquals(node1, response.to);
        assertEquals(startingTotalMetricCount + (isOutOfRange ? 1 : 0), StorageMetrics.totalOpsForInvalidToken.getCount());
        assertEquals(startingKeyspaceMetricCount + (isOutOfRange ? 1 : 0), keyspaceMetricValue());
    }

    private static Commit commit(int key)
    {
        TableMetadata tmd = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        UUID ballot = UUIDGen.getRandomTimeUUIDFromMicros(FBUtilities.timestampMicros());
        return Commit.newPrepare(key(tmd, key), tmd, ballot);
    }

    private static long keyspaceMetricValue()
    {
        return Keyspace.open(KEYSPACE).metric.outOfRangeTokenPaxosRequests.getCount();
    }
}
