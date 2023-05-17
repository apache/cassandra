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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.paxos.v1.AbstractPaxosVerbHandler;
import org.apache.cassandra.service.paxos.v1.PrepareVerbHandler;
import org.apache.cassandra.service.paxos.v1.ProposeVerbHandler;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper.*;
import static org.junit.Assert.assertEquals;

public class PaxosVerbHandlerOutOfRangeTest // PaxosV1 out of range tests - V2 implements OOTR checks at the protocol level
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
        ServerTestUtils.prepareServerNoRegister();
        SchemaLoader.schemaDefinition(TEST_NAME);
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

    private void acceptRequestForNaturalEndpoint(Verb requestVerb, Verb responseVerb, AbstractPaxosVerbHandler handler) throws Exception
    {
        ListenableFuture<MessageDelivery> messageSink = registerOutgoingMessageSink();
        int messageId = randomInt();
        int key = 50;
        Commit commit = commit(key);
        handler.doVerb(Message.builder(requestVerb, commit).from(node1).withId(messageId).build());
        getAndVerifyResponse(messageSink, responseVerb, messageId, false);
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

    private void acceptRequestForPendingEndpoint(Verb requestVerb, Verb responseVerb, AbstractPaxosVerbHandler handler) throws Exception
    {
        // reset ClusterMetadata then join the remote node and partially
        // join the localhost one to emulate pending ranges
        ServerTestUtils.resetCMS();
        ClusterMetadataTestHelper.addEndpoint(node1, bytesToken(0));
        ClusterMetadataTestHelper.register(broadcastAddress);
        ClusterMetadataTestHelper.joinPartially(broadcastAddress, bytesToken(100));
        assertEquals(NodeState.BOOTSTRAPPING, ClusterMetadata.current().directory.peerState(broadcastAddress));

        ListenableFuture<MessageDelivery> messageSink = registerOutgoingMessageSink();
        int messageId = randomInt();
        int key = 50;
        Commit commit = commit(key);
        handler.doVerb(Message.builder(requestVerb, commit).from(node1).withId(messageId).build());
        getAndVerifyResponse(messageSink, responseVerb, messageId, false);
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

    private void rejectRequestForTokenOutOfRange(Verb requestVerb, Verb responseVerb, AbstractPaxosVerbHandler handler) throws Exception
    {
        // reject a commit for a token the node neither owns nor is pending
        ListenableFuture<MessageDelivery> messageSink = registerOutgoingMessageSink();
        int messageId = randomInt();
        int key = 200;
        Commit commit = commit(key);
        handler.doVerb(Message.builder(requestVerb, commit).from(node1).withId(messageId).build());
        getAndVerifyResponse(messageSink, responseVerb, messageId, true);
    }

    private void getAndVerifyResponse(ListenableFuture<MessageDelivery> messageSink,
                                      Verb verb,
                                      int messageId,
                                      boolean isOutOfRange) throws InterruptedException, ExecutionException, TimeoutException
    {
        MessageDelivery response = messageSink.get(100, TimeUnit.MILLISECONDS);
        assertEquals(verb, response.message.verb());
        Assert.assertEquals(broadcastAddress, response.message.from());
        assertEquals(isOutOfRange, response.message.payload instanceof RequestFailureReason);
        assertEquals(messageId, response.message.id());
        Assert.assertEquals(node1, response.to);
        assertEquals(startingTotalMetricCount + (isOutOfRange ? 1 : 0), StorageMetrics.totalOpsForInvalidToken.getCount());
        assertEquals(startingKeyspaceMetricCount + (isOutOfRange ? 1 : 0), keyspaceMetricValue());
    }

    private static Commit commit(int key)
    {
        TableMetadata tmd = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        return Commit.newPrepare(key(tmd, key), tmd, BallotGenerator.Global.nextBallot(Ballot.Flag.NONE));
    }

    private static long keyspaceMetricValue()
    {
        return Keyspace.open(KEYSPACE).metric.outOfRangeTokenPaxosRequests.getCount();
    }
}
