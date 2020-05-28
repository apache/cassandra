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

package org.apache.cassandra.streaming;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.messages.StreamMessage;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.apache.cassandra.streaming.StreamTestUtils.session;
import static org.apache.cassandra.streaming.messages.StreamMessage.Type.COMPLETE;
import static org.apache.cassandra.streaming.messages.StreamMessage.Type.PREPARE_SYNACK;
import static org.apache.cassandra.utils.TokenRangeTestUtil.generateRangesAtEndpoint;
import static org.apache.cassandra.utils.TokenRangeTestUtil.node1;
import static org.apache.cassandra.utils.TokenRangeTestUtil.setLocalTokens;
import static org.apache.cassandra.utils.TokenRangeTestUtil.token;

public class StreamSessionOwnedRangesTest
{
    private static final String TEST_NAME = "strmsn_owned_ranges_test_";
    private static final String KEYSPACE = TEST_NAME + "cql_keyspace";
    private static final String TABLE = "table1";

    @BeforeClass
    public static void setupClass() throws Exception
    {
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
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
        // All tests suppose a 2 node ring, with the other peer having the tokens 0, 200, 300
        // Initially, the local node has no tokens so when indivividual test set owned tokens or
        // pending ranges for the local node, they're always in relation to this.
        // e.g. test calls setLocalTokens(100, 300) the ring now looks like
        // peer  -> (min, 0], (100, 200], (300, 400]
        // local -> (0, 100], (200, 300], (400, max]
        //
        // Pending ranges are set in test using start/end pairs.
        // Ring is initialised:
        // peer  -> (min, max]
        // local -> (,]
        // e.g. test calls setPendingRanges(0, 100, 200, 300)
        // the pending ranges for local would be calculated as:
        // local -> (0, 100], (200, 300]
        StorageService.instance.getTokenMetadata().updateNormalTokens(Lists.newArrayList(token(0),
                                                                                         token(200),
                                                                                         token(400)),
                                                                      node1);
    }

    @Test
    public void testPrepareWithAllRequestedRangesWithinOwned()
    {
        setLocalTokens(100);
        InetAddressAndPort endpoint = FBUtilities.getBroadcastAddressAndPort();
        Collection<StreamRequest> requests = streamRequests(generateRangesAtEndpoint(endpoint, 0, 10, 70, 80),
                                                            RangesAtEndpoint.empty(endpoint));

        // prepare request should succeed with or without rejection enabled
        tryPrepareExpectingSuccess(requests, false);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryPrepareExpectingSuccess(requests, false);
    }

    @Test
    public void testPrepareWithAllRequestedRangesOutsideOwned()
    {
        setLocalTokens(100);
        InetAddressAndPort endpoint = FBUtilities.getBroadcastAddressAndPort();

        Collection<StreamRequest> requests = streamRequests(generateRangesAtEndpoint(endpoint, -20, -10, 110, 120, 310, 320),
                                                            RangesAtEndpoint.empty(endpoint));

        tryPrepareExpectingFailure(requests);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryPrepareExpectingSuccess(requests, true);
    }

    @Test
    public void testPrepareWithSomeRequestedRangesOutsideOwned()
    {
        setLocalTokens(100);
        InetAddressAndPort endpoint = FBUtilities.getBroadcastAddressAndPort();

        Collection<StreamRequest> requests = streamRequests(generateRangesAtEndpoint(endpoint, -20, -10, 30, 40, 310, 320),
                                                            RangesAtEndpoint.empty(endpoint));

        tryPrepareExpectingFailure(requests);

        DatabaseDescriptor.setRejectOutOfTokenRangeRequests(false);
        tryPrepareExpectingSuccess(requests, true);
    }

    private static void tryPrepareExpectingSuccess(Collection<StreamRequest> requests, boolean isOutOfRange)
    {
        StreamSession session = session();

        StreamTestUtils.StubMessageSender handler = (StreamTestUtils.StubMessageSender) session.getMessageSender();
        handler.reset();
        long startMetricCount = StorageMetrics.totalOpsForInvalidToken.getCount();

        session.state(StreamSession.State.PREPARING);
        session.prepareAsync(requests, Collections.emptySet());

        List<StreamMessage> sent = handler.sentMessages;
        assertEquals(2, sent.size());
        assertEquals(PREPARE_SYNACK, sent.get(0).type);
        assertEquals(COMPLETE, sent.get(1).type);

        assertEquals(startMetricCount + (isOutOfRange ? 1 : 0), StorageMetrics.totalOpsForInvalidToken.getCount());
    }

    private static void tryPrepareExpectingFailure(Collection<StreamRequest> requests)
    {
        StreamSession session = session();
        StreamTestUtils.StubMessageSender handler = (StreamTestUtils.StubMessageSender) session.getMessageSender();
        handler.reset();
        long startMetricCount = StorageMetrics.totalOpsForInvalidToken.getCount();
        try
        {
            session.state(StreamSession.State.PREPARING);
            session.prepareAsync(requests, Collections.emptySet());
            fail("Expected StreamRequestOfTokenRangeException");
        }
        catch (StreamRequestOutOfTokenRangeException e)
        {
            // expected
        }
        assertTrue(handler.sentMessages.isEmpty());
        assertEquals(startMetricCount + 1, StorageMetrics.totalOpsForInvalidToken.getCount());
    }

    private static Collection<StreamRequest> streamRequests(RangesAtEndpoint fullRanges,
                                                            RangesAtEndpoint transientRanges)
    {
        return Collections.singleton(new StreamRequest(KEYSPACE,
                                                       fullRanges,
                                                       transientRanges,
                                                       Collections.singleton(TABLE)));

    }
}
