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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.netty.util.concurrent.Future;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.streaming.messages.SessionFailedMessage;
import org.apache.cassandra.streaming.messages.StreamMessage;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

import static org.apache.cassandra.config.CassandraRelevantProperties.ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION;
import static org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper.*;
import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.streaming.StreamingChannel.Factory.Global.streamingFactory;
import static org.apache.cassandra.streaming.messages.StreamMessage.Type.*;
import static org.apache.cassandra.tcm.ownership.OwnershipUtils.generateRangesAtEndpoint;
import static org.apache.cassandra.tcm.ownership.OwnershipUtils.setLocalTokens;
import static org.apache.cassandra.tcm.ownership.OwnershipUtils.token;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class StreamSessionOwnedRangesTest
{
    private static final String TEST_NAME = "stream_owned_ranges_test_";
    private static final String KEYSPACE = TEST_NAME + "cql_keyspace";
    private static final String TABLE = "table1";

    static
    {
        ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION.setBoolean(true);
    }

    @BeforeClass
    public static void setupClass() throws Exception
    {
        SchemaLoader.loadSchema();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ServerTestUtils.recreateCMS();
        SchemaLoader.schemaDefinition(TEST_NAME);
        ClusterMetadataTestHelper.register(broadcastAddress);
        ServerTestUtils.markCMS();
        StorageService.instance.unsafeSetInitialized();
    }

    @Before
    public void setup() throws Exception
    {
        ServerTestUtils.resetCMS();
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
        ClusterMetadataTestHelper.addEndpoint(node1, Lists.newArrayList(token(0), token(200), token(400)));
    }

    @Test
    public void testPrepareWithAllRequestedRangesWithinOwned()
    {
        setLocalTokens(100);
        InetAddressAndPort endpoint = FBUtilities.getBroadcastAddressAndPort();
        Collection<StreamRequest> requests = streamRequests(generateRangesAtEndpoint(endpoint, 0, 10, 70, 80),
                                                            RangesAtEndpoint.empty(endpoint));

        tryPrepareExpectingSuccess(requests);
    }

    @Test
    public void testPrepareWithAllRequestedRangesOutsideOwned() throws Exception
    {
        setLocalTokens(100);
        InetAddressAndPort endpoint = FBUtilities.getBroadcastAddressAndPort();

        Collection<StreamRequest> requests = streamRequests(generateRangesAtEndpoint(endpoint, -20, -10, 110, 120, 310, 320),
                                                            RangesAtEndpoint.empty(endpoint));

        tryPrepareExpectingFailure(requests);
    }

    @Test
    public void testPrepareWithSomeRequestedRangesOutsideOwned() throws Exception
    {
        setLocalTokens(100);
        InetAddressAndPort endpoint = FBUtilities.getBroadcastAddressAndPort();

        Collection<StreamRequest> requests = streamRequests(generateRangesAtEndpoint(endpoint, -20, -10, 30, 40, 310, 320),
                                                            RangesAtEndpoint.empty(endpoint));

        tryPrepareExpectingFailure(requests);
    }

    private static void tryPrepareExpectingSuccess(Collection<StreamRequest> requests)
    {
        final List<StreamMessage> sent = new ArrayList<>();
        StreamSession session = session(sent);

        sent.clear();
        long startMetricCount = StorageMetrics.totalOpsForInvalidToken.getCount();

        session.state(StreamSession.State.PREPARING);
        session.prepareAsync(requests, Collections.emptySet());

        assertEquals(2, sent.size());
        assertEquals(PREPARE_SYNACK, sent.get(0).type);
        assertEquals(COMPLETE, sent.get(1).type);

        assertEquals(startMetricCount, StorageMetrics.totalOpsForInvalidToken.getCount());
    }

    private static void tryPrepareExpectingFailure(Collection<StreamRequest> requests) throws Exception
    {
        final List<StreamMessage> sent = new ArrayList<>();
        StreamSession session = session(sent);
        sent.clear();
        long startMetricCount = StorageMetrics.totalOpsForInvalidToken.getCount();

        session.state(StreamSession.State.PREPARING);
        java.util.concurrent.Future<Exception> f = session.prepare(requests, Collections.emptySet());
        Exception ex = f.get();
        assertNotNull(ex);
        if (!(ex instanceof StreamRequestOutOfTokenRangeException))
        {   // Unexpected exception
            throw ex;
        }

        // make sure we sent a SessionFailedMessage
        assertEquals(1, sent.size());
        for (StreamMessage msg : sent)
            assertTrue(msg instanceof SessionFailedMessage);
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

    static StreamSession session(List<StreamMessage> sentMessages)
    {
        StreamCoordinator streamCoordinator = new StreamCoordinator(StreamOperation.REPAIR, 1, streamingFactory(), false, false, null, PreviewKind.NONE);
        StreamResultFuture future = StreamResultFuture.createInitiator(nextTimeUUID(), StreamOperation.REPAIR, Collections.<StreamEventHandler>emptyList(), streamCoordinator);

        StreamSession session = new StreamSession(StreamOperation.REPAIR,
                                                  node1,
                                                  StreamReaderTest.channelFactory,
                                                  null,
                                                  current_version,
                                                  true,
                                                  0,
                                                  null,
                                                  PreviewKind.NONE)
        {
            @Override
            public void progress(String filename, ProgressInfo.Direction direction, long bytes, long delta, long total)
            {
                //no-op
            }

            @Override
            protected Future<?> sendControlMessage(StreamMessage message)
            {
                sentMessages.add(message);
                return ImmediateFuture.success(null);
            }

        };
        session.init(future);
        return session;
    }
}
