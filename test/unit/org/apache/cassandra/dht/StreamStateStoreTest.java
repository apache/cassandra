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
package org.apache.cassandra.dht;

import java.util.Collections;

import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.streaming.async.NettyStreamingConnectionFactory;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.StreamEvent;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.net.MessagingService.current_version;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StreamStateStoreTest
{

    @BeforeClass
    public static void initDD()
    {
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
    }

    @Test
    public void testUpdateAndQueryAvailableRanges()
    {
        // let range (0, 100] of keyspace1 be bootstrapped.
        IPartitioner p = new Murmur3Partitioner();
        Token.TokenFactory factory = p.getTokenFactory();
        Range<Token> range = new Range<>(factory.fromString("0"), factory.fromString("100"));

        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();
        StreamSession session = new StreamSession(StreamOperation.BOOTSTRAP, local, new NettyStreamingConnectionFactory(), null, current_version, false, 0, null, PreviewKind.NONE);
        session.addStreamRequest("keyspace1", RangesAtEndpoint.toDummyList(Collections.singleton(range)), RangesAtEndpoint.toDummyList(Collections.emptyList()), Collections.singleton("cf"));

        StreamStateStore store = new StreamStateStore();
        // session complete event that is not completed makes data not available for keyspace/ranges
        store.handleStreamEvent(new StreamEvent.SessionCompleteEvent(session));
        assertFalse(store.isDataAvailable("keyspace1", factory.fromString("50")));

        // successfully completed session adds available keyspace/ranges
        session.state(StreamSession.State.COMPLETE);
        store.handleStreamEvent(new StreamEvent.SessionCompleteEvent(session));
        // check if token in range (0, 100] appears available.
        assertTrue(store.isDataAvailable("keyspace1", factory.fromString("50")));
        // check if token out of range returns false
        assertFalse(store.isDataAvailable("keyspace1", factory.fromString("0")));
        assertFalse(store.isDataAvailable("keyspace1", factory.fromString("101")));
        // check if different keyspace returns false
        assertFalse(store.isDataAvailable("keyspace2", factory.fromString("50")));

        // add different range within the same keyspace
        Range<Token> range2 = new Range<>(factory.fromString("100"), factory.fromString("200"));
        session = new StreamSession(StreamOperation.BOOTSTRAP, local, new NettyStreamingConnectionFactory(), null, current_version,false, 0, null, PreviewKind.NONE);
        session.addStreamRequest("keyspace1", RangesAtEndpoint.toDummyList(Collections.singleton(range2)), RangesAtEndpoint.toDummyList(Collections.emptyList()), Collections.singleton("cf"));
        session.state(StreamSession.State.COMPLETE);
        store.handleStreamEvent(new StreamEvent.SessionCompleteEvent(session));

        // newly added range should be available
        assertTrue(store.isDataAvailable("keyspace1", factory.fromString("101")));
        // as well as the old one
        assertTrue(store.isDataAvailable("keyspace1", factory.fromString("50")));
    }
}
