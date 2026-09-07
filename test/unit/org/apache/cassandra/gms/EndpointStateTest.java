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

package org.apache.cassandra.gms;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EndpointStateTest
{
    public volatile VersionedValue.VersionedValueFactory valueFactory =
        new VersionedValue.VersionedValueFactory(DatabaseDescriptor.getPartitioner());

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testMultiThreadedReadConsistency() throws InterruptedException
    {
        for (int i = 0; i < 500; i++)
            innerTestMultiThreadedReadConsistency();
    }

    /**
     * Test that a thread reading values whilst they are updated by another thread will
     * not see an entry unless it sees the entry previously added as well, even though
     * we are accessing the map via an iterator backed by the underlying map. This
     * works because EndpointState copies the map each time values are added.
     */
    private void innerTestMultiThreadedReadConsistency() throws InterruptedException
    {
        final Token token = DatabaseDescriptor.getPartitioner().getRandomToken();
        final List<Token> tokens = Collections.singletonList(token);
        final HeartBeatState hb = new HeartBeatState(0);
        final EndpointState state = new EndpointState(hb);
        final AtomicInteger numFailures = new AtomicInteger();

        Thread t1 = new Thread(new Runnable()
        {
            public void run()
            {
                state.addApplicationState(ApplicationState.TOKENS, valueFactory.tokens(tokens));
                state.addApplicationState(ApplicationState.STATUS_WITH_PORT, valueFactory.normal(tokens));
            }
        });

        Thread t2 = new Thread(new Runnable()
        {
            public void run()
            {
                for (int i = 0; i < 50; i++)
                {
                    Map<ApplicationState, VersionedValue> values = new EnumMap<>(ApplicationState.class);
                    for (Map.Entry<ApplicationState, VersionedValue> entry : state.states())
                        values.put(entry.getKey(), entry.getValue());

                    if (values.containsKey(ApplicationState.STATUS_WITH_PORT) && !values.containsKey(ApplicationState.TOKENS))
                    {
                        numFailures.incrementAndGet();
                        System.out.println(String.format("Failed: %s", values));
                    }
                }
            }
        });

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        assertTrue(numFailures.get() == 0);
    }

    @Test
    public void testMultiThreadWriteConsistency() throws InterruptedException, UnknownHostException
    {
        for (int i = 0; i < 500; i++)
            innerTestMultiThreadWriteConsistency();
    }

    /**
     * Test that two threads can update the state map concurrently.
     */
    private void innerTestMultiThreadWriteConsistency() throws InterruptedException, UnknownHostException
    {
        final Token token = DatabaseDescriptor.getPartitioner().getRandomToken();
        final List<Token> tokens = Collections.singletonList(token);
        final InetAddress ip = InetAddress.getByAddress(null, new byte[] { 127, 0, 0, 1});
        final UUID hostId = UUID.randomUUID();
        final HeartBeatState hb = new HeartBeatState(0);
        final EndpointState state = new EndpointState(hb);

        Thread t1 = new Thread(new Runnable()
        {
            public void run()
            {
                Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
                states.put(ApplicationState.TOKENS, valueFactory.tokens(tokens));
                states.put(ApplicationState.STATUS_WITH_PORT, valueFactory.normal(tokens));
                state.addApplicationStates(states);
            }
        });

        Thread t2 = new Thread(new Runnable()
        {
            public void run()
            {
                Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
                states.put(ApplicationState.INTERNAL_IP, valueFactory.internalIP(ip));
                states.put(ApplicationState.HOST_ID, valueFactory.hostId(hostId));
                state.addApplicationStates(states);
            }
        });

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        Set<Map.Entry<ApplicationState, VersionedValue>> states = state.states();
        assertEquals(4, states.size());

        Map<ApplicationState, VersionedValue> values = new EnumMap<>(ApplicationState.class);
        for (Map.Entry<ApplicationState, VersionedValue> entry : states)
            values.put(entry.getKey(), entry.getValue());

        assertTrue(values.containsKey(ApplicationState.STATUS_WITH_PORT));
        assertTrue(values.containsKey(ApplicationState.TOKENS));
        assertTrue(values.containsKey(ApplicationState.INTERNAL_IP));
        assertTrue(values.containsKey(ApplicationState.HOST_ID));
    }

    /**
     * TOKENS is stored as a serialized token collection wrapped in an ISO-8859-1 string purely to round-trip
     * the raw bytes losslessly, and toString() must not print that raw data (see CASSANDRA-21417).
     */
    @Test
    public void testToStringDoesNotLeakRawTokenBytes()
    {
        List<Token> tokens = new ArrayList<>();
        for (int i = 0; i < 16; i++)
            tokens.add(DatabaseDescriptor.getPartitioner().getRandomToken());

        HeartBeatState hb = new HeartBeatState(0);
        EndpointState state = new EndpointState(hb);
        VersionedValue tokensValue = valueFactory.tokens(tokens);
        state.addApplicationState(ApplicationState.TOKENS, tokensValue);
        state.addApplicationState(ApplicationState.RELEASE_VERSION, valueFactory.releaseVersion());

        String rendered = state.toString();

        assertTrue(rendered.contains("TOKENS=Value(<16 tokens>," + tokensValue.version + ')'));
        assertFalse(rendered.contains(tokensValue.value));
        // other states still render normally
        assertTrue(rendered.contains("RELEASE_VERSION=Value("));
    }

    /**
     * If the TOKENS value can't be deserialized for some reason, toString() must still avoid printing the
     * raw bytes rather than throwing. Uses a truncated-but-otherwise-valid length-prefixed token blob (claims
     * a 5 byte token but only supplies 2) so deserialization fails with a plain EOFException, rather than
     * arbitrary garbage bytes whose first 4 bytes can decode to a huge length prefix and make
     * TokenSerializer attempt a multi-gigabyte array allocation (see CASSANDRA-21417 discussion).
     */
    @Test
    public void testToStringHandlesUndecodableTokensValue()
    {
        HeartBeatState hb = new HeartBeatState(0);
        EndpointState state = new EndpointState(hb);
        byte[] truncatedTokenBytes = { 0, 0, 0, 5, 'a', 'b' }; // claims a 5-byte token, only 2 bytes follow
        String truncatedTokenBlob = new String(truncatedTokenBytes, ISO_8859_1);
        state.addApplicationState(ApplicationState.TOKENS, VersionedValue.unsafeMakeVersionedValue(truncatedTokenBlob, 1));

        String rendered = state.toString();

        assertTrue(rendered.contains("TOKENS=Value(<6 undecodable bytes>,1)"));
    }
}
