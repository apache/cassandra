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

import static org.junit.Assert.assertEquals;
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
                state.addApplicationState(ApplicationState.STATUS, valueFactory.normal(tokens));
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

                    if (values.containsKey(ApplicationState.STATUS) && !values.containsKey(ApplicationState.TOKENS))
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
    public void testMultiThreadWriteConsistency() throws InterruptedException
    {
        for (int i = 0; i < 500; i++)
            innerTestMultiThreadWriteConsistency();
    }

    /**
     * Test that two threads can update the state map concurrently.
     */
    private void innerTestMultiThreadWriteConsistency() throws InterruptedException
    {
        final Token token = DatabaseDescriptor.getPartitioner().getRandomToken();
        final List<Token> tokens = Collections.singletonList(token);
        final String ip = "127.0.0.1";
        final UUID hostId = UUID.randomUUID();
        final HeartBeatState hb = new HeartBeatState(0);
        final EndpointState state = new EndpointState(hb);

        Thread t1 = new Thread(new Runnable()
        {
            public void run()
            {
                Map<ApplicationState, VersionedValue> states = new EnumMap<>(ApplicationState.class);
                states.put(ApplicationState.TOKENS, valueFactory.tokens(tokens));
                states.put(ApplicationState.STATUS, valueFactory.normal(tokens));
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

        assertTrue(values.containsKey(ApplicationState.STATUS));
        assertTrue(values.containsKey(ApplicationState.TOKENS));
        assertTrue(values.containsKey(ApplicationState.INTERNAL_IP));
        assertTrue(values.containsKey(ApplicationState.HOST_ID));
    }
}
