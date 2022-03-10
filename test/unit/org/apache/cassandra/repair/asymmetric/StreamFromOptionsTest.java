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

package org.apache.cassandra.repair.asymmetric;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Iterables;
import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StreamFromOptionsTest
{
    @Test
    public void addAllDiffingTest() throws UnknownHostException
    {
        StreamFromOptions sfo = new StreamFromOptions(new MockDiffs(true), range(0, 10));
        Set<InetAddressAndPort> toAdd = new HashSet<>();
        toAdd.add(InetAddressAndPort.getByName("127.0.0.1"));
        toAdd.add(InetAddressAndPort.getByName("127.0.0.2"));
        toAdd.add(InetAddressAndPort.getByName("127.0.0.3"));
        toAdd.forEach(sfo::add);

        // if all added have differences, each set will contain a single host
        assertEquals(3, Iterables.size(sfo.allStreams()));
        Set<InetAddressAndPort> allStreams = new HashSet<>();
        for (Set<InetAddressAndPort> streams : sfo.allStreams())
        {
            assertEquals(1, streams.size());
            allStreams.addAll(streams);
        }
        assertEquals(toAdd, allStreams);
    }

    @Test
    public void addAllMatchingTest() throws UnknownHostException
    {
        StreamFromOptions sfo = new StreamFromOptions(new MockDiffs(false), range(0, 10));
        Set<InetAddressAndPort> toAdd = new HashSet<>();
        toAdd.add(InetAddressAndPort.getByName("127.0.0.1"));
        toAdd.add(InetAddressAndPort.getByName("127.0.0.2"));
        toAdd.add(InetAddressAndPort.getByName("127.0.0.3"));
        toAdd.forEach(sfo::add);

        // if all added match, the set will contain all hosts
        assertEquals(1, Iterables.size(sfo.allStreams()));
        assertEquals(toAdd, sfo.allStreams().iterator().next());
    }

    @Test
    public void splitTest() throws UnknownHostException
    {
        splitTestHelper(true);
        splitTestHelper(false);
    }

    private void splitTestHelper(boolean diffing) throws UnknownHostException
    {
        StreamFromOptions sfo = new StreamFromOptions(new MockDiffs(diffing), range(0, 10));
        Set<InetAddressAndPort> toAdd = new HashSet<>();
        toAdd.add(InetAddressAndPort.getByName("127.0.0.1"));
        toAdd.add(InetAddressAndPort.getByName("127.0.0.2"));
        toAdd.add(InetAddressAndPort.getByName("127.0.0.3"));
        toAdd.forEach(sfo::add);
        StreamFromOptions sfo1 = sfo.copy(range(0, 5));
        StreamFromOptions sfo2 = sfo.copy(range(5, 10));
        assertEquals(range(0, 10), sfo.range);
        assertEquals(range(0, 5), sfo1.range);
        assertEquals(range(5, 10), sfo2.range);
        assertTrue(Iterables.elementsEqual(sfo1.allStreams(), sfo2.allStreams()));
        // verify the backing set is not shared between the copies:
        sfo1.add(InetAddressAndPort.getByName("127.0.0.4"));
        sfo2.add(InetAddressAndPort.getByName("127.0.0.5"));
        assertFalse(Iterables.elementsEqual(sfo1.allStreams(), sfo2.allStreams()));
    }

    private Range<Token> range(long left, long right)
    {
        return new Range<>(new Murmur3Partitioner.LongToken(left), new Murmur3Partitioner.LongToken(right));
    }

    private static class MockDiffs extends DifferenceHolder
    {
        private final boolean hasDifference;

        public MockDiffs(boolean hasDifference)
        {
            super(Collections.emptyMap());
            this.hasDifference = hasDifference;
        }

        @Override
        public boolean hasDifferenceBetween(InetAddressAndPort node1, InetAddressAndPort node2, Range<Token> range)
        {
            return hasDifference;
        }
    }
}
