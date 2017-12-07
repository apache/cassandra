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


import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.Test;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReduceHelperTest
{
    private static final InetAddress[] addresses;
    private static final InetAddress A;
    private static final InetAddress B;
    private static final InetAddress C;
    private static final InetAddress D;
    private static final InetAddress E;

    static
    {
        try
        {
            A = InetAddress.getByName("127.0.0.0");
            B = InetAddress.getByName("127.0.0.1");
            C = InetAddress.getByName("127.0.0.2");
            D = InetAddress.getByName("127.0.0.3");
            E = InetAddress.getByName("127.0.0.4");
            // for diff creation in loops:
            addresses = new InetAddress[]{ A, B, C, D, E };
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testSimpleReducing()
    {
        /*
        A == B and D == E =>
        A streams from C, {D, E} since D==E
        B streams from C, {D, E} since D==E
        C streams from {A, B}, {D, E} since A==B and D==E
        D streams from {A, B}, C since A==B
        E streams from {A, B}, C since A==B

          A   B   C   D   E
        A     =   x   x   x
        B         x   x   x
        C             x   x
        D                 =
         */
        Map<InetAddress, HostDifferences> differences = new HashMap<>();
        for (int i = 0; i < 4; i++)
        {
            HostDifferences hostDiffs = new HostDifferences();
            for (int j = i + 1; j < 5; j++)
            {
                // no diffs between A, B and D, E:
                if (addresses[i] == A && addresses[j] == B || addresses[i] == D && addresses[j] == E)
                    continue;
                List<Range<Token>> diff = list(new Range<>(new Murmur3Partitioner.LongToken(0), new Murmur3Partitioner.LongToken(10)));
                hostDiffs.add(addresses[j], diff);
            }
            differences.put(addresses[i], hostDiffs);

        }
        DifferenceHolder differenceHolder = new DifferenceHolder(differences);
        Map<InetAddress, IncomingRepairStreamTracker> tracker = ReduceHelper.createIncomingRepairStreamTrackers(differenceHolder);

        assertEquals(set(set(C), set(E,D)), streams(tracker.get(A)));
        assertEquals(set(set(C), set(E,D)), streams(tracker.get(B)));
        assertEquals(set(set(A,B), set(E,D)), streams(tracker.get(C)));
        assertEquals(set(set(A,B), set(C)), streams(tracker.get(D)));
        assertEquals(set(set(A,B), set(C)), streams(tracker.get(E)));

        ImmutableMap<InetAddress, HostDifferences> reduced = ReduceHelper.reduce(differenceHolder, (x,y) -> y);

        HostDifferences n0 = reduced.get(A);
        assertEquals(0, n0.get(A).size());
        assertEquals(0, n0.get(B).size());
        assertTrue(n0.get(C).size() > 0);
        assertStreamFromEither(n0.get(D), n0.get(E));

        HostDifferences n1 = reduced.get(B);
        assertEquals(0, n1.get(A).size());
        assertEquals(0, n1.get(B).size());
        assertTrue(n1.get(C).size() > 0);
        assertStreamFromEither(n1.get(D), n1.get(E));

        HostDifferences n2 = reduced.get(C);
        // we are either streaming from node 0 or node 1, not both:
        assertStreamFromEither(n2.get(A), n2.get(B));
        assertEquals(0, n2.get(C).size());
        assertStreamFromEither(n2.get(D), n2.get(E));

        HostDifferences n3 = reduced.get(D);
        assertStreamFromEither(n3.get(A), n3.get(B));
        assertTrue(n3.get(C).size() > 0);
        assertEquals(0, n3.get(D).size());
        assertEquals(0, n3.get(E).size());

        HostDifferences n4 = reduced.get(E);
        assertStreamFromEither(n4.get(A), n4.get(B));
        assertTrue(n4.get(C).size() > 0);
        assertEquals(0, n4.get(D).size());
        assertEquals(0, n4.get(E).size());
    }

    @Test
    public void testSimpleReducingWithPreferedNodes()
    {
        /*
        A == B and D == E =>
        A streams from C, {D, E} since D==E
        B streams from C, {D, E} since D==E
        C streams from {A, B}, {D, E} since A==B and D==E
        D streams from {A, B}, C since A==B
        E streams from {A, B}, C since A==B

          A   B   C   D   E
        A     =   x   x   x
        B         x   x   x
        C             x   x
        D                 =
         */
        Map<InetAddress, HostDifferences> differences = new HashMap<>();
        for (int i = 0; i < 4; i++)
        {
            HostDifferences hostDifferences = new HostDifferences();
            for (int j = i + 1; j < 5; j++)
            {
                // no diffs between A, B and D, E:
                if (addresses[i] == A && addresses[j] == B || addresses[i] == D && addresses[j] == E)
                    continue;
                List<Range<Token>> diff = list(new Range<>(new Murmur3Partitioner.LongToken(0), new Murmur3Partitioner.LongToken(10)));
                hostDifferences.add(addresses[j], diff);
            }
            differences.put(addresses[i], hostDifferences);
        }

        DifferenceHolder differenceHolder = new DifferenceHolder(differences);
        Map<InetAddress, IncomingRepairStreamTracker> tracker = ReduceHelper.createIncomingRepairStreamTrackers(differenceHolder);
        assertEquals(set(set(C), set(E, D)), streams(tracker.get(A)));
        assertEquals(set(set(C), set(E, D)), streams(tracker.get(B)));
        assertEquals(set(set(A, B), set(E, D)), streams(tracker.get(C)));
        assertEquals(set(set(A, B), set(C)), streams(tracker.get(D)));
        assertEquals(set(set(A, B), set(C)), streams(tracker.get(E)));

        // if there is an option, never stream from node 1:
        ImmutableMap<InetAddress, HostDifferences> reduced = ReduceHelper.reduce(differenceHolder, (x,y) -> Sets.difference(y, set(B)));

        HostDifferences n0 = reduced.get(A);
        assertEquals(0, n0.get(A).size());
        assertEquals(0, n0.get(B).size());
        assertTrue(n0.get(C).size() > 0);
        assertStreamFromEither(n0.get(D), n0.get(E));

        HostDifferences n1 = reduced.get(B);
        assertEquals(0, n1.get(A).size());
        assertEquals(0, n1.get(B).size());
        assertTrue(n1.get(C).size() > 0);
        assertStreamFromEither(n1.get(D), n1.get(E));


        HostDifferences n2 = reduced.get(C);
        assertTrue(n2.get(A).size() > 0);
        assertEquals(0, n2.get(B).size());
        assertEquals(0, n2.get(C).size());
        assertStreamFromEither(n2.get(D), n2.get(E));

        HostDifferences n3 = reduced.get(D);
        assertTrue(n3.get(A).size() > 0);
        assertEquals(0, n3.get(B).size());
        assertTrue(n3.get(C).size() > 0);
        assertEquals(0, n3.get(D).size());
        assertEquals(0, n3.get(E).size());

        HostDifferences n4 = reduced.get(E);
        assertTrue(n4.get(A).size() > 0);
        assertEquals(0, n4.get(B).size());
        assertTrue(n4.get(C).size() > 0);
        assertEquals(0, n4.get(D).size());
        assertEquals(0, n4.get(E).size());
    }

    private Iterable<Set<InetAddress>> streams(IncomingRepairStreamTracker incomingRepairStreamTracker)
    {
        return incomingRepairStreamTracker.getIncoming().values().iterator().next().allStreams();
    }

    @Test
    public void testOverlapDifference()
    {
        /*
            |A     |B     |C
         ---+------+------+--------
         A  |=     |50,100|0,50
         B  |      |=     |0,100
         C  |      |      |=

         A needs to stream (50, 100] from B, (0, 50] from C
         B needs to stream (50, 100] from A, (0, 100] from C
         C needs to stream (0, 50] from A, (0, 100] from B
         A == B on (0, 50]   => C can stream (0, 50] from either A or B
         A == C on (50, 100] => B can stream (50, 100] from either A or C
         =>
         A streams (50, 100] from {B}, (0, 50] from C
         B streams (0, 50] from {C}, (50, 100] from {A, C}
         C streams (0, 50] from {A, B}, (50, 100] from B
         */
        Map<InetAddress, HostDifferences> differences = new HashMap<>();
        addDifference(A, differences, B, list(range(50, 100)));
        addDifference(A, differences, C, list(range(0, 50)));
        addDifference(B, differences, C, list(range(0, 100)));
        DifferenceHolder differenceHolder = new DifferenceHolder(differences);
        Map<InetAddress, IncomingRepairStreamTracker> tracker = ReduceHelper.createIncomingRepairStreamTrackers(differenceHolder);
        assertEquals(set(set(C)), tracker.get(A).getIncoming().get(range(0, 50)).allStreams());
        assertEquals(set(set(B)), tracker.get(A).getIncoming().get(range(50, 100)).allStreams());
        assertEquals(set(set(C)), tracker.get(B).getIncoming().get(range(0, 50)).allStreams());
        assertEquals(set(set(A,C)), tracker.get(B).getIncoming().get(range(50, 100)).allStreams());
        assertEquals(set(set(A,B)), tracker.get(C).getIncoming().get(range(0, 50)).allStreams());
        assertEquals(set(set(B)), tracker.get(C).getIncoming().get(range(50, 100)).allStreams());

        ImmutableMap<InetAddress, HostDifferences> reduced = ReduceHelper.reduce(differenceHolder, (x, y) -> y);

        HostDifferences n0 = reduced.get(A);

        assertTrue(n0.get(B).equals(list(range(50, 100))));
        assertTrue(n0.get(C).equals(list(range(0, 50))));

        HostDifferences n1 = reduced.get(B);
        assertEquals(0, n1.get(B).size());
        if (n1.get(A) != null)
        {
            assertTrue(n1.get(C).equals(list(range(0, 50))));
            assertTrue(n1.get(A).equals(list(range(50, 100))));
        }
        else
        {
            assertTrue(n1.get(C).equals(list(range(0, 50), range(50, 100))));
        }
        HostDifferences n2 = reduced.get(C);
        assertEquals(0, n2.get(C).size());
        if (n2.get(A) != null)
        {
            assertTrue(n2.get(A).equals(list(range(0,50))));
            assertTrue(n2.get(B).equals(list(range(50, 100))));
        }
        else
        {
            assertTrue(n2.get(A).equals(list(range(0, 50), range(50, 100))));
        }


    }

    @Test
    public void testOverlapDifference2()
    {
        /*
            |A               |B               |C
         ---+----------------+----------------+------------------
         A  |=               |5,45            |0,10 40,50
         B  |                |=               |0,5 10,40 45,50
         C  |                |                |=

         A needs to stream (5, 45] from B, (0, 10], (40, 50) from C
         B needs to stream (5, 45] from A, (0, 5], (10, 40], (45, 50] from C
         C needs to stream (0, 10], (40,50] from A, (0,5], (10,40], (45,50] from B
         A == B on (0, 5], (45, 50]
         A == C on (10, 40]
         B == C on (5, 10], (40, 45]
         */

        Map<InetAddress, HostDifferences> differences = new HashMap<>();
        addDifference(A, differences, B, list(range(5, 45)));
        addDifference(A, differences, C, list(range(0, 10), range(40,50)));
        addDifference(B, differences, C, list(range(0, 5), range(10,40), range(45,50)));

        DifferenceHolder differenceHolder = new DifferenceHolder(differences);
        Map<InetAddress, IncomingRepairStreamTracker> tracker = ReduceHelper.createIncomingRepairStreamTrackers(differenceHolder);

        Map<Range<Token>, StreamFromOptions> ranges = tracker.get(A).getIncoming();
        assertEquals(5, ranges.size());

        assertEquals(set(set(C)), ranges.get(range(0, 5)).allStreams());
        assertEquals(set(set(B, C)), ranges.get(range(5, 10)).allStreams());
        assertEquals(set(set(B)), ranges.get(range(10, 40)).allStreams());
        assertEquals(set(set(B, C)), ranges.get(range(40, 45)).allStreams());
        assertEquals(set(set(C)), ranges.get(range(45, 50)).allStreams());

        ranges = tracker.get(B).getIncoming();
        assertEquals(5, ranges.size());
        assertEquals(set(set(C)), ranges.get(range(0, 5)).allStreams());
        assertEquals(set(set(A)), ranges.get(range(5, 10)).allStreams());
        assertEquals(set(set(A, C)), ranges.get(range(10, 40)).allStreams());
        assertEquals(set(set(A)), ranges.get(range(40, 45)).allStreams());
        assertEquals(set(set(C)), ranges.get(range(45, 50)).allStreams());

        ranges = tracker.get(C).getIncoming();
        assertEquals(5, ranges.size());
        assertEquals(set(set(A, B)), ranges.get(range(0, 5)).allStreams());
        assertEquals(set(set(A)), ranges.get(range(5, 10)).allStreams());
        assertEquals(set(set(B)), ranges.get(range(10, 40)).allStreams());
        assertEquals(set(set(A)), ranges.get(range(40, 45)).allStreams());
        assertEquals(set(set(A,B)), ranges.get(range(45, 50)).allStreams());
        ImmutableMap<InetAddress, HostDifferences> reduced = ReduceHelper.reduce(differenceHolder, (x, y) -> y);

        assertNoOverlap(A, reduced.get(A), list(range(0, 50)));
        assertNoOverlap(B, reduced.get(B), list(range(0, 50)));
        assertNoOverlap(C, reduced.get(C), list(range(0, 50)));
    }

    private void assertNoOverlap(InetAddress incomingNode, HostDifferences node, List<Range<Token>> expectedAfterNormalize)
    {
        Set<Range<Token>> allRanges = new HashSet<>();
        Set<InetAddress> remoteNodes = Sets.newHashSet(A,B,C);
        remoteNodes.remove(incomingNode);
        Iterator<InetAddress> iter = remoteNodes.iterator();
        allRanges.addAll(node.get(iter.next()));
        InetAddress i = iter.next();
        for (Range<Token> r : node.get(i))
        {
            for (Range<Token> existing : allRanges)
                if (r.intersects(existing))
                    fail();
        }
        allRanges.addAll(node.get(i));
        List<Range<Token>> normalized = Range.normalize(allRanges);
        assertEquals(expectedAfterNormalize, normalized);
    }

    @SafeVarargs
    private static List<Range<Token>> list(Range<Token> r, Range<Token> ... rs)
    {
        List<Range<Token>> ranges = new ArrayList<>();
        ranges.add(r);
        Collections.addAll(ranges, rs);
        return ranges;
    }

    private static Set<InetAddress> set(InetAddress ... elem)
    {
        return Sets.newHashSet(elem);
    }
    @SafeVarargs
    private static Set<Set<InetAddress>> set(Set<InetAddress> ... elem)
    {
        Set<Set<InetAddress>> ret = Sets.newHashSet();
        ret.addAll(Arrays.asList(elem));
        return ret;
    }

    static Murmur3Partitioner.LongToken longtok(long l)
    {
        return new Murmur3Partitioner.LongToken(l);
    }

    static Range<Token> range(long t, long t2)
    {
        return new Range<>(longtok(t), longtok(t2));
    }

    @Test
    public void testSubtractAllRanges()
    {
        Set<Range<Token>> ranges = new HashSet<>();
        ranges.add(range(10, 20)); ranges.add(range(40, 60));
        assertEquals(0, RangeDenormalizer.subtractFromAllRanges(ranges, range(0, 100)).size());
        ranges.add(range(90, 110));
        assertEquals(Sets.newHashSet(range(100, 110)), RangeDenormalizer.subtractFromAllRanges(ranges, range(0, 100)));
        ranges.add(range(-10, 10));
        assertEquals(Sets.newHashSet(range(-10, 0), range(100, 110)), RangeDenormalizer.subtractFromAllRanges(ranges, range(0, 100)));
    }

    private void assertStreamFromEither(List<Range<Token>> r1, List<Range<Token>> r2)
    {
        assertTrue(r1.size() > 0 ^ r2.size() > 0);
    }

    private void addDifference(InetAddress host1, Map<InetAddress, HostDifferences> differences, InetAddress host2, List<Range<Token>> ranges)
    {
        differences.computeIfAbsent(host1, (x) -> new HostDifferences()).add(host2, ranges);
    }
}
