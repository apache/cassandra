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

package org.apache.cassandra.locator;

import com.google.common.base.Predicates;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.ReplicaCollection.Mutable.Conflict;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.Assert;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.cassandra.locator.Replica.fullReplica;
import static org.apache.cassandra.locator.Replica.transientReplica;

public class ReplicaCollectionTest
{

    static final InetAddressAndPort EP1, EP2, EP3, EP4, EP5, BROADCAST_EP, NULL_EP;
    static final Range<Token> R1, R2, R3, R4, R5, BROADCAST_RANGE, NULL_RANGE;

    static
    {
        try
        {
            EP1 = InetAddressAndPort.getByName("127.0.0.1");
            EP2 = InetAddressAndPort.getByName("127.0.0.2");
            EP3 = InetAddressAndPort.getByName("127.0.0.3");
            EP4 = InetAddressAndPort.getByName("127.0.0.4");
            EP5 = InetAddressAndPort.getByName("127.0.0.5");
            BROADCAST_EP = FBUtilities.getBroadcastAddressAndPort();
            NULL_EP = InetAddressAndPort.getByName("127.255.255.255");
            R1 = range(0, 1);
            R2 = range(1, 2);
            R3 = range(2, 3);
            R4 = range(3, 4);
            R5 = range(4, 5);
            BROADCAST_RANGE = range(10, 11);
            NULL_RANGE = range(10000, 10001);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    static Token tk(long t)
    {
        return new Murmur3Partitioner.LongToken(t);
    }

    static Range<Token> range(long left, long right)
    {
        return new Range<>(tk(left), tk(right));
    }

    static class TestCase<C extends AbstractReplicaCollection<C>>
    {
        final C test;
        final List<Replica> canonicalList;
        final Multimap<InetAddressAndPort, Replica> canonicalByEndpoint;
        final Multimap<Range<Token>, Replica> canonicalByRange;

        TestCase(C test, List<Replica> canonicalList)
        {
            this.test = test;
            this.canonicalList = canonicalList;
            this.canonicalByEndpoint = HashMultimap.create();
            this.canonicalByRange = HashMultimap.create();
            for (Replica replica : canonicalList)
                canonicalByEndpoint.put(replica.endpoint(), replica);
            for (Replica replica : canonicalList)
                canonicalByRange.put(replica.range(), replica);
        }

        void testSize()
        {
            Assert.assertEquals(canonicalList.size(), test.size());
        }

        void testEquals()
        {
            Assert.assertTrue(Iterables.elementsEqual(canonicalList, test));
        }

        void testEndpoints()
        {
            // TODO: we should do more exhaustive tests of the collection
            Assert.assertEquals(ImmutableSet.copyOf(canonicalByEndpoint.keySet()), ImmutableSet.copyOf(test.endpoints()));
            try
            {
                test.endpoints().add(EP5);
                Assert.fail();
            } catch (UnsupportedOperationException e) {}
            try
            {
                test.endpoints().remove(EP5);
                Assert.fail();
            } catch (UnsupportedOperationException e) {}

            Assert.assertTrue(test.endpoints().containsAll(canonicalByEndpoint.keySet()));
            for (InetAddressAndPort ep : canonicalByEndpoint.keySet())
                Assert.assertTrue(test.endpoints().contains(ep));
            for (InetAddressAndPort ep : ImmutableList.of(EP1, EP2, EP3, EP4, EP5, BROADCAST_EP))
                if (!canonicalByEndpoint.containsKey(ep))
                    Assert.assertFalse(test.endpoints().contains(ep));
        }

        public void testOrderOfIteration()
        {
            Assert.assertEquals(canonicalList, ImmutableList.copyOf(test));
            Assert.assertEquals(canonicalList, test.stream().collect(Collectors.toList()));
            Assert.assertEquals(new LinkedHashSet<>(Lists.transform(canonicalList, Replica::endpoint)), test.endpoints());
        }

        void testSelect(int subListDepth, int filterDepth, int sortDepth, int selectDepth)
        {
            TestCase<C> allMatchZeroCapacity = new TestCase<>(test.select().add(Predicates.alwaysTrue(), 0).get(), Collections.emptyList());
            allMatchZeroCapacity.testAll(subListDepth, filterDepth, sortDepth, selectDepth - 1);

            TestCase<C> noMatchFullCapacity = new TestCase<>(test.select().add(Predicates.alwaysFalse(), canonicalList.size()).get(), Collections.emptyList());
            noMatchFullCapacity.testAll(subListDepth, filterDepth, sortDepth,selectDepth - 1);

            if (canonicalList.size() <= 2)
                return;

            List<Replica> newOrderList = ImmutableList.of(canonicalList.get(2), canonicalList.get(1), canonicalList.get(0));
            TestCase<C> newOrder = new TestCase<>(
                    test.select()
                            .add(r -> r == newOrderList.get(0), 3)
                            .add(r -> r == newOrderList.get(1), 3)
                            .add(r -> r == newOrderList.get(2), 3)
                            .get(), newOrderList
            );
            newOrder.testAll(subListDepth, filterDepth, sortDepth,selectDepth - 1);
        }

        private void assertSubList(C subCollection, int from, int to)
        {
            Assert.assertTrue(subCollection.isSnapshot);
            if (from == to)
            {
                Assert.assertTrue(subCollection.isEmpty());
            }
            else
            {
                List<Replica> subList = this.test.list.subList(from, to);
                if (test.isSnapshot)
                    Assert.assertSame(subList.getClass(), subCollection.list.getClass());
                Assert.assertEquals(subList, subCollection.list);
            }
        }

        void testSubList(int subListDepth, int filterDepth, int sortDepth, int selectDepth)
        {
            if (test.isSnapshot)
                Assert.assertSame(test, test.subList(0, test.size()));

            if (test.isEmpty())
                return;

            TestCase<C> skipFront = new TestCase<>(test.subList(1, test.size()), canonicalList.subList(1, canonicalList.size()));
            assertSubList(skipFront.test, 1, canonicalList.size());
            skipFront.testAll(subListDepth - 1, filterDepth, sortDepth, selectDepth);
            TestCase<C> skipBack = new TestCase<>(test.subList(0, test.size() - 1), canonicalList.subList(0, canonicalList.size() - 1));
            assertSubList(skipBack.test, 0, canonicalList.size() - 1);
            skipBack.testAll(subListDepth - 1, filterDepth, sortDepth, selectDepth);
        }

        void testFilter(int subListDepth, int filterDepth, int sortDepth, int selectDepth)
        {
            if (test.isSnapshot)
                Assert.assertSame(test, test.filter(Predicates.alwaysTrue()));

            if (test.isEmpty())
                return;
            // remove start
            // we recurse on the same subset in testSubList, so just corroborate we have the correct list here
            assertSubList(test.filter(r -> r != canonicalList.get(0)), 1, canonicalList.size());

            if (test.size() <= 1)
                return;
            // remove end
            // we recurse on the same subset in testSubList, so just corroborate we have the correct list here
            assertSubList(test.filter(r -> r != canonicalList.get(canonicalList.size() - 1)), 0, canonicalList.size() - 1);

            if (test.size() <= 2)
                return;
            Predicate<Replica> removeMiddle = r -> r != canonicalList.get(canonicalList.size() / 2);
            TestCase<C> filtered = new TestCase<>(test.filter(removeMiddle), ImmutableList.copyOf(Iterables.filter(canonicalList, removeMiddle::test)));
            filtered.testAll(subListDepth, filterDepth - 1, sortDepth, selectDepth);
        }

        void testContains()
        {
            for (Replica replica : canonicalList)
                Assert.assertTrue(test.contains(replica));
            Assert.assertFalse(test.contains(fullReplica(NULL_EP, NULL_RANGE)));
        }

        void testGet()
        {
            for (int i = 0 ; i < canonicalList.size() ; ++i)
                Assert.assertEquals(canonicalList.get(i), test.get(i));
        }

        void testSort(int subListDepth, int filterDepth, int sortDepth, int selectDepth)
        {
            final Comparator<Replica> comparator = (o1, o2) ->
            {
                boolean f1 = o1 == canonicalList.get(0);
                boolean f2 = o2 == canonicalList.get(0);
                return f1 == f2 ? 0 : f1 ? 1 : -1;
            };
            TestCase<C> sorted = new TestCase<>(test.sorted(comparator), ImmutableList.sortedCopyOf(comparator, canonicalList));
            sorted.testAll(subListDepth, filterDepth, sortDepth - 1, selectDepth);
        }

        private void testAll(int subListDepth, int filterDepth, int sortDepth, int selectDepth)
        {
            testEndpoints();
            testOrderOfIteration();
            testContains();
            testGet();
            testEquals();
            testSize();
            if (subListDepth > 0)
                testSubList(subListDepth, filterDepth, sortDepth, selectDepth);
            if (filterDepth > 0)
                testFilter(subListDepth, filterDepth, sortDepth, selectDepth);
            if (sortDepth > 0)
                testSort(subListDepth, filterDepth, sortDepth, selectDepth);
            if (selectDepth > 0)
                testSelect(subListDepth, filterDepth, sortDepth, selectDepth);
        }

        public void testAll()
        {
            testAll(2, 2, 2, 2);
        }
    }

    static class RangesAtEndpointTestCase extends TestCase<RangesAtEndpoint>
    {
        RangesAtEndpointTestCase(RangesAtEndpoint test, List<Replica> canonicalList)
        {
            super(test, canonicalList);
        }

        void testRanges()
        {
            Assert.assertEquals(ImmutableSet.copyOf(canonicalByRange.keySet()), ImmutableSet.copyOf(test.ranges()));
            try
            {
                test.ranges().add(R5);
                Assert.fail();
            } catch (UnsupportedOperationException e) {}
            try
            {
                test.ranges().remove(R5);
                Assert.fail();
            } catch (UnsupportedOperationException e) {}

            Assert.assertTrue(test.ranges().containsAll(canonicalByRange.keySet()));
            for (Range<Token> range : canonicalByRange.keySet())
                Assert.assertTrue(test.ranges().contains(range));
            for (Range<Token> range : ImmutableList.of(R1, R2, R3, R4, R5, BROADCAST_RANGE))
                if (!canonicalByRange.containsKey(range))
                    Assert.assertFalse(test.ranges().contains(range));
        }

        @Override
        public void testOrderOfIteration()
        {
            super.testOrderOfIteration();
            Assert.assertEquals(new LinkedHashSet<>(Lists.transform(canonicalList, Replica::range)), test.ranges());
        }

        @Override
        public void testAll()
        {
            super.testAll();
            testRanges();
        }
    }

    private static final ImmutableList<Replica> RANGES_AT_ENDPOINT = ImmutableList.of(
            fullReplica(EP1, R1),
            fullReplica(EP1, R2),
            transientReplica(EP1, R3),
            fullReplica(EP1, R4),
            transientReplica(EP1, R5)
    );

    @Test
    public void testRangesAtEndpoint()
    {
        ImmutableList<Replica> canonical = RANGES_AT_ENDPOINT;
        new RangesAtEndpointTestCase(
                RangesAtEndpoint.copyOf(canonical), canonical
        ).testAll();
    }

    @Test
    public void testMutableRangesAtEndpoint()
    {
        ImmutableList<Replica> canonical1 = RANGES_AT_ENDPOINT.subList(0, RANGES_AT_ENDPOINT.size());
        RangesAtEndpoint.Mutable test = new RangesAtEndpoint.Mutable(RANGES_AT_ENDPOINT.get(0).endpoint(), canonical1.size());
        test.addAll(canonical1, Conflict.NONE);
        try
        {   // incorrect range
            test.addAll(canonical1, Conflict.NONE);
            Assert.fail();
        } catch (IllegalArgumentException e) { }
        test.addAll(canonical1, Conflict.DUPLICATE); // we ignore exact duplicates
        try
        {   // invalid endpoint; always error
            test.add(fullReplica(EP2, BROADCAST_RANGE), Conflict.ALL);
            Assert.fail();
        } catch (IllegalArgumentException e) { }
        try
        {   // conflict on isFull/isTransient
            test.add(fullReplica(EP1, R3), Conflict.DUPLICATE);
            Assert.fail();
        } catch (IllegalArgumentException e) { }
        test.add(fullReplica(EP1, R3), Conflict.ALL);

        new RangesAtEndpointTestCase(test, canonical1).testAll();

        RangesAtEndpoint view = test.asImmutableView();
        RangesAtEndpoint snapshot = view.subList(0, view.size());

        ImmutableList<Replica> canonical2 = RANGES_AT_ENDPOINT;
        test.addAll(canonical2.reverse(), Conflict.DUPLICATE);
        new TestCase<>(snapshot, canonical1).testAll();
        new TestCase<>(view, canonical2).testAll();
        new TestCase<>(test, canonical2).testAll();
    }

    private static final ImmutableList<Replica> ENDPOINTS_FOR_X = ImmutableList.of(
            fullReplica(EP1, R1),
            fullReplica(EP2, R1),
            transientReplica(EP3, R1),
            fullReplica(EP4, R1),
            transientReplica(EP5, R1)
    );

    @Test
    public void testEndpointsForRange()
    {
        ImmutableList<Replica> canonical = ENDPOINTS_FOR_X;
        new TestCase<>(
                EndpointsForRange.copyOf(canonical), canonical
        ).testAll();
    }

    @Test
    public void testMutableEndpointsForRange()
    {
        ImmutableList<Replica> canonical1 = ENDPOINTS_FOR_X.subList(0, ENDPOINTS_FOR_X.size() - 1);
        EndpointsForRange.Mutable test = new EndpointsForRange.Mutable(R1, canonical1.size());
        test.addAll(canonical1, Conflict.NONE);
        try
        {   // incorrect range
            test.addAll(canonical1, Conflict.NONE);
            Assert.fail();
        } catch (IllegalArgumentException e) { }
        test.addAll(canonical1, Conflict.DUPLICATE); // we ignore exact duplicates
        try
        {   // incorrect range
            test.add(fullReplica(BROADCAST_EP, R2), Conflict.ALL);
            Assert.fail();
        } catch (IllegalArgumentException e) { }
        try
        {   // conflict on isFull/isTransient
            test.add(transientReplica(EP1, R1), Conflict.DUPLICATE);
            Assert.fail();
        } catch (IllegalArgumentException e) { }
        test.add(transientReplica(EP1, R1), Conflict.ALL);

        new TestCase<>(test, canonical1).testAll();

        EndpointsForRange view = test.asImmutableView();
        EndpointsForRange snapshot = view.subList(0, view.size());

        ImmutableList<Replica> canonical2 = ENDPOINTS_FOR_X;
        test.addAll(canonical2.reverse(), Conflict.DUPLICATE);
        new TestCase<>(snapshot, canonical1).testAll();
        new TestCase<>(view, canonical2).testAll();
        new TestCase<>(test, canonical2).testAll();
    }

    @Test
    public void testEndpointsForToken()
    {
        ImmutableList<Replica> canonical = ENDPOINTS_FOR_X;
        new TestCase<>(
                EndpointsForToken.copyOf(tk(1), canonical), canonical
        ).testAll();
    }

    @Test
    public void testMutableEndpointsForToken()
    {
        ImmutableList<Replica> canonical1 = ENDPOINTS_FOR_X.subList(0, ENDPOINTS_FOR_X.size() - 1);
        EndpointsForToken.Mutable test = new EndpointsForToken.Mutable(tk(1), canonical1.size());
        test.addAll(canonical1, Conflict.NONE);
        try
        {   // incorrect range
            test.addAll(canonical1, Conflict.NONE);
            Assert.fail();
        } catch (IllegalArgumentException e) { }
        test.addAll(canonical1, Conflict.DUPLICATE); // we ignore exact duplicates
        try
        {   // incorrect range
            test.add(fullReplica(BROADCAST_EP, R2), Conflict.ALL);
            Assert.fail();
        } catch (IllegalArgumentException e) { }
        try
        {   // conflict on isFull/isTransient
            test.add(transientReplica(EP1, R1), Conflict.DUPLICATE);
            Assert.fail();
        } catch (IllegalArgumentException e) { }
        test.add(transientReplica(EP1, R1), Conflict.ALL);

        new TestCase<>(test, canonical1).testAll();

        EndpointsForToken view = test.asImmutableView();
        EndpointsForToken snapshot = view.subList(0, view.size());

        ImmutableList<Replica> canonical2 = ENDPOINTS_FOR_X;
        test.addAll(canonical2.reverse(), Conflict.DUPLICATE);
        new TestCase<>(snapshot, canonical1).testAll();
        new TestCase<>(view, canonical2).testAll();
        new TestCase<>(test, canonical2).testAll();
    }
}
