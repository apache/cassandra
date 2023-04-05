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

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.ReplicaCollection.Builder.Conflict;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.*;
import static org.apache.cassandra.locator.Replica.fullReplica;
import static org.apache.cassandra.locator.Replica.transientReplica;
import static org.apache.cassandra.locator.ReplicaUtils.*;

public class ReplicaCollectionTest
{

    static class TestCase<C extends AbstractReplicaCollection<C>>
    {
        final boolean isBuilder;
        final C test;
        final List<Replica> canonicalList;
        final Multimap<InetAddressAndPort, Replica> canonicalByEndpoint;
        final Multimap<Range<Token>, Replica> canonicalByRange;

        TestCase(boolean isBuilder, C test, List<Replica> canonicalList)
        {
            this.isBuilder = isBuilder;
            this.test = test;
            this.canonicalList = canonicalList;
            this.canonicalByEndpoint = HashMultimap.create();
            this.canonicalByRange = HashMultimap.create();
            for (Replica replica : canonicalList)
                canonicalByEndpoint.put(replica.endpoint(), replica);
            for (Replica replica : canonicalList)
                canonicalByRange.put(replica.range(), replica);
            if (isBuilder)
                Assert.assertTrue(test instanceof ReplicaCollection.Builder<?>);
        }

        void testSize()
        {
            Assert.assertEquals(canonicalList.size(), test.size());
        }

        void testEquals()
        {
            Assert.assertTrue(elementsEqual(canonicalList, test));
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
            for (InetAddressAndPort ep : ALL_EP)
                if (!canonicalByEndpoint.containsKey(ep))
                    Assert.assertFalse(test.endpoints().contains(ep));
        }

        public void testOrderOfIteration()
        {
            Assert.assertEquals(canonicalList, ImmutableList.copyOf(test));
            Assert.assertEquals(canonicalList, test.stream().collect(Collectors.toList()));
            Assert.assertTrue(Iterables.elementsEqual(new LinkedHashSet<>(Lists.transform(canonicalList, Replica::endpoint)), test.endpoints()));
        }

        private void assertSubList(C subCollection, int from, int to)
        {
            if (from == to)
            {
                Assert.assertTrue(subCollection.isEmpty());
            }
            else
            {
                AbstractReplicaCollection.ReplicaList subList = this.test.list.subList(from, to);
                if (!isBuilder)
                    Assert.assertSame(subList.contents, subCollection.list.contents);
                Assert.assertEquals(subList, subCollection.list);
            }
        }

        private void assertSubSequence(Iterable<Replica> subSequence, int from, int to)
        {
            AbstractReplicaCollection.ReplicaList subList = this.test.list.subList(from, to);
            if (!elementsEqual(subList, subSequence))
            {
                elementsEqual(subList, subSequence);
            }
            Assert.assertTrue(elementsEqual(subList, subSequence));
        }

        void testSubList(int subListDepth, int filterDepth, int sortDepth)
        {
            if (!isBuilder)
                Assert.assertSame(test, test.subList(0, test.size()));

            if (test.isEmpty())
                return;

            Assert.assertSame(test.list.contents, test.subList(0, 1).list.contents);
            TestCase<C> skipFront = new TestCase<>(false, test.subList(1, test.size()), canonicalList.subList(1, canonicalList.size()));
            assertSubList(skipFront.test, 1, canonicalList.size());
            skipFront.testAll(subListDepth - 1, filterDepth, sortDepth);
            TestCase<C> skipBack = new TestCase<>(false, test.subList(0, test.size() - 1), canonicalList.subList(0, canonicalList.size() - 1));
            assertSubList(skipBack.test, 0, canonicalList.size() - 1);
            skipBack.testAll(subListDepth - 1, filterDepth, sortDepth);
        }

        void testFilter(int subListDepth, int filterDepth, int sortDepth)
        {
            if (!isBuilder)
                Assert.assertSame(test, test.filter(Predicates.alwaysTrue()));

            if (test.isEmpty())
                return;

            // remove start
            // we recurse on the same subset in testSubList, so just corroborate we have the correct list here
            {
                Predicate<Replica> removeFirst = r -> !r.equals(canonicalList.get(0));
                assertSubList(test.filter(removeFirst), 1, canonicalList.size());
                assertSubList(test.filter(removeFirst, 1), 1, Math.min(canonicalList.size(), 2));
                assertSubSequence(test.filterLazily(removeFirst), 1, canonicalList.size());
                assertSubSequence(test.filterLazily(removeFirst, 1), 1, Math.min(canonicalList.size(), 2));
            }

            if (test.size() <= 1)
                return;

            // remove end
            // we recurse on the same subset in testSubList, so just corroborate we have the correct list here
            {
                int last = canonicalList.size() - 1;
                Predicate<Replica> removeLast = r -> !r.equals(canonicalList.get(last));
                assertSubList(test.filter(removeLast), 0, last);
                assertSubSequence(test.filterLazily(removeLast), 0, last);
            }

            if (test.size() <= 2)
                return;

            Predicate<Replica> removeMiddle = r -> !r.equals(canonicalList.get(canonicalList.size() / 2));
            TestCase<C> filtered = new TestCase<>(false, test.filter(removeMiddle), ImmutableList.copyOf(filter(canonicalList, removeMiddle::test)));
            filtered.testAll(subListDepth, filterDepth - 1, sortDepth);
            Assert.assertTrue(elementsEqual(filtered.canonicalList, test.filterLazily(removeMiddle, Integer.MAX_VALUE)));
            Assert.assertTrue(elementsEqual(limit(filter(canonicalList, removeMiddle::test), canonicalList.size() - 2), test.filterLazily(removeMiddle, canonicalList.size() - 2)));
        }

        void testCount()
        {
            Assert.assertEquals(0, test.count(Predicates.alwaysFalse()));

            if (test.isEmpty())
            {
                Assert.assertEquals(0, test.count(Predicates.alwaysTrue()));
                return;
            }

            for (int i = 0 ; i < canonicalList.size() ; ++i)
            {
                Replica discount = canonicalList.get(i);
                Assert.assertEquals(canonicalList.size() - 1, test.count(r -> !r.equals(discount)));
            }
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

        void testSort(int subListDepth, int filterDepth, int sortDepth)
        {
            final Comparator<Replica> comparator = (o1, o2) ->
            {
                boolean f1 = o1.equals(canonicalList.get(0));
                boolean f2 = o2.equals(canonicalList.get(0));
                return f1 == f2 ? 0 : f1 ? 1 : -1;
            };
            TestCase<C> sorted = new TestCase<>(false, test.sorted(comparator), ImmutableList.sortedCopyOf(comparator, canonicalList));
            sorted.testAll(subListDepth, filterDepth, sortDepth - 1);
        }

        void testAll(int subListDepth, int filterDepth, int sortDepth)
        {
            testEndpoints();
            testOrderOfIteration();
            testContains();
            testGet();
            testEquals();
            testSize();
            testCount();
            if (subListDepth > 0)
                testSubList(subListDepth, filterDepth, sortDepth);
            if (filterDepth > 0)
                testFilter(subListDepth, filterDepth, sortDepth);
            if (sortDepth > 0)
                testSort(subListDepth, filterDepth, sortDepth);
        }

        public void testAll()
        {
            testAll(2, 2, 2);
        }
    }

    static class RangesAtEndpointTestCase extends TestCase<RangesAtEndpoint>
    {
        RangesAtEndpointTestCase(boolean isBuilder, RangesAtEndpoint test, List<Replica> canonicalList)
        {
            super(isBuilder, test, canonicalList);
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
            for (Range<Token> range : ALL_R)
                if (!canonicalByRange.containsKey(range))
                    Assert.assertFalse(test.ranges().contains(range));
        }

        void testByRange()
        {
            // check byEndppint() and byRange().entrySet()
            Assert.assertFalse(test.byRange().containsKey(EP1));
            Assert.assertFalse(test.byRange().entrySet().contains(EP1));
            try
            {
                test.byRange().entrySet().contains(null);
                Assert.fail();
            } catch (NullPointerException | IllegalArgumentException e) {}
            try
            {
                test.byRange().containsKey(null);
                Assert.fail();
            } catch (NullPointerException | IllegalArgumentException e) {}

            for (Range<Token> r : ALL_R)
            {
                if (canonicalByRange.containsKey(r))
                {
                    Assert.assertTrue(test.byRange().containsKey(r));
                    Assert.assertEquals(canonicalByRange.get(r), ImmutableSet.of(test.byRange().get(r)));
                    for (Replica replica : canonicalByRange.get(r))
                        Assert.assertTrue(test.byRange().entrySet().contains(new AbstractMap.SimpleImmutableEntry<>(r, replica)));
                }
                else
                {
                    Assert.assertFalse(test.byRange().containsKey(r));
                    Assert.assertFalse(test.byRange().entrySet().contains(new AbstractMap.SimpleImmutableEntry<>(r, Replica.fullReplica(EP1, r))));
                }
            }
        }

        @Override
        public void testOrderOfIteration()
        {
            super.testOrderOfIteration();
            Assert.assertTrue(Iterables.elementsEqual(Lists.transform(canonicalList, Replica::range), test.ranges()));
            Assert.assertTrue(Iterables.elementsEqual(canonicalList, test.byRange().values()));
            Assert.assertTrue(Iterables.elementsEqual(
                    Lists.transform(canonicalList, r -> new AbstractMap.SimpleImmutableEntry<>(r.range(), r)),
                    test.byRange().entrySet()));
        }

        public void testUnwrap(int subListDepth, int filterDepth, int sortDepth)
        {
            List<Replica> canonUnwrap = new ArrayList<>();
            for (Replica replica : canonicalList)
                for (Range<Token> range : replica.range().unwrap())
                    canonUnwrap.add(replica.decorateSubrange(range));
            RangesAtEndpoint testUnwrap = test.unwrap();
            if (testUnwrap == test)
            {
                Assert.assertEquals(canonicalList, canonUnwrap);
            }
            else
            {
                new RangesAtEndpointTestCase(false, testUnwrap, canonUnwrap)
                        .testAllExceptUnwrap(subListDepth, filterDepth, sortDepth);
            }
        }

        void testAllExceptUnwrap(int subListDepth, int filterDepth, int sortDepth)
        {
            super.testAll(subListDepth, filterDepth, sortDepth);
            testRanges();
            testByRange();
        }

        @Override
        void testAll(int subListDepth, int filterDepth, int sortDepth)
        {
            testAllExceptUnwrap(subListDepth, filterDepth, sortDepth);
            testUnwrap(subListDepth, filterDepth, sortDepth);
        }
    }

    static class EndpointsTestCase<E extends Endpoints<E>> extends TestCase<E>
    {
        EndpointsTestCase(boolean isBuilder, E test, List<Replica> canonicalList)
        {
            super(isBuilder, test, canonicalList);
        }

        void testByEndpoint()
        {
            // check byEndppint() and byEndpoint().entrySet()
            Assert.assertFalse(test.byEndpoint().containsKey(R1));
            Assert.assertFalse(test.byEndpoint().entrySet().contains(EP1));
            try
            {
                test.byEndpoint().entrySet().contains(null);
                Assert.fail();
            } catch (NullPointerException | IllegalArgumentException e) {}
            try
            {
                test.byEndpoint().containsKey(null);
                Assert.fail();
            } catch (NullPointerException | IllegalArgumentException e) {}

            for (InetAddressAndPort ep : ALL_EP)
            {
                if (canonicalByEndpoint.containsKey(ep))
                {
                    Assert.assertTrue(test.byEndpoint().containsKey(ep));
                    Assert.assertEquals(canonicalByEndpoint.get(ep), ImmutableSet.of(test.byEndpoint().get(ep)));
                    for (Replica replica : canonicalByEndpoint.get(ep))
                        Assert.assertTrue(test.byEndpoint().entrySet().contains(new AbstractMap.SimpleImmutableEntry<>(ep, replica)));
                }
                else
                {
                    Assert.assertFalse(test.byEndpoint().containsKey(ep));
                    Assert.assertFalse(test.byEndpoint().entrySet().contains(new AbstractMap.SimpleImmutableEntry<>(ep, Replica.fullReplica(ep, R1))));
                }
            }
        }

        @Override
        public void testOrderOfIteration()
        {
            super.testOrderOfIteration();
            Assert.assertTrue(Iterables.elementsEqual(canonicalList, test.byEndpoint().values()));
            Assert.assertTrue(Iterables.elementsEqual(
                    Lists.transform(canonicalList, r -> new AbstractMap.SimpleImmutableEntry<>(r.endpoint(), r)),
                    test.byEndpoint().entrySet()));
        }

        @Override
        void testAll(int subListDepth, int filterDepth, int sortDepth)
        {
            super.testAll(subListDepth, filterDepth, sortDepth);
            testByEndpoint();
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
                false, RangesAtEndpoint.copyOf(canonical), canonical
        ).testAll();
    }

    @Test
    public void testMutableRangesAtEndpoint()
    {
        ImmutableList<Replica> canonical1 = RANGES_AT_ENDPOINT.subList(0, RANGES_AT_ENDPOINT.size());
        RangesAtEndpoint.Builder test = new RangesAtEndpoint.Builder(RANGES_AT_ENDPOINT.get(0).endpoint(), canonical1.size());
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

        new RangesAtEndpointTestCase(true, test, canonical1).testAll();

        RangesAtEndpoint snapshot = test.subList(0, test.size());

        ImmutableList<Replica> canonical2 = RANGES_AT_ENDPOINT;
        test.addAll(canonical2.reverse(), Conflict.DUPLICATE);
        new TestCase<>(false, snapshot, canonical1).testAll();
        new TestCase<>(true, test, canonical2).testAll();
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
        new EndpointsTestCase<>(
                false, EndpointsForRange.copyOf(canonical), canonical
        ).testAll();
    }

    @Test
    public void testMutableEndpointsForRange()
    {
        ImmutableList<Replica> canonical1 = ENDPOINTS_FOR_X.subList(0, ENDPOINTS_FOR_X.size() - 1);
        EndpointsForRange.Builder test = new EndpointsForRange.Builder(R1, canonical1.size());
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

        new EndpointsTestCase<>(true, test, canonical1).testAll();

        EndpointsForRange snapshot = test.subList(0, test.size());

        ImmutableList<Replica> canonical2 = ENDPOINTS_FOR_X;
        test.addAll(canonical2.reverse(), Conflict.DUPLICATE);
        new EndpointsTestCase<>(false, snapshot, canonical1).testAll();
        new EndpointsTestCase<>(true, test, canonical2).testAll();
    }

    @Test
    public void testEndpointsForToken()
    {
        ImmutableList<Replica> canonical = ENDPOINTS_FOR_X;
        new EndpointsTestCase<>(
                false, EndpointsForToken.copyOf(tk(1), canonical), canonical
        ).testAll();
    }

    @Test
    public void testMutableEndpointsForToken()
    {
        ImmutableList<Replica> canonical1 = ENDPOINTS_FOR_X.subList(0, ENDPOINTS_FOR_X.size() - 1);
        EndpointsForToken.Builder test = new EndpointsForToken.Builder(tk(1), canonical1.size());
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

        new EndpointsTestCase<>(true, test, canonical1).testAll();

        EndpointsForToken snapshot = test.subList(0, test.size());

        ImmutableList<Replica> canonical2 = ENDPOINTS_FOR_X;
        test.addAll(canonical2.reverse(), Conflict.DUPLICATE);
        new EndpointsTestCase<>(false, snapshot, canonical1).testAll();
        new EndpointsTestCase<>(true, test, canonical2).testAll();
    }
}
