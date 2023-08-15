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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import accord.local.ShardDistributor;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.utils.Gens;
import accord.utils.RandomSource;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.utils.AccordGenerators;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;

public class AccordSplitterTest
{
    @BeforeClass
    public static void setup() throws NoSuchFieldException, IllegalAccessException
    {
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Test
    public void split()
    {
        qt().forAll(AccordGenerators.range(), Gens.random()).check((range, rs) -> {
            AccordRoutingKey startKey = (AccordRoutingKey) range.start();
            AccordRoutingKey endKey = (AccordRoutingKey) range.end();
            IPartitioner partitioner = getPartitioner(range, rs);
            // this section is filtering out known bugs
            // TODO (now): fix the fact accordSplitter returns AccordBytesSplitter which will fail for java.lang.ClassCastException: org.apache.cassandra.dht.LocalPartitioner$LocalToken cannot be cast to org.apache.cassandra.dht.ByteOrderedPartitioner$BytesToken
            // spoke with Benedict and he agrees that it doesn't make sense to split a local partitioner range, but this requires pushing this back into the API (similar to how C* returns Optional)
            if (partitioner instanceof LocalPartitioner)
                return;
            // TODO (now): java.lang.AssertionError: [size is not larger than 0 for partitioner org.apache.cassandra.dht.OrderPreservingPartitioner@54a67a45]
            if (partitioner instanceof OrderPreservingPartitioner && endKey.kindOfRoutingKey() == AccordRoutingKey.RoutingKeyKind.SENTINEL)
                return;
            // TODO (now): [size is not larger than 0 for partitioner org.apache.cassandra.dht.ByteOrderedPartitioner@44e3a2b2]
            if (partitioner instanceof ByteOrderedPartitioner && endKey.kindOfRoutingKey() == AccordRoutingKey.RoutingKeyKind.SENTINEL)
                return;
            // TODO (now): [num splits not as expected for partitioner org.apache.cassandra.dht.ByteOrderedPartitioner@4c550889]\nExpected size to be between: <47> and <48> but was:<62> in:
            if (partitioner instanceof ByteOrderedPartitioner && startKey.kindOfRoutingKey() == AccordRoutingKey.RoutingKeyKind.SENTINEL)
                return;
            // TODO (now): [num splits not as expected for partitioner org.apache.cassandra.dht.ByteOrderedPartitioner@13518f37]\nExpected size to be between: <11> and <12> but was:<13> in:
            if (partitioner instanceof ByteOrderedPartitioner)
                return;
            // TODO (now): [num splits not as expected for partitioner org.apache.cassandra.dht.OrderPreservingPartitioner@4233e892]\nExpected size to be between: <51> and <52> but was:<54> in:
            if (partitioner instanceof OrderPreservingPartitioner)
                return;
            AccordSplitter splitter = partitioner.accordSplitter().apply(Ranges.of(range));

            BigInteger size = splitter.sizeOf(range);
            Assertions.assertThat(size).describedAs("size is not larger than 0 for partitioner %s", partitioner).isGreaterThan(BigInteger.ZERO);
            int maxSplits = 100;
            int minSplits = 10;
            if (size.compareTo(BigInteger.valueOf(maxSplits)) < 0)
                maxSplits = size.intValue();
            if (size.compareTo(BigInteger.TEN) < 0)
                minSplits = Math.min(2, maxSplits - 1);
            int numSplits = rs.nextInt(minSplits, maxSplits);
            List<Range> ranges = new ArrayList<>(numSplits);
            BigInteger update = splitter.divide(size, numSplits);
            BigInteger offset = BigInteger.ZERO;
            while (offset.compareTo(size) < 0)
            {
                BigInteger end = offset.add(update);
                ranges.add(splitter.subRange(range, offset, end));
                offset = end;
            }

            // accord.local.ShardDistributor.EvenSplit.split attempts to detect this and work around it; a splitter is allowed to return slightly more in this case
            Assertions.assertThat(ranges).describedAs("num splits not as expected for partitioner %s", partitioner).hasSizeBetween(numSplits, numSplits + 1);

            Ranges split = Ranges.of(ranges.toArray(new Range[0])).mergeTouching();
            Ranges missing = Ranges.of(range).subtract(split);
            Assertions.assertThat(missing).isEmpty();

            testEventSplit(partitioner, range, rs, numSplits);
        });
    }

    private static void testEventSplit(IPartitioner partitioner, Range range, RandomSource rs, int numSplits)
    {
        ShardDistributor.EvenSplit<BigInteger> splitter = new ShardDistributor.EvenSplit<>(numSplits, partitioner.accordSplitter());

        Ranges topLevel = Ranges.of(range);
        List<Ranges> ranges = splitter.split(topLevel);

        Assertions.assertThat(ranges).describedAs("num splits not as expected for partitioner %s", partitioner).hasSize(numSplits);

        Ranges split = ranges.stream().reduce(Ranges.EMPTY, Ranges::with).mergeTouching();
        Ranges missing = topLevel.subtract(split);
        Assertions.assertThat(missing).isEmpty();
    }

    private static IPartitioner getPartitioner(Range range, RandomSource rs)
    {
        AccordRoutingKey key = (AccordRoutingKey) range.start();
        if (key.kindOfRoutingKey() == AccordRoutingKey.RoutingKeyKind.SENTINEL)
            key = (AccordRoutingKey) range.end();
        if (key.kindOfRoutingKey() == AccordRoutingKey.RoutingKeyKind.SENTINEL)
            return AccordGenerators.partitioner().next(rs);

        return key.token().getPartitioner();
    }
}