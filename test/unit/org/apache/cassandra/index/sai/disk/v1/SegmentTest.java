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
package org.apache.cassandra.index.sai.disk.v1;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.disk.v1.segment.Segment;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SegmentTest
{
    private static IPartitioner partitioner;
    private static Token min, max;
    private static List<Token> tokens;

    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.toolInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        partitioner = DatabaseDescriptor.getPartitioner();
        min = partitioner.getMinimumToken();
        max = partitioner.getMaximumToken();
        tokens = IntStream.rangeClosed(0, 10).boxed().map(i -> partitioner.getRandomToken())
                          .distinct().sorted().collect(Collectors.toList());
    }

    @Test
    public void testNoOverlapping()
    {
        // wrap around
        AbstractBounds<PartitionPosition> wrapAround = inclusiveRight(tokens.get(7), tokens.get(2));
        assertWrapAround(wrapAround);
        assertNoOverlapping(seg(tokens.get(5), tokens.get(6)), wrapAround);
        wrapAround = inclusiveRight(tokens.get(7), min);
        assertWrapAround(wrapAround);
        assertNoOverlapping(seg(tokens.get(5), tokens.get(6)), wrapAround);

        // exclusive intersection
        assertFalse(inclusiveRight(tokens.get(0), tokens.get(1)).contains(tokens.get(0).maxKeyBound()));
        assertNoOverlapping(seg(min, tokens.get(0)), inclusiveRight(tokens.get(0), tokens.get(1)));

        assertFalse(exclusive(tokens.get(0), tokens.get(1)).contains(tokens.get(1).minKeyBound()));
        assertNoOverlapping(seg(tokens.get(1), tokens.get(2)), exclusive(tokens.get(0), tokens.get(1)));

        assertFalse(inclusiveLeft(tokens.get(0), tokens.get(3)).contains(tokens.get(3).minKeyBound()));
        assertNoOverlapping(seg(tokens.get(3), max), inclusiveLeft(tokens.get(0), tokens.get(3)));

        // disjoint
        assertNoOverlapping(seg(min, tokens.get(0)), inclusiveRight(tokens.get(7), tokens.get(9)));
        assertNoOverlapping(seg(tokens.get(2), tokens.get(4)), inclusiveRight(tokens.get(5), tokens.get(6)));
        assertNoOverlapping(seg(tokens.get(3), max), inclusiveLeft(tokens.get(0), tokens.get(2)));
    }

    @Test
    public void testOverlapping()
    {
        // wrap around
        AbstractBounds<PartitionPosition> wrapAround = inclusiveRight(tokens.get(7), tokens.get(7));
        assertWrapAround(wrapAround);
        assertOverlapping(seg(tokens.get(5), tokens.get(6)), wrapAround);
        wrapAround = inclusiveRight(tokens.get(7), min);
        assertWrapAround(wrapAround);
        assertOverlapping(seg(tokens.get(7), tokens.get(8)), wrapAround);
        wrapAround = inclusiveRight(tokens.get(7), tokens.get(5));
        assertWrapAround(wrapAround);
        assertOverlapping(seg(tokens.get(1), tokens.get(2)), wrapAround);
        wrapAround = inclusiveRight(min, min);
        assertWrapAround(wrapAround);
        assertOverlapping(seg(tokens.get(1), tokens.get(2)), wrapAround);

        // inclusive intersection
        assertOverlapping(seg(min, tokens.get(0)), inclusiveLeft(tokens.get(0), tokens.get(1)));
        assertOverlapping(seg(tokens.get(1), tokens.get(2)), inclusive(tokens.get(0), tokens.get(1)));
        assertOverlapping(seg(tokens.get(3), max), inclusiveRight(tokens.get(0), tokens.get(3)));

        // intersect
        assertOverlapping(seg(min, tokens.get(7)), exclusive(tokens.get(5), tokens.get(9)));
        assertOverlapping(seg(tokens.get(5), tokens.get(7)), exclusive(tokens.get(5), tokens.get(6)));

        // contains
        assertOverlapping(seg(tokens.get(2), tokens.get(6)), inclusiveRight(tokens.get(4), tokens.get(5)));
        assertOverlapping(seg(tokens.get(3), max), inclusiveLeft(tokens.get(5), tokens.get(8)));
        assertOverlapping(seg(tokens.get(3), tokens.get(5)), inclusiveLeft(tokens.get(1), tokens.get(6)));
    }

    private static void assertNoOverlapping(Segment segment, AbstractBounds<PartitionPosition> keyRange)
    {
        assertFalse("Expect no overlapping", segment.intersects(keyRange));
    }

    private static void assertOverlapping(Segment segment, AbstractBounds<PartitionPosition> keyRange)
    {
        assertTrue("Expect overlapping", segment.intersects(keyRange));
    }


    private static void assertWrapAround(AbstractBounds<PartitionPosition> keyRange)
    {
        assertTrue("Expect wrap around range, but it's not", keyRange instanceof Range && ((Range<?>)keyRange).isWrapAround());
    }

    private static Segment seg(Token left, Token right)
    {
        return new Segment(left, right);
    }

    private static AbstractBounds<PartitionPosition> inclusiveLeft(Token left, Token right)
    {
        return keyRange(left, true, right, false);
    }

    private static AbstractBounds<PartitionPosition> inclusiveRight(Token left, Token right)
    {
        return keyRange(left, false, right, true);
    }

    private static AbstractBounds<PartitionPosition> inclusive(Token left, Token right)
    {
        return keyRange(left, true, right, true);
    }

    private static AbstractBounds<PartitionPosition> exclusive(Token left, Token right)
    {
        return keyRange(left, false, right, false);
    }

    private static AbstractBounds<PartitionPosition> keyRange(Token left, boolean inclusiveLeft, Token right, boolean inclusiveRight)
    {
        return Bounds.bounds(inclusiveLeft ? left.minKeyBound() : left.maxKeyBound(), inclusiveLeft,
                             inclusiveRight ? right.maxKeyBound() : right.minKeyBound(), inclusiveRight);
    }
}
