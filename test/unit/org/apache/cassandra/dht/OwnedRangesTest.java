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

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

import static org.apache.cassandra.utils.TokenRangeTestUtil.generateRanges;

public class OwnedRangesTest
{
    @Test
    public void testFilterRangesWithEmptySuperset()
    {
        List<Range<Token>> toFilter = generateRanges(0, 50);
        Collection<Range<Token>> rejected = new OwnedRanges(Collections.emptyList()).testRanges(toFilter);
        assertRangesEqual(toFilter, rejected);
    }

    @Test
    public void testFilterRangesWithEmptySupersetEmptySubset()
    {
        assertTrue(new OwnedRanges(Collections.emptyList()).testRanges(Collections.emptyList()).isEmpty());
    }

    @Test
    public void testFilterRangesWithSingleSupersetEmptySubset()
    {
        assertTrue(new OwnedRanges(generateRanges(0, 100)).testRanges(Collections.emptyList()).isEmpty());
    }

    @Test
    public void testFilterRangesWithSingleSupersetSingleSubsetContained()
    {
        assertTrue(new OwnedRanges(generateRanges(0, 100)).testRanges(generateRanges(10 , 20)).isEmpty());
    }

    @Test
    public void testFilterRangesWithSingleSupersetSingleSubsetExactMatch()
    {
        assertTrue(new OwnedRanges(generateRanges(0, 100)).testRanges(generateRanges(0, 100)).isEmpty());
    }

    @Test
    public void testFilterRangesWithSingleSupersetSingleSubsetStrictlyLessThan()
    {
        Collection<Range<Token>> toFilter = generateRanges(-100, 0);
        Collection<Range<Token>> rejected = new OwnedRanges(generateRanges(0, 100)).testRanges(toFilter);
        assertRangesEqual(toFilter, rejected);
    }

    @Test
    public void testFilterRangesWithSingleSupersetSingleSubsetStrictlyGreaterThan()
    {
        Collection<Range<Token>> toFilter = generateRanges(101, 200);
        Collection<Range<Token>> rejected = new OwnedRanges(generateRanges(0, 100)).testRanges(toFilter);
        assertRangesEqual(toFilter, rejected);
    }

    @Test
    public void testFilterRangesWithSingleSupersetSingleSubsetOverlapping()
    {
        Collection<Range<Token>> toFilter = generateRanges(80, 120);
        Collection<Range<Token>> rejected = new OwnedRanges(generateRanges(0, 100)).testRanges(toFilter);
        assertRangesEqual(toFilter, rejected);
    }

    @Test
    public void testFilterRangesWithSingleSupersetSingleSubsetOverlappingOnRight()
    {
        Collection<Range<Token>> toFilter = generateRanges(0, 120);
        Collection<Range<Token>> rejected = new OwnedRanges(generateRanges(0, 100)).testRanges(toFilter);
        assertRangesEqual(toFilter, rejected);
    }

    @Test
    public void testFilterRangesWithSingleSupersetSingleSubsetOverlappingOnLeft()
    {
        Collection<Range<Token>> toFilter = generateRanges(-10, 100);
        Collection<Range<Token>> rejected = new OwnedRanges(generateRanges(0, 100)).testRanges(toFilter);
        assertRangesEqual(toFilter, rejected);
    }

    @Test
    public void testFilterRangesWithSingleSupersetMultipleSubsetAllContained()
    {
        Collection<Range<Token>> toFilter = generateRanges(10, 20, 30, 40, 50, 60);
        assertTrue(new OwnedRanges(generateRanges(0, 100)).testRanges(toFilter).isEmpty());
    }

    @Test
    public void testFilterRangesWithSingleSupersetMultipleSubsetSomeContained()
    {
        Collection<Range<Token>> toFilter = generateRanges(-20, -10, 0, 10, 20, 30, 110, 120);
        Collection<Range<Token>> rejected = new OwnedRanges(generateRanges(0, 100)).testRanges(toFilter);
        assertRangesEqual(generateRanges(-20, -10, 110, 120), rejected);
    }

    @Test
    public void testFilterRangesWithSingleSupersetMultipleSubsetNoneContained()
    {
        Collection<Range<Token>> toFilter = generateRanges(-40, -20, -20, -10, 110, 120, 230, 140);
        Collection<Range<Token>> rejected = new OwnedRanges(generateRanges(0, 100)).testRanges(toFilter);
        assertRangesEqual(toFilter, rejected);
    }

    @Test
    public void testFilterWithMultipleSupersetSingleSubsetContainedInFirst()
    {
        Collection<Range<Token>> toFilter = generateRanges(10, 20);
        assertTrue(new OwnedRanges(generateRanges(0, 100, 200, 300, 400, 500)).testRanges(toFilter).isEmpty());
    }

    @Test
    public void testFilterWithMultipleSupersetSingleSubsetContainedInLast()
    {
        Collection<Range<Token>> toFilter = generateRanges(450, 460);
        assertTrue(new OwnedRanges(generateRanges(0, 100, 200, 300, 400, 500)).testRanges(toFilter).isEmpty());
    }

    @Test
    public void testFilterRangesWithMultipleSupersetSingleSubsetStrictlyGreaterThan()
    {
        Collection<Range<Token>> toFilter =  generateRanges(510, 520);
        Collection<Range<Token>> rejected = new OwnedRanges(generateRanges(0, 100, 200, 300, 400, 500)).testRanges(toFilter);
        assertRangesEqual(toFilter, rejected);
    }

    @Test
    public void testFilterRangesWithMultipleSupersetSingleSubsetStrictlyLessThan()
    {
        Collection<Range<Token>> toFilter = generateRanges(-20, -10);
        Collection<Range<Token>> rejected = new OwnedRanges(generateRanges(0, 100, 200, 300, 400, 500)).testRanges(toFilter);
        assertRangesEqual(toFilter, rejected);
    }

    @Test
    public void testFilterRangesWithMultipleSupersetSingleSubsetOverlapping()
    {
        Collection<Range<Token>> toFilter = generateRanges(80, 120);
        Collection<Range<Token>> rejected = new OwnedRanges(generateRanges(0, 100, 200, 300, 400, 500)).testRanges(toFilter);
        assertRangesEqual(toFilter, rejected);
    }

    @Test
    public void testFilterRangesWithMultipleSupersetSingleSubsetOverlappingOnRight()
    {
        Collection<Range<Token>> toFilter = generateRanges(0, 120);
        Collection<Range<Token>> rejected = new OwnedRanges(generateRanges(0, 100, 200, 300, 400, 500)).testRanges(toFilter);
        assertRangesEqual(toFilter, rejected);
    }

    @Test
    public void testFilterRangesWithMultipleSupersetSingleSubsetOverlappingOnLeft()
    {
        Collection<Range<Token>> toFilter = generateRanges(-10, 100);
        Collection<Range<Token>> rejected = new OwnedRanges(generateRanges(0, 100, 200, 300, 400, 500)).testRanges(toFilter);
        assertRangesEqual(toFilter, rejected);
    }

    @Test
    public void testFilterRangesWithMultipleSupersetMultipleSubsetAllDisjoint()
    {
        Collection<Range<Token>> toFilter = generateRanges(-20, -10, 110, 120, 310, 320);
        Collection<Range<Token>> rejected = new OwnedRanges(generateRanges(0, 100, 200, 300, 400, 500)).testRanges(toFilter);
        assertRangesEqual(toFilter, rejected);
    }

    @Test
    public void testFilterRangesWithMultipleSupersetMultipleSubsetAllContained()
    {
        Collection<Range<Token>> toFilter = generateRanges(10, 20, 210, 220, 410, 420);
        assertTrue(new OwnedRanges(generateRanges(0, 100, 200, 300, 400, 500)).testRanges(toFilter).isEmpty());
    }

    @Test
    public void testFilterRangesWithMultipleSupersetMultipleSubsetSomeContained()
    {
        Collection<Range<Token>> toFilter = generateRanges(10, 20, 210, 220, 310, 320, 410, 420);
        Collection<Range<Token>> rejected = new OwnedRanges(generateRanges(0, 100, 200, 300, 400, 500)).testRanges(toFilter);
        assertRangesEqual(rejected, generateRanges(310, 320));
    }

    @Test
    public void testFilterRangesWithMultipleSupersetMultipleSubsetSomeExactMatchSomeContained()
    {
        Collection<Range<Token>> toFilter = generateRanges(10, 20, 200, 300, 410, 420);
        assertTrue(new OwnedRanges(generateRanges(0, 100, 200, 300, 400, 500)).testRanges(toFilter).isEmpty());
    }

    private static void assertRangesEqual(Collection<Range<Token>> first, Collection<Range<Token>> second)
    {
        assertTrue(CollectionUtils.isEqualCollection(Range.normalize(first), Range.normalize(second)));
    }
}
