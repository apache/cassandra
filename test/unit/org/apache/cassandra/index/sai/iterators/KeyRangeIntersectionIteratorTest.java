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
package org.apache.cassandra.index.sai.iterators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.index.sai.iterators.LongIterator.convert;

public class KeyRangeIntersectionIteratorTest extends AbstractKeyRangeIteratorTest
{
    @Test
    public void testNoOverlappingValues()
    {
        KeyRangeIterator.Builder builder = KeyRangeIntersectionIterator.builder();

        builder.add(new LongIterator(new long[] { 2L, 3L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 1L, 7L }));
        builder.add(new LongIterator(new long[] { 4L, 8L, 9L, 10L }));

        Assert.assertEquals(convert(), convert(builder.build()));

        builder = KeyRangeIntersectionIterator.builder();
        // both ranges overlap by min/max but not by value
        builder.add(new LongIterator(new long[] { 1L, 5L, 7L, 9L }));
        builder.add(new LongIterator(new long[] { 6L }));

        KeyRangeIterator range = builder.build();

        Assert.assertNotNull(range);
        Assert.assertFalse(range.hasNext());

        builder = KeyRangeIntersectionIterator.builder();
        // both ranges overlap by min/max but not by value
        builder.add(new LongIterator(new long[] { 1L, 5L, 7L, 9L }));
        builder.add(new LongIterator(new long[] { 0L, 10L, 12L }));

        range = builder.build();

        Assert.assertNotNull(range);
        Assert.assertFalse(range.hasNext());
    }

    @Test
    public void testOverlappingValues()
    {
        KeyRangeIterator.Builder builder = KeyRangeIntersectionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L, 4L, 6L, 7L }));
        builder.add(new LongIterator(new long[] { 2L, 4L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 4L, 6L, 8L, 9L, 10L }));

        Assert.assertEquals(convert(4L, 6L), convert(builder.build()));
    }

    @Test
    public void testSameValues()
    {
        KeyRangeIterator.Builder builder = KeyRangeIntersectionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L, 2L, 3L, 4L }));
        builder.add(new LongIterator(new long[] { 1L, 2L, 3L, 4L }));

        Assert.assertEquals(convert(1L, 2L, 3L, 4L), convert(builder.build()));
    }

    @Test
    public void testSingleIterator()
    {
        KeyRangeIntersectionIterator.Builder builder = KeyRangeIntersectionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L, 2L, 4L, 9L }));

        KeyRangeIterator range = builder.build();
        // no need to wrap single input iterator in an intersection
        Assert.assertTrue("Single iterator wrapped in KeyRangeIntersectionIterator", range instanceof LongIterator);
        Assert.assertEquals(convert(1L, 2L, 4L, 9L), convert(range));
    }

    @Test
    public void testSkipTo()
    {
        KeyRangeIterator.Builder builder = KeyRangeIntersectionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L, 4L, 6L, 7L, 9L, 10L }));
        builder.add(new LongIterator(new long[] { 2L, 4L, 5L, 6L, 7L, 10L, 12L }));
        builder.add(new LongIterator(new long[] { 4L, 6L, 7L, 9L, 10L }));

        KeyRangeIterator range = builder.build();
        Assert.assertNotNull(range);

        // first let's skipTo something before range
        range.skipTo(LongIterator.makeKey(3L));
        Assert.assertEquals(4L, range.peek().token().getLongValue());

        // now let's skip right to the send value
        range.skipTo(LongIterator.makeKey(5L));
        Assert.assertEquals(6L, range.peek().token().getLongValue());

        // now right to the element
        range.skipTo(LongIterator.makeKey(7L));
        Assert.assertEquals(7L, range.peek().token().getLongValue());
        Assert.assertEquals(7L, range.next().token().getLongValue());

        Assert.assertTrue(range.hasNext());
        Assert.assertEquals(10L, range.peek().token().getLongValue());

        // now right after the last element
        range.skipTo(LongIterator.makeKey(11L));
        Assert.assertFalse(range.hasNext());
    }

    @Test
    public void testMinMaxAndCount()
    {
        KeyRangeIterator.Builder builder = KeyRangeIntersectionIterator.builder();

        builder.add(new LongIterator(new long[]{1L, 2L, 9L}));
        builder.add(new LongIterator(new long[]{4L, 5L, 9L}));
        builder.add(new LongIterator(new long[]{7L, 8L, 9L}));

        assertEquals(9L, builder.getMaximum().token().getLongValue());
        assertEquals(3L, builder.getTokenCount());

        KeyRangeIterator tokens = builder.build();

        assertNotNull(tokens);
        assertEquals(7L, tokens.getMinimum().token().getLongValue());
        assertEquals(9L, tokens.getMaximum().token().getLongValue());
        assertEquals(3L, tokens.getMaxKeys());

        Assert.assertEquals(convert(9L), convert(builder.build()));
    }

    @Test
    public void testBuilder()
    {
        KeyRangeIntersectionIterator.Builder builder = KeyRangeIntersectionIterator.builder();

        Assert.assertNull(builder.getMinimum());
        Assert.assertNull(builder.getMaximum());
        Assert.assertEquals(0L, builder.getTokenCount());
        Assert.assertEquals(0L, builder.rangeCount());

        builder.add(new LongIterator(new long[] { 1L, 2L, 6L }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 6L, 8L, 9L }));

        assertEquals(6L, builder.getMinimum().token().getLongValue());
        assertEquals(6L, builder.getMaximum().token().getLongValue());
        assertEquals(3L, builder.getTokenCount());
        assertEquals(3L, builder.rangeCount());
        assertFalse(builder.statistics.isEmptyOrDisjoint());

        Assert.assertEquals(1L, builder.rangeIterators.get(0).getMinimum().token().getLongValue());
        Assert.assertEquals(4L, builder.rangeIterators.get(1).getMinimum().token().getLongValue());
        Assert.assertEquals(6L, builder.rangeIterators.get(2).getMinimum().token().getLongValue());

        builder.add(new LongIterator(new long[] { 1L, 2L, 6L }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 6L, 8L, 9L }));

        Assert.assertEquals(convert(6L), convert(builder.build()));

        builder = KeyRangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[]{ 1L, 5L, 6L }));
        builder.add(new LongIterator(new long[]{ 3L, 5L, 6L }));

        var tokens = builder.build();

        Assert.assertEquals(convert(5L, 6L), convert(tokens));

        FileUtils.closeQuietly(tokens);

        var emptyTokens = KeyRangeIntersectionIterator.builder().build();
        Assert.assertEquals(0, emptyTokens.getMaxKeys());

        builder = KeyRangeIntersectionIterator.builder();
        Assert.assertEquals(0L, builder.add((KeyRangeIterator) null).rangeCount());
        Assert.assertEquals(0L, builder.add((List<KeyRangeIterator>) null).getTokenCount());
        Assert.assertEquals(0L, builder.add(new LongIterator(new long[] {})).rangeCount());

        var single = new LongIterator(new long[] { 1L, 2L, 3L });

        // Make a difference between empty and null ranges.
        builder = KeyRangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[] {}));
        Assert.assertEquals(0L, builder.rangeCount());
        builder.add(single);
        Assert.assertEquals(1L, builder.rangeCount());
        var range = builder.build();
        Assert.assertEquals(0, range.getMaxKeys());

        // disjoint case
        builder = KeyRangeIntersectionIterator.builder();

        // In the disjoint case, the input iterators should be eagerly closed on build and an empty iterator is
        // returned. These mocks are used to verify that the input iterators are closed.
        final AtomicBoolean firstIteratorClosed = new AtomicBoolean(false);
        final AtomicBoolean secondIteratorClosed = new AtomicBoolean(false);
        LongIterator firstIterator = new LongIterator(new long[] { 1L, 2L, 3L }) {
            @Override
            public void close()
            {
                firstIteratorClosed.set(true);
            }
        };
        LongIterator secondIterator = new LongIterator(new long[] { 4L, 5L, 6L }) {
            @Override
            public void close()
            {
                secondIteratorClosed.set(true);
            }
        };

        builder.add(firstIterator);
        builder.add(secondIterator);

        Assert.assertFalse(firstIteratorClosed.get());
        Assert.assertFalse(secondIteratorClosed.get());
        Assert.assertTrue(builder.statistics.isEmptyOrDisjoint());

        var disjointIntersection = builder.build();
        Assert.assertNotNull(disjointIntersection);
        Assert.assertFalse(disjointIntersection.hasNext());
        Assert.assertTrue("First input iterator was not closed", firstIteratorClosed.get());
        Assert.assertTrue("Second input iterator was not closed", secondIteratorClosed.get());
    }

    @Test
    public void emptyRangeTest()
    {
        KeyRangeIterator.Builder builder;

        // empty, then non-empty
        builder = KeyRangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[] {}));
        builder.add(new LongIterator(new long[] {10}));
        assertEmpty(builder.build());

        builder = KeyRangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[] {}));
        for (int i = 0; i < 10; i++)
            builder.add(new LongIterator(new long[] {0, i + 10}));
        assertEmpty(builder.build());

        // non-empty, then empty
        builder = KeyRangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[] {10}));
        builder.add(new LongIterator(new long[] {}));
        assertEmpty(builder.build());

        builder = KeyRangeIntersectionIterator.builder();
        for (int i = 0; i < 10; i++)
            builder.add(new LongIterator(new long[] {0, i + 10}));

        builder.add(new LongIterator(new long[] {}));
        assertEmpty(builder.build());

        // empty, then non-empty then empty again
        builder = KeyRangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[] {}));
        builder.add(new LongIterator(new long[] {0, 10}));
        builder.add(new LongIterator(new long[] {}));
        assertEmpty(builder.build());

        builder = KeyRangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[] {}));
        for (int i = 0; i < 10; i++)
            builder.add(new LongIterator(new long[] {0, i + 10}));
        builder.add(new LongIterator(new long[] {}));
        assertEmpty(builder.build());

        // non-empty, empty, then non-empty again
        builder = KeyRangeIntersectionIterator.builder();
        builder.add(new LongIterator(new long[] {0, 10}));
        builder.add(new LongIterator(new long[] {}));
        builder.add(new LongIterator(new long[] {0, 10}));
        assertEmpty(builder.build());

        builder = KeyRangeIntersectionIterator.builder();
        for (int i = 0; i < 5; i++)
            builder.add(new LongIterator(new long[] {0, i + 10}));
        builder.add(new LongIterator(new long[] {}));
        for (int i = 5; i < 10; i++)
            builder.add(new LongIterator(new long[] {0, i + 10}));
        assertEmpty(builder.build());
    }

    public static void assertEmpty(KeyRangeIterator range)
    {
        Assert.assertNull(range.getMinimum());
        Assert.assertNull(range.getMaximum());
        Assert.assertFalse(range.hasNext());
        Assert.assertEquals(0, range.getMaxKeys());
    }

    @Test
    public void testClose() throws IOException
    {
        var tokens = KeyRangeIntersectionIterator.<PrimaryKey>builder()
                                                 .add(new LongIterator(new long[] { 1L, 2L, 3L }))
                                                 .add(new LongIterator(new long[] { 2L, 5L, 6L }))
                                                 .build();

        Assert.assertNotNull(tokens);
        tokens.close();
    }

    @Test
    public void testIsOverlapping()
    {
        KeyRangeIterator rangeA, rangeB;

        rangeA = new LongIterator(new long[] { 1L, 5L });
        rangeB = new LongIterator(new long[] { 5L, 9L });
        Assert.assertTrue(KeyRangeIterator.isOverlapping(rangeA, rangeB));

        rangeA = new LongIterator(new long[] { 5L, 9L });
        rangeB = new LongIterator(new long[] { 1L, 6L });
        Assert.assertTrue(KeyRangeIterator.isOverlapping(rangeA, rangeB));

        rangeA = new LongIterator(new long[] { 5L, 9L });
        rangeB = new LongIterator(new long[] { 5L, 9L });
        Assert.assertTrue(KeyRangeIterator.isOverlapping(rangeA, rangeB));

        rangeA = new LongIterator(new long[] { 1L, 4L });
        rangeB = new LongIterator(new long[] { 5L, 9L });
        Assert.assertFalse(KeyRangeIterator.isOverlapping(rangeA, rangeB));

        rangeA = new LongIterator(new long[] { 6L, 9L });
        rangeB = new LongIterator(new long[] { 1L, 4L });
        Assert.assertFalse(KeyRangeIterator.isOverlapping(rangeA, rangeB));
    }

    @Test
    public void testIntersectionOfRandomRanges()
    {
        for (int attempt = 0; attempt < 16; attempt++)
        {
            var p = createRandom(nextInt(2, 16));
            validateWithSkipping(p.left, p.right);
        }
    }

    /**
     * @return a long[][] of random elements, and a long[] of the intersection of those elements
     */
    static Pair<KeyRangeIterator, long[]> createRandom(int nRanges)
    {
        // generate randomize ranges
        long[][] ranges = new long[nRanges][];
        for (int i = 0; i < ranges.length; i++)
        {
            int rangeSize = nextInt(16, 512);
            LongSet range = new LongHashSet(rangeSize);

            for (int j = 0; j < rangeSize; j++)
                range.add(nextInt(1024));

            ranges[i] = range.toArray();
            Arrays.sort(ranges[i]);
        }
        var builder = KeyRangeIntersectionIterator.builder();
        for (long[] range : ranges)
            builder.add(new LongIterator(range));

        Set<Long> expectedSet = toSet(ranges[0]);
        IntStream.range(1, ranges.length).forEach(i -> expectedSet.retainAll(toSet(ranges[i])));
        return Pair.create(builder.build(), expectedSet.stream().mapToLong(Long::longValue).sorted().toArray());
    }

    @Test
    public void testRandomKeys() throws Throwable
    {
        for (int iteratorCount = 2; iteratorCount <= 5; iteratorCount++)
        {
            for (int testIteration = 0; testIteration < 200; testIteration++)
            {
                var inputs = new ArrayList<List<PrimaryKey>>(iteratorCount);
                for (int j = 0; j < iteratorCount; j++)
                    inputs.add(randomPrimaryKeysOfMixedTypes(1 + testIteration / 10, 1 + testIteration / 10));

                testMerge(inputs,
                          KeyRangeIntersectionIteratorTest::intersection,
                          KeyRangeIntersectionIteratorTest::validateIntersectionResults);
            }
        }
    }

    @Test
    public void testSkippingWithRandomKeys() throws Throwable
    {
        for (int iteratorCount = 2; iteratorCount <= 5; iteratorCount++)
        {
            for (int testIteration = 0; testIteration < 200; testIteration++)
            {
                int avgPartitions = 1 + testIteration / 10;
                int avgRowsPerPartition = 1 + testIteration / 10;

                var inputs = new ArrayList<List<PrimaryKey>>(iteratorCount);
                for (int j = 0; j < iteratorCount; j++)
                    inputs.add(randomPrimaryKeysOfMixedTypes(avgPartitions, avgRowsPerPartition));

                // Generate random skip positions.
                // Use a different data set so that some skip positions exist in the merged result and some do not.
                var skips = randomSkips(randomPrimaryKeysOfMixedTypes(avgPartitions, avgRowsPerPartition));

                testSkipping(inputs, skips, KeyRangeIntersectionIteratorTest::intersectionIterator);
            }
        }
    }

    private static List<PrimaryKey> intersection(List<List<PrimaryKey>> inputs)
    {
        // Limit the size of the result to avoid test timeouts.
        // We don't need to throw, because excessive results will be checked by validation logic
        // and that way we get better diagnostics. If we threw an assertion error here, the results wouldn't be printed.
        var sizeLimit = inputs.stream().mapToInt(List::size).sum() + 10;
        return collectKeys(intersectionIterator(inputs), sizeLimit);
    }

    private static KeyRangeIterator intersectionIterator(List<List<PrimaryKey>> inputs)
    {
        var builder = KeyRangeIntersectionIterator.builder();
        for (List<PrimaryKey> input : inputs)
            builder.add(PrimaryKeyListIterator.create(input));

        return builder.build();
    }

    private static void validateIntersectionResults(List<List<PrimaryKey>> inputs, List<PrimaryKey> result)
    {
        // Check for order and duplicates:
        assertIncreasing(result);

        // Index the keys we got for faster search:
        ArrayList<PrimaryKeySet> inputSets = new ArrayList<>(inputs.size());
        for (List<PrimaryKey> input : inputs)
            inputSets.add(new PrimaryKeySet(input));

        // Check if all keys are present:
        PrimaryKeySet resultSet = new PrimaryKeySet(result);
        for (List<PrimaryKey> keys : inputs)
        {
            for (PrimaryKey key : keys)
            {
                if (!inputSets.stream().allMatch(input -> input.contains(key)))
                    continue;

                assertTrue("Missing key in intersection result:\n" + key, resultSet.contains(key));
            }
        }

        // Check if we inluded only the rows that are present in all inputs;
        // excessive rows are likely not a correctness issue, but they may degrade performance:
        for (int i = 0; i < inputs.size(); i++)
        {
            for (PrimaryKey key : result)
                assertTrue("Unexpected key in intersection result, not covered by input " + i +
                           ":\n" + key, inputSets.get(i).contains(key));
        }
    }
}
