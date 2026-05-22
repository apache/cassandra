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

import java.util.*;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.index.sai.iterators.LongIterator.convert;

public class KeyRangeUnionIteratorTest extends AbstractKeyRangeIteratorTest
{
    @Test
    public void testNoOverlappingValues()
    {
        KeyRangeUnionIterator.Builder builder = KeyRangeUnionIterator.builder();

        builder.add(new LongIterator(new long[] { 2L, 3L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 1L, 7L }));
        builder.add(new LongIterator(new long[] { 4L, 8L, 9L, 10L }));

        Assert.assertEquals(convert(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L), convert(builder.build()));
    }

    @Test
    public void testSingleIterator()
    {
        KeyRangeUnionIterator.Builder builder = KeyRangeUnionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L, 2L, 4L, 9L }));

        Assert.assertEquals(convert(1L, 2L, 4L, 9L), convert(builder.build()));
    }

    @Test
    public void testOverlappingValues()
    {
        KeyRangeUnionIterator.Builder builder = KeyRangeUnionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L, 4L, 6L, 7L }));
        builder.add(new LongIterator(new long[] { 2L, 3L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 4L, 6L, 8L, 9L, 10L }));

        List<Long> values = convert(builder.build());

        Assert.assertEquals(values.toString(), convert(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L), values);
    }

    @Test
    public void testNoOverlappingRanges()
    {
        KeyRangeUnionIterator.Builder builder = KeyRangeUnionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L, 2L, 3L }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 7L, 8L, 9L }));

        Assert.assertEquals(convert(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L), convert(builder.build()));
    }

    @Test
    public void testTwoIteratorsWithSingleValues()
    {
        KeyRangeUnionIterator.Builder builder = KeyRangeUnionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L }));
        builder.add(new LongIterator(new long[] { 1L }));

        Assert.assertEquals(convert(1L), convert(builder.build()));
    }

    @Test
    public void testDifferentSizeIterators()
    {
        KeyRangeUnionIterator.Builder builder = KeyRangeUnionIterator.builder();

        builder.add(new LongIterator(new long[] { 2L, 3L, 5L, 6L, 12L, 13L }));
        builder.add(new LongIterator(new long[] { 1L, 7L, 14L, 15 }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 8L, 9L, 10L }));

        Assert.assertEquals(convert(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 12L, 13L, 14L, 15L), convert(builder.build()));
    }

    @Test
    public void testRandomSequences()
    {
        for (int testIteration = 0; testIteration < 16; testIteration++)
        {
            var p = createRandom(nextInt(1, 20));
            validateWithSkipping(p.left, p.right);
        }
    }

    static Pair<KeyRangeIterator, long[]> createRandom(int nRanges)
    {
        long[][] values = new long[nRanges][];
        KeyRangeUnionIterator.Builder builder = KeyRangeUnionIterator.builder();

        var allValues = new HashSet<Long>();
        // add a random number of random values
        for (int i = 0; i < values.length; i++)
        {
            int partLength = nextInt(1, 500);
            var part = new HashSet<Long>(partLength);
            for (int j = 0; j < partLength; j++)
            {
                long m = nextLong(0, 1024);
                part.add(m);
                allValues.add(m);
            }

            // all of the parts have to be sorted to mimic SSTable
            builder.add(new LongIterator(part.stream().mapToLong(Long::longValue).sorted().toArray()));
        }

        long[] totalOrdering = allValues.stream().mapToLong(Long::longValue).sorted().toArray();
        KeyRangeIterator tokens = builder.build();
        return Pair.create(tokens, totalOrdering);
    }

    @Test
    public void testMinMaxAndCount()
    {
        KeyRangeUnionIterator.Builder builder = KeyRangeUnionIterator.builder();

        builder.add(new LongIterator(new long[] { 1L, 2L, 3L }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 7L, 8L, 9L }));

        Assert.assertEquals(9L, builder.getMaximum().token().getLongValue());
        Assert.assertEquals(9L, builder.getTokenCount());

        KeyRangeIterator tokens = builder.build();

        Assert.assertNotNull(tokens);
        Assert.assertEquals(1L, tokens.getMinimum().token().getLongValue());
        Assert.assertEquals(9L, tokens.getMaximum().token().getLongValue());
        Assert.assertEquals(9L, tokens.getMaxKeys());

        for (long i = 1; i < 10; i++)
        {
            Assert.assertTrue(tokens.hasNext());
            Assert.assertEquals(i, tokens.next().token().getLongValue());
        }

        Assert.assertFalse(tokens.hasNext());
        Assert.assertEquals(1L, tokens.getMinimum().token().getLongValue());
    }

    @Test
    public void testBuilder()
    {
        KeyRangeUnionIterator.Builder builder = KeyRangeUnionIterator.builder();

        Assert.assertNull(builder.getMinimum());
        Assert.assertNull(builder.getMaximum());
        Assert.assertEquals(0L, builder.getTokenCount());
        Assert.assertEquals(0L, builder.rangeCount());

        builder.add(new LongIterator(new long[] { 1L, 2L, 3L }));
        builder.add(new LongIterator(new long[] { 4L, 5L, 6L }));
        builder.add(new LongIterator(new long[] { 7L, 8L, 9L }));

        Assert.assertEquals(1L, builder.getMinimum().token().getLongValue());
        Assert.assertEquals(9L, builder.getMaximum().token().getLongValue());
        Assert.assertEquals(9L, builder.getTokenCount());
        Assert.assertEquals(3L, builder.rangeCount());
        Assert.assertFalse(builder.statistics.isEmptyOrDisjoint());

        Assert.assertEquals(1L, builder.rangeIterators.get(0).getMinimum().token().getLongValue());
        Assert.assertEquals(4L, builder.rangeIterators.get(1).getMinimum().token().getLongValue());
        Assert.assertEquals(7L, builder.rangeIterators.get(2).getMinimum().token().getLongValue());

        KeyRangeIterator tokens = KeyRangeUnionIterator.build(new ArrayList<KeyRangeIterator>()
        {{
            add(new LongIterator(new long[]{1L, 2L, 4L}));
            add(new LongIterator(new long[]{3L, 5L, 6L}));
        }});

        Assert.assertEquals(convert(1L, 2L, 3L, 4L, 5L, 6L), convert(tokens));

        FileUtils.closeQuietly(tokens);

        var emptyTokens = KeyRangeUnionIterator.builder().build();
        Assert.assertEquals(0, emptyTokens.getMaxKeys());

        builder = KeyRangeUnionIterator.builder();
        Assert.assertEquals(0L, builder.add((KeyRangeIterator) null).rangeCount());
        Assert.assertEquals(0L, builder.add((List<KeyRangeIterator>) null).getTokenCount());
        Assert.assertEquals(0L, builder.add(new LongIterator(new long[] {})).rangeCount());
    }

    @Test
    public void testSkipTo()
    {
        var builder = KeyRangeUnionIterator.<PrimaryKey>builder();

        builder.add(new LongIterator(new long[]{1L, 2L, 3L}));
        builder.add(new LongIterator(new long[]{4L, 5L, 6L}));
        builder.add(new LongIterator(new long[]{7L, 8L, 9L}));

        KeyRangeIterator tokens = builder.build();
        Assert.assertNotNull(tokens);

        tokens.skipTo(LongIterator.makeKey(5L));
        Assert.assertTrue(tokens.hasNext());
        Assert.assertEquals(5L, tokens.next().token().getLongValue());

        tokens.skipTo(LongIterator.makeKey(7L));
        Assert.assertTrue(tokens.hasNext());
        Assert.assertEquals(7L, tokens.next().token().getLongValue());

        tokens.skipTo(LongIterator.makeKey(10L));
        Assert.assertFalse(tokens.hasNext());
        Assert.assertEquals(1L, tokens.getMinimum().token().getLongValue());
        Assert.assertEquals(9L, tokens.getMaximum().token().getLongValue());
    }

    @Test
    public void testMergingMultipleIterators()
    {
        KeyRangeUnionIterator.Builder builderA = KeyRangeUnionIterator.builder();

        builderA.add(new LongIterator(new long[] { 1L, 3L, 5L }));
        builderA.add(new LongIterator(new long[] { 8L, 10L, 12L }));

        KeyRangeUnionIterator.Builder builderB = KeyRangeUnionIterator.builder();

        builderB.add(new LongIterator(new long[] { 7L, 9L, 11L }));
        builderB.add(new LongIterator(new long[] { 2L, 4L, 6L }));

        KeyRangeIterator union = KeyRangeUnionIterator.build(Arrays.asList(builderA.build(), builderB.build()));
        Assert.assertEquals(convert(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L), convert(union));
    }

    @Test
    public void testRangeIterator()
    {
        LongIterator tokens = new LongIterator(new long[] { 0L, 1L, 2L, 3L });

        Assert.assertEquals(0L, tokens.getMinimum().token().getLongValue());
        Assert.assertEquals(3L, tokens.getMaximum().token().getLongValue());

        for (int i = 0; i <= 3; i++)
        {
            Assert.assertTrue(tokens.hasNext());
            Assert.assertEquals(i, tokens.peek().token().getLongValue());
            Assert.assertEquals(i, tokens.next().token().getLongValue());
        }

        tokens = new LongIterator(new long[] { 0L, 1L, 3L, 5L });

        tokens.skipTo(LongIterator.makeKey(2L));
        Assert.assertTrue(tokens.hasNext());
        Assert.assertEquals(3L, tokens.peek().token().getLongValue());
        Assert.assertEquals(3L, tokens.next().token().getLongValue());

        tokens.skipTo(LongIterator.makeKey(5L));
        Assert.assertTrue(tokens.hasNext());
        Assert.assertEquals(5L, tokens.peek().token().getLongValue());
        Assert.assertEquals(5L, tokens.next().token().getLongValue());

        LongIterator empty = new LongIterator(new long[0]);

        empty.skipTo(LongIterator.makeKey(3L));
        Assert.assertFalse(empty.hasNext());
    }

    @Test
    public void emptyRangeTest() {
        KeyRangeIterator.Builder builder;
        KeyRangeIterator range;
        // empty, then non-empty
        builder = KeyRangeUnionIterator.builder();
        builder.add(new LongIterator(new long[] {}));
        for (int i = 0; i < 10; i++)
            builder.add(new LongIterator(new long[] {i + 10}));
        range = builder.build();
        Assert.assertEquals(10L, range.getMinimum().token().getLongValue());
        Assert.assertEquals(19L, range.getMaximum().token().getLongValue());
        Assert.assertTrue(range.hasNext());
        Assert.assertEquals(10, range.getMaxKeys());

        builder = KeyRangeUnionIterator.builder();
        builder.add(new LongIterator(new long[] {}));
        builder.add(new LongIterator(new long[] {10}));
        range = builder.build();
        Assert.assertEquals(10L, range.getMinimum().token().getLongValue());
        Assert.assertEquals(10L, range.getMaximum().token().getLongValue());
        Assert.assertTrue(range.hasNext());
        Assert.assertEquals(1, range.getMaxKeys());

        // non-empty, then empty
        builder = KeyRangeUnionIterator.builder();
        for (int i = 0; i < 10; i++)
            builder.add(new LongIterator(new long[] {i + 10}));
        builder.add(new LongIterator(new long[] {}));
        range = builder.build();
        Assert.assertEquals(10, range.getMinimum().token().getLongValue());
        Assert.assertEquals(19, range.getMaximum().token().getLongValue());
        Assert.assertTrue(range.hasNext());
        Assert.assertEquals(10, range.getMaxKeys());

        builder = KeyRangeUnionIterator.builder();
        builder.add(new LongIterator(new long[] {10}));
        builder.add(new LongIterator(new long[] {}));
        range = builder.build();
        Assert.assertEquals(10L, range.getMinimum().token().getLongValue());
        Assert.assertEquals(10L, range.getMaximum().token().getLongValue());
        Assert.assertTrue(range.hasNext());
        Assert.assertEquals(1, range.getMaxKeys());

        // empty, then non-empty then empty again
        builder = KeyRangeUnionIterator.builder();
        builder.add(new LongIterator(new long[] {}));
        for (int i = 0; i < 10; i++)
            builder.add(new LongIterator(new long[] {i + 10}));
        builder.add(new LongIterator(new long[] {}));
        range = builder.build();
        Assert.assertEquals(10L, range.getMinimum().token().getLongValue());
        Assert.assertEquals(19L, range.getMaximum().token().getLongValue());
        Assert.assertTrue(range.hasNext());
        Assert.assertEquals(10, range.getMaxKeys());

        // non-empty, empty, then non-empty again
        builder = KeyRangeUnionIterator.builder();
        for (int i = 0; i < 5; i++)
            builder.add(new LongIterator(new long[] {i + 10}));
        builder.add(new LongIterator(new long[] {}));
        for (int i = 5; i < 10; i++)
            builder.add(new LongIterator(new long[] {i + 10}));
        range = builder.build();
        Assert.assertEquals(10L, range.getMinimum().token().getLongValue());
        Assert.assertEquals(19L, range.getMaximum().token().getLongValue());
        Assert.assertTrue(range.hasNext());
        Assert.assertEquals(10, range.getMaxKeys());
    }

    // SAI specific tests
    @Test
    public void testUnionOfIntersection()
    {
        // union of two non-intersected intersections
        KeyRangeIterator intersectionA = buildIntersection(arr(1L, 2L, 3L), arr(4L, 5L, 6L));
        KeyRangeIterator intersectionB = buildIntersection(arr(6L, 7L, 8L), arr(9L, 10L, 11L));

        KeyRangeIterator union = buildUnion(intersectionA, intersectionB);
        assertEquals(convert(), convert(union));

        // union of two intersected intersections
        intersectionA = buildIntersection(arr(1L, 2L, 3L), arr(2L, 3L, 4L));
        intersectionB = buildIntersection(arr(6L, 7L, 8L), arr(7L, 8L, 9L));

        union = buildUnion(intersectionA, intersectionB);
        assertEquals(convert(2L, 3L, 7L, 8L), convert(union));
        // Because the iterators are disjoint, the constructor optimizes the union and returns a concat iterator
        assertEquals(KeyRangeConcatIterator.class, union.getClass());

        // union of one intersected intersection and one non-intersected intersection
        intersectionA = buildIntersection(arr(1L, 2L, 3L), arr(2L, 3L, 4L ));
        intersectionB = buildIntersection(arr(6L, 7L, 8L), arr(10L ));

        union = buildUnion(intersectionA, intersectionB);
        assertEquals(convert(2L, 3L), convert(union));
    }

    @Test
    public void testUnionOfRandom()
    {
        for (int testIteration = 0; testIteration < 16; testIteration++)
        {
            var allValues = new HashSet<Long>();
            var builder = KeyRangeUnionIterator.builder();
            for (int i = 0; i < nextInt(2, 3); i++)
            {
                var p = createRandomIterator();
                builder.add(p.left);
                allValues.addAll(Arrays.stream(p.right).boxed().collect(Collectors.toList()));
            }
            long[] totalOrdered = allValues.stream().mapToLong(Long::longValue).sorted().toArray();
            validateWithSkipping(builder.build(), totalOrdered);
        }
    }

    @Test
    public void testEmptyClusteringTwoWayMerge() {
        PrimaryKey[] keysA = {
        makeKeyForRegularRow(1, 1L),
        makeKeyForRegularRow(2, 1L),
        makeKeyForRegularRow(2, 1000L),
        makePartitionAwareKey(3),
        makeKeyForRegularRow(3, 30L),
        makeKeyForRegularRow(3, 31L),
        makeKeyForRegularRow(3, 32L),
        makeKeyForRegularRow(3, 33L),
        makePartitionAwareKey(4)
        };

        PrimaryKey[] keysB = {
        makePartitionAwareKey(0),
        makeKeyForRegularRow(1, 2L),
        makePartitionAwareKey(2),
        makeKeyForRegularRow(3, 31L),
        makePartitionAwareKey(4)
        };

        List<PrimaryKey> expected = Arrays.asList(
        makePartitionAwareKey(0),
        makeKeyForRegularRow(1, 1L),
        makeKeyForRegularRow(1, 2L),
        makePartitionAwareKey(2),
        makePartitionAwareKey(3),
        makeKeyForStaticRow(4));

        testUnion(expected, keysA, keysB);
    }

    @Test
    public void testEmptyClusteringThreeWayMerge() {
        PrimaryKey[] keysA = {
        makeKeyForRegularRow(1, 11L),
        makeKeyForRegularRow(2, 21L),
        makeKeyForRegularRow(2, 1000L),
        makePartitionAwareKey(3),
        makeKeyForRegularRow(3, 0L),
        makeKeyForRegularRow(3, 1L),
        makeKeyForRegularRow(3, 2L),
        makeKeyForRegularRow(4, 41L),
        makePartitionAwareKey(6),
        makeKeyForRegularRow(7, 72L),
        makeKeyForRegularRow(7, 73L)
        };

        PrimaryKey[] keysB = {
        makeKeyForStaticRow(0),
        makeKeyForRegularRow(1, 13L),
        makePartitionAwareKey(2),
        makeKeyForRegularRow(3, 1L),
        makeKeyForRegularRow(4, 40L),
        makeKeyForRegularRow(4, 42L),
        makeKeyForRegularRow(4, 43L),
        makeKeyForRegularRow(4, 45L),
        makeKeyForRegularRow(5, 50L),
        makeKeyForRegularRow(7, 71L),
        makeKeyForRegularRow(7, 73L),
        makeKeyForRegularRow(7, 74L)
        };

        PrimaryKey[] keysC = {
        makeKeyForRegularRow(1, 12L),
        makeKeyForRegularRow(2, 22L),
        makeKeyForRegularRow(2, 5L),
        makeKeyForRegularRow(3, 1L),
        makePartitionAwareKey(4),
        makeKeyForRegularRow(6, 60L),
        makePartitionAwareKey(7)
        };

        List<PrimaryKey> expected = Arrays.asList(
        makePartitionAwareKey(0),
        makeKeyForRegularRow(1, 11L),
        makeKeyForRegularRow(1, 12L),
        makeKeyForRegularRow(1, 13L),
        makePartitionAwareKey(2),
        makePartitionAwareKey(3),
        makePartitionAwareKey(4),
        makeKeyForRegularRow(5, 50L),
        makePartitionAwareKey(6),
        makePartitionAwareKey(7)
        );

        testUnion(expected, keysA, keysB, keysC);
    }

    private void testUnion(List<PrimaryKey> expected, PrimaryKey[]... inputs) {
        // Test all permutations of input arrays to ensure order of iterators does not matter
        for (int[] permutation : permutations(inputs.length))
        {
            KeyRangeUnionIterator.Builder builder = KeyRangeUnionIterator.builder();

            for (int i = 0; i < inputs.length; i++)
                builder.add(PrimaryKeyListIterator.create(inputs[permutation[i]]));

            KeyRangeIterator union = builder.build();

            List<PrimaryKey> result = new ArrayList<>();
            while (union.hasNext()) {
                result.add(union.next());
            }

            Collections.sort(expected);
            assertKeysEqual(expected, result);
        }
    }

    @Test
    public void testRandomKeys() throws Throwable
    {
        for (int iteratorCount = 2; iteratorCount <= 5; iteratorCount++)
        {
            for (int i = 0; i < 200; i++)
            {
                var inputs = new ArrayList<List<PrimaryKey>>(iteratorCount);
                for (int j = 0; j < iteratorCount; j++)
                    inputs.add(randomPrimaryKeysOfMixedTypes(1 + i / 10, 1 + i / 10));

                testMerge(inputs,
                          KeyRangeUnionIteratorTest::union,
                          KeyRangeUnionIteratorTest::validateUnionResults);
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

                testSkipping(inputs, skips, KeyRangeUnionIteratorTest::unionIterator);
            }
        }
    }


    private static List<PrimaryKey> union(List<List<PrimaryKey>> inputs)
    {
        var iterator = unionIterator(inputs);

        // Limit the size of the result to avoid test timeouts.
        // We don't need to throw, because excessive results will be checked by validation logic
        // and that way we get better diagnostics. If we threw an assertion error here, the results wouldn't be printed.
        var sizeLimit = inputs.stream().mapToInt(List::size).sum() + 10;
        return collectKeys(iterator, sizeLimit);
    }

    private static KeyRangeIterator unionIterator(List<List<PrimaryKey>> inputs)
    {
        var builder = KeyRangeUnionIterator.builder();
        for (List<PrimaryKey> input : inputs)
            builder.add(PrimaryKeyListIterator.create(input));
        return builder.build();
    }


        private static void validateUnionResults(List<List<PrimaryKey>> inputs, List<PrimaryKey> result)
    {
        // Check for order and duplicates:
        assertIncreasing(result);

        // Check if we're not missing anything - all keys from input lists must be found in the output
        PrimaryKeySet resultKeySet = new PrimaryKeySet(result);
        for (List<PrimaryKey> input : inputs)
            for (PrimaryKey key : input)
                assertTrue("Missing key in union result:\n" + key, resultKeySet.contains(key));
    }
}

