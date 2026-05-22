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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.checkerframework.checker.nullness.qual.NonNull;

public class KeyRangeAntiJoinIteratorTest extends AbstractKeyRangeIteratorTest
{
    public static final long[] EMPTY = { };

    @Test
    public void testEmpty()
    {
        LongIterator left = new LongIterator(EMPTY);
        LongIterator right = new LongIterator(EMPTY);
        KeyRangeAntiJoinIterator iter = KeyRangeAntiJoinIterator.create(left, right);
        assertEquals(convert(), convert(iter));
    }

    @Test
    public void testEmptyLeft()
    {
        LongIterator left = new LongIterator(EMPTY);
        LongIterator right = new LongIterator(new long[] { 1L, 3L, 5L, 7L });
        KeyRangeAntiJoinIterator iter = KeyRangeAntiJoinIterator.create(left, right);
        assertEquals(convert(), convert(iter));
    }

    @Test
    public void testEmptyRight()
    {
        LongIterator left = new LongIterator(new long[] { 1L, 2L });
        LongIterator right = new LongIterator(EMPTY);
        KeyRangeAntiJoinIterator iter = KeyRangeAntiJoinIterator.create(left, right);
        assertEquals(convert(1, 2), convert(iter));
    }

    @Test
    public void testNoOverlappingValues()
    {
        LongIterator left = new LongIterator(new long[] { 2L, 4L, 6L, 8L });
        LongIterator right = new LongIterator(new long[] { 1L, 3L, 5L, 7L });
        KeyRangeAntiJoinIterator iter = KeyRangeAntiJoinIterator.create(left, right);
        assertEquals(convert(2, 4, 6, 8), convert(iter));
    }

    @Test
    public void testOverlappingValues()
    {
        LongIterator left = new LongIterator(new long[] { 2L, 3L, 4L, 6L, 8L, 9L, 10L });
        LongIterator right = new LongIterator(new long[] { 4L, 8L, 9L });
        KeyRangeAntiJoinIterator iter = KeyRangeAntiJoinIterator.create(left, right);
        assertEquals(convert(2, 3, 6, 10), convert(iter));
    }

    @Test
    public void testOverlappingPrefixes()
    {
        LongIterator left = new LongIterator(new long[] { 2L, 3L, 4L, 6L, 8L, 9L, 10L });
        LongIterator right = new LongIterator(new long[] { 2L, 3L, 4L });
        KeyRangeAntiJoinIterator iter = KeyRangeAntiJoinIterator.create(left, right);
        assertEquals(convert(6, 8, 9, 10), convert(iter));
    }

    @Test
    public void testRandomRegularRowKeys() throws Throwable
    {
        for (int i = 0; i < 200; i++)
        {
            var inputs = new ArrayList<List<PrimaryKey>>(2);
            for (int j = 0; j < 2; j++)
                inputs.add(randomPrimaryKeysWithRegularClusterings(1 + i / 10, 1 + i / 10));

            testMerge(inputs,
                      KeyRangeAntiJoinIteratorTest::antiJoin,
                      KeyRangeAntiJoinIteratorTest::validateAntiJoinResults);
        }
    }

    @Test
    public void testRandomStaticRowKeys() throws Throwable
    {
        for (int i = 0; i < 200; i++)
        {
            var inputs = new ArrayList<List<PrimaryKey>>(2);
            for (int j = 0; j < 2; j++)
                inputs.add(randomPrimaryKeysWithStaticClusterings(1 + i / 10, 1));

            testMerge(inputs,
                      KeyRangeAntiJoinIteratorTest::antiJoin,
                      KeyRangeAntiJoinIteratorTest::validateAntiJoinResults);
        }
    }

    @Test
    public void testSkippingWithRandomKeys() throws Throwable
    {
        for (int testIteration = 0; testIteration < 200; testIteration++)
        {
            int avgPartitions = 1 + testIteration / 10;
            int avgRowsPerPartition = 1 + testIteration / 10;

            var inputs = new ArrayList<List<PrimaryKey>>(2);
            for (int j = 0; j < 2; j++)
                inputs.add(randomPrimaryKeysWithRegularClusterings(avgPartitions, avgRowsPerPartition));

            // Generate random skip positions.
            // Use a different data set so that some skip positions exist in the merged result and some do not.
            var skips = randomSkips(randomPrimaryKeysWithRegularClusterings(avgPartitions, avgRowsPerPartition));
            testSkipping(inputs, skips, KeyRangeAntiJoinIteratorTest::antiJoinIterator);
        }
    }

    private static List<PrimaryKey> antiJoin(List<List<PrimaryKey>> inputs)
    {
        var iterator = antiJoinIterator(inputs);

        // Limit the size of the result to guarantee the test completes even if the code under test erroneously
        // generates an iterator that never completes.
        // We don't need to throw, because excessive results will be checked by validation logic
        // and that way we get better diagnostics. If we threw an assertion error here, the results wouldn't be printed.
        int sizeLimit = inputs.get(0).size() + 10;
        return collectKeys(iterator, sizeLimit);
    }

    private static @NonNull KeyRangeAntiJoinIterator antiJoinIterator(List<List<PrimaryKey>> inputs)
    {
        assert inputs.size() == 2;
        var left = PrimaryKeyListIterator.create(inputs.get(0));
        var right = PrimaryKeyListIterator.create(inputs.get(1));
        return KeyRangeAntiJoinIterator.create(left, right);
    }

    private static void validateAntiJoinResults(List<List<PrimaryKey>> inputs, List<PrimaryKey> result)
    {
        assert inputs.size() == 2;
        Set<PrimaryKey> left = new HashSet<>(inputs.get(0));
        Set<PrimaryKey> right = new HashSet<>(inputs.get(1));

        // Check for order and duplicates:
        assertIncreasing(result);

        var resultSet = new HashSet<>(result);

        // Check that the result contains all the keys on the left except the keys on the right:
        for (PrimaryKey pk : left)
            assertTrue("Result should contain key " + pk,
                       resultSet.contains(pk) || right.contains(pk));

        // Check that the result doesn't contain any of the single-row keys on the right:
        for (PrimaryKey pk : right)
            assertFalse("Result should not contain key " + pk, result.contains(pk));
    }

    public static List<Long> convert(KeyRangeIterator tokens)
    {
        List<Long> results = new ArrayList<>();
        while (tokens.hasNext())
            results.add(tokens.next().token().getLongValue());

        return results;
    }

    public static List<Long> convert(final long... nums)
    {
        return new ArrayList<>(nums.length)
        {{
            for (long n : nums)
                add(n);
        }};
    }
}
