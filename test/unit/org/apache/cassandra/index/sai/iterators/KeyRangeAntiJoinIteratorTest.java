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
import java.util.List;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class KeyRangeAntiJoinIteratorTest
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
