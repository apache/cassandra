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

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.utils.ArrayPostingList;
import org.apache.cassandra.index.sai.utils.NdiRandomizedTest;
import org.apache.lucene.util.FixedBitSet;

public class FilteringPostingListTest extends NdiRandomizedTest
{
    @Test
    public void shouldMatchAllWithoutAdvance() throws IOException
    {
        int[] postingsArray = { 0, 1, 2, 3, 4, 5 };
        verifyFilteringWithoutAdvance(postingsArray, 0, postingsArray.length);
    }

    @Test
    public void shouldMatchEndRangeWithoutAdvance() throws IOException
    {
        int[] postingsArray = { 0, 1, 2, 3, 4, 5 };
        verifyFilteringWithoutAdvance(postingsArray, 2, postingsArray.length);
    }

    @Test
    public void shouldMatchStartRangeWithoutAdvance() throws IOException
    {
        int[] postingsArray = { 0, 1, 2, 3, 4, 5 };
        verifyFilteringWithoutAdvance(postingsArray, 0, 3);
    }

    @Test
    public void shouldMatchMiddleRangeWithoutAdvance() throws IOException
    {
        int[] postingsArray = { 0, 1, 2, 3, 4, 5 };
        verifyFilteringWithoutAdvance(postingsArray, 2, 4);
    }

    @Test
    public void shouldAdvanceToMiddleMatchingAll() throws IOException
    {
        int[] postingsArray = { 0, 1, 2, 3, 4, 5 };
        verifyFilteringWithAdvance(postingsArray, 0, postingsArray.length, 2);
    }

    @Test
    public void shouldAdvanceToStartMatchingAll() throws IOException
    {
        int[] postingsArray = { 0, 1, 2, 3, 4, 5 };
        verifyFilteringWithAdvance(postingsArray, 0, postingsArray.length, 0);
    }

    @Test
    public void shouldAdvanceToEndMatchingAll() throws IOException
    {
        int[] postingsArray = { 0, 1, 2, 3, 4, 5 };
        verifyFilteringWithAdvance(postingsArray, 0, postingsArray.length, 5);
    }

    @Test
    public void shouldAdvancePastEndMatchingAll() throws IOException
    {
        int[] postingsArray = { 0, 1, 2, 3, 4, 5 };
        verifyFilteringWithAdvance(postingsArray, 0, postingsArray.length, 6);
    }

    @Test
    public void shouldAdvanceToBeforeMatchStart() throws IOException
    {
        int[] postingsArray = { 0, 1, 2, 3, 4, 5 };
        verifyFilteringWithAdvance(postingsArray, 3, postingsArray.length, 1);
    }

    @Test
    public void shouldAdvanceToAfterMatchEnd() throws IOException
    {
        int[] postingsArray = { 0, 1, 2, 3, 4, 5 };
        verifyFilteringWithAdvance(postingsArray, 0, 2, 3);
    }

    @Test
    public void shouldAdvanceToExactMatchStart() throws IOException
    {
        int[] postingsArray = { 0, 1, 2, 3, 4, 5 };
        verifyFilteringWithAdvance(postingsArray, 2, postingsArray.length, 2);
    }

    private void verifyFilteringWithoutAdvance(int[] postingsArray, int from, int toExclusive) throws IOException
    {
        ArrayPostingList delegate = new ArrayPostingList(postingsArray);

        FixedBitSet filter = new FixedBitSet((int)delegate.size());
        filter.set(from, toExclusive);

        FilteringPostingList filteringPostings = new FilteringPostingList(filter, delegate);

        ArrayPostingList expected = new ArrayPostingList(Arrays.copyOfRange(postingsArray, from, toExclusive));
        assertPostingListEquals(expected, filteringPostings);
    }

    private void verifyFilteringWithAdvance(int[] postingsArray, int from, int toExclusive, int target) throws IOException
    {
        ArrayPostingList delegate = new ArrayPostingList(postingsArray);

        FixedBitSet filter = new FixedBitSet((int)delegate.size());
        filter.set(from, toExclusive);

        FilteringPostingList filteringPostings = new FilteringPostingList(filter, delegate);

        // Make sure the expected advance ID is either in the range of matches or the sentinel value.
        long expectedAdvanceTo = target < from ? from : target >= toExclusive ? PostingList.END_OF_STREAM : target;

        try
        {
            long id = filteringPostings.advance(target);

            assertEquals(expectedAdvanceTo, id);

            if (id == PostingList.END_OF_STREAM)
            {
                return;
            }
        }
        catch (Exception e)
        {
            long id = filteringPostings.advance(target);

            assertEquals(expectedAdvanceTo, id);

            if (id == PostingList.END_OF_STREAM)
            {
                return;
            }
        }

        ArrayPostingList expected = new ArrayPostingList(postingsArray);
        expected.advance(target);

        // Advance to the first actual match...
        while (expected.getOrdinal() <= from)
        {
            expected.nextPosting();
        }

        assertPostingListEquals(expected, filteringPostings);
    }
}
