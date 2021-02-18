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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.primitives.Ints;
import org.junit.Test;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.utils.ArrayPostingList;
import org.apache.cassandra.index.sai.utils.NdiRandomizedTest;

public class MergePostingListTest extends NdiRandomizedTest
{
    @Test
    public void shouldMergeInterleavedPostingLists() throws IOException
    {
        final PriorityQueue<PostingList.PeekablePostingList> lists = newPriorityQueue(
                new ArrayPostingList(new int[]{ 1, 4, 6 }),
                new ArrayPostingList(new int[]{ 2, 3, 4 }),
                new ArrayPostingList(new int[]{ 1, 6 }),
                new ArrayPostingList(new int[]{ 2, 5 }),
                new ArrayPostingList(new int[]{ 3, 6 }),
                new ArrayPostingList(new int[]{ 3, 5, 6 }));

        final PostingList merged = MergePostingList.merge(lists);

        assertPostingListEquals(new ArrayPostingList(new int[]{ 1, 2, 3, 4, 5, 6 }), merged);
    }

    @Test
    public void shouldMergeDisjointPostingLists() throws IOException
    {
        final PriorityQueue<PostingList.PeekablePostingList> lists = newPriorityQueue(
                new ArrayPostingList(new int[]{ 1, 6 }),
                new ArrayPostingList(new int[]{ 8, 9, 11 }),
                new ArrayPostingList(new int[]{ 15 }));

        final PostingList merged = MergePostingList.merge(lists);

        assertPostingListEquals(new ArrayPostingList(new int[]{ 1, 6, 8, 9, 11, 15 }), merged);
    }

    @Test
    public void shouldMergeSinglePostingList() throws IOException
    {
        final PriorityQueue<PostingList.PeekablePostingList> lists = newPriorityQueue(new ArrayPostingList(new int[]{ 1, 4, 6 }));

        final PostingList merged = MergePostingList.merge(lists);

        assertPostingListEquals(new ArrayPostingList(new int[]{ 1, 4, 6 }), merged);
    }

    @Test
    public void shouldMergeSamePostingLists() throws IOException
    {
        final PriorityQueue<PostingList.PeekablePostingList> lists = newPriorityQueue(new ArrayPostingList(new int[]{ 0 }),
                                                                                      new ArrayPostingList(new int[]{ 0 }));

        final PostingList merged = MergePostingList.merge(lists);

        assertPostingListEquals(new ArrayPostingList(new int[]{ 0 }), merged);
    }

    @Test
    public void shouldAdvanceAllMergedLists() throws IOException
    {
        final PriorityQueue<PostingList.PeekablePostingList> lists = newPriorityQueue(
                new ArrayPostingList(new int[]{ 1, 5, 10 }),
                new ArrayPostingList(new int[]{ 2, 3, 8 }),
                new ArrayPostingList(new int[]{ 3, 5, 9 }));

        final PostingList merged = MergePostingList.merge(lists);
        final PostingList expected = new ArrayPostingList(new int[]{ 1, 2, 3, 5, 8, 9, 10 });

        assertEquals(expected.advance(9),
                     merged.advance(9));

        assertPostingListEquals(expected, merged);
    }


    @Test
    public void shouldConsumeDuplicatedPostingOnAdvance() throws IOException
    {
        final PriorityQueue<PostingList.PeekablePostingList> lists = newPriorityQueue(
                new ArrayPostingList(new int[]{ 1, 4, 6 }),
                new ArrayPostingList(new int[]{ 2, 3, 4 }),
                new ArrayPostingList(new int[]{ 1, 6 }),
                new ArrayPostingList(new int[]{ 2, 5 }),
                new ArrayPostingList(new int[]{ 3, 6 }),
                new ArrayPostingList(new int[]{ 3, 5, 6 }));

        final PostingList merged = MergePostingList.merge(lists);

        assertEquals(2, merged.advance(2));
        assertEquals(4, merged.advance(4));
        assertPostingListEquals(new ArrayPostingList(new int[]{ 5, 6 }), merged);
    }

    @Test
    public void shouldInterleaveNextAndAdvance() throws IOException
    {
        final PriorityQueue<PostingList.PeekablePostingList> lists = newPriorityQueue(
                new ArrayPostingList(new int[]{ 1, 4, 6 }),
                new ArrayPostingList(new int[]{ 2, 3, 4 }),
                new ArrayPostingList(new int[]{ 1, 6 }),
                new ArrayPostingList(new int[]{ 2, 5 }),
                new ArrayPostingList(new int[]{ 3, 6 }),
                new ArrayPostingList(new int[]{ 3, 5, 6 }));

        final PostingList merged = MergePostingList.merge(lists);

        assertEquals(2, merged.advance(2));
        assertEquals(3, merged.nextPosting());
        assertEquals(5, merged.advance(5));
        assertEquals(6, merged.nextPosting());
    }

    @Test
    public void shouldAdvanceToAllElementsWithoutFailures()
    {
        testAdvancingToAllElements();
    }

    @Test
    public void shouldNotSkipUnconsumedElementOnAdvance() throws IOException
    {
        final PriorityQueue<PostingList.PeekablePostingList> lists = newPriorityQueue(
                new ArrayPostingList(new int[]{ 1, 2 }),
                new ArrayPostingList(new int[]{ 3 }));

        final PostingList merged = MergePostingList.merge(lists);
        assertEquals(1, merged.nextPosting());
        assertEquals(2, merged.advance(2));
        assertEquals(3, merged.nextPosting());
    }

    @Test
    public void shouldNotReadFromExhaustedChild() throws IOException
    {
        final PriorityQueue<PostingList.PeekablePostingList> lists = newPriorityQueue(
                new ArrayPostingList(new int[]{ 2 }),
                new ArrayPostingList(new int[]{ 1, 3, 4 }));

        final PostingList merged = MergePostingList.merge(lists);
        assertEquals(1, merged.nextPosting());
        assertEquals(3, merged.advance(3));
        assertEquals(4, merged.advance(4));
    }

    @Test
    public void shouldSkipDuplicates() throws IOException
    {
        final PriorityQueue<PostingList.PeekablePostingList> lists = newPriorityQueue(new ArrayPostingList(new int[]{ 1, 1, 2, 2, 2, 2, 5, 5 }),
                                                                                      new ArrayPostingList(new int[]{ 1, 2, 2, 3, 3, 4, 4, 5 }));

        final PostingList merged = MergePostingList.merge(lists);
        assertEquals(1, merged.nextPosting());
        assertEquals(2, merged.nextPosting());
        assertEquals(3, merged.advance(3));
        assertEquals(4, merged.advance(4));
        assertEquals(5, merged.nextPosting());
        assertEquals(PostingList.END_OF_STREAM, merged.nextPosting());
    }

    @Test
    public void shouldInterleaveNextAndAdvanceOnRandom() throws IOException
    {
        for (int i = 0; i < 1000; ++i)
        {
            testAdvancingOnRandom();
        }
    }

    private PriorityQueue<PostingList.PeekablePostingList> newPriorityQueue(PostingList...postingLists)
    {
        PriorityQueue<PostingList.PeekablePostingList> queue = new PriorityQueue<>(postingLists.length, Comparator.comparingLong(PostingList.PeekablePostingList::peek));
        for (PostingList postingList : postingLists)
            queue.add(postingList.peekable());
        return queue;
    }
    
    private void testAdvancingOnRandom() throws IOException
    {
        final int postingsCount = nextInt(1, 50_000);
        final int postingListCount = nextInt(5, 50);

        final AtomicInteger rowId = new AtomicInteger();
        final int[] postings = IntStream.generate(() -> rowId.addAndGet(nextInt(0, 10)))
                                        .limit(postingsCount)
                                        .toArray();
        final int[] postingsWithoutDuplicates = IntStream.of(postings)
                                                         .distinct()
                                                         .toArray();

        // split postings into multiple lists
        final Map<Integer, List<Integer>> splitPostings = Arrays.stream(postings)
                                                                .boxed()
                                                                .collect(Collectors.groupingBy(it -> nextInt(postingListCount)));

        final PriorityQueue<PostingList.PeekablePostingList> splitPostingLists = new PriorityQueue<>(splitPostings.size(), Comparator.comparingLong(PostingList.PeekablePostingList::peek));
        for (List<Integer> split : splitPostings.values())
        {
            splitPostingLists.add(new ArrayPostingList(Ints.toArray(split)).peekable());
        }

        final PostingList merge = MergePostingList.merge(splitPostingLists);
        final PostingList expected = new ArrayPostingList(postingsWithoutDuplicates);

        final List<PostingListAdvance> actions = new ArrayList<>();
        for (int idx = 0; idx < postingsWithoutDuplicates.length; idx++)
        {
            if (nextInt(0, 8) == 0)
            {
                actions.add((postingList) -> {
                    try
                    {
                        return postingList.nextPosting();
                    }
                    catch (IOException e)
                    {
                        fail(e.getMessage());
                        throw new RuntimeException(e);
                    }
                });
            }
            else
            {
                final int skips = nextInt(0, 10);
                idx = Math.min(idx + skips, postingsWithoutDuplicates.length - 1);
                final int rowID = postingsWithoutDuplicates[idx];
                actions.add((postingList) -> {
                    while (true)
                    {
                        try
                        {
                            return postingList.advance(rowID);
                        }
                        catch (ArrayPostingList.LookupException ignore)
                        {
                            // continue
                        }
                        catch (Exception e)
                        {
                            fail();
                        }
                    }
                });
            }
        }

        for (PostingListAdvance action : actions)
        {
            long expectedResult = action.advance(expected);
            long actualResult = action.advance(merge);

            assertEquals(expectedResult, actualResult);
        }
    }

    private void testAdvancingToAllElements()
    {
        final int[] postings1 = randomPostings();
        final int[] postings2 = randomPostings();

        final int[] mergedPostings = IntStream.concat(IntStream.of(postings1), IntStream.of(postings2))
                                              .distinct()
                                              .sorted()
                                              .toArray();

        final PriorityQueue<PostingList.PeekablePostingList> lists = newPriorityQueue(new ArrayPostingList(postings1), new ArrayPostingList(postings2));

        final PostingList merged = MergePostingList.merge(lists);

        // tokens are equal row IDs in this test case
        for (int targetRowID : mergedPostings)
        {
            long rowID;
            while (true)
            {
                try
                {
                    rowID = merged.advance(targetRowID);
                    break;
                }
                catch (ArrayPostingList.LookupException ignore)
                {
                    // continue
                }
                catch (Exception e)
                {
                    fail();
                }
            }
            assertEquals(targetRowID, rowID);
        }
    }

    private int[] randomPostings()
    {
        final AtomicInteger rowId = new AtomicInteger();
        return IntStream.generate(() -> rowId.getAndAdd(randomIntBetween(0, 5)))
                        .limit(randomIntBetween(1 << 10, 1 << 12))
                        .toArray();
    }

    private interface PostingListAdvance
    {
        long advance(PostingList list) throws IOException;
    }
}
