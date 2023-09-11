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

package org.apache.cassandra.index.sai.postings;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.index.sai.disk.ArrayPostingList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PostingListIntersectionTest
{
    @Test
    public void overlapShouldReturnResults() throws Exception
    {
        PeekablePostingList postingList1 = PeekablePostingList.makePeekable(new ArrayPostingList(1, 2, 3));
        PeekablePostingList postingList2 = PeekablePostingList.makePeekable(new ArrayPostingList(2, 3, 4));

        try (PostingList intersection = PostingListIntersection.createFromPeekable(List.of(postingList1, postingList2)))
        {
            assertTrue(intersection instanceof PostingListIntersection);

            assertEquals(2L, intersection.nextPosting());
            assertEquals(3L, intersection.nextPosting());
            assertEquals(PostingList.END_OF_STREAM, intersection.nextPosting());
        }
    }

    @Test
    public void noOverlapShouldReturnNoResults() throws Exception
    {
        PeekablePostingList postingList1 = PeekablePostingList.makePeekable(new ArrayPostingList(1, 2, 3));
        PeekablePostingList postingList2 = PeekablePostingList.makePeekable(new ArrayPostingList(4, 5, 6));

        try (PostingList intersection = PostingListIntersection.createFromPeekable(List.of(postingList1, postingList2)))
        {
            assertEquals(PostingList.END_OF_STREAM, intersection.nextPosting());
        }
    }

    @Test
    public void nullListShouldReturnEmptyList() throws Exception
    {
        try (PostingList intersection = PostingListIntersection.createFromPeekable(null))
        {
            assertTrue(intersection instanceof PostingList.EmptyPostingList);
            assertEquals(PostingList.END_OF_STREAM, intersection.nextPosting());
        }

        try (PostingList intersection = PostingListIntersection.createFromPostings(null))
        {
            assertTrue(intersection instanceof PostingList.EmptyPostingList);
            assertEquals(PostingList.END_OF_STREAM, intersection.nextPosting());
        }
    }

    @Test
    public void emptyListShouldReturnEmptyList() throws Exception
    {
        try (PostingList intersection = PostingListIntersection.createFromPeekable(Collections.emptyList()))
        {
            assertTrue(intersection instanceof PostingList.EmptyPostingList);
            assertEquals(PostingList.END_OF_STREAM, intersection.nextPosting());
        }

        try (PostingList intersection = PostingListIntersection.createFromPostings(Collections.emptyList()))
        {
            assertTrue(intersection instanceof PostingList.EmptyPostingList);
            assertEquals(PostingList.END_OF_STREAM, intersection.nextPosting());
        }
    }

    @Test
    public void singleListShouldReturnSame() throws Exception
    {
        try (PostingList intersection = PostingListIntersection.createFromPeekable(List.of(PeekablePostingList.makePeekable(new ArrayPostingList(1, 2, 3)))))
        {
            assertTrue(intersection instanceof PeekablePostingList);
            assertEquals(1L, intersection.nextPosting());
            assertEquals(2L, intersection.nextPosting());
            assertEquals(3L, intersection.nextPosting());
            assertEquals(PostingList.END_OF_STREAM, intersection.nextPosting());
        }

        try (PostingList intersection = PostingListIntersection.createFromPostings(List.of(new ArrayPostingList(1, 2, 3))))
        {
            assertTrue(intersection instanceof ArrayPostingList);
            assertEquals(1L, intersection.nextPosting());
            assertEquals(2L, intersection.nextPosting());
            assertEquals(3L, intersection.nextPosting());
            assertEquals(PostingList.END_OF_STREAM, intersection.nextPosting());
        }
    }
}
