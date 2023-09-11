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

public class PostingListUnionTest
{
    @Test
    public void unionNonOverlappingValues() throws Exception
    {
        PeekablePostingList postingList1 = PeekablePostingList.makePeekable(new ArrayPostingList(1, 2, 3));
        PeekablePostingList postingList2 = PeekablePostingList.makePeekable(new ArrayPostingList(4, 5, 6));

        try (PostingList union = PostingListUnion.createFromPeekable(List.of(postingList1, postingList2)))
        {
            assertTrue(union instanceof PostingListUnion);

            assertEquals(1L, union.nextPosting());
            assertEquals(2L, union.nextPosting());
            assertEquals(3L, union.nextPosting());
            assertEquals(4L, union.nextPosting());
            assertEquals(5L, union.nextPosting());
            assertEquals(6L, union.nextPosting());
            assertEquals(PostingList.END_OF_STREAM, union.nextPosting());
        }
    }
    @Test
    public void unionNonOverlappingValuesOutOfOrder() throws Exception
    {
        PeekablePostingList postingList1 = PeekablePostingList.makePeekable(new ArrayPostingList(4, 5, 6));
        PeekablePostingList postingList2 = PeekablePostingList.makePeekable(new ArrayPostingList(1, 2, 3));

        try (PostingList union = PostingListUnion.createFromPeekable(List.of(postingList1, postingList2)))
        {
            assertTrue(union instanceof PostingListUnion);

            assertEquals(1L, union.nextPosting());
            assertEquals(2L, union.nextPosting());
            assertEquals(3L, union.nextPosting());
            assertEquals(4L, union.nextPosting());
            assertEquals(5L, union.nextPosting());
            assertEquals(6L, union.nextPosting());
            assertEquals(PostingList.END_OF_STREAM, union.nextPosting());
        }
    }

    @Test
    public void unionOverlappingValues() throws Exception
    {
        PeekablePostingList postingList1 = PeekablePostingList.makePeekable(new ArrayPostingList(1, 2, 3));
        PeekablePostingList postingList2 = PeekablePostingList.makePeekable(new ArrayPostingList(2, 3, 4));

        try (PostingList union = PostingListUnion.createFromPeekable(List.of(postingList1, postingList2)))
        {
            assertTrue(union instanceof PostingListUnion);

            assertEquals(1L, union.nextPosting());
            assertEquals(2L, union.nextPosting());
            assertEquals(3L, union.nextPosting());
            assertEquals(4L, union.nextPosting());
            assertEquals(PostingList.END_OF_STREAM, union.nextPosting());
        }
    }
    @Test
    public void unionOverlappingValuesOutOfOrder() throws Exception
    {
        PeekablePostingList postingList1 = PeekablePostingList.makePeekable(new ArrayPostingList(2, 3, 4));
        PeekablePostingList postingList2 = PeekablePostingList.makePeekable(new ArrayPostingList(1, 2, 3));

        try (PostingList union = PostingListUnion.createFromPeekable(List.of(postingList1, postingList2)))
        {
            assertTrue(union instanceof PostingListUnion);

            assertEquals(1L, union.nextPosting());
            assertEquals(2L, union.nextPosting());
            assertEquals(3L, union.nextPosting());
            assertEquals(4L, union.nextPosting());
            assertEquals(PostingList.END_OF_STREAM, union.nextPosting());
        }
    }

    @Test
    public void nullListShouldReturnEmptyList() throws Exception
    {
        try (PostingList union = PostingListUnion.createFromPeekable(null))
        {
            assertTrue(union instanceof PostingList.EmptyPostingList);
            assertEquals(PostingList.END_OF_STREAM, union.nextPosting());
        }

        try (PostingList union = PostingListUnion.createFromPostings(null))
        {
            assertTrue(union instanceof PostingList.EmptyPostingList);
            assertEquals(PostingList.END_OF_STREAM, union.nextPosting());
        }
    }

    @Test
    public void emptyListShouldReturnEmptyList() throws Exception
    {
        try (PostingList union = PostingListUnion.createFromPeekable(Collections.emptyList()))
        {
            assertTrue(union instanceof PostingList.EmptyPostingList);
            assertEquals(PostingList.END_OF_STREAM, union.nextPosting());
        }

        try (PostingList union = PostingListUnion.createFromPostings(Collections.emptyList()))
        {
            assertTrue(union instanceof PostingList.EmptyPostingList);
            assertEquals(PostingList.END_OF_STREAM, union.nextPosting());
        }
    }

    @Test
    public void singleListShouldReturnSame() throws Exception
    {
        try (PostingList union = PostingListUnion.createFromPeekable(List.of(PeekablePostingList.makePeekable(new ArrayPostingList(1, 2, 3)))))
        {
            assertTrue(union instanceof PeekablePostingList);
            assertEquals(1L, union.nextPosting());
            assertEquals(2L, union.nextPosting());
            assertEquals(3L, union.nextPosting());
            assertEquals(PostingList.END_OF_STREAM, union.nextPosting());
        }

        try (PostingList union = PostingListUnion.createFromPostings(List.of(new ArrayPostingList(1, 2, 3))))
        {
            assertTrue(union instanceof ArrayPostingList);
            assertEquals(1L, union.nextPosting());
            assertEquals(2L, union.nextPosting());
            assertEquals(3L, union.nextPosting());
            assertEquals(PostingList.END_OF_STREAM, union.nextPosting());
        }
    }
}
