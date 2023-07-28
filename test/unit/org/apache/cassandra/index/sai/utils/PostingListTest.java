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

package org.apache.cassandra.index.sai.utils;

import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.index.sai.disk.PostingList;


public class PostingListTest extends NdiRandomizedTest
{
    @Test
    public void testPeekablePostingList() throws IOException
    {
        PostingList.PeekablePostingList postingList = new ArrayPostingList(new int[]{ 1, 2, 3 }).peekable();
        assertEquals(3, postingList.size());
        assertEquals(1, postingList.peek());
        assertEquals(1, postingList.nextPosting());
        assertEquals(2, postingList.peek());
        assertEquals(2, postingList.nextPosting());
        assertEquals(3, postingList.peek());
        assertEquals(3, postingList.nextPosting());
        assertEquals(PostingList.END_OF_STREAM, postingList.peek());
        assertEquals(PostingList.END_OF_STREAM, postingList.nextPosting());

        postingList = new ArrayPostingList(new int[]{ 10, 20, 30, 40, 50, 60 }).peekable();
        assertEquals(10, postingList.peek());
        assertEquals(50, postingList.advance(45));
        assertEquals(60, postingList.peek());
        assertEquals(60, postingList.advance(60));
        assertEquals(PostingList.END_OF_STREAM, postingList.advance(60));
        assertEquals(PostingList.END_OF_STREAM, postingList.peek());
        assertEquals(PostingList.END_OF_STREAM, postingList.nextPosting());


        postingList = new ArrayPostingList(new int[]{ 10, 20, 30, 40, 50, 60 }).peekable();
        assertEquals(10, postingList.peek());
        assertEquals(50, postingList.advanceWithoutConsuming(45));
        assertEquals(50, postingList.peek());
        assertEquals(50, postingList.advance(45));
        assertEquals(60, postingList.advanceWithoutConsuming(60));
        assertEquals(60, postingList.advance(60));
        assertEquals(PostingList.END_OF_STREAM, postingList.peek());
        assertEquals(PostingList.END_OF_STREAM, postingList.nextPosting());
    }
}