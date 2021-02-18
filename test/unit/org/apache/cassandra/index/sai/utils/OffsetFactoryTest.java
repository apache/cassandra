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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;

public class OffsetFactoryTest
{
    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testTokenSkippingCacheWidePartition() throws IOException
    {
        // wide partition: row id to token value mapping
        LongArray token = LongArrays.from(new long[]{
                -10L, // 0
                -10L, // 1
                -10L, // 2
                -9L,  // 3
                -9L,  // 4
                -8L,  // 5 : pk = 1, ck = 1, v1 = 11, v2 = 101
                -8L,  // 6 : pk = 1, ck = 2, v1 = 11, v2 = 102
        }).build();

        try (OffsetFactory.TokenLongArray offsetLongArray = new OffsetFactory.TokenLongArray(SSTableQueryContext.forTest(), token, 0))
        {
            // for PostingListRangeIterator of column v1 = 11:
            //    find the next matched row id and its token, -8 is used to skip column v2.
            //    RangeIntersectionIterator will call another getNext() via hasNext after getting first match from getNext()..
            long tokenValue = offsetLongArray.get(5);
            assertEquals(-8L, tokenValue);
            tokenValue = offsetLongArray.get(6);
            assertEquals(-8L, tokenValue);

            // for PostingListRangeIterator of column v2 = 101: use -8 token to skip
            long rowId = offsetLongArray.findTokenRowID(-8);
            assertEquals(5, rowId);
        }
    }

    @Test
    public void testTokenSkippingOnLargeSSTable() throws Throwable
    {
        LongArray tokens = Mockito.mock(LongArray.class);

        long segmentRowIdOffset = Long.MAX_VALUE - 100L;

        Mockito.when(tokens.findTokenRowID(Mockito.anyLong())).thenReturn(segmentRowIdOffset + 1);

        SSTableQueryContext context = SSTableQueryContext.forTest();

        OffsetFactory.TokenLongArray offsetLongArray = new OffsetFactory.TokenLongArray(context, tokens, segmentRowIdOffset);

        long rowId = offsetLongArray.findTokenRowID(1L);

        assertEquals(1L, rowId);

    }
}