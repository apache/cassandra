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
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.index.sai.Token;

import static org.apache.cassandra.index.sai.utils.LongIterator.convert;

// Test that the different iterators cope with handling incoming ranges that are out of order
public class DeferredRangeIteratorTest
{
    @Test
    public void rangeUnionIteratorTest() throws Throwable
    {
        RangeUnionIterator.Builder builder = RangeUnionIterator.builder();

        builder.add(createDeferred(8, 20, 30, 50, 60, 60, 80));
        builder.add(createDeferred(10, 10, 30, 30, 60, 70, 80));
        builder.add(createDeferred(5, 30, 40, 60, 80, 80, 90, 100));

        Assert.assertEquals(convert(10L, 20L, 30L, 40L, 50L, 60L, 70L, 80L, 90L, 100L), convert(builder.build()));
    }

    @Test
    public void rangeIntersectionIteratorTest() throws Throwable
    {
        RangeIterator.Builder builder = RangeIntersectionIterator.selectiveBuilder();

        builder.add(createDeferred(8, 20, 30, 50, 60, 60, 80));
        builder.add(createDeferred(10, 10, 30, 30, 60, 70, 80));
        builder.add(createDeferred(5, 30, 40, 60, 80, 80, 90, 100));

        Assert.assertEquals(convert(30L, 60L, 80L), convert(builder.build()));
    }

    private RangeIterator createDeferred(long min, long... tokens)
    {
        return new DeferredRangeIterator(min, tokens);
    }

    private static class DeferredRangeIterator extends RangeIterator
    {
        private final List<LongIterator.LongToken> tokens;
        private int currentIdx = 0;

        public DeferredRangeIterator(long min, long[] tokens)
        {
            super(min, min + 100, 1000);
            this.tokens = new ArrayList<>(tokens.length);
            for (long token : tokens)
                this.tokens.add(new LongIterator.LongToken(token, token));
        }

        @Override
        protected Token computeNext()
        {
            if (currentIdx >= tokens.size())
                return endOfData();

            return tokens.get(currentIdx++);
        }

        @Override
        protected void performSkipTo(Long nextToken)
        {
            for (int i = currentIdx == 0 ? 0 : currentIdx - 1; i < tokens.size(); i++)
            {
                LongIterator.LongToken token = tokens.get(i);
                if (token.get() >= nextToken)
                {
                    currentIdx = i;
                    break;
                }
            }
        }

        @Override
        public void close() throws IOException
        {}
    }
}
