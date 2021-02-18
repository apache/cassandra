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

import java.util.Arrays;

public final class LongArrays
{
    public static LongArrayDecorator identity()
    {
        return new LongArrayDecorator(IDENTITY);
    }

    public static LongArrayDecorator from(long[] values)
    {
        return new LongArrayDecorator(new PrimitiveLongArray(values));
    }

    public static class LongArrayDecorator
    {
        private final LongArray base;

        private LongArrayDecorator(LongArray base)
        {
            this.base = base;
        }

        public LongArray build()
        {
            return base;
        }
    }

    private static final LongArray IDENTITY = new LongArray()
    {
        @Override
        public long get(long index)
        {
            return index;
        }

        @Override
        public long length()
        {
            return Long.MAX_VALUE;
        }

        @Override
        public long findTokenRowID(long value)
        {
            return value;
        }
    };

    private static class PrimitiveLongArray implements LongArray
    {
        private final long[] tokens;

        private PrimitiveLongArray(long[] tokens)
        {
            this.tokens = tokens;
        }

        @Override
        public long get(long idx)
        {
            return tokens[Math.toIntExact(idx)];
        }

        @Override
        public long length()
        {
            return tokens.length;
        }

        @Override
        public long findTokenRowID(long value)
        {
            return Arrays.binarySearch(tokens, value);
        }
    }
}
