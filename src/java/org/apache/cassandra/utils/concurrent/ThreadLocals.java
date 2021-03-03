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

package org.apache.cassandra.utils.concurrent;

import java.util.ArrayDeque;

import io.netty.util.concurrent.FastThreadLocal;

public final class ThreadLocals
{
    private ThreadLocals()
    {
    }

    public static <T> LightweightRecycler<T> createLightweightRecycler(int limit)
    {
        return new FastThreadLocalLightweightRecycler<>(limit);
    }

    /**
     * A {@link LightweightRecycler} which is backed by a {@link FastThreadLocal}.
     */
    private static final class FastThreadLocalLightweightRecycler<T> extends FastThreadLocal<ArrayDeque<T>> implements LightweightRecycler<T>
    {
        private final int capacity;

        public FastThreadLocalLightweightRecycler(int capacity)
        {
            super();
            this.capacity = capacity;
        }

        protected ArrayDeque<T> initialValue()
        {
            return new ArrayDeque<>(capacity);
        }

        /**
         * @return maximum capacity of the recycler
         */
        public int capacity()
        {
            return capacity;
        }
    }
}
