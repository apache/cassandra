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

package org.apache.cassandra.test.microbench.btree;

import java.util.function.IntFunction;

import org.apache.cassandra.utils.BulkIterator;
import org.apache.cassandra.utils.btree.UpdateFunction;
import org.apache.cassandra.utils.caching.TinyThreadLocalPool;

public class Megamorphism
{
    // All functionallity noops,
    public enum UpdateF { SIMPLE, SIMPLE_MEGAMORPH, UNSIMPLE }

    private static final UpdateFunction SIMPLE_KEEP_OLD_1 = UpdateFunction.noOp();
    private static final UpdateFunction SIMPLE_KEEP_OLD_2 = UpdateFunction.Simple.of((a, b) -> a);
    private static final UpdateFunction SIMPLE_KEEP_OLD_3 = UpdateFunction.Simple.of((a, b) -> a);

    private static final UpdateFunction SIMPLE_KEEP_NEW_1 = UpdateFunction.Simple.of((a, b) -> b);
    private static final UpdateFunction SIMPLE_KEEP_NEW_2 = UpdateFunction.Simple.of((a, b) -> b);
    private static final UpdateFunction SIMPLE_KEEP_NEW_3 = UpdateFunction.Simple.of((a, b) -> b);

    private static final UpdateFunction UNSIMPLE_KEEP_OLD = new UpdateFunction()
    {
        @Override
        public Object merge(Object replacing, Object update) { return replacing; }
        @Override
        public void onAllocatedOnHeap(long heapSize) { }
        @Override
        public Object insert(Object v) { return v; }
    };

    private static final UpdateFunction UNSIMPLE_KEEP_NEW = new UpdateFunction()
    {
        @Override
        public Object merge(Object replacing, Object update) { return update; }
        @Override
        public void onAllocatedOnHeap(long heapSize) { }
        @Override
        public Object insert(Object v) { return v; }
    };

    static <V> IntFunction<UpdateFunction<V, V>> updateFGetter(boolean keepOld, BTreeBench.UpdateF updateF)
    {
        switch (updateF)
        {
            case SIMPLE: return keepOld ? i -> SIMPLE_KEEP_OLD_1 : i -> SIMPLE_KEEP_NEW_1;
            case UNSIMPLE: return keepOld ? i -> UNSIMPLE_KEEP_OLD : i -> UNSIMPLE_KEEP_NEW;
            case SIMPLE_MEGAMORPH:
                if (keepOld)
                {
                    return i -> {
                        switch (i % 3)
                        {
                            case 0: return SIMPLE_KEEP_OLD_1;
                            case 1: return SIMPLE_KEEP_OLD_2;
                            case 2: return SIMPLE_KEEP_OLD_3;
                            default: throw new IllegalStateException();
                        }
                    };
                }
                else
                {
                    return i -> {
                        switch (i % 3)
                        {
                            case 0: return SIMPLE_KEEP_NEW_1;
                            case 1: return SIMPLE_KEEP_NEW_2;
                            case 2: return SIMPLE_KEEP_NEW_3;
                            default: throw new IllegalStateException();
                        }
                    };
                }
            default:
                throw new IllegalStateException();
        }
    }

    static class FromArrayCopy<V> implements BulkIterator<V>, AutoCloseable
    {
        private static final TinyThreadLocalPool<FromArrayCopy> cache = new TinyThreadLocalPool<>();

        private Object[] from;
        private int i;
        private TinyThreadLocalPool.TinyPool<FromArrayCopy> pool;

        public static <V> FromArrayCopy<V> of(Object[] from)
        {
            TinyThreadLocalPool.TinyPool<FromArrayCopy> pool = cache.get();
            FromArrayCopy<V> result = pool.poll();
            if (result == null)
                result = new FromArrayCopy<>();
            result.from = from;
            result.i = 0;
            result.pool = pool;
            return result;
        }

        public void close()
        {
            pool.offer(this);
            from = null;
            pool = null;
        }

        public void fetch(Object[] into, int offset, int count)
        {
            System.arraycopy(from, i, into, offset, count);
            i += count;
        }

        public V next()
        {

            return (V) from[i++];
        }
    }

    static class FromArrayCopy2<V> implements BulkIterator<V>, AutoCloseable
    {
        private static final TinyThreadLocalPool<FromArrayCopy2> cache = new TinyThreadLocalPool<>();

        private Object[] from;
        private int i;
        private TinyThreadLocalPool.TinyPool<FromArrayCopy2> pool;

        public static <V> FromArrayCopy2<V> of(Object[] from)
        {
            TinyThreadLocalPool.TinyPool<FromArrayCopy2> pool = cache.get();
            FromArrayCopy2<V> result = pool.poll();
            if (result == null)
                result = new FromArrayCopy2<>();
            result.from = from;
            result.i = 0;
            result.pool = pool;
            return result;
        }

        public void close()
        {
            pool.offer(this);
            from = null;
            pool = null;
        }

        public void fetch(Object[] into, int offset, int count)
        {
            System.arraycopy(from, i, into, offset, count);
            i += count;
        }

        public V next()
        {

            return (V) from[i++];
        }
    }
}
