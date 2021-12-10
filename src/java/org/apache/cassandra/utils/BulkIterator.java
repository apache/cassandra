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

package org.apache.cassandra.utils;

import java.util.Iterator;

import org.apache.cassandra.utils.caching.TinyThreadLocalPool;

public interface BulkIterator<V> extends AutoCloseable
{
    void fetch(Object[] into, int offset, int count);
    V next();
    default void close() {};

    public static class FromArray<V> implements BulkIterator<V>, AutoCloseable
    {
        private static final TinyThreadLocalPool<FromArray> POOL = new TinyThreadLocalPool<>();

        private Object[] from;
        private int i;
        private TinyThreadLocalPool.TinyPool<FromArray> pool;

        private void init(Object[] from, int offset)
        {
            this.from = from;
            this.i = offset;
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

    public static class Adapter<V> implements BulkIterator<V>
    {
        final Iterator<V> adapt;

        private Adapter(Iterator<V> adapt)
        {
            this.adapt = adapt;
        }

        public void fetch(Object[] into, int offset, int count)
        {
            count += offset;
            while (offset < count && adapt.hasNext())
                into[offset++] = adapt.next();
        }

        public boolean hasNext()
        {
            return adapt.hasNext();
        }

        public V next()
        {
            return adapt.next();
        }
    }

    public static <V> FromArray<V> of(Object[] from)
    {
        return of(from, 0);
    }

    public static <V> FromArray<V> of(Object[] from, int offset)
    {
        TinyThreadLocalPool.TinyPool<FromArray> pool = FromArray.POOL.get();
        FromArray<V> result = pool.poll();
        if (result == null)
            result = new FromArray<>();
        result.init(from, offset);
        result.pool = pool;
        return result;
    }

    public static <V> Adapter<V> of(Iterator<V> from)
    {
        return new Adapter<>(from);
    }
}

