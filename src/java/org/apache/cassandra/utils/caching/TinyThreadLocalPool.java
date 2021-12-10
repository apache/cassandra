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

package org.apache.cassandra.utils.caching;

import io.netty.util.concurrent.FastThreadLocal;

public class TinyThreadLocalPool<V> extends FastThreadLocal<TinyThreadLocalPool.TinyPool<V>>
{
    protected TinyPool<V> initialValue()
    {
        return new TinyPool<>();
    }

    // a super-simple pool containing at most two items; useful because we primarily use btrees in a two-tier hierarchy
    // so a single thread local item would be insufficient, but an arbitrary length queue too much
    public static class TinyPool<V>
    {
        final Thread thread;
        Object val1, val2, val3;

        public TinyPool()
        {
            this.thread = Thread.currentThread();
        }

        public void offer(V value)
        {
            if (Thread.currentThread() == thread)
                offerSafe(value);
        }
        private void offerSafe(V value)
        {
            if (val1 == null) val1 = value;
            else if (val2 == null) val2 = value;
            else if (val3 == null) val3 = value;
        }
        public V poll()
        {
            Object result;
            if (val1 != null)
            {
                result = val1;
                val1 = null;
            }
            else if (val2 != null)
            {
                result = val2;
                val2 = null;
            }
            else if (val3 != null)
            {
                result = val3;
                val3 = null;
            }
            else result = null;
            return (V) result;
        }
    }

    public void offer(V value)
    {
        get().offer(value);
    }

    public V poll()
    {
        return get().poll();
    }
}
