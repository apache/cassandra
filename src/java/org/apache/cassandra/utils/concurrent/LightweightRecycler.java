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
import java.util.Collection;
import java.util.Objects;
import java.util.function.Supplier;


interface LightweightRecyclerPoolHolder<T>
{
    ArrayDeque<T> get();
}

/**
 * A simple thread local object reuse facility with limited capacity and no attempt at rebalancing pooling between
 * threads. This is meant to be put in place where churn is high, but single object allocation and footprint are not
 * so high to justify a more sophisticated approach.
 *
 * @param <T>
 * @see ThreadLocals#createLightweightRecycler(int)
 */
public interface LightweightRecycler<T> extends LightweightRecyclerPoolHolder<T>
{
    /**
     * @return a reusable instance, or null if none is available
     */
    default T reuse()
    {
        return get().pollFirst();
    }

    /**
     * @return a reusable instance, or allocate one via the provided supplier
     */
    default T reuseOrAllocate(Supplier<T> supplier)
    {
        final T reuse = reuse();
        return reuse != null ? reuse : supplier.get();
    }

    /**
     * @param t to be recycled, if t is a collection it will be cleared before recycling, but not cleared if not
     *          recycled
     * @return true if t was recycled, false otherwise
     */
    default boolean tryRecycle(T t)
    {
        Objects.requireNonNull(t);

        final ArrayDeque<T> pool = get();
        if (pool.size() < capacity())
        {
            if (t instanceof Collection)
                ((Collection<?>) t).clear();
            pool.offerFirst(t);
            return true;
        }
        else
        {
            return false;
        }
    }

    /**
     * @return current count of available instances for reuse
     */
    default int available()
    {
        return get().size();
    }


    /**
     * @return maximum capacity of the recycler
     */
    int capacity();
}
