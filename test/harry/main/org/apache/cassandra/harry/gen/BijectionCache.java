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

package org.apache.cassandra.harry.gen;

import java.util.Comparator;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import org.apache.cassandra.harry.MagicConstants;

public class BijectionCache<T> implements Bijections.Bijection<T>
{
    private final BiMap<T, Long> valueToDescriptor = HashBiMap.create();
    private final Comparator<? super T> comparator;
    private long counter = 0;

    public BijectionCache(Comparator<? super T> comparator)
    {
        this.comparator = comparator;
    }

    @Override
    public T inflate(long descriptor)
    {
        if (MagicConstants.NIL_DESCR == descriptor)
            throw new IllegalArgumentException("Asked for NIL_DESCR");
        T value = valueToDescriptor.inverse().get(descriptor);
        if (value == null)
            throw new IllegalArgumentException(String.format("Attempted to inflate %d, but it is undefined", descriptor));
        return value;
    }

    @Override
    public long deflate(T value)
    {
        Preconditions.checkNotNull(value, "Attempted to deflate 'null'");
        if (valueToDescriptor.containsKey(value))
            return valueToDescriptor.get(value);
        long d = counter++;
        valueToDescriptor.put(value, d);
        return d;
    }

    public long deflateOrUndefined(T value)
    {
        return valueToDescriptor.containsKey(value) ? valueToDescriptor.get(value) : MagicConstants.UNSET_DESCR;
    }

    public Set<Long> descriptors()
    {
        return valueToDescriptor.inverse().keySet();
    }

    public Set<T> values()
    {
        return valueToDescriptor.keySet();
    }

    @Override
    public int population()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long adjustEntropyDomain(long descriptor)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long minValue()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long maxValue()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean unsigned()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Comparator<Long> descriptorsComparator()
    {
        return (a, b) -> comparator.compare(inflate(a), inflate(b));
    }

    @Override
    public String toString(long pd)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int byteSize()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compare(long l, long r)
    {
        T lhs = inflate(l);
        T rhs = inflate(r);
        return comparator.compare(lhs, rhs);
    }
}
