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

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.harry.MagicConstants;
import org.apache.cassandra.utils.ByteBufferUtil;

public class BijectionCache<T> implements Bijections.Bijection<T>
{
    private final BiMap<T, Long> valueToDescriptor = HashBiMap.create();
    private final Function<? super T, String> toString;
    private final Comparator<? super T> comparator;
    private long counter = 0;

    public BijectionCache(Function<? super T, String> toString,
                          Comparator<? super T> comparator)
    {
        this.toString = toString;
        this.comparator = comparator;
    }

    public static BijectionCache<Value> valueCache()
    {
        return new BijectionCache<>(v -> v.type.toCQLString(v.value), (l, r) -> {
            if (!l.type.equals(r.type))
                throw new IllegalArgumentException("Unable to compare different types: " + l.type.asCQL3Type() + " != " + r.type.asCQL3Type());
            // Cells resolve based off unsigned byte order and not type order
            return ByteBufferUtil.compareUnsigned(l.value, r.value);
        });
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
        return this::compare;
    }

    @Override
    public String toString(long pd)
    {
        T value = inflate(pd);
        return toString.apply(value);
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

    public static class Value
    {
        public final AbstractType<?> type;
        @Nullable
        public final ByteBuffer value;

        public Value(AbstractType<?> type, @Nullable ByteBuffer value)
        {
            this.type = Objects.requireNonNull(type);
            this.value = value;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Value value1 = (Value) o;
            return type.equals(value1.type) && Objects.equals(value, value1.value);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(type, value);
        }

        @Override
        public String toString()
        {
            if (value == null) return "null";
            if (value == ByteBufferUtil.EMPTY_BYTE_BUFFER) return "<empty>";
            return type.toCQLString(value);
        }
    }
}
