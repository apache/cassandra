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
package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

public class ReversedType<T> extends AbstractType<T>
{
    // interning instances
    private static final Map<AbstractType<?>, ReversedType<?>> instances = new ConcurrentHashMap<>();

    public final AbstractType<T> baseType;

    public static <T> ReversedType<T> getInstance(TypeParser parser)
    {
        List<AbstractType<?>> types = parser.getTypeParameters();
        if (types.size() != 1)
            throw new ConfigurationException("ReversedType takes exactly one argument, " + types.size() + " given");
        return getInstance((AbstractType<T>) types.get(0));
    }

    public static <T> ReversedType<T> getInstance(AbstractType<T> baseType)
    {
        ReversedType<?> type = instances.get(baseType);
        if (type != null)
            return (ReversedType<T>) type;

        Preconditions.checkArgument(!(baseType instanceof ReversedType),
                                    "Detected a type with 2 ReversedType() back-to-back, which is not allowed.");

        // We avoid constructor calls in Map#computeIfAbsent to avoid recursive update exceptions because the automatic
        // fixing of subtypes done by the top-level constructor might attempt a recursive update to the instances map.
        ReversedType<T> instance = new ReversedType<>(baseType);
        return (ReversedType<T>) instances.computeIfAbsent(baseType, k -> instance);
    }

    private ReversedType(AbstractType<T> baseType)
    {
        super(ComparisonType.CUSTOM, baseType.isMultiCell(), ImmutableList.of(baseType));
        this.baseType = baseType;
    }

    @Override
    public AbstractType<?> with(ImmutableList<AbstractType<?>> subTypes, boolean isMultiCell)
    {
        Preconditions.checkArgument(subTypes.size() == 1,
                                    "Invalid number of subTypes for ReversedType (got %s)", subTypes.size());
        return getInstance(subTypes.get(0));
    }

    public boolean isEmptyValueMeaningless()
    {
        return baseType.isEmptyValueMeaningless();
    }

    @Override
    public <V> ByteSource asComparableBytes(ValueAccessor<V> accessor, V data, ByteComparable.Version version)
    {
        ByteSource src = baseType.asComparableBytes(accessor, data, version);
        if (src == null)    // Note: this will only compare correctly if used within a sequence
            return null;
        // Invert all bytes.
        // The comparison requirements for the original type ensure that this encoding will compare correctly with
        // respect to the reversed comparator function (and, specifically, prefixes of escaped byte-ordered types will
        // compare as larger). Additionally, the weak prefix-freedom requirement ensures this encoding will also be
        // weakly prefix-free.
        return () ->
        {
            int v = src.next();
            if (v == ByteSource.END_OF_STREAM)
                return v;
            return v ^ 0xFF;
        };
    }

    @Override
    public <V> V fromComparableBytes(ValueAccessor<V> accessor, ByteSource.Peekable comparableBytes, ByteComparable.Version version)
    {
        return baseType.fromComparableBytes(accessor, ReversedPeekableByteSource.of(comparableBytes), version);
    }

    public <VL, VR> int compareCustom(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR)
    {
        return baseType.compare(right, accessorR, left, accessorL);
    }

    @Override
    public int compareForCQL(ByteBuffer v1, ByteBuffer v2)
    {
        return baseType.compare(v1, v2);
    }

    public <V> String getString(V value, ValueAccessor<V> accessor)
    {
        return baseType.getString(value, accessor);
    }

    public ByteBuffer fromString(String source)
    {
        return baseType.fromString(source);
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        return baseType.fromJSONObject(parsed);
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        return baseType.toJSONString(buffer, protocolVersion);
    }

    @Override
    public boolean isCompatibleWith(AbstractType<?> otherType)
    {
        if (!(otherType instanceof ReversedType))
            return false;

        return this.baseType.isCompatibleWith(otherType.unwrap());
    }

    @Override
    public CQL3Type asCQL3Type()
    {
        return baseType.asCQL3Type();
    }

    @Override
    public TypeSerializer<T> getSerializer()
    {
        return baseType.getSerializer();
    }

    @Override
    public ArgumentDeserializer getArgumentDeserializer()
    {
        return baseType.getArgumentDeserializer();
    }

    @Override
    public int valueLengthIfFixed()
    {
        return baseType.valueLengthIfFixed();
    }

    @Override
    public boolean isReversed()
    {
        return true;
    }

    @Override
    public String toString()
    {
        return getClass().getName() + "(" + baseType + ")";
    }

    private static final class ReversedPeekableByteSource extends ByteSource.Peekable
    {
        private final ByteSource.Peekable original;

        static ByteSource.Peekable of(ByteSource.Peekable original)
        {
            return original != null ? new ReversedPeekableByteSource(original) : null;
        }

        private ReversedPeekableByteSource(ByteSource.Peekable original)
        {
            super(null);
            this.original = original;
        }

        @Override
        public int next()
        {
            int v = original.next();
            if (v != END_OF_STREAM)
                return v ^ 0xFF;
            return END_OF_STREAM;
        }

        @Override
        public int peek()
        {
            int v = original.peek();
            if (v != END_OF_STREAM)
                return v ^ 0xFF;
            return END_OF_STREAM;
        }
    }

    @Override
    public ByteBuffer getMaskedValue()
    {
        return baseType.getMaskedValue();
    }
}
