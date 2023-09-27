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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.Vectors;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.JsonUtils;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

public final class VectorType<T> extends AbstractType<List<T>>
{
    private static class Key
    {
        private final AbstractType<?> type;
        private final int dimension;

        private Key(AbstractType<?> type, int dimension)
        {
            this.type = type;
            this.dimension = dimension;
        }

        private VectorType<?> create()
        {
            return new VectorType<>(type, dimension);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Key key = (Key) o;
            return dimension == key.dimension && Objects.equals(type, key.type);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(type, dimension);
        }
    }
    @SuppressWarnings("rawtypes")
    private static final ConcurrentHashMap<Key, VectorType> instances = new ConcurrentHashMap<>();

    public final AbstractType<T> elementType;
    public final int dimension;
    private final TypeSerializer<T> elementSerializer;
    private final int valueLengthIfFixed;
    private final VectorSerializer serializer;

    private VectorType(AbstractType<T> elementType, int dimension)
    {
        super(ComparisonType.CUSTOM);
        if (dimension <= 0)
            throw new InvalidRequestException(String.format("vectors may only have positive dimensions; given %d", dimension));
        this.elementType = elementType;
        this.dimension = dimension;
        this.elementSerializer = elementType.getSerializer();
        this.valueLengthIfFixed = elementType.isValueLengthFixed() ?
                                  elementType.valueLengthIfFixed() * dimension :
                                  super.valueLengthIfFixed();
        this.serializer = elementType.isValueLengthFixed() ?
                          new FixedLengthSerializer() :
                          new VariableLengthSerializer();
    }

    @SuppressWarnings("unchecked")
    public static <T> VectorType<T> getInstance(AbstractType<T> elements, int dimension)
    {
        Key key = new Key(elements, dimension);
        return instances.computeIfAbsent(key, Key::create);
    }

    public static VectorType<?> getInstance(TypeParser parser)
    {
        TypeParser.Vector v = parser.getVectorParameters();
        return getInstance(v.type.freeze(), v.dimension);
    }

    @Override
    public boolean isVector()
    {
        return true;
    }

    @Override
    public <VL, VR> int compareCustom(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR)
    {
        return getSerializer().compareCustom(left, accessorL, right, accessorR);
    }

    @Override
    public int valueLengthIfFixed()
    {
        return valueLengthIfFixed;
    }

    @Override
    public VectorSerializer getSerializer()
    {
        return serializer;
    }

    public List<ByteBuffer> split(ByteBuffer buffer)
    {
        return split(buffer, ByteBufferAccessor.instance);
    }

    public <V> List<V> split(V buffer, ValueAccessor<V> accessor)
    {
        return getSerializer().split(buffer, accessor);
    }

    public float[] composeAsFloat(ByteBuffer input)
    {
        return composeAsFloat(input, ByteBufferAccessor.instance);
    }

    public <V> float[] composeAsFloat(V input, ValueAccessor<V> accessor)
    {
        if (!(elementType instanceof FloatType))
            throw new IllegalStateException("Attempted to read as float, but element type is " + elementType.asCQL3Type());

        if (isNull(input, accessor))
            return null;
        float[] array = new float[dimension];
        int offset = 0;
        for (int i = 0; i < dimension; i++)
        {
            array[i] = accessor.getFloat(input, offset);
            offset += Float.BYTES;
        }
        return array;
    }

    public ByteBuffer decompose(T... values)
    {
        return decompose(Arrays.asList(values));
    }

    public ByteBuffer decomposeAsFloat(float[] value)
    {
        return decomposeAsFloat(ByteBufferAccessor.instance, value);
    }

    public <V> V decomposeAsFloat(ValueAccessor<V> accessor, float[] value)
    {
        if (value == null)
            rejectNullOrEmptyValue();
        if (!(elementType instanceof FloatType))
            throw new IllegalStateException("Attempted to read as float, but element type is " + elementType.asCQL3Type());
        if (value.length != dimension)
            throw new IllegalArgumentException(String.format("Attempted to add float vector of dimension %d to %s", value.length, asCQL3Type()));
        // TODO : should we use TypeSizes to be consistent with other code?  Its the same value at the end of the day...
        V buffer = accessor.allocate(Float.BYTES * dimension);
        int offset = 0;
        for (int i = 0; i < dimension; i++)
        {
            accessor.putFloat(buffer, offset, value[i]);
            offset+= Float.BYTES;
        }
        return buffer;
    }

    public ByteBuffer decomposeRaw(List<ByteBuffer> elements)
    {
        return decomposeRaw(elements, ByteBufferAccessor.instance);
    }

    public <V> V decomposeRaw(List<V> elements, ValueAccessor<V> accessor)
    {
        return getSerializer().serializeRaw(elements, accessor);
    }

    @Override
    public <V> ByteSource asComparableBytes(ValueAccessor<V> accessor, V value, ByteComparable.Version version)
    {
        if (isNull(value, accessor))
            return null;
        ByteSource[] srcs = new ByteSource[dimension];
        List<V> split = split(value, accessor);
        for (int i = 0; i < dimension; i++)
            srcs[i] = elementType.asComparableBytes(accessor, split.get(i), version);
        return ByteSource.withTerminatorMaybeLegacy(version, 0x00, srcs);
    }

    @Override
    public <V> V fromComparableBytes(ValueAccessor<V> accessor, ByteSource.Peekable comparableBytes, ByteComparable.Version version)
    {
        if (comparableBytes == null)
            rejectNullOrEmptyValue();

        assert version != ByteComparable.Version.LEGACY; // legacy translation is not reversible

        List<V> buffers = new ArrayList<>();
        int separator = comparableBytes.next();
        while (separator != ByteSource.TERMINATOR)
        {
            buffers.add(elementType.fromComparableBytes(accessor, comparableBytes, version));
            separator = comparableBytes.next();
        }
        return decomposeRaw(buffers, accessor);
    }

    @Override
    public CQL3Type asCQL3Type()
    {
        return new CQL3Type.Vector(this);
    }

    public AbstractType<T> getElementsType()
    {
        return elementType;
    }

    // vector of nested types is hard to parse, so fall back to bytes string matching ListType
    @Override
    public <V> String getString(V value, ValueAccessor<V> accessor)
    {
        return BytesType.instance.getString(value, accessor);
    }

    @Override
    public ByteBuffer fromString(String source) throws MarshalException
    {
        try
        {
            return ByteBufferUtil.hexToBytes(source);
        }
        catch (NumberFormatException e)
        {
            throw new MarshalException(String.format("cannot parse '%s' as hex bytes", source), e);
        }
    }

    @Override
    public List<AbstractType<?>> subTypes()
    {
        return Collections.singletonList(elementType);
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        return toJSONString(buffer, ByteBufferAccessor.instance, protocolVersion);
    }

    @Override
    public <V> String toJSONString(V value, ValueAccessor<V> accessor, ProtocolVersion protocolVersion)
    {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        List<V> split = split(value, accessor);
        for (int i = 0; i < dimension; i++)
        {
            if (i > 0)
                sb.append(", ");
            sb.append(elementType.toJSONString(split.get(i), accessor, protocolVersion));
        }
        sb.append(']');
        return sb.toString();
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        if (parsed instanceof String)
            parsed = JsonUtils.decodeJson((String) parsed);

        if (!(parsed instanceof List))
            throw new MarshalException(String.format(
            "Expected a list, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));

        List<?> list = (List<?>) parsed;
        if (list.size() != dimension)
            throw new MarshalException(String.format("List had incorrect size: expected %d but given %d; %s", dimension, list.size(), list));
        List<Term> terms = new ArrayList<>(list.size());
        for (Object element : list)
        {
            if (element == null)
                throw new MarshalException("Invalid null element in list");
            terms.add(elementType.fromJSONObject(element));
        }

        return new Vectors.DelayedValue<>(this, terms);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VectorType<?> that = (VectorType<?>) o;
        return dimension == that.dimension && Objects.equals(elementType, that.elementType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(elementType, dimension);
    }

    @Override
    public String toString()
    {
        return toString(false);
    }

    @Override
    public String toString(boolean ignoreFreezing)
    {
        return getClass().getName() + TypeParser.stringifyVectorParameters(elementType, ignoreFreezing, dimension);
    }

    private void check(List<?> values)
    {
        if (values.size() != dimension)
            throw new MarshalException(String.format("Required %d elements, but saw %d", dimension, values.size()));

        // This code base always works with a list that is RandomAccess, so can use .get to avoid allocation
        for (int i = 0; i < dimension; i++)
        {
            Object value = values.get(i);
            if (value == null || (value instanceof ByteBuffer && elementSerializer.isNull((ByteBuffer) value)))
                throw new MarshalException(String.format("Element at index %d is null (expected type %s); given %s", i, elementType.asCQL3Type(), values));
        }
    }

    private <V> void checkConsumedFully(V buffer, ValueAccessor<V> accessor, int offset)
    {
        int remaining = accessor.sizeFromOffset(buffer, offset);
        if (remaining > 0)
            throw new MarshalException("Unexpected " + remaining + " extraneous bytes after " + asCQL3Type() + " value");
    }
    
    private static void rejectNullOrEmptyValue()
    {
        throw new MarshalException("Invalid empty vector value");
    }

    @Override
    public ByteBuffer getMaskedValue()
    {
        List<ByteBuffer> values = Collections.nCopies(dimension, elementType.getMaskedValue());
        return serializer.serializeRaw(values, ByteBufferAccessor.instance);
    }

    public abstract class VectorSerializer extends TypeSerializer<List<T>>
    {
        public abstract <VL, VR> int compareCustom(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR);

        public abstract <V> List<V> split(V buffer, ValueAccessor<V> accessor);
        public abstract <V> V serializeRaw(List<V> elements, ValueAccessor<V> accessor);

        @Override
        public String toString(List<T> value)
        {
            StringBuilder sb = new StringBuilder();
            boolean isFirst = true;
            sb.append('[');
            for (T element : value)
            {
                if (isFirst)
                    isFirst = false;
                else
                    sb.append(", ");
                sb.append(elementSerializer.toString(element));
            }
            sb.append(']');
            return sb.toString();
        }

        @Override
        @SuppressWarnings({ "rawtypes", "unchecked" })
        public Class<List<T>> getType()
        {
            return (Class) List.class;
        }

        @Override
        public <V> boolean isNull(@Nullable V buffer, ValueAccessor<V> accessor)
        {
            // we don't allow empty vectors, so we can just check for null
            return buffer == null;
        }
    }

    private class FixedLengthSerializer extends VectorSerializer
    {
        private FixedLengthSerializer()
        {
        }

        @Override
        public <VL, VR> int compareCustom(VL left, ValueAccessor<VL> accessorL,
                                          VR right, ValueAccessor<VR> accessorR)
        {
            if (elementType.isByteOrderComparable)
                return ValueAccessor.compare(left, accessorL, right, accessorR);
            int offset = 0;
            int elementLength = elementType.valueLengthIfFixed();
            for (int i = 0; i < dimension; i++)
            {
                VL leftBytes = accessorL.slice(left, offset, elementLength);
                VR rightBytes = accessorR.slice(right, offset, elementLength);
                int rc = elementType.compare(leftBytes, accessorL, rightBytes, accessorR);
                if (rc != 0)
                    return rc;

                offset += elementLength;
            }
            return 0;
        }

        @Override
        public <V> List<V> split(V buffer, ValueAccessor<V> accessor)
        {
            List<V> result = new ArrayList<>(dimension);
            int offset = 0;
            int elementLength = elementType.valueLengthIfFixed();
            for (int i = 0; i < dimension; i++)
            {
                V bb = accessor.slice(buffer, offset, elementLength);
                offset += elementLength;
                elementSerializer.validate(bb, accessor);
                result.add(bb);
            }
            checkConsumedFully(buffer, accessor, offset);

            return result;
        }

        @Override
        public <V> V serializeRaw(List<V> value, ValueAccessor<V> accessor)
        {
            if (value == null)
                rejectNullOrEmptyValue();

            check(value);

            int size = elementType.valueLengthIfFixed();
            V bb = accessor.allocate(size * dimension);
            int position = 0;
            for (V v : value)
                position += accessor.copyTo(v, 0, bb, accessor, position, size);
            return bb;
        }

        @Override
        public ByteBuffer serialize(List<T> value)
        {
            if (value == null)
                rejectNullOrEmptyValue();

            check(value);

            ByteBuffer bb = ByteBuffer.allocate(elementType.valueLengthIfFixed() * dimension);
            for (T v : value)
                bb.put(elementSerializer.serialize(v).duplicate());
            bb.flip();
            return bb;
        }

        @Override
        public <V> List<T> deserialize(V input, ValueAccessor<V> accessor)
        {
            if (isNull(input, accessor))
                return null;
            List<T> result = new ArrayList<>(dimension);
            int offset = 0;
            int elementLength = elementType.valueLengthIfFixed();
            for (int i = 0; i < dimension; i++)
            {
                V bb = accessor.slice(input, offset, elementLength);
                offset += elementLength;
                elementSerializer.validate(bb, accessor);
                result.add(elementSerializer.deserialize(bb, accessor));
            }
            checkConsumedFully(input, accessor, offset);

            return result;
        }

        @Override
        public <V> void validate(V input, ValueAccessor<V> accessor) throws MarshalException
        {
            if (accessor.isEmpty(input))
                rejectNullOrEmptyValue();

            int offset = 0;
            int elementSize = elementType.valueLengthIfFixed();

            int expectedSize = elementSize * dimension;
            if (accessor.size(input) < expectedSize)
                throw new MarshalException("Not enough bytes to read a " + asCQL3Type());

            for (int i = 0; i < dimension; i++)
            {
                V bb = accessor.slice(input, offset, elementSize);
                offset += elementSize;
                elementSerializer.validate(bb, accessor);
            }
            checkConsumedFully(input, accessor, offset);
        }
    }

    private class VariableLengthSerializer extends VectorSerializer
    {
        private VariableLengthSerializer()
        {
        }

        @Override
        public <VL, VR> int compareCustom(VL left, ValueAccessor<VL> accessorL,
                                          VR right, ValueAccessor<VR> accessorR)
        {
            int leftOffset = 0;
            int rightOffset = 0;
            for (int i = 0; i < dimension; i++)
            {
                VL leftBytes = readValue(left, accessorL, leftOffset);
                leftOffset += sizeOf(leftBytes, accessorL);

                VR rightBytes = readValue(right, accessorR, rightOffset);
                rightOffset += sizeOf(rightBytes, accessorR);

                int rc = elementType.compare(leftBytes, accessorL, rightBytes, accessorR);
                if (rc != 0)
                    return rc;
            }
            return 0;
        }

        private <V> V readValue(V input, ValueAccessor<V> accessor, int offset)
        {
            int size = accessor.getUnsignedVInt32(input, offset);
            if (size < 0)
                throw new AssertionError("Invalidate data at offset " + offset + "; saw size of " + size + " but only >= 0 is expected");

            return accessor.slice(input, offset + TypeSizes.sizeofUnsignedVInt(size), size);
        }

        private <V> int writeValue(V src, V dst, ValueAccessor<V> accessor, int offset)
        {
            int size = accessor.size(src);
            int written = 0;
            written += accessor.putUnsignedVInt32(dst, offset + written, size);
            written += accessor.copyTo(src, 0, dst, accessor, offset + written, size);
            return written;
        }

        private <V> int sizeOf(V bb, ValueAccessor<V> accessor)
        {
            return accessor.sizeWithVIntLength(bb);
        }

        @Override
        public <V> List<V> split(V buffer, ValueAccessor<V> accessor)
        {
            List<V> result = new ArrayList<>(dimension);
            int offset = 0;
            for (int i = 0; i < dimension; i++)
            {
                V bb = readValue(buffer, accessor, offset);
                offset += sizeOf(bb, accessor);
                elementSerializer.validate(bb, accessor);
                result.add(bb);
            }
            checkConsumedFully(buffer, accessor, offset);

            return result;
        }

        @Override
        public <V> V serializeRaw(List<V> value, ValueAccessor<V> accessor)
        {
            if (value == null)
                rejectNullOrEmptyValue();

            check(value);

            V bb = accessor.allocate(value.stream().mapToInt(v -> sizeOf(v, accessor)).sum());
            int offset = 0;
            for (V b : value)
                offset += writeValue(b, bb, accessor, offset);
            return bb;
        }

        @Override
        public ByteBuffer serialize(List<T> value)
        {
            if (value == null)
                rejectNullOrEmptyValue();

            check(value);

            List<ByteBuffer> bbs = new ArrayList<>(dimension);
            for (int i = 0; i < dimension; i++)
                bbs.add(elementSerializer.serialize(value.get(i)));
            return serializeRaw(bbs, ByteBufferAccessor.instance);
        }

        @Override
        public <V> List<T> deserialize(V input, ValueAccessor<V> accessor)
        {
            if (isNull(input, accessor))
                return null;
            List<T> result = new ArrayList<>(dimension);
            int offset = 0;
            for (int i = 0; i < dimension; i++)
            {
                V bb = readValue(input, accessor, offset);
                offset += sizeOf(bb, accessor);
                elementSerializer.validate(bb, accessor);
                result.add(elementSerializer.deserialize(bb, accessor));
            }
            checkConsumedFully(input, accessor, offset);

            return result;
        }

        @Override
        public <V> void validate(V input, ValueAccessor<V> accessor) throws MarshalException
        {
            if (accessor.isEmpty(input))
                rejectNullOrEmptyValue();

            int offset = 0;
            for (int i = 0; i < dimension; i++)
            {
                if (offset >= accessor.size(input))
                    throw new MarshalException("Not enough bytes to read a " + asCQL3Type());

                V bb = readValue(input, accessor, offset);
                offset += sizeOf(bb, accessor);
                elementSerializer.validate(bb, accessor);
            }
            checkConsumedFully(input, accessor, offset);
        }
    }
}
