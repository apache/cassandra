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

package org.apache.cassandra.cql3.functions.types;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.apache.cassandra.cql3.functions.types.exceptions.InvalidTypeException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.vint.VIntCoding;

/**
 * {@link TypeCodec} for the CQL type {@code vector}. Vectors are represented as {@link List}s for convenience, since
 * it's probably easier for UDFs trying to return a newly created vector to create it as a standard Java list, rather
 * than using a custom, not-standard vector class.
 *
 * @param <E> The type of the vector elements.
 */
public abstract class VectorCodec<E> extends TypeCodec<List<E>>
{
    protected final VectorType type;
    protected final TypeCodec<E> subtypeCodec;

    private VectorCodec(VectorType type, TypeCodec<E> subtypeCodec)
    {
        super(type, TypeTokens.vectorOf(subtypeCodec.getJavaType()));
        this.type = type;
        this.subtypeCodec = subtypeCodec;
    }

    public static <E> VectorCodec<E> of(VectorType type, TypeCodec<E> subtypeCodec)
    {
        return subtypeCodec.isSerializedSizeFixed()
               ? new FixedLength<>(type, subtypeCodec)
               : new VariableLength<>(type, subtypeCodec);
    }

    @Override
    public List<E> parse(String value) throws InvalidTypeException
    {
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
            return null;

        ImmutableList.Builder<E> values = ImmutableList.builder();
        for (String element : Splitter.on(", ").split(value.substring(1, value.length() - 1)))
        {
            values.add(subtypeCodec.parse(element));
        }

        return values.build();
    }

    @Override
    public String format(List<E> value) throws InvalidTypeException
    {
        return value == null ? "NULL" : Iterables.toString(value);
    }

    /**
     * {@link VectorCodec} for vectors of elements using a fixed-length encoding.
     */
    private static class FixedLength<E> extends VectorCodec<E>
    {
        private final int valueLength;

        public FixedLength(VectorType type, TypeCodec<E> subtypeCodec)
        {
            super(type, subtypeCodec);
            valueLength = subtypeCodec.serializedSize() * type.getDimensions();
        }

        @Override
        public int serializedSize()
        {
            return valueLength;
        }

        @Override
        public ByteBuffer serialize(List<E> value, ProtocolVersion protocolVersion) throws InvalidTypeException
        {
            if (value == null || type.getDimensions() <= 0)
                return null;

            Iterator<E> values = value.iterator();
            ByteBuffer rv = ByteBuffer.allocate(valueLength);
            for (int i = 0; i < type.getDimensions(); ++i)
            {
                ByteBuffer valueBuff = subtypeCodec.serialize(values.next(), protocolVersion);
                valueBuff.rewind();
                rv.put(valueBuff);
            }
            rv.flip();
            return rv;
        }

        @Override
        public List<E> deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException
        {
            if (bytes == null || bytes.remaining() == 0)
                return null;

            // Determine element size by dividing count of remaining bytes by number of elements.
            // This should have a remainder of zero since all elements are of the same fixed size.
            int elementSize = Math.floorDiv(bytes.remaining(), type.getDimensions());
            assert bytes.remaining() % type.getDimensions() == 0
            : String.format("Expected elements of uniform size, observed %d elements with total bytes %d",
                            type.getDimensions(), bytes.remaining());

            ByteBuffer bb = bytes.slice();
            ImmutableList.Builder<E> values = ImmutableList.builder();
            for (int i = 0; i < type.getDimensions(); ++i)
            {
                int originalPosition = bb.position();
                // Set the limit for the current element
                bb.limit(originalPosition + elementSize);
                values.add(subtypeCodec.deserialize(bb, protocolVersion));
                // Move to the start of the next element
                bb.position(originalPosition + elementSize);
                // Reset the limit to the end of the buffer
                bb.limit(bb.capacity());
            }

            return values.build();
        }
    }

    /**
     * {@link VectorCodec} for vectors of elements using a varaible-length encoding.
     */
    private static class VariableLength<E> extends VectorCodec<E>
    {
        public VariableLength(VectorType type, TypeCodec<E> subtypeCodec)
        {
            super(type, subtypeCodec);
        }

        @Override
        public ByteBuffer serialize(List<E> values, ProtocolVersion version) throws InvalidTypeException
        {
            if (values == null)
                return null;

            assert values.size() == type.getDimensions();

            int i = 0;
            int outputSize = 0;
            ByteBuffer[] buffers = new ByteBuffer[values.size()];
            for (E value : values)
            {
                ByteBuffer bb = subtypeCodec.serialize(value, version);
                buffers[i++] = bb;
                int elemSize = bb.remaining();
                outputSize += elemSize + VIntCoding.computeUnsignedVIntSize(elemSize);
            }

            ByteBuffer output = ByteBuffer.allocate(outputSize);
            for (ByteBuffer bb : buffers)
            {
                VIntCoding.writeUnsignedVInt32(bb.remaining(), output);
                output.put(bb.duplicate());
            }
            return (ByteBuffer) output.flip();
        }

        @Override
        public List<E> deserialize(ByteBuffer bytes, ProtocolVersion version) throws InvalidTypeException
        {
            if (bytes == null || bytes.remaining() == 0)
                return null;

            ByteBuffer input = bytes.duplicate();
            ImmutableList.Builder<E> values = ImmutableList.builder();

            for (int i = 0; i < type.getDimensions(); i++)
            {
                int size = VIntCoding.getUnsignedVInt32(input, input.position());
                input.position(input.position() + VIntCoding.computeUnsignedVIntSize(size));

                ByteBuffer value = size < 0 ? null : CodecUtils.readBytes(input, size);
                values.add(subtypeCodec.deserialize(value, version));
            }
            return values.build();
        }
    }
}
