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

package org.apache.cassandra.serializers;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class CollectionSerializer<T> implements TypeSerializer<T>
{
    protected abstract List<ByteBuffer> serializeValues(T value);
    protected abstract int getElementCount(T value);

    public abstract T deserializeForNativeProtocol(ByteBuffer buffer, ProtocolVersion version);
    public abstract void validateForNativeProtocol(ByteBuffer buffer, ProtocolVersion version);

    public ByteBuffer serialize(T value)
    {
        List<ByteBuffer> values = serializeValues(value);
        // See deserialize() for why using the protocol v3 variant is the right thing to do.
        return pack(values, getElementCount(value), ProtocolVersion.V3);
    }

    public T deserialize(ByteBuffer bytes)
    {
        // The only cases we serialize/deserialize collections internally (i.e. not for the protocol sake),
        // is:
        //  1) when collections are frozen
        //  2) for internal calls.
        // In both case, using the protocol 3 version variant is the right thing to do.
        return deserializeForNativeProtocol(bytes, ProtocolVersion.V3);
    }

    public void validate(ByteBuffer bytes) throws MarshalException
    {
        // Same thing as above
        validateForNativeProtocol(bytes, ProtocolVersion.V3);
    }

    public static ByteBuffer pack(Collection<ByteBuffer> buffers, int elements, ProtocolVersion version)
    {
        int size = 0;
        for (ByteBuffer bb : buffers)
            size += sizeOfValue(bb, version);

        ByteBuffer result = ByteBuffer.allocate(sizeOfCollectionSize(elements, version) + size);
        writeCollectionSize(result, elements, version);
        for (ByteBuffer bb : buffers)
            writeValue(result, bb, version);
        return (ByteBuffer)result.flip();
    }

    protected static void writeCollectionSize(ByteBuffer output, int elements, ProtocolVersion version)
    {
        output.putInt(elements);
    }

    public static int readCollectionSize(ByteBuffer input, ProtocolVersion version)
    {
        return input.getInt();
    }

    protected static int sizeOfCollectionSize(int elements, ProtocolVersion version)
    {
        return 4;
    }

    public static void writeValue(ByteBuffer output, ByteBuffer value, ProtocolVersion version)
    {
        if (value == null)
        {
            output.putInt(-1);
            return;
        }

        output.putInt(value.remaining());
        output.put(value.duplicate());
    }

    public static ByteBuffer readValue(ByteBuffer input, ProtocolVersion version)
    {
        int size = input.getInt();
        if (size < 0)
            return null;

        return ByteBufferUtil.readBytes(input, size);
    }

    protected static void skipValue(ByteBuffer input, ProtocolVersion version)
    {
        int size = input.getInt();
        input.position(input.position() + size);
    }

    public static int sizeOfValue(ByteBuffer value, ProtocolVersion version)
    {
        return value == null ? 4 : 4 + value.remaining();
    }

    /**
     * Extract an element from a serialized collection.
     * <p>
     * Note that this is only supported to sets and maps. For sets, this mostly ends up being
     * a check for the presence of the provide key: it will return the key if it's present and
     * {@code null} otherwise.
     *
     * @param collection the serialized collection. This cannot be {@code null}.
     * @param key the key to extract (This cannot be {@code null} nor {@code ByteBufferUtil.UNSET_BYTE_BUFFER}).
     * @param comparator the type to use to compare the {@code key} value to those
     * in the collection.
     * @return the value associated with {@code key} if one exists, {@code null} otherwise
     */
    public abstract ByteBuffer getSerializedValue(ByteBuffer collection, ByteBuffer key, AbstractType<?> comparator);

    /**
     * Returns the slice of a collection directly from its serialized value.
     *
     * @param collection the serialized collection. This cannot be {@code null}.
     * @param from the left bound of the slice to extract. This cannot be {@code null} but if this is
     * {@code ByteBufferUtil.UNSET_BYTE_BUFFER}, then the returned slice starts at the beginning
     * of {@code collection}.
     * @param comparator the type to use to compare the {@code from} and {@code to} values to those
     * in the collection.
     * @return a valid serialized collection (possibly empty) corresponding to slice {@code [from, to]}
     * of {@code collection}.
     */
    public abstract ByteBuffer getSliceFromSerialized(ByteBuffer collection, ByteBuffer from, ByteBuffer to, AbstractType<?> comparator);

    /**
     * Creates a new serialized map composed from the data from {@code input} between {@code startPos}
     * (inclusive) and {@code endPos} (exclusive), assuming that data holds {@code count} elements.
     */
    protected ByteBuffer copyAsNewCollection(ByteBuffer input, int count, int startPos, int endPos, ProtocolVersion version)
    {
        int sizeLen = sizeOfCollectionSize(count, version);
        if (count == 0)
            return ByteBuffer.allocate(sizeLen);

        int bodyLen = endPos - startPos;
        ByteBuffer output = ByteBuffer.allocate(sizeLen + bodyLen);
        writeCollectionSize(output, count, version);
        output.position(0);
        ByteBufferUtil.arrayCopy(input, startPos, output, sizeLen, bodyLen);
        return output;
    }
}
