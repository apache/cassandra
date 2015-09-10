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

import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class CollectionSerializer<T> implements TypeSerializer<T>
{
    protected abstract List<ByteBuffer> serializeValues(T value);
    protected abstract int getElementCount(T value);

    public abstract T deserializeForNativeProtocol(ByteBuffer buffer, int version);
    public abstract void validateForNativeProtocol(ByteBuffer buffer, int version);

    public ByteBuffer serialize(T value)
    {
        List<ByteBuffer> values = serializeValues(value);
        // See deserialize() for why using the protocol v3 variant is the right thing to do.
        return pack(values, getElementCount(value), Server.VERSION_3);
    }

    public T deserialize(ByteBuffer bytes)
    {
        // The only cases we serialize/deserialize collections internally (i.e. not for the protocol sake),
        // is:
        //  1) when collections are frozen
        //  2) for internal calls.
        // In both case, using the protocol 3 version variant is the right thing to do.
        return deserializeForNativeProtocol(bytes, Server.VERSION_3);
    }

    public void validate(ByteBuffer bytes) throws MarshalException
    {
        // Same thing as above
        validateForNativeProtocol(bytes, Server.VERSION_3);
    }

    public static ByteBuffer pack(Collection<ByteBuffer> buffers, int elements, int version)
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

    protected static void writeCollectionSize(ByteBuffer output, int elements, int version)
    {
            output.putInt(elements);
    }

    public static int readCollectionSize(ByteBuffer input, int version)
    {
        return input.getInt();
    }

    protected static int sizeOfCollectionSize(int elements, int version)
    {
        return 4;
    }

    public static void writeValue(ByteBuffer output, ByteBuffer value, int version)
    {
        if (value == null)
        {
            output.putInt(-1);
            return;
        }

        output.putInt(value.remaining());
        output.put(value.duplicate());
    }

    public static ByteBuffer readValue(ByteBuffer input, int version)
    {
        int size = input.getInt();
        if (size < 0)
            return null;

        return ByteBufferUtil.readBytes(input, size);
    }

    public static int sizeOfValue(ByteBuffer value, int version)
    {
        return value == null ? 4 : 4 + value.remaining();
    }
}
