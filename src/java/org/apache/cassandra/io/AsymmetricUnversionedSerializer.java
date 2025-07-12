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

package org.apache.cassandra.io;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;

public interface AsymmetricUnversionedSerializer<In, Out>
{
    void serialize(In t, DataOutputPlus out) throws IOException;

    /**
     * Note: it is not guaranteed that this output is compatible with the DataInput/OutputPlus variations,
     * as the ByteBuffer has an implied length.
     */
    default ByteBuffer serialize(In t) throws IOException
    {
        int size = Math.toIntExact(serializedSize(t));
        try (DataOutputBuffer buffer = new DataOutputBuffer(size))
        {
            serialize(t, buffer);
            ByteBuffer bb = buffer.buffer();
            assert size == bb.remaining() : String.format("Expected to write %d but wrote %d", size, bb.remaining());
            return bb;
        }
    }

    default void skip(DataInputPlus in) throws IOException
    {
        deserialize(in);
    }

    default ByteBuffer serializeUnchecked(In t)
    {
        try
        {
            return serialize(t);
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }
    Out deserialize(DataInputPlus in) throws IOException;

    /**
     * Note: it is not guaranteed to be safe to provide an input created by the DataOutputPlus serializer varation
     * as the ByteBuffer has an implied length.
     */
    default Out deserialize(ByteBuffer buffer) throws IOException
    {
        try (DataInputBuffer in = new DataInputBuffer(buffer, true))
        {
            return deserialize(in);
        }
    }

    default Out deserializeUnchecked(ByteBuffer buffer)
    {
        try
        {
            return deserialize(buffer);
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    long serializedSize(In t);
}
