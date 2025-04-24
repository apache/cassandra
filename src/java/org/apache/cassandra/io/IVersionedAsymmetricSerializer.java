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

public interface IVersionedAsymmetricSerializer<In, Out>
{
    /**
     * Serialize the specified type into the specified DataOutputStream instance.
     *
     * @param t type that needs to be serialized
     * @param out DataOutput into which serialization needs to happen.
     * @param version protocol version
     * @throws IOException if serialization fails
     */
    void serialize(In t, DataOutputPlus out, int version) throws IOException;

    default ByteBuffer serialize(In t, int version) throws IOException
    {
        int size = Math.toIntExact(serializedSize(t, version));
        try (DataOutputBuffer buffer = new DataOutputBuffer(size))
        {
            serialize(t, buffer, version);
            ByteBuffer bb = buffer.buffer();
            assert size == bb.remaining() : String.format("Expected to write %d but wrote %d", size, bb.remaining());
            return bb;
        }
    }

    default ByteBuffer serializeUnchecked(In t, int version)
    {
        try
        {
            return serialize(t, version);
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Deserialize into the specified DataInputStream instance.
     * @param in DataInput from which deserialization needs to happen.
     * @param version protocol version
     * @return the type that was deserialized
     * @throws IOException if deserialization fails
     */
    Out deserialize(DataInputPlus in, int version) throws IOException;

    default Out deserialize(ByteBuffer buffer, int version) throws IOException
    {
        try (DataInputBuffer in = new DataInputBuffer(buffer, true))
        {
            return deserialize(in, version);
        }
    }

    default Out deserializeUnchecked(ByteBuffer buffer, int version)
    {
        try
        {
            return deserialize(buffer, version);
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Calculate serialized size of object without actually serializing.
     * @param t object to calculate serialized size
     * @param version protocol version
     * @return serialized size of object t
     */
    long serializedSize(In t, int version);
}
