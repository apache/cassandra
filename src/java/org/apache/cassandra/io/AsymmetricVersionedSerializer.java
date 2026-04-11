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

public interface AsymmetricVersionedSerializer<In, Out, Version>
{
    void serialize(In t, DataOutputPlus out, Version version) throws IOException;
    default ByteBuffer serialize(In t, Version version) throws IOException
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

    default ByteBuffer serializeUnchecked(In t, Version version)
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
    Out deserialize(DataInputPlus in, Version version) throws IOException;

    default void skip(DataInputPlus in, Version version) throws IOException
    {
        deserialize(in, version);
    }

    default Out deserialize(ByteBuffer buffer, Version version) throws IOException
    {
        try (DataInputBuffer in = new DataInputBuffer(buffer, true))
        {
            return deserialize(in, version);
        }
    }

    default Out deserializeUnchecked(ByteBuffer buffer, Version version)
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
    long serializedSize(In t, Version version);

    static <In, Out, Version> AsymmetricVersionedSerializer<In, Out, Version> from(AsymmetricUnversionedSerializer<In, Out> delegate)
    {
        return new AsymmetricVersionedSerializer<>()
        {
            @Override
            public void serialize(In t, DataOutputPlus out, Version version) throws IOException
            {
                delegate.serialize(t, out);
            }

            @Override
            public Out deserialize(DataInputPlus in, Version version) throws IOException
            {
                return delegate.deserialize(in);
            }

            @Override
            public long serializedSize(In t, Version version)
            {
                return delegate.serializedSize(t);
            }
        };
    }
}
