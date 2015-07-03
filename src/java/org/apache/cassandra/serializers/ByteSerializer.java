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

import org.apache.cassandra.utils.ByteBufferUtil;

import java.nio.ByteBuffer;

public class ByteSerializer implements TypeSerializer<Byte>
{
    public static final ByteSerializer instance = new ByteSerializer();

    public Byte deserialize(ByteBuffer bytes)
    {
        return bytes == null || bytes.remaining() == 0 ? null : bytes.get(bytes.position());
    }

    public ByteBuffer serialize(Byte value)
    {
        return value == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : ByteBuffer.allocate(1).put(0, value);
    }

    public void validate(ByteBuffer bytes) throws MarshalException
    {
        if (bytes.remaining() != 1)
            throw new MarshalException(String.format("Expected 1 byte for a tinyint (%d)", bytes.remaining()));
    }

    public String toString(Byte value)
    {
        return value == null ? "" : String.valueOf(value);
    }

    public Class<Byte> getType()
    {
        return Byte.class;
    }
}
