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

package org.apache.cassandra.type;

import org.apache.cassandra.utils.ByteBufferUtil;

import java.nio.ByteBuffer;

public class Int32Serializer extends AbstractSerializer<Integer>
{
    public static final Int32Serializer instance = new Int32Serializer();

    @Override
    public Integer serialize(ByteBuffer bytes)
    {
        return ByteBufferUtil.toInt(bytes);
    }

    @Override
    public ByteBuffer deserialize(Integer value)
    {
        return value == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : ByteBufferUtil.bytes(value);
    }

    @Override
    public void validate(ByteBuffer bytes) throws MarshalException
    {
        if (bytes.remaining() != 4 && bytes.remaining() != 0)
            throw new MarshalException(String.format("Expected 4 or 0 byte int (%d)", bytes.remaining()));
    }

    @Override
    public String getString(ByteBuffer bytes)
    {
        if (bytes.remaining() == 0)
        {
            return "";
        }
        if (bytes.remaining() != 4)
        {
            throw new MarshalException("A int is exactly 4 bytes: " + bytes.remaining());
        }

        return String.valueOf(ByteBufferUtil.toInt(bytes));
    }

    @Override
    public String toString(Integer value)
    {
        return value == null ? "" : String.valueOf(value);
    }

    @Override
    public Class<Integer> getType()
    {
        return Integer.class;
    }
}
