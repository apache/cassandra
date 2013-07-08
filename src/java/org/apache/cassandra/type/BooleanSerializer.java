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

public class BooleanSerializer extends AbstractSerializer<Boolean>
{
    public static final BooleanSerializer instance = new BooleanSerializer();

    @Override
    public Boolean serialize(ByteBuffer bytes)
    {
        byte value = bytes.get(bytes.position());
        return value != 0;
    }

    @Override
    public ByteBuffer deserialize(Boolean value)
    {
        return (value == null) ? ByteBufferUtil.EMPTY_BYTE_BUFFER
                : value ? ByteBuffer.wrap(new byte[] {1})  // true
                : ByteBuffer.wrap(new byte[] {0}); // false
    }

    @Override
    public void validate(ByteBuffer bytes) throws MarshalException
    {
        if (bytes.remaining() != 1 && bytes.remaining() != 0)
            throw new MarshalException(String.format("Expected 1 or 0 byte value (%d)", bytes.remaining()));
    }

    @Override
    public String getString(ByteBuffer bytes)
    {
        if (bytes.remaining() == 0)
        {
            return Boolean.FALSE.toString();
        }
        if (bytes.remaining() != 1)
        {
            throw new MarshalException("A boolean is stored in exactly 1 byte: " + bytes.remaining());
        }
        byte value = bytes.get(bytes.position());

        return value == 0 ? Boolean.FALSE.toString() : Boolean.TRUE.toString();
    }

    @Override
    public String toString(Boolean value)
    {
        return value == null ? "" : value.toString();
    }

    @Override
    public Class<Boolean> getType()
    {
        return Boolean.class;
    }

}
