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

import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.nio.ByteBuffer;

public class BytesSerializer extends TypeSerializer<ByteBuffer>
{
    public static final BytesSerializer instance = new BytesSerializer();

    public ByteBuffer serialize(ByteBuffer bytes)
    {
        // We make a copy in case the user modifies the input
        return bytes.duplicate();
    }

    public <V> ByteBuffer deserialize(V value, ValueAccessor<V> accessor)
    {
        // This is from the DB, so it is not shared with someone else
        return accessor.toBuffer(value);
    }

    public <V> void validate(V value, ValueAccessor<V> accessor) throws MarshalException
    {
        // all bytes are legal.
    }

    public String toString(ByteBuffer value)
    {
        return ByteBufferUtil.bytesToHex(value);
    }

    public Class<ByteBuffer> getType()
    {
        return ByteBuffer.class;
    }

    @Override
    public <V> boolean isNull(V buffer, ValueAccessor<V> accessor)
    {
        // !buffer.hasRemaining() is not "null" for bytes types, it is byte[0]
        return buffer == null;
    }

    @Override
    protected String toCQLLiteralNonNull(ByteBuffer buffer)
    {
        return "0x" + toString(deserialize(buffer));
    }
}
