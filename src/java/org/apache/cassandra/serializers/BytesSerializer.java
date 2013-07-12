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

public class BytesSerializer implements TypeSerializer<ByteBuffer>
{
    public static final BytesSerializer instance = new BytesSerializer();

    public ByteBuffer serialize(ByteBuffer bytes)
    {
        // We make a copy in case the user modifies the input
        return bytes.duplicate();
    }

    public ByteBuffer deserialize(ByteBuffer value)
    {
        // This is from the DB, so it is not shared with someone else
        return value;
    }

    public void validate(ByteBuffer bytes) throws MarshalException
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
}
