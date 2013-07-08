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
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;

public class AsciiSerializer extends AbstractSerializer<String>
{
    public static final AsciiSerializer instance = new AsciiSerializer();
    private static final Charset US_ASCII = Charset.forName("US-ASCII");

    @Override
    public String serialize(ByteBuffer bytes)
    {
        return getString(bytes);
    }

    @Override
    public ByteBuffer deserialize(String value)
    {
        return ByteBufferUtil.bytes(value, US_ASCII);
    }

    @Override
    public void validate(ByteBuffer bytes) throws MarshalException
    {
        // 0-127
        for (int i = bytes.position(); i < bytes.limit(); i++)
        {
            byte b = bytes.get(i);
            if (b < 0 || b > 127)
                throw new MarshalException("Invalid byte for ascii: " + Byte.toString(b));
        }
    }

    @Override
    public String getString(ByteBuffer bytes)
    {
        try
        {
            return ByteBufferUtil.string(bytes, US_ASCII);
        }
        catch (CharacterCodingException e)
        {
            throw new MarshalException("Invalid ascii bytes " + ByteBufferUtil.bytesToHex(bytes));
        }
    }

    @Override
    public String toString(String value)
    {
        return value;
    }

    @Override
    public Class<String> getType()
    {
        return String.class;
    }
}
