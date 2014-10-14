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
package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CharacterCodingException;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.AsciiSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;

public class AsciiType extends AbstractType<String>
{
    public static final AsciiType instance = new AsciiType();

    AsciiType() {} // singleton

    private final ThreadLocal<CharsetEncoder> encoder = new ThreadLocal<CharsetEncoder>()
    {
        @Override
        protected CharsetEncoder initialValue()
        {
            return Charset.forName("US-ASCII").newEncoder();
        }
    };

    public int compare(ByteBuffer o1, ByteBuffer o2)
    {
        return ByteBufferUtil.compareUnsigned(o1, o2);
    }

    public ByteBuffer fromString(String source)
    {
        // the encoder must be reset each time it's used, hence the thread-local storage
        CharsetEncoder theEncoder = encoder.get();
        theEncoder.reset();

        try
        {
            return theEncoder.encode(CharBuffer.wrap(source));
        }
        catch (CharacterCodingException exc)
        {
            throw new MarshalException(String.format("Invalid ASCII character in string literal: %s", exc));
        }
    }

    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.ASCII;
    }

    public TypeSerializer<String> getSerializer()
    {
        return AsciiSerializer.instance;
    }

    public boolean isByteOrderComparable()
    {
        return true;
    }
}
