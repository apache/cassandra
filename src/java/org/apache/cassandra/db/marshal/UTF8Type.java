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
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;

import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Json;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.UTF8Serializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

public class UTF8Type extends AbstractType<String>
{
    public static final UTF8Type instance = new UTF8Type();

    UTF8Type() {super(ComparisonType.BYTE_ORDER);} // singleton

    public ByteBuffer fromString(String source)
    {
        return decompose(source);
    }


    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        try
        {
            return new Constants.Value(fromString((String) parsed));
        }
        catch (ClassCastException exc)
        {
            throw new MarshalException(String.format(
                    "Expected a UTF-8 string, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));
        }
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        try
        {
            return '"' + Json.quoteAsJsonString(ByteBufferUtil.string(buffer, Charset.forName("UTF-8"))) + '"';
        }
        catch (CharacterCodingException exc)
        {
            throw new AssertionError("UTF-8 value contained non-utf8 characters: ", exc);
        }
    }

    @Override
    public boolean isCompatibleWith(AbstractType<?> previous)
    {
        // Anything that is ascii is also utf8, and they both use bytes
        // comparison
        return this == previous || previous == AsciiType.instance;
    }

    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.TEXT;
    }

    public TypeSerializer<String> getSerializer()
    {
        return UTF8Serializer.instance;
    }
}
