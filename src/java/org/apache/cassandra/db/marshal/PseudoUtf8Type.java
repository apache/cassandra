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
import java.nio.charset.StandardCharsets;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.terms.Constants;
import org.apache.cassandra.cql3.terms.Term;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.JsonUtils;

public abstract class PseudoUtf8Type extends StringType
{
    PseudoUtf8Type() {super(ComparisonType.CUSTOM);} // singleton

    abstract String describe();

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
                    "Expected a %s string, but got a %s: %s", describe(), parsed.getClass().getSimpleName(), parsed));
        }
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        try
        {
            return '"' + JsonUtils.quoteAsJsonString(ByteBufferUtil.string(buffer, StandardCharsets.UTF_8)) + '"';
        }
        catch (CharacterCodingException exc)
        {
            throw new AssertionError(describe() + " value contained non-utf8 characters: ", exc);
        }
    }

    @Override
    public boolean isCompatibleWith(AbstractType<?> previous)
    {
        // Anything that is ascii is also utf8, and they both use bytes comparison
        return previous == AsciiType.instance || previous == UTF8Type.instance || previous instanceof PseudoUtf8Type;
    }

    @Override
    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.TEXT;
    }
}
