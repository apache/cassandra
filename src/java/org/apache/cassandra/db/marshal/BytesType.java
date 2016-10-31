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

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.BytesSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Hex;

public class BytesType extends AbstractType<ByteBuffer>
{
    public static final BytesType instance = new BytesType();

    BytesType() {super(ComparisonType.BYTE_ORDER);} // singleton

    public ByteBuffer fromString(String source)
    {
        try
        {
            return ByteBuffer.wrap(Hex.hexToBytes(source));
        }
        catch (NumberFormatException e)
        {
            throw new MarshalException(String.format("cannot parse '%s' as hex bytes", source), e);
        }
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        try
        {
            String parsedString = (String) parsed;
            if (!parsedString.startsWith("0x"))
                throw new MarshalException(String.format("String representation of blob is missing 0x prefix: %s", parsedString));

            return new Constants.Value(BytesType.instance.fromString(parsedString.substring(2)));
        }
        catch (ClassCastException | MarshalException exc)
        {
            throw new MarshalException(String.format("Value '%s' is not a valid blob representation: %s", parsed, exc.getMessage()));
        }
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        return "\"0x" + ByteBufferUtil.bytesToHex(buffer) + '"';
    }

    @Override
    public boolean isCompatibleWith(AbstractType<?> previous)
    {
        // Both asciiType and utf8Type really use bytes comparison and
        // bytesType validate everything, so it is compatible with the former.
        return this == previous || previous == AsciiType.instance || previous == UTF8Type.instance;
    }

    @Override
    public boolean isValueCompatibleWithInternal(AbstractType<?> otherType)
    {
        // BytesType can read anything
        return true;
    }

    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.BLOB;
    }

    public TypeSerializer<ByteBuffer> getSerializer()
    {
        return BytesSerializer.instance;
    }
}
