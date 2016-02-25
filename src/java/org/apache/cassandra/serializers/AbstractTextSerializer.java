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

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;

import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class AbstractTextSerializer implements TypeSerializer<String>
{
    private final Charset charset;

    protected AbstractTextSerializer(Charset charset)
    {
        this.charset = charset;
    }

    public String deserialize(ByteBuffer bytes)
    {
        try
        {
            return ByteBufferUtil.string(bytes, charset);
        }
        catch (CharacterCodingException e)
        {
            throw new MarshalException("Invalid " + charset + " bytes " + ByteBufferUtil.bytesToHex(bytes));
        }
    }

    public ByteBuffer serialize(String value)
    {
        return ByteBufferUtil.bytes(value, charset);
    }

    public String toString(String value)
    {
        return value;
    }

    public Class<String> getType()
    {
        return String.class;
    }

    /**
     * Generates CQL literal for TEXT/VARCHAR/ASCII types.
     * Caveat: it does only generate literals with single quotes and not pg-style literals.
     */
    @Override
    public String toCQLLiteral(ByteBuffer buffer)
    {
        return buffer == null
             ? "null"
             : '\'' + StringUtils.replace(deserialize(buffer), "'", "''") + '\'';
    }
}
