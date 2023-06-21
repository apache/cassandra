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

import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.utils.ByteBufferUtil;

public abstract class AbstractTextSerializer extends TypeSerializer<String>
{
    private final Charset charset;

    protected AbstractTextSerializer(Charset charset)
    {
        this.charset = charset;
    }

    public <V> String deserialize(V value, ValueAccessor<V> accessor)
    {
        try
        {
            return accessor.toString(value, charset);
        }
        catch (CharacterCodingException e)
        {
            throw new MarshalException("Invalid " + charset + " bytes " + accessor.toHex(value));
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

    @Override
    public boolean shouldQuoteCQLLiterals()
    {
        return true;
    }

    @Override
    public <V> boolean isNull(V buffer, ValueAccessor<V> accessor)
    {
        // !buffer.hasRemaining() is not "null" for string types, it is the empty string
        return buffer == null;
    }

    @Override
    protected String toCQLLiteralNonNull(ByteBuffer buffer)
    {
        return StringUtils.replace(deserialize(buffer), "'", "''");
    }
}
