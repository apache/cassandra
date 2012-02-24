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
package org.apache.cassandra.cql.jdbc;

import java.nio.ByteBuffer;
import java.sql.Types;

import org.apache.cassandra.utils.ByteBufferUtil;

public class JdbcLong extends AbstractJdbcType<Long>
{
    public static final JdbcLong instance = new JdbcLong();

    JdbcLong()
    {
    }

    public boolean isCaseSensitive()
    {
        return false;
    }

    public int getScale(Long obj)
    {
        return 0;
    }

    public int getPrecision(Long obj)
    {
        return obj.toString().length();
    }

    public boolean isCurrency()
    {
        return false;
    }

    public boolean isSigned()
    {
        return true;
    }

    public String toString(Long obj)
    {
        return obj.toString();
    }

    public boolean needsQuotes()
    {
        return false;
    }

    public String getString(ByteBuffer bytes)
    {
        if (bytes.remaining() == 0)
        {
            return "";
        }
        if (bytes.remaining() != 8)
        {
            throw new MarshalException("A long is exactly 8 bytes: " + bytes.remaining());
        }

        return String.valueOf(bytes.getLong(bytes.position()));
    }

    public Class<Long> getType()
    {
        return Long.class;
    }

    public int getJdbcType()
    {
        return Types.BIGINT;
    }

    public Long compose(ByteBuffer bytes)
    {
        return ByteBufferUtil.toLong(bytes);
    }

    public ByteBuffer decompose(Long value)
    {
        return ByteBufferUtil.bytes(value);
    }
}
