package org.apache.cassandra.cql.jdbc;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.nio.ByteBuffer;
import java.sql.Types;

import org.apache.cassandra.utils.ByteBufferUtil;

public class JdbcInt32 extends AbstractJdbcType<Integer>
{
    public static final JdbcInt32 instance = new JdbcInt32();

    JdbcInt32()
    {
    }

    public boolean isCaseSensitive()
    {
        return false;
    }

    public int getScale(Integer obj)
    {
        return 0;
    }

    public int getPrecision(Integer obj)
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

    public String toString(Integer obj)
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
        if (bytes.remaining() != 4)
        {
            throw new MarshalException("A int is exactly 4 bytes: " + bytes.remaining());
        }

        return String.valueOf(bytes.getInt(bytes.position()));
    }

    public Class<Integer> getType()
    {
        return Integer.class;
    }

    public int getJdbcType()
    {
        return Types.INTEGER;
    }

    public Integer compose(ByteBuffer bytes)
    {
        return ByteBufferUtil.toInt(bytes);
    }
}
