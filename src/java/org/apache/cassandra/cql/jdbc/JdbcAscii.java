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
import java.nio.charset.CharacterCodingException;
import java.sql.Types;

import com.google.common.base.Charsets;

import org.apache.cassandra.utils.ByteBufferUtil;

public class JdbcAscii extends AbstractJdbcType<String>
{
    public static final JdbcAscii instance = new JdbcAscii();
    
    JdbcAscii() {}
    
    public boolean isCaseSensitive()
    {
        return true;
    }

    public int getScale(String obj)
    {
        return -1;
    }

    public int getPrecision(String obj)
    {
        return -1;
    }

    public boolean isCurrency()
    {
        return false;
    }

    public boolean isSigned()
    {
        return false;
    }

    public String toString(String obj)
    {
        return obj;
    }

    public boolean needsQuotes()
    {
        return true;
    }

    public String getString(ByteBuffer bytes)
    {
        try
        {
            return ByteBufferUtil.string(bytes, Charsets.US_ASCII);
        }
        catch (CharacterCodingException e)
        {
            throw new MarshalException("Invalid ascii bytes " + ByteBufferUtil.bytesToHex(bytes));
        }
    }

    public Class<String> getType()
    {
        return String.class;
    }

    public int getJdbcType()
    {
        return Types.VARCHAR;
    }

    public String compose(ByteBuffer bytes)
    {
        return getString(bytes);
    }

}
