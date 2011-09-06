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

public class JdbcFloat extends AbstractJdbcType<Float>
{
    public static final JdbcFloat instance = new JdbcFloat();
    
    JdbcFloat() {}
    
    public boolean isCaseSensitive()
    {
        return false;
    }

    public int getScale(Float obj)
    {
        return 40;
    }

    public int getPrecision(Float obj)
    {
        return 7;
    }

    public boolean isCurrency()
    {
        return false;
    }

    public boolean isSigned()
    {
        return true;
    }

    public String toString(Float obj)
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
            throw new MarshalException("A float is exactly 4 bytes : "+bytes.remaining());
        }
        
        return ((Float)ByteBufferUtil.toFloat(bytes)).toString();
    }

    public Class<Float> getType()
    {
        return Float.class;
    }

    public int getJdbcType()
    {
        return Types.FLOAT;
    }

    public Float compose(ByteBuffer bytes)
    {
        return ByteBufferUtil.toFloat(bytes);
    }
}
