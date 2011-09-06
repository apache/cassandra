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
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.cassandra.utils.ByteBufferUtil;

public class JdbcDate extends AbstractJdbcType<Date>
{
    public static final String[] iso8601Patterns = new String[] {
        "yyyy-MM-dd HH:mm",
        "yyyy-MM-dd HH:mm:ss",
        "yyyy-MM-dd HH:mmZ",
        "yyyy-MM-dd HH:mm:ssZ",
        "yyyy-MM-dd'T'HH:mm",
        "yyyy-MM-dd'T'HH:mmZ",
        "yyyy-MM-dd'T'HH:mm:ss",
        "yyyy-MM-dd'T'HH:mm:ssZ",
        "yyyy-MM-dd",
        "yyyy-MM-ddZ"
    };
    static final String DEFAULT_FORMAT = iso8601Patterns[3];
    static final SimpleDateFormat FORMATTER = new SimpleDateFormat(DEFAULT_FORMAT);
    
    public static final JdbcDate instance = new JdbcDate();
    
    JdbcDate() {}
    
    public boolean isCaseSensitive()
    {
        return false;
    }

    public int getScale(Date obj)
    {
        return -1;
    }

    public int getPrecision(Date obj)
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

    public String toString(Date obj)
    {
        return FORMATTER.format(obj);
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
            throw new MarshalException("A date is exactly 8 bytes (stored as a long): " + bytes.remaining());
        }
        
        // uses ISO-8601 formatted string
        return FORMATTER.format(new Date(bytes.getLong(bytes.position())));
    }

    public Class<Date> getType()
    {
        return Date.class;
    }

    public int getJdbcType()
    {
        return Types.DATE;
    }

    public Date compose(ByteBuffer bytes)
    {
        return new Date(ByteBufferUtil.toLong(bytes));
    }
}
