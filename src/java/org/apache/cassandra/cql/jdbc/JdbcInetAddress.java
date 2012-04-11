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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.sql.Types;

import org.apache.cassandra.utils.ByteBufferUtil;

public class JdbcInetAddress extends AbstractJdbcType<InetAddress>
{
    public static final JdbcInetAddress instance = new JdbcInetAddress();

    JdbcInetAddress()
    {
    }

    public boolean isCaseSensitive()
    {
        return false;
    }

    public int getScale(InetAddress obj)
    {
        return 0;
    }

    public int getPrecision(InetAddress obj)
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

    public String toString(InetAddress obj)
    {
        return obj.getHostAddress();
    }

    public boolean needsQuotes()
    {
        return false;
    }

    public String getString(ByteBuffer bytes)
    {
        return compose(bytes).getHostAddress();
    }

    public Class<InetAddress> getType()
    {
        return InetAddress.class;
    }

    public int getJdbcType()
    {
        return Types.OTHER;
    }

    public InetAddress compose(ByteBuffer bytes)
    {
        try
        {
            return InetAddress.getByAddress(ByteBufferUtil.getArray(bytes));
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    }

    public ByteBuffer decompose(InetAddress value)
    {
        return ByteBuffer.wrap(value.getAddress());
    }
}
