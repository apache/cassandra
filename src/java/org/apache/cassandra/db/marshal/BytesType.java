package org.apache.cassandra.db.marshal;
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

import org.apache.cassandra.cql.jdbc.JdbcBytes;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Hex;

public class BytesType extends AbstractType<ByteBuffer>
{
    public static final BytesType instance = new BytesType();

    BytesType() {} // singleton

    public ByteBuffer compose(ByteBuffer bytes)
    {
        return JdbcBytes.instance.compose(bytes);
    }

    public ByteBuffer decompose(ByteBuffer value)
    {
        return value;
    }
    
    public int compare(ByteBuffer o1, ByteBuffer o2)
    {
        return BytesType.bytesCompare(o1, o2);
    }

    public static int bytesCompare(ByteBuffer o1, ByteBuffer o2)
    {
        if(null == o1){
            if(null == o2) return 0;
            else return -1;
        }
              
        return ByteBufferUtil.compareUnsigned(o1, o2);
    }

    public String getString(ByteBuffer bytes)
    {
        return JdbcBytes.instance.getString(bytes);
    }

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

    public void validate(ByteBuffer bytes) throws MarshalException
    {
        // all bytes are legal.
    }
}
