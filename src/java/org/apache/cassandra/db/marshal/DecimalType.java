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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.apache.cassandra.cql.jdbc.JdbcDecimal;
import org.apache.cassandra.utils.ByteBufferUtil;

public class DecimalType extends AbstractType<BigDecimal>
{
    public static final DecimalType instance = new DecimalType();

    DecimalType() {} // singleton    

    public int compare(ByteBuffer bb0, ByteBuffer bb1)
    {
        if (bb0.remaining() == 0)
        {
            return bb1.remaining() == 0 ? 0 : -1;
        }
        if (bb1.remaining() == 0)
        {
            return 1;
        }
        
        return compose(bb0).compareTo(compose(bb1));
    }

    public BigDecimal compose(ByteBuffer bytes)
    {
        return JdbcDecimal.instance.compose(bytes);
    }

    /**
     * The bytes of the ByteBuffer are made up of 4 bytes of int containing the scale
     * followed by the n bytes it takes to store a BigInteger.
     */
    public ByteBuffer decompose(BigDecimal value)
    {
        if (value == null) return ByteBufferUtil.EMPTY_BYTE_BUFFER;
        
        BigInteger bi = value.unscaledValue();
        Integer scale = value.scale();
        byte[] bibytes = bi.toByteArray();
        byte[] sbytes = ByteBufferUtil.bytes(scale).array();
        byte[] bytes = new byte[bi.toByteArray().length+4];
        
        for (int i = 0 ; i < 4 ; i++) bytes[i] = sbytes[i];
        for (int i = 4 ; i < bibytes.length+4 ; i++) bytes[i] = bibytes[i-4];
        
        return ByteBuffer.wrap(bytes);
    }

    public String getString(ByteBuffer bytes)
    {
        return JdbcDecimal.instance.getString(bytes);
    }

    public ByteBuffer fromString(String source) throws MarshalException
    {
        // Return an empty ByteBuffer for an empty string.
        if (source.isEmpty()) return ByteBufferUtil.EMPTY_BYTE_BUFFER;
        
        BigDecimal decimal;

        try
        {
            decimal = new BigDecimal(source);
        }
        catch (Exception e)
        {
            throw new MarshalException(String.format("unable to make BigDecimal from '%s'", source), e);
        }

        return decompose(decimal);
    }

    public void validate(ByteBuffer bytes) throws MarshalException
    {
        // no useful check for invalid decimals.
    }
}
