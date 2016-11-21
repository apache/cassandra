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
package org.apache.cassandra.db.marshal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.nio.ByteBuffer;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.DecimalSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

public class DecimalType extends NumberType<BigDecimal>
{
    public static final DecimalType instance = new DecimalType();

    DecimalType() {super(ComparisonType.CUSTOM);} // singleton

    public boolean isEmptyValueMeaningless()
    {
        return true;
    }

    @Override
    public boolean isFloatingPoint()
    {
        return true;
    }

    public int compareCustom(ByteBuffer o1, ByteBuffer o2)
    {
        if (!o1.hasRemaining() || !o2.hasRemaining())
            return o1.hasRemaining() ? 1 : o2.hasRemaining() ? -1 : 0;

        return compose(o1).compareTo(compose(o2));
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

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        try
        {
            return new Constants.Value(getSerializer().serialize(new BigDecimal(parsed.toString())));
        }
        catch (NumberFormatException exc)
        {
            throw new MarshalException(String.format("Value '%s' is not a valid representation of a decimal value", parsed));
        }
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        return getSerializer().deserialize(buffer).toString();
    }

    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.DECIMAL;
    }

    public TypeSerializer<BigDecimal> getSerializer()
    {
        return DecimalSerializer.instance;
    }

    @Override
    protected int toInt(ByteBuffer value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected float toFloat(ByteBuffer value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected long toLong(ByteBuffer value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected double toDouble(ByteBuffer value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected BigInteger toBigInteger(ByteBuffer value)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected BigDecimal toBigDecimal(ByteBuffer value)
    {
        return compose(value);
    }

    public ByteBuffer add(NumberType<?> leftType, ByteBuffer left, NumberType<?> rightType, ByteBuffer right)
    {
        return decompose(leftType.toBigDecimal(left).add(rightType.toBigDecimal(right), MathContext.DECIMAL128));
    }

    public ByteBuffer substract(NumberType<?> leftType, ByteBuffer left, NumberType<?> rightType, ByteBuffer right)
    {
        return decompose(leftType.toBigDecimal(left).subtract(rightType.toBigDecimal(right), MathContext.DECIMAL128));
    }

    public ByteBuffer multiply(NumberType<?> leftType, ByteBuffer left, NumberType<?> rightType, ByteBuffer right)
    {
        return decompose(leftType.toBigDecimal(left).multiply(rightType.toBigDecimal(right), MathContext.DECIMAL128));
    }

    public ByteBuffer divide(NumberType<?> leftType, ByteBuffer left, NumberType<?> rightType, ByteBuffer right)
    {
        return decompose(leftType.toBigDecimal(left).divide(rightType.toBigDecimal(right), MathContext.DECIMAL128));
    }

    public ByteBuffer mod(NumberType<?> leftType, ByteBuffer left, NumberType<?> rightType, ByteBuffer right)
    {
        return decompose(leftType.toBigDecimal(left).remainder(rightType.toBigDecimal(right), MathContext.DECIMAL128));
    }

    public ByteBuffer negate(ByteBuffer input)
    {
        return decompose(toBigDecimal(input).negate());
    }
}
