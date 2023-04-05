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
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.Objects;

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
    private static final int MIN_SCALE = 32;
    private static final int MIN_SIGNIFICANT_DIGITS = MIN_SCALE;
    private static final int MAX_SCALE = 1000;
    private static final MathContext MAX_PRECISION = new MathContext(10000);

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

    public <VL, VR> int compareCustom(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR)
    {
        return compareComposed(left, accessorL, right, accessorR, this);
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
            return new Constants.Value(fromString(Objects.toString(parsed)));
        }
        catch (NumberFormatException | MarshalException exc)
        {
            throw new MarshalException(String.format("Value '%s' is not a valid representation of a decimal value", parsed));
        }
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        return Objects.toString(getSerializer().deserialize(buffer), "\"\"");
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
        return decompose(leftType.toBigDecimal(left).add(rightType.toBigDecimal(right), MAX_PRECISION));
    }

    public ByteBuffer substract(NumberType<?> leftType, ByteBuffer left, NumberType<?> rightType, ByteBuffer right)
    {
        return decompose(leftType.toBigDecimal(left).subtract(rightType.toBigDecimal(right), MAX_PRECISION));
    }

    public ByteBuffer multiply(NumberType<?> leftType, ByteBuffer left, NumberType<?> rightType, ByteBuffer right)
    {
        return decompose(leftType.toBigDecimal(left).multiply(rightType.toBigDecimal(right), MAX_PRECISION));
    }

    public ByteBuffer divide(NumberType<?> leftType, ByteBuffer left, NumberType<?> rightType, ByteBuffer right)
    {
        BigDecimal leftOperand = leftType.toBigDecimal(left);
        BigDecimal rightOperand = rightType.toBigDecimal(right);

        // Predict position of first significant digit in the quotient.
        // Note: it is possible to improve prediction accuracy by comparing first significant digits in operands
        // but it requires additional computations so this step is omitted
        int quotientFirstDigitPos = (leftOperand.precision() - leftOperand.scale()) - (rightOperand.precision() - rightOperand.scale());

        int scale = MIN_SIGNIFICANT_DIGITS - quotientFirstDigitPos;
        scale = Math.max(scale, leftOperand.scale());
        scale = Math.max(scale, rightOperand.scale());
        scale = Math.max(scale, MIN_SCALE);
        scale = Math.min(scale, MAX_SCALE);

        return decompose(leftOperand.divide(rightOperand, scale, RoundingMode.HALF_UP).stripTrailingZeros());
    }

    public ByteBuffer mod(NumberType<?> leftType, ByteBuffer left, NumberType<?> rightType, ByteBuffer right)
    {
        return decompose(leftType.toBigDecimal(left).remainder(rightType.toBigDecimal(right)));
    }

    public ByteBuffer negate(ByteBuffer input)
    {
        return decompose(toBigDecimal(input).negate());
    }
}
