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

import com.google.common.primitives.Ints;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.DecimalSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteComparable;
import org.apache.cassandra.utils.ByteSource;

public class DecimalType extends NumberType<BigDecimal>
{
    public static final DecimalType instance = new DecimalType();
    private static final int MIN_SCALE = 32;
    private static final int MIN_SIGNIFICANT_DIGITS = MIN_SCALE;
    private static final int MAX_SCALE = 1000;
    private static final MathContext MAX_PRECISION = new MathContext(10000);

    // Constants or escaping values needed to encode/decode variable-length floating point numbers (decimals) in our
    // custom byte-ordered encoding scheme.
    private static final int POSITIVE_DECIMAL_HEADER_MASK = 0x80;
    private static final int NEGATIVE_DECIMAL_HEADER_MASK = 0x00;
    private static final int DECIMAL_EXPONENT_LENGTH_HEADER_MASK = 0x40;

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

    /**
     * Constructs a byte-comparable representation.
     * This is rather difficult and involves reconstructing the decimal.
     *
     * To compare, we need a normalized value, i.e. one with a sign, exponent and (0,1) mantissa. To avoid
     * loss of precision, both exponent and mantissa need to be base-100.  We can't get this directly off the serialized
     * bytes, as they have base-10 scale and base-256 unscaled part.
     *
     * We store:
     *     - sign bit inverted * 0x80 + 0x40 + signed exponent length, where exponent is negated if value is negative
     *     - zero or more exponent bytes (as given by length)
     *     - 0x80 + first pair of decimal digits, negative is value is negative, rounded to -inf
     *     - zero or more 0x80 + pair of decimal digits, always positive
     *     - trailing 0x00
     * Zero is special-cased as 0x80.
     *
     * Because the trailing 00 cannot be produced from a pair of decimal digits (positive or not), no value can be
     * a prefix of another.
     *
     * Encoding examples:
     *    1.1    as       c1 = 0x80 (positive number) + 0x40 + (positive exponent) 0x01 (exp length 1)
     *                    01 = exponent 1 (100^1)
     *                    81 = 0x80 + 01 (0.01)
     *                    8a = 0x80 + 10 (....10)   0.0110e2
     *                    00
     *    -1     as       3f = 0x00 (negative number) + 0x40 - (negative exponent) 0x01 (exp length 1)
     *                    ff = exponent -1. negative number, thus 100^1
     *                    7f = 0x80 - 01 (-0.01)    -0.01e2
     *                    00
     *    -99.9  as       3f = 0x00 (negative number) + 0x40 - (negative exponent) 0x01 (exp length 1)
     *                    ff = exponent -1. negative number, thus 100^1
     *                    1c = 0x80 - 100 (-1.00)
     *                    8a = 0x80 + 10  (+....10) -0.999e2
     *                    00
     *
     */
    @Override
    public ByteSource asComparableBytes(ByteBuffer buf, ByteComparable.Version version)
    {
        BigDecimal value = compose(buf);
        if (value == null)
            return null;
        if (value.compareTo(BigDecimal.ZERO) == 0)  // Note: 0.equals(0.0) returns false!
            return ByteSource.oneByte(POSITIVE_DECIMAL_HEADER_MASK);
        long scale = (((long) value.scale()) - value.precision()) & ~1;
        boolean negative = value.signum() < 0;
        final int negmul = negative ? -1 : 1;
        // This should always fit into an int
        final long exponent = (-scale * negmul) / 2;
        // We should never have scale > Integer.MAX_VALUE, as we're always subtracting the non-negative precision of
        // the encoded BigDecimal, and furthermore we're rounding to negative infinity.
        if (scale > Integer.MAX_VALUE || scale < Integer.MIN_VALUE)
        {
            // We are practically out of range here, but let's handle that anyway
            int mv = Long.signum(scale) * Integer.MAX_VALUE;
            value = value.scaleByPowerOfTen(mv);
            scale -= mv;
        }
        final BigDecimal mantissa = value.scaleByPowerOfTen(Ints.checkedCast(scale)).stripTrailingZeros();
        assert mantissa.abs().compareTo(BigDecimal.ONE) < 0;

        return new ByteSource()
        {
            int posInExp = 0;
            BigDecimal current = mantissa;

            @Override
            public int next()
            {
                if (posInExp < 5)
                {
                    if (posInExp == 0)
                    {
                        int absexp = (int) (exponent < 0 ? -exponent : exponent);
                        while (posInExp < 5 && absexp >> (32 - ++posInExp * 8) == 0) {}
                        int explen = DECIMAL_EXPONENT_LENGTH_HEADER_MASK + (exponent < 0 ? -1 : 1) * (5 - posInExp);
                        return explen + (negative ? NEGATIVE_DECIMAL_HEADER_MASK : POSITIVE_DECIMAL_HEADER_MASK);
                    }
                    else
                        return (int) ((exponent >> (32 - posInExp++ * 8))) & 0xFF;
                }
                if (current == null)
                    return END_OF_STREAM;
                if (current.compareTo(BigDecimal.ZERO) == 0)
                {
                    current = null;
                    return 0x00;
                }
                else
                {
                    BigDecimal v = current.scaleByPowerOfTen(2);
                    BigDecimal floor = v.setScale(0, BigDecimal.ROUND_FLOOR);
                    current = v.subtract(floor);
                    return floor.byteValueExact() + 0x80;
                }
            }
        };
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
