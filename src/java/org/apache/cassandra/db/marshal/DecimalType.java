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
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.DecimalSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import ch.obermuhlner.math.big.BigDecimalMath;

public class DecimalType extends NumberType<BigDecimal>
{
    public static final DecimalType instance = new DecimalType();

    private static final ArgumentDeserializer ARGUMENT_DESERIALIZER = new DefaultArgumentDeserializer(instance);

    private static final ByteBuffer MASKED_VALUE = instance.decompose(BigDecimal.ZERO);

    private static final int MIN_SCALE = 32;
    private static final int MIN_SIGNIFICANT_DIGITS = MIN_SCALE;
    private static final int MAX_SCALE = 1000;
    private static final MathContext MAX_PRECISION = new MathContext(10000);

    // Constants or escaping values needed to encode/decode variable-length floating point numbers (decimals) in our
    // custom byte-ordered encoding scheme.
    private static final int POSITIVE_DECIMAL_HEADER_MASK = 0x80;
    private static final int NEGATIVE_DECIMAL_HEADER_MASK = 0x00;
    private static final int DECIMAL_EXPONENT_LENGTH_HEADER_MASK = 0x40;
    private static final byte DECIMAL_LAST_BYTE = (byte) 0x00;
    private static final BigInteger HUNDRED = BigInteger.valueOf(100);

    private static final ByteBuffer ZERO_BUFFER = instance.decompose(BigDecimal.ZERO);

    DecimalType() {super(ComparisonType.CUSTOM);} // singleton

    @Override
    public boolean allowsEmpty()
    {
        return true;
    }

    @Override
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
     *     - 0x80 + first pair of decimal digits, negative if value is negative, rounded to -inf
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
    public <V> ByteSource asComparableBytes(ValueAccessor<V> accessor, V data, ByteComparable.Version version)
    {
        BigDecimal value = compose(data, accessor);
        if (value == null)
            return null;
        if (value.compareTo(BigDecimal.ZERO) == 0)  // Note: 0.equals(0.0) returns false!
            return ByteSource.oneByte(POSITIVE_DECIMAL_HEADER_MASK);

        long scale = (((long) value.scale()) - value.precision()) & ~1;
        boolean negative = value.signum() < 0;
        // Make a base-100 exponent (this will always fit in an int).
        int exponent = Math.toIntExact(-scale >> 1);
        // Flip the exponent sign for negative numbers, so that ones with larger magnitudes are propely treated as smaller.
        final int modulatedExponent = negative ? -exponent : exponent;
        // We should never have scale > Integer.MAX_VALUE, as we're always subtracting the non-negative precision of
        // the encoded BigDecimal, and furthermore we're rounding to negative infinity.
        assert scale <= Integer.MAX_VALUE;
        // However, we may end up overflowing on the negative side.
        if (scale < Integer.MIN_VALUE)
        {
            // As scaleByPowerOfTen needs an int scale, do the scaling in two steps.
            int mv = Integer.MIN_VALUE;
            value = value.scaleByPowerOfTen(mv);
            scale -= mv;
        }
        final BigDecimal mantissa = value.scaleByPowerOfTen(Ints.checkedCast(scale)).stripTrailingZeros();
        // We now have a smaller-than-one signed mantissa, and a signed and modulated base-100 exponent.
        assert mantissa.abs().compareTo(BigDecimal.ONE) < 0;

        return new ByteSource()
        {
            // Start with up to 5 bytes for sign + exponent.
            int exponentBytesLeft = 5;
            BigDecimal current = mantissa;

            @Override
            public int next()
            {
                if (exponentBytesLeft > 0)
                {
                    --exponentBytesLeft;
                    if (exponentBytesLeft == 4)
                    {
                        // Skip leading zero bytes in the modulatedExponent.
                        exponentBytesLeft -= Integer.numberOfLeadingZeros(Math.abs(modulatedExponent)) / 8;
                        // Now prepare the leading byte which includes the sign of the number plus the sign and length of the modulatedExponent.
                        int explen = DECIMAL_EXPONENT_LENGTH_HEADER_MASK + (modulatedExponent < 0 ? -exponentBytesLeft : exponentBytesLeft);
                        return explen + (negative ? NEGATIVE_DECIMAL_HEADER_MASK : POSITIVE_DECIMAL_HEADER_MASK);
                    }
                    else
                        return (modulatedExponent >> (exponentBytesLeft * 8)) & 0xFF;
                }
                else if (current == null)
                {
                    return END_OF_STREAM;
                }
                else if (current.compareTo(BigDecimal.ZERO) == 0)
                {
                    current = null;
                    return 0x00;
                }
                else
                {
                    BigDecimal v = current.scaleByPowerOfTen(2);
                    BigDecimal floor = v.setScale(0, RoundingMode.FLOOR);
                    current = v.subtract(floor);
                    return floor.byteValueExact() + 0x80;
                }
            }
        };
    }

    @Override
    public <V> V fromComparableBytes(ValueAccessor<V> accessor, ByteSource.Peekable comparableBytes, ByteComparable.Version version)
    {
        if (comparableBytes == null)
            return accessor.empty();

        int headerBits = comparableBytes.next();
        if (headerBits == POSITIVE_DECIMAL_HEADER_MASK)
            return accessor.valueOf(ZERO_BUFFER);

        // I. Extract the exponent.
        // The sign of the decimal, and the sign and the length (in bytes) of the decimal exponent, are all encoded in
        // the first byte.
        // Get the sign of the decimal...
        boolean isNegative = headerBits < POSITIVE_DECIMAL_HEADER_MASK;
        headerBits -= isNegative ? NEGATIVE_DECIMAL_HEADER_MASK : POSITIVE_DECIMAL_HEADER_MASK;
        headerBits -= DECIMAL_EXPONENT_LENGTH_HEADER_MASK;
        // Get the sign and the length of the exponent (the latter is encoded as its negative if the sign of the
        // exponent is negative)...
        boolean isExponentNegative = headerBits < 0;
        headerBits = isExponentNegative ? -headerBits : headerBits;
        // Now consume the exponent bytes. If the exponent is negative and uses less than 4 bytes, the remaining bytes
        // should be padded with 1s, in order for the constructed int to contain the correct (negative) exponent value.
        // So, if the exponent is negative, we can just start with all bits set to 1 (i.e. we can start with -1).
        int exponent = isExponentNegative ? -1 : 0;
        for (int i = 0; i < headerBits; ++i)
            exponent = (exponent << 8) | comparableBytes.next();
        // The encoded exponent also contains the decimal sign, in order to correctly compare exponents in case of
        // negative decimals (e.g. x * 10^y > x * 10^z if x < 0 && y < z). After the decimal sign is "removed", what's
        // left is a base-100 exponent following BigDecimal's convention for the exponent sign.
        exponent = isNegative ? -exponent : exponent;

        // II. Extract the mantissa as a BigInteger value. It was encoded as a BigDecimal value between 0 and 1, in
        // order to be used for comparison (after the sign of the decimal and the sign and the value of the exponent),
        // but when decoding we don't need that property on the transient mantissa value.
        BigInteger mantissa = BigInteger.ZERO;
        int curr = comparableBytes.next();
        while (curr != DECIMAL_LAST_BYTE)
        {
            // The mantissa value is constructed by a standard positional notation value calculation.
            // The value of the next digit is the next most-significant mantissa byte as an unsigned integer,
            // offset by a predetermined value (in this case, 0x80)...
            int currModified = curr - 0x80;
            // ...multiply the current value by the base (in this case, 100)...
            mantissa = mantissa.multiply(HUNDRED);
            // ...then add the next digit to the modified current value...
            mantissa = mantissa.add(BigInteger.valueOf(currModified));
            // ...and finally, adjust the base-100, BigDecimal format exponent accordingly.
            --exponent;
            curr = comparableBytes.next();
        }

        // III. Construct the final BigDecimal value, by combining the mantissa and the exponent, guarding against
        // underflow or overflow when exponents are close to their boundary values.
        long base10NonBigDecimalFormatExp = 2L * exponent;
        // When expressing a sufficiently big decimal, BigDecimal's internal scale value will be negative with very
        // big absolute value. To compute the encoded exponent, this internal scale has the number of digits of the
        // unscaled value subtracted from it, after which it's divided by 2, rounding down to negative infinity
        // (before accounting for the decimal sign). When decoding, this exponent is converted to a base-10 exponent in
        // non-BigDecimal format, which means that it can very well overflow Integer.MAX_VALUE.
        // For example, see how <code>new BigDecimal(BigInteger.TEN, Integer.MIN_VALUE)</code> is encoded and decoded.
        if (base10NonBigDecimalFormatExp > Integer.MAX_VALUE)
        {
            // If the base-10 exponent will result in an overflow, some of its powers of 10 need to be absorbed by the
            // mantissa. How much exactly? As little as needed, in order to avoid complex BigInteger operations, which
            // means exactly as much as to have a scale of -Integer.MAX_VALUE.
            int exponentReduction = (int) (base10NonBigDecimalFormatExp - Integer.MAX_VALUE);
            mantissa = mantissa.multiply(BigInteger.TEN.pow(exponentReduction));
            base10NonBigDecimalFormatExp = Integer.MAX_VALUE;
        }
        assert base10NonBigDecimalFormatExp >= Integer.MIN_VALUE && base10NonBigDecimalFormatExp <= Integer.MAX_VALUE;
        // Here we negate the exponent, as we are not using BigDecimal.scaleByPowerOfTen, where a positive number means
        // "multiplying by a positive power of 10", but to BigDecimal's internal scale representation, where a positive
        // number means "dividing by a positive power of 10".
        byte[] mantissaBytes = mantissa.toByteArray();
        V resultBuf = accessor.allocate(4 + mantissaBytes.length);
        accessor.putInt(resultBuf, 0, (int) -base10NonBigDecimalFormatExp);
        accessor.copyByteArrayTo(mantissaBytes, 0, resultBuf, 4, mantissaBytes.length);
        return resultBuf;
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
    public ArgumentDeserializer getArgumentDeserializer()
    {
        return ARGUMENT_DESERIALIZER;
    }

    /**
     * Converts the specified number into a {@link BigDecimal}.
     *
     * @param number the value to convert
     * @return the converted value
     */
    protected BigDecimal toBigDecimal(Number number)
    {
        if (number instanceof BigDecimal)
            return (BigDecimal) number;

        if (number instanceof BigInteger)
            return new BigDecimal((BigInteger) number);

        double d = number.doubleValue();

        if (Double.isNaN(d))
            throw new NumberFormatException("A NaN cannot be converted into a decimal");

        if (Double.isInfinite(d))
            throw new NumberFormatException("An infinite number cannot be converted into a decimal");

        return BigDecimal.valueOf(d);
    }

    @Override
    public ByteBuffer add(Number left, Number right)
    {
        return decompose(toBigDecimal(left).add(toBigDecimal(right), MAX_PRECISION));
    }

    @Override
    public ByteBuffer substract(Number left, Number right)
    {
        return decompose(toBigDecimal(left).subtract(toBigDecimal(right), MAX_PRECISION));
    }

    @Override
    public ByteBuffer multiply(Number left, Number right)
    {
        return decompose(toBigDecimal(left).multiply(toBigDecimal(right), MAX_PRECISION));
    }

    @Override
    public ByteBuffer divide(Number left, Number right)
    {
        BigDecimal leftOperand = toBigDecimal(left);
        BigDecimal rightOperand = toBigDecimal(right);

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

    @Override
    public ByteBuffer mod(Number left, Number right)
    {
        return decompose(toBigDecimal(left).remainder(toBigDecimal(right)));
    }

    @Override
    public ByteBuffer negate(Number input)
    {
        return decompose(toBigDecimal(input).negate());
    }

    @Override
    public ByteBuffer abs(Number input)
    {
        return decompose(toBigDecimal(input).abs());
    }

    @Override
    public ByteBuffer exp(Number input)
    {
        return decompose(exp(toBigDecimal(input)));
    }

    protected BigDecimal exp(BigDecimal input)
    {
        int precision = input.precision();
        precision = Math.max(MIN_SIGNIFICANT_DIGITS, precision);
        precision = Math.min(MAX_PRECISION.getPrecision(), precision);
        return BigDecimalMath.exp(input, new MathContext(precision, RoundingMode.HALF_EVEN));
    }

    @Override
    public ByteBuffer log(Number input)
    {
        return decompose(log(toBigDecimal(input)));
    }

    protected BigDecimal log(BigDecimal input)
    {
        if (input.compareTo(BigDecimal.ZERO) <= 0) throw new ArithmeticException("Natural log of number zero or less");
        int precision = input.precision();
        precision = Math.max(MIN_SIGNIFICANT_DIGITS, precision);
        precision = Math.min(MAX_PRECISION.getPrecision(), precision);
        return BigDecimalMath.log(input, new MathContext(precision, RoundingMode.HALF_EVEN));
    }

    @Override
    public ByteBuffer log10(Number input)
    {
        return decompose(log10(toBigDecimal(input)));
    }

    protected BigDecimal log10(BigDecimal input)
    {
        if (input.compareTo(BigDecimal.ZERO) <= 0) throw new ArithmeticException("Log10 of number zero or less");
        int precision = input.precision();
        precision = Math.max(MIN_SIGNIFICANT_DIGITS, precision);
        precision = Math.min(MAX_PRECISION.getPrecision(), precision);
        return BigDecimalMath.log10(input, new MathContext(precision, RoundingMode.HALF_EVEN));
    }

    @Override
    public ByteBuffer round(Number input)
    {
        return DecimalType.instance.decompose(
        toBigDecimal(input).setScale(0, RoundingMode.HALF_UP));
    }

    @Override
    public ByteBuffer getMaskedValue()
    {
        return MASKED_VALUE;
    }
}
