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
import java.nio.ByteBuffer;
import java.util.Objects;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.IntegerSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

public final class IntegerType extends NumberType<BigInteger>
{
    public static final IntegerType instance = new IntegerType();

    private static final ArgumentDeserializer ARGUMENT_DESERIALIZER = new DefaultArgumentDeserializer(instance);

    private static final ByteBuffer MASKED_VALUE = instance.decompose(BigInteger.ZERO);

    // Constants or escaping values needed to encode/decode variable-length integers in our custom byte-ordered
    // encoding scheme.
    private static final int POSITIVE_VARINT_HEADER = 0x80;
    private static final int NEGATIVE_VARINT_LENGTH_HEADER = 0x00;
    private static final int POSITIVE_VARINT_LENGTH_HEADER = 0xFF;
    private static final byte BIG_INTEGER_NEGATIVE_LEADING_ZERO = (byte) 0xFF;
    private static final byte BIG_INTEGER_POSITIVE_LEADING_ZERO = (byte) 0x00;
    public static final int FULL_FORM_THRESHOLD = 7;

    private static <V> int findMostSignificantByte(V value, ValueAccessor<V> accessor)
    {
        int len = accessor.size(value) - 1;
        int i = 0;
        for (; i < len; i++)
        {
            byte b0 = accessor.getByte(value, i);
            if (b0 != 0 && b0 != -1)
                break;
            byte b1 = accessor.getByte(value, i + 1);
            if (b0 == 0 && b1 != 0)
            {
                if (b1 > 0)
                    i++;
                break;
            }
            if (b0 == -1 && b1 != -1)
            {
                if (b1 < 0)
                    i++;
                break;
            }
        }
        return i;
    }

    IntegerType() {super(ComparisonType.CUSTOM);}/* singleton */

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

    public <VL, VR> int compareCustom(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR)
    {
        return IntegerType.compareIntegers(left, accessorL, right, accessorR);
    }

    public static <VL, VR> int compareIntegers(VL lhs, ValueAccessor<VL> accessorL, VR rhs, ValueAccessor<VR> accessorR)
    {
        int lhsLen = accessorL.size(lhs);
        int rhsLen = accessorR.size(rhs);

        if (lhsLen == 0)
            return rhsLen == 0 ? 0 : -1;
        if (rhsLen == 0)
            return 1;

        int lhsMsbIdx = findMostSignificantByte(lhs, accessorL);
        int rhsMsbIdx = findMostSignificantByte(rhs, accessorR);

        //diffs contain number of "meaningful" bytes (i.e. ignore padding)
        int lhsLenDiff = lhsLen - lhsMsbIdx;
        int rhsLenDiff = rhsLen - rhsMsbIdx;

        byte lhsMsb = accessorL.getByte(lhs, lhsMsbIdx);
        byte rhsMsb = accessorR.getByte(rhs, rhsMsbIdx);

        /*         +    -
         *      -----------
         *    + | -d |  1 |
         * LHS  -----------
         *    - | -1 |  d |
         *      -----------
         *          RHS
         *
         * d = difference of length in significant bytes
         */
        if (lhsLenDiff != rhsLenDiff)
        {
            if (lhsMsb < 0)
                return rhsMsb < 0 ? rhsLenDiff - lhsLenDiff : -1;
            if (rhsMsb < 0)
                return 1;
            return lhsLenDiff - rhsLenDiff;
        }

        // msb uses signed comparison
        if (lhsMsb != rhsMsb)
            return lhsMsb - rhsMsb;
        lhsMsbIdx++;
        rhsMsbIdx++;

        // remaining bytes are compared unsigned
        while (lhsMsbIdx < lhsLen)
        {
            lhsMsb = accessorL.getByte(lhs, lhsMsbIdx++);
            rhsMsb = accessorR.getByte(rhs, rhsMsbIdx++);

            if (lhsMsb != rhsMsb)
                return (lhsMsb & 0xFF) - (rhsMsb & 0xFF);
        }

        return 0;
    }

    /**
     * Constructs a byte-comparable representation of the number.
     *
     * In the current format we represent it:
     *    directly as varint, if the length is 6 or smaller (the encoding has non-00/FF first byte)
     *    {@code <signbyte><length as unsigned integer - 7><7 or more bytes>}, otherwise
     * where {@code <signbyte>} is 00 for negative numbers and FF for positive ones, and the length's bytes are inverted if
     * the number is negative (so that longer length sorts smaller).
     *
     * Because we present the sign separately, we don't need to include 0x00 prefix for positive integers whose first
     * byte is >= 0x80 or 0xFF prefix for negative integers whose first byte is < 0x80. Note that we do this before
     * taking the length for the purposes of choosing between varint and full-form encoding.
     *
     * The representations are prefix-free, because the choice between varint and full-form encoding is determined by
     * the first byte where varints are properly ordered between full-form negative and full-form positive, varint
     * encoding is prefix-free, and full-form representations of different length always have length bytes that differ.
     *
     * Examples:
     *    -1            as 7F
     *    0             as 80
     *    1             as 81
     *    127           as C07F
     *    255           as C0FF
     *    2^32-1        as F8FFFFFFFF
     *    2^32          as F900000000
     *    2^56-1        as FEFFFFFFFFFFFFFF
     *    2^56          as FF000100000000000000
     *
     * See {@link #asComparableBytesLegacy} for description of the legacy format.
     */
    @Override
    public <V> ByteSource asComparableBytes(ValueAccessor<V> accessor, V data, ByteComparable.Version version)
    {
        final int limit = accessor.size(data);
        if (limit == 0)
            return null;

        // skip any leading sign-only byte(s)
        int p = 0;
        final byte signbyte = accessor.getByte(data, p);
        if (signbyte == BIG_INTEGER_NEGATIVE_LEADING_ZERO || signbyte == BIG_INTEGER_POSITIVE_LEADING_ZERO)
        {
            while (p + 1 < limit)
            {
                if (accessor.getByte(data, ++p) != signbyte)
                    break;
            }
        }

        if (version != ByteComparable.Version.LEGACY)
            return (limit - p < FULL_FORM_THRESHOLD)
                   ? encodeAsVarInt(accessor, data, limit)
                   : asComparableBytesCurrent(accessor, data, p, limit, (signbyte >> 7) & 0xFF);
        else
            return asComparableBytesLegacy(accessor, data, p, limit, signbyte);
    }

    /**
     * Encode the BigInteger stored in the given buffer as a variable-length signed integer.
     * The length of the number is given in the limit argument, and must be <= 8.
     */
    private <V> ByteSource encodeAsVarInt(ValueAccessor<V> accessor, V data, int limit)
    {
        long v;
        switch (limit)
        {
            case 1:
                v = accessor.getByte(data, 0);
                break;
            case 2:
                v = accessor.getShort(data, 0);
                break;
            case 3:
                v = (accessor.getShort(data, 0) << 8) | (accessor.getByte(data, 2) & 0xFF);
                break;
            case 4:
                v = accessor.getInt(data, 0);
                break;
            case 5:
                v = ((long) accessor.getInt(data, 0) << 8) | (accessor.getByte(data, 4) & 0xFF);
                break;
            case 6:
                v = ((long) accessor.getInt(data, 0) << 16) | (accessor.getShort(data, 4) & 0xFFFF);
                break;
            case 7:
                v = ((long) accessor.getInt(data, 0) << 24) | ((accessor.getShort(data, 4) & 0xFFFF) << 8) | (accessor.getByte(data, 6) & 0xFF);
                break;
            case 8:
                // This is not reachable within the encoding; added for completeness.
                v = accessor.getLong(data, 0);
                break;
            default:
                throw new AssertionError();
        }
        return ByteSource.variableLengthInteger(v);
    }

    /**
     * Constructs a full-form byte-comparable representation of the number in the current format.
     *
     * This contains:
     *    {@code <signbyte><length as unsigned integer - 7><7 or more bytes>}, otherwise
     * where {@code <signbyte>} is 00 for negative numbers and FF for positive ones, and the length's bytes are inverted if
     * the number is negative (so that longer length sorts smaller).
     *
     * Because we present the sign separately, we don't need to include 0x00 prefix for positive integers whose first
     * byte is >= 0x80 or 0xFF prefix for negative integers whose first byte is < 0x80.
     *
     * The representations are prefix-free, because representations of different length always have length bytes that
     * differ.
     */
    private <V> ByteSource asComparableBytesCurrent(ValueAccessor<V> accessor, V data, int startpos, int limit, int signbyte)
    {
        // start with sign as a byte, then variable-length-encoded length, then bytes (stripped leading sign)
        return new ByteSource()
        {
            int pos = -2;
            ByteSource lengthEncoding = new VariableLengthUnsignedInteger(limit - startpos - FULL_FORM_THRESHOLD);

            @Override
            public int next()
            {
                if (pos == -2)
                {
                    ++pos;
                    return signbyte ^ 0xFF; // 00 for negative/FF for positive (01-FE for direct varint encoding)
                }
                else if (pos == -1)
                {
                    int nextByte = lengthEncoding.next();
                    if (nextByte != END_OF_STREAM)
                        return nextByte ^ signbyte;
                    pos = startpos;
                }

                if (pos == limit)
                    return END_OF_STREAM;

                return accessor.getByte(data, pos++) & 0xFF;
            }
        };
    }

    /**
     * Constructs a byte-comparable representation of the number in the legacy format.
     * We represent it as
     *    {@code <zero or more length_bytes where length = 128> <length_byte> <first_significant_byte> <zero or more bytes>}
     * where a length_byte is:
     *    - 0x80 + (length - 1) for positive numbers (so that longer length sorts bigger)
     *    - 0x7F - (length - 1) for negative numbers (so that longer length sorts smaller)
     *
     * Because we include the sign in the length byte:
     * - unlike fixed-length ints, we don't need to sign-invert the first significant byte,
     * - unlike BigInteger, we don't need to include 0x00 prefix for positive integers whose first byte is >= 0x80
     *   or 0xFF prefix for negative integers whose first byte is < 0x80.
     *
     * The representations are prefix-free, because representations of different length always have length bytes that
     * differ.
     *
     * Examples:
     *    0             as 8000
     *    1             as 8001
     *    127           as 807F
     *    255           as 80FF
     *    2^31-1        as 837FFFFFFF
     *    2^31          as 8380000000
     *    2^32          as 840100000000
     */
    private <V> ByteSource asComparableBytesLegacy(ValueAccessor<V> accessor, V data, int startpos, int limit, int signbyte)
    {
        return new ByteSource()
        {
            int pos = startpos;
            int sizeToReport = limit - startpos;
            boolean sizeReported = false;

            public int next()
            {
                if (!sizeReported)
                {
                    if (sizeToReport >= 128)
                    {
                        sizeToReport -= 128;
                        return signbyte >= 0
                               ? POSITIVE_VARINT_LENGTH_HEADER
                               : NEGATIVE_VARINT_LENGTH_HEADER;
                    }
                    else
                    {
                        sizeReported = true;
                        return signbyte >= 0
                               ? POSITIVE_VARINT_HEADER + (sizeToReport - 1)
                               : POSITIVE_VARINT_HEADER - sizeToReport;
                    }
                }

                if (pos == limit)
                    return END_OF_STREAM;

                return accessor.getByte(data, pos++) & 0xFF;
            }
        };
    }

    @Override
    public <V> V fromComparableBytes(ValueAccessor<V> accessor, ByteSource.Peekable comparableBytes, ByteComparable.Version version)
    {
        assert version != ByteComparable.Version.LEGACY;
        if (comparableBytes == null)
            return accessor.empty();

        // Consume the first byte to determine whether the encoded number is positive and
        // start iterating through the length header bytes and collecting the number of value bytes.
        int sign = comparableBytes.peek() ^ 0xFF;   // FF if negative, 00 if positive
        if (sign != 0xFF && sign != 0x00)
            return extractVarIntBytes(accessor, ByteSourceInverse.getVariableLengthInteger(comparableBytes));

        // consume the sign byte
        comparableBytes.next();

        // Read the length (inverted if the number is negative)
        int valueBytes = Math.toIntExact(ByteSourceInverse.getVariableLengthUnsignedIntegerXoring(comparableBytes, sign) + FULL_FORM_THRESHOLD);
        // Get the bytes.
        return extractBytes(accessor, comparableBytes, sign, valueBytes);
    }

    private <V> V extractVarIntBytes(ValueAccessor<V> accessor, long value)
    {
        int length = (64 - Long.numberOfLeadingZeros(value ^ (value >> 63)) + 8) / 8;   // number of bytes needed: 7 bits -> one byte, 8 bits -> 2 bytes
        V buf = accessor.allocate(length);
        switch (length)
        {
            case 1:
                accessor.putByte(buf, 0, (byte) value);
                break;
            case 2:
                accessor.putShort(buf, 0, (short) value);
                break;
            case 3:
                accessor.putShort(buf, 0, (short) (value >> 8));
                accessor.putByte(buf, 2, (byte) value);
                break;
            case 4:
                accessor.putInt(buf, 0, (int) value);
                break;
            case 5:
                accessor.putInt(buf, 0, (int) (value >> 8));
                accessor.putByte(buf, 4, (byte) value);
                break;
            case 6:
                accessor.putInt(buf, 0, (int) (value >> 16));
                accessor.putShort(buf, 4, (short) value);
                break;
            case 7:
                accessor.putInt(buf, 0, (int) (value >> 24));
                accessor.putShort(buf, 4, (short) (value >> 8));
                accessor.putByte(buf, 6, (byte) value);
                break;
            case 8:
                // This is not reachable within the encoding; added for completeness.
                accessor.putLong(buf, 0, value);
                break;
            default:
                throw new AssertionError();
        }
        return buf;
    }

    private <V> V extractBytes(ValueAccessor<V> accessor, ByteSource.Peekable comparableBytes, int sign, int valueBytes)
    {
        int writtenBytes = 0;
        V buf;
        // Add "leading zero" if needed (i.e. in case the leading byte of a positive number corresponds to a negative
        // value, or in case the leading byte of a negative number corresponds to a non-negative value).
        // Size the array containing all the value bytes accordingly.
        int curr = comparableBytes.next();
        if ((curr & 0x80) != (sign & 0x80))
        {
            ++valueBytes;
            buf = accessor.allocate(valueBytes);
            accessor.putByte(buf, writtenBytes++, (byte) sign);
        }
        else
            buf = accessor.allocate(valueBytes);
        // Don't forget to add the first consumed value byte after determining whether leading zero should be added
        // and sizing the value bytes array.
        accessor.putByte(buf, writtenBytes++, (byte) curr);

        // Consume exactly the number of expected value bytes.
        while (writtenBytes < valueBytes)
            accessor.putByte(buf, writtenBytes++, (byte) comparableBytes.next());

        return buf;
    }

    public ByteBuffer fromString(String source) throws MarshalException
    {
        // Return an empty ByteBuffer for an empty string.
        if (source.isEmpty())
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        BigInteger integerType;

        try
        {
            integerType = new BigInteger(source);
        }
        catch (Exception e)
        {
            throw new MarshalException(String.format("unable to make int from '%s'", source), e);
        }

        return decompose(integerType);
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        try
        {
            return new Constants.Value(getSerializer().serialize(new BigInteger(parsed.toString())));
        }
        catch (NumberFormatException exc)
        {
            throw new MarshalException(String.format(
                    "Value '%s' is not a valid representation of a varint value", parsed));
        }
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        return Objects.toString(getSerializer().deserialize(buffer), "\"\"");
    }

    @Override
    public boolean isValueCompatibleWithInternal(AbstractType<?> otherType)
    {
        return this == otherType || Int32Type.instance.isValueCompatibleWith(otherType) || LongType.instance.isValueCompatibleWith(otherType);
    }

    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.VARINT;
    }

    public TypeSerializer<BigInteger> getSerializer()
    {
        return IntegerSerializer.instance;
    }

    @Override
    public ArgumentDeserializer getArgumentDeserializer()
    {
        return ARGUMENT_DESERIALIZER;
    }

    private BigInteger toBigInteger(Number number)
    {
        if (number instanceof BigInteger)
            return (BigInteger) number;

        return BigInteger.valueOf(number.longValue());
    }

    public ByteBuffer add(Number left, Number right)
    {
        return decompose(toBigInteger(left).add(toBigInteger(right)));
    }

    public ByteBuffer substract(Number left, Number right)
    {
        return decompose(toBigInteger(left).subtract(toBigInteger(right)));
    }

    public ByteBuffer multiply(Number left, Number right)
    {
        return decompose(toBigInteger(left).multiply(toBigInteger(right)));
    }

    public ByteBuffer divide(Number left, Number right)
    {
        return decompose(toBigInteger(left).divide(toBigInteger(right)));
    }

    public ByteBuffer mod(Number left, Number right)
    {
        return decompose(toBigInteger(left).remainder(toBigInteger(right)));
    }

    public ByteBuffer negate(Number input)
    {
        return decompose(toBigInteger(input).negate());
    }

    @Override
    public ByteBuffer abs(Number input)
    {
        return decompose(toBigInteger(input).abs());
    }

    @Override
    public ByteBuffer exp(Number input)
    {
        BigInteger bi = toBigInteger(input);
        BigDecimal bd = new BigDecimal(bi);
        BigDecimal result = DecimalType.instance.exp(bd);
        BigInteger out = result.toBigInteger();
        return IntegerType.instance.decompose(out);
    }

    @Override
    public ByteBuffer log(Number input)
    {
        BigInteger bi = toBigInteger(input);
        if (bi.compareTo(BigInteger.ZERO) <= 0) throw new ArithmeticException("Natural log of number zero or less");
        BigDecimal bd = new BigDecimal(bi);
        BigDecimal result = DecimalType.instance.log(bd);
        BigInteger out = result.toBigInteger();
        return IntegerType.instance.decompose(out);
    }

    @Override
    public ByteBuffer log10(Number input)
    {
        BigInteger bi = toBigInteger(input);
        if (bi.compareTo(BigInteger.ZERO) <= 0) throw new ArithmeticException("Log10 of number zero or less");
        BigDecimal bd = new BigDecimal(bi);
        BigDecimal result = DecimalType.instance.log10(bd);
        BigInteger out = result.toBigInteger();
        return IntegerType.instance.decompose(out);
    }

    @Override
    public ByteBuffer round(Number input)
    {
        return decompose(toBigInteger(input));
    }

    @Override
    public ByteBuffer getMaskedValue()
    {
        return MASKED_VALUE;
    }
}
