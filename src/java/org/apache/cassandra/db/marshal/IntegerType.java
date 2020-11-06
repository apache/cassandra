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
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.IntegerSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteComparable;
import org.apache.cassandra.utils.ByteSource;

public final class IntegerType extends NumberType<BigInteger>
{
    public static final IntegerType instance = new IntegerType();

    // Constants or escaping values needed to encode/decode variable-length integers in our custom byte-ordered
    // encoding scheme.
    private static final int POSITIVE_VARINT_HEADER = 0x80;
    private static final int NEGATIVE_VARINT_LENGTH_HEADER = 0x00;
    private static final int POSITIVE_VARINT_LENGTH_HEADER = 0xFF;

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
     * We represent it as
     *    <zero or more length_bytes where length = 128> <length_byte> <first_significant_byte> <zero or more bytes>
     * where a length_byte is:
     *    - 0x80 + (length - 1) for positive numbers (so that longer length sorts bigger)
     *    - 0x7F - (length - 1) for negative numbers (so that longer length sorts smaller)
     * we don't need to sign-invert the first significant byte as the order there is already determined by the length
     * byte.
     *
     * The representations are prefix-free, because representations of different length always have length bytes that
     * differ.
     *
     * Examples:
     *    0             as 8000
     *    1             as 8001
     *    127           as 807F
     *    255           as 80FF
     *    2^32-1        as 837FFFFFFF
     *    2^32          as 8380000000
     *    2^33          as 840100000000
     */
    @Override
    public ByteSource asComparableBytes(ByteBuffer buf, ByteComparable.Version version)
    {
        int p = buf.position();
        final int limit = buf.limit();
        if (p == limit)
            return null;

        // skip padding
        final byte signbyte = buf.get(p);
        if (signbyte == (byte) POSITIVE_VARINT_LENGTH_HEADER || signbyte == (byte) NEGATIVE_VARINT_LENGTH_HEADER)
            while (p + 1 < limit && buf.get(++p) == signbyte) {}
        final int startpos = p;

        return new ByteSource()
        {
            int pos = startpos;
            int sizeToReport = limit - startpos;
            boolean sizeReported = false;

            public int next()
            {
                if (!sizeReported)
                {
                    int v = sizeToReport;
                    if (v >= 128)
                        v = 128;
                    else
                        sizeReported = true;

                    sizeToReport -= v;
                    return signbyte >= 0
                           ? POSITIVE_VARINT_HEADER + (v - 1)
                           : POSITIVE_VARINT_HEADER - v;
                }
                if (pos == limit)
                    return END_OF_STREAM;

                return buf.get(pos++) & 0xFF;
            }
        };
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
        return compose(value);
    }

    @Override
    protected BigDecimal toBigDecimal(ByteBuffer value)
    {
        return new BigDecimal(compose(value));
    }

    public ByteBuffer add(NumberType<?> leftType, ByteBuffer left, NumberType<?> rightType, ByteBuffer right)
    {
        return decompose(leftType.toBigInteger(left).add(rightType.toBigInteger(right)));
    }

    public ByteBuffer substract(NumberType<?> leftType, ByteBuffer left, NumberType<?> rightType, ByteBuffer right)
    {
        return decompose(leftType.toBigInteger(left).subtract(rightType.toBigInteger(right)));
    }

    public ByteBuffer multiply(NumberType<?> leftType, ByteBuffer left, NumberType<?> rightType, ByteBuffer right)
    {
        return decompose(leftType.toBigInteger(left).multiply(rightType.toBigInteger(right)));
    }

    public ByteBuffer divide(NumberType<?> leftType, ByteBuffer left, NumberType<?> rightType, ByteBuffer right)
    {
        return decompose(leftType.toBigInteger(left).divide(rightType.toBigInteger(right)));
    }

    public ByteBuffer mod(NumberType<?> leftType, ByteBuffer left, NumberType<?> rightType, ByteBuffer right)
    {
        return decompose(leftType.toBigInteger(left).remainder(rightType.toBigInteger(right)));
    }

    public ByteBuffer negate(ByteBuffer input)
    {
        return decompose(toBigInteger(input).negate());
    }
}
