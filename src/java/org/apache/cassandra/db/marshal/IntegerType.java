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

import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.IntegerSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;

public final class IntegerType extends AbstractType<BigInteger>
{
    public static final IntegerType instance = new IntegerType();

    private static int findMostSignificantByte(ByteBuffer bytes)
    {
        int len = bytes.remaining() - 1;
        int i = 0;
        for (; i < len; i++)
        {
            byte b0 = bytes.get(bytes.position() + i);
            if (b0 != 0 && b0 != -1)
                break;
            byte b1 = bytes.get(bytes.position() + i + 1);
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

    public int compareCustom(ByteBuffer lhs, ByteBuffer rhs)
    {
        return IntegerType.compareIntegers(lhs, rhs);
    }

    public static int compareIntegers(ByteBuffer lhs, ByteBuffer rhs)
    {
        int lhsLen = lhs.remaining();
        int rhsLen = rhs.remaining();

        if (lhsLen == 0)
            return rhsLen == 0 ? 0 : -1;
        if (rhsLen == 0)
            return 1;

        int lhsMsbIdx = findMostSignificantByte(lhs);
        int rhsMsbIdx = findMostSignificantByte(rhs);

        //diffs contain number of "meaningful" bytes (i.e. ignore padding)
        int lhsLenDiff = lhsLen - lhsMsbIdx;
        int rhsLenDiff = rhsLen - rhsMsbIdx;

        byte lhsMsb = lhs.get(lhs.position() + lhsMsbIdx);
        byte rhsMsb = rhs.get(rhs.position() + rhsMsbIdx);

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
            lhsMsb = lhs.get(lhs.position() + lhsMsbIdx++);
            rhsMsb = rhs.get(rhs.position() + rhsMsbIdx++);

            if (lhsMsb != rhsMsb)
                return (lhsMsb & 0xFF) - (rhsMsb & 0xFF);
        }

        return 0;
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
        return getSerializer().deserialize(buffer).toString();
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
}
