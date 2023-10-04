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

import java.nio.ByteBuffer;
import java.util.Objects;

import org.apache.commons.lang3.mutable.MutableLong;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.LongSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

public class LongType extends NumberType<Long>
{
    public static final LongType instance = new LongType();

    private static final ByteBuffer MASKED_VALUE = instance.decompose(0L);

    LongType() {super(ComparisonType.CUSTOM);} // singleton

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
        return compareLongs(left, accessorL, right, accessorR);
    }

    public static <VL, VR> int compareLongs(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR)
    {
        if (accessorL.isEmpty(left)|| accessorR.isEmpty(right))
            return Boolean.compare(accessorR.isEmpty(right), accessorL.isEmpty(left));

        int diff = accessorL.getByte(left, 0) - accessorR.getByte(right, 0);
        if (diff != 0)
            return diff;

        return ValueAccessor.compare(left, accessorL, right, accessorR);
    }

    @Override
    public <V> ByteSource asComparableBytes(ValueAccessor<V> accessor, V data, ByteComparable.Version version)
    {
        if (accessor.isEmpty(data))
            return null;
        if (version == ByteComparable.Version.LEGACY)
            return ByteSource.signedFixedLengthNumber(accessor, data);
        else
            return ByteSource.variableLengthInteger(accessor.getLong(data, 0));
    }

    @Override
    public <V> V fromComparableBytes(ValueAccessor<V> accessor, ByteSource.Peekable comparableBytes, ByteComparable.Version version)
    {
        if (comparableBytes == null)
            return accessor.empty();
        if (version == ByteComparable.Version.LEGACY)
            return ByteSourceInverse.getSignedFixedLength(accessor, comparableBytes, 8);
        else
            return accessor.valueOf(ByteSourceInverse.getVariableLengthInteger(comparableBytes));
    }

    public ByteBuffer fromString(String source) throws MarshalException
    {
        // Return an empty ByteBuffer for an empty string.
        if (source.isEmpty())
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        long longType;

        try
        {
            longType = Long.parseLong(source);
        }
        catch (Exception e)
        {
            throw new MarshalException(String.format("Unable to make long from '%s'", source), e);
        }

        return decompose(longType);
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        try
        {
            if (parsed instanceof String)
                return new Constants.Value(fromString((String) parsed));

            Number parsedNumber = (Number) parsed;
            if (!(parsedNumber instanceof Integer || parsedNumber instanceof Long))
                throw new MarshalException(String.format("Expected a bigint value, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));

            return new Constants.Value(getSerializer().serialize(parsedNumber.longValue()));
        }
        catch (ClassCastException exc)
        {
            throw new MarshalException(String.format(
                    "Expected a bigint value, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));
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
        return this == otherType || otherType == DateType.instance || otherType == TimestampType.instance;
    }

    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.BIGINT;
    }

    public TypeSerializer<Long> getSerializer()
    {
        return LongSerializer.instance;
    }

    @Override
    public ArgumentDeserializer getArgumentDeserializer()
    {
        return new NumberArgumentDeserializer<MutableLong>(new MutableLong())
        {
            @Override
            protected void setMutableValue(MutableLong mutable, ByteBuffer buffer)
            {
                mutable.setValue(ByteBufferUtil.toLong(buffer));
            }
        };
    }

    @Override
    public int valueLengthIfFixed()
    {
        return 8;
    }

    @Override
    public ByteBuffer add(Number left, Number right)
    {
        return ByteBufferUtil.bytes(left.longValue() + right.longValue());
    }

    @Override
    public ByteBuffer substract(Number left, Number right)
    {
        return ByteBufferUtil.bytes(left.longValue() - right.longValue());
    }

    @Override
    public ByteBuffer multiply(Number left, Number right)
    {
        return ByteBufferUtil.bytes(left.longValue() * right.longValue());
    }

    @Override
    public ByteBuffer divide(Number left, Number right)
    {
        return ByteBufferUtil.bytes(left.longValue() / right.longValue());
    }

    @Override
    public ByteBuffer mod(Number left, Number right)
    {
        return ByteBufferUtil.bytes(left.longValue() % right.longValue());
    }

    @Override
    public ByteBuffer negate(Number input)
    {
        return ByteBufferUtil.bytes(-input.longValue());
    }

    @Override
    public ByteBuffer abs(Number input)
    {
        return ByteBufferUtil.bytes(Math.abs(input.longValue()));
    }

    @Override
    public ByteBuffer exp(Number input)
    {
        return ByteBufferUtil.bytes((long) Math.exp(input.longValue()));
    }

    @Override
    public ByteBuffer log(Number input)
    {
        return ByteBufferUtil.bytes((long) Math.log(input.longValue()));
    }

    @Override
    public ByteBuffer log10(Number input)
    {
        return ByteBufferUtil.bytes((long) Math.log10(input.longValue()));
    }

    @Override
    public ByteBuffer round(Number input)
    {
        return decompose(input.longValue());
    }

    @Override
    public ByteBuffer getMaskedValue()
    {
        return MASKED_VALUE;
    }
}
