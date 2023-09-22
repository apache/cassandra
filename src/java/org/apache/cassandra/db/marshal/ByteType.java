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

import org.apache.commons.lang3.mutable.MutableByte;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.serializers.ByteSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteComparable.Version;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

public class ByteType extends NumberType<Byte>
{
    public static final ByteType instance = new ByteType();

    private static final ByteBuffer MASKED_VALUE = instance.decompose((byte) 0);

    ByteType()
    {
        super(ComparisonType.CUSTOM);
    } // singleton

    public <VL, VR> int compareCustom(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR)
    {
        return accessorL.getByte(left, 0) - accessorR.getByte(right, 0);
    }

    @Override
    public <V> ByteSource asComparableBytes(ValueAccessor<V> accessor, V data, Version version)
    {
        // This type does not allow non-present values, but we do just to avoid future complexity.
        return ByteSource.optionalSignedFixedLengthNumber(accessor, data);
    }

    @Override
    public <V> V fromComparableBytes(ValueAccessor<V> accessor, ByteSource.Peekable comparableBytes, ByteComparable.Version version)
    {
        return ByteSourceInverse.getOptionalSignedFixedLength(accessor, comparableBytes, 1);
    }

    public ByteBuffer fromString(String source) throws MarshalException
    {
        // Return an empty ByteBuffer for an empty string.
        if (source.isEmpty())
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        byte b;

        try
        {
            b = Byte.parseByte(source);
        }
        catch (Exception e)
        {
            throw new MarshalException(String.format("Unable to make byte from '%s'", source), e);
        }

        return decompose(b);
    }

    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        if (parsed instanceof String || parsed instanceof Number)
            return new Constants.Value(fromString(String.valueOf(parsed)));

        throw new MarshalException(String.format(
                "Expected a byte value, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        return getSerializer().deserialize(buffer).toString();
    }

    @Override
    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.TINYINT;
    }

    @Override
    public TypeSerializer<Byte> getSerializer()
    {
        return ByteSerializer.instance;
    }

    @Override
    public ArgumentDeserializer getArgumentDeserializer()
    {
        return new NumberArgumentDeserializer<MutableByte>(new MutableByte())
        {
            @Override
            protected void setMutableValue(MutableByte mutable, ByteBuffer buffer)
            {
                mutable.setValue(ByteBufferUtil.toByte(buffer));
            }
        };
    }

    @Override
    public ByteBuffer add(Number left, Number right)
    {
        return ByteBufferUtil.bytes((byte) (left.byteValue() + right.byteValue()));
    }

    @Override
    public ByteBuffer substract(Number left, Number right)
    {
        return ByteBufferUtil.bytes((byte) (left.byteValue() - right.byteValue()));
    }

    @Override
    public ByteBuffer multiply(Number left, Number right)
    {
        return ByteBufferUtil.bytes((byte) (left.byteValue() * right.byteValue()));
    }

    @Override
    public ByteBuffer divide(Number left, Number right)
    {
        return ByteBufferUtil.bytes((byte) (left.byteValue() / right.byteValue()));
    }

    @Override
    public ByteBuffer mod(Number left, Number right)
    {
        return ByteBufferUtil.bytes((byte) (left.byteValue() % right.byteValue()));
    }

    public ByteBuffer negate(Number input)
    {
        return ByteBufferUtil.bytes((byte) -input.byteValue());
    }

    @Override
    public ByteBuffer abs(Number input)
    {
        return ByteBufferUtil.bytes((byte) Math.abs(input.byteValue()));
    }

    @Override
    public ByteBuffer exp(Number input)
    {
        return ByteBufferUtil.bytes((byte) Math.exp(input.byteValue()));
    }

    @Override
    public ByteBuffer log(Number input)
    {
        return ByteBufferUtil.bytes((byte) Math.log(input.byteValue()));
    }

    @Override
    public ByteBuffer log10(Number input)
    {
        return ByteBufferUtil.bytes((byte) Math.log10(input.byteValue()));
    }

    @Override
    public ByteBuffer round(Number input)
    {
        return decompose(input.byteValue());
    }

    @Override
    public ByteBuffer getMaskedValue()
    {
        return MASKED_VALUE;
    }
}
