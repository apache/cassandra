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

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.serializers.Int32Serializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;

public class Int32Type extends NumberType<Integer>
{
    public static final Int32Type instance = new Int32Type();

    Int32Type()
    {
        super(ComparisonType.CUSTOM);
    } // singleton

    public boolean isEmptyValueMeaningless()
    {
        return true;
    }

    public <VL, VR> int compareCustom(VL left, ValueAccessor<VL> accessorL, VR right, ValueAccessor<VR> accessorR)
    {
        if (accessorL.isEmpty(left) || accessorR.isEmpty(right))
            return Boolean.compare(accessorR.isEmpty(right), accessorL.isEmpty(left));

        int diff = accessorL.getByte(left, 0) - accessorR.getByte(right, 0);
        if (diff != 0)
            return diff;

        return ValueAccessor.compare(left, accessorL, right, accessorR);
    }

    public ByteBuffer fromString(String source) throws MarshalException
    {
        // Return an empty ByteBuffer for an empty string.
        if (source.isEmpty())
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        int int32Type;

        try
        {
            int32Type = Integer.parseInt(source);
        }
        catch (Exception e)
        {
            throw new MarshalException(String.format("Unable to make int from '%s'", source), e);
        }

        return decompose(int32Type);
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        try
        {
            if (parsed instanceof String)
                return new Constants.Value(fromString((String) parsed));

            Number parsedNumber = (Number) parsed;
            if (!(parsedNumber instanceof Integer))
                throw new MarshalException(String.format("Expected an int value, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));

            return new Constants.Value(getSerializer().serialize(parsedNumber.intValue()));
        }
        catch (ClassCastException exc)
        {
            throw new MarshalException(String.format(
                    "Expected an int value, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));
        }
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        return Objects.toString(getSerializer().deserialize(buffer), "\"\"");
    }

    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.INT;
    }

    public TypeSerializer<Integer> getSerializer()
    {
        return Int32Serializer.instance;
    }

    @Override
    public int valueLengthIfFixed()
    {
        return 4;
    }

    @Override
    protected int toInt(ByteBuffer value)
    {
        return ByteBufferUtil.toInt(value);
    }

    @Override
    protected float toFloat(ByteBuffer value)
    {
        return toInt(value);
    }

    public ByteBuffer add(NumberType<?> leftType, ByteBuffer left, NumberType<?> rightType, ByteBuffer right)
    {
        return ByteBufferUtil.bytes(leftType.toInt(left) + rightType.toInt(right));
    }

    public ByteBuffer substract(NumberType<?> leftType, ByteBuffer left, NumberType<?> rightType, ByteBuffer right)
    {
        return ByteBufferUtil.bytes(leftType.toInt(left) - rightType.toInt(right));
    }

    public ByteBuffer multiply(NumberType<?> leftType, ByteBuffer left, NumberType<?> rightType, ByteBuffer right)
    {
        return ByteBufferUtil.bytes(leftType.toInt(left) * rightType.toInt(right));
    }

    public ByteBuffer divide(NumberType<?> leftType, ByteBuffer left, NumberType<?> rightType, ByteBuffer right)
    {
        return ByteBufferUtil.bytes(leftType.toInt(left) / rightType.toInt(right));
    }

    public ByteBuffer mod(NumberType<?> leftType, ByteBuffer left, NumberType<?> rightType, ByteBuffer right)
    {
        return ByteBufferUtil.bytes(leftType.toInt(left) % rightType.toInt(right));
    }

    public ByteBuffer negate(ByteBuffer input)
    {
        return ByteBufferUtil.bytes(-toInt(input));
    }
}
