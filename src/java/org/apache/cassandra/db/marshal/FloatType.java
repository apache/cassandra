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

import org.apache.commons.lang3.mutable.MutableFloat;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Constants;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.serializers.FloatSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;


public class FloatType extends NumberType<Float>
{
    public static final FloatType instance = new FloatType();

    private static final ByteBuffer MASKED_VALUE = instance.decompose(0f);

    FloatType() {super(ComparisonType.CUSTOM);} // singleton

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

    @Override
    public <V> ByteSource asComparableBytes(ValueAccessor<V> accessor, V data, ByteComparable.Version version)
    {
        return ByteSource.optionalSignedFixedLengthFloat(accessor, data);
    }

    @Override
    public <V> V fromComparableBytes(ValueAccessor<V> accessor, ByteSource.Peekable comparableBytes, ByteComparable.Version version)
    {
        return ByteSourceInverse.getOptionalSignedFixedLengthFloat(accessor, comparableBytes, 4);
    }

    public ByteBuffer fromString(String source) throws MarshalException
    {
      // Return an empty ByteBuffer for an empty string.
      if (source.isEmpty())
          return ByteBufferUtil.EMPTY_BYTE_BUFFER;

      try
      {
          return decompose(Float.parseFloat(source));
      }
      catch (NumberFormatException e1)
      {
          throw new MarshalException(String.format("Unable to make float from '%s'", source), e1);
      }
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        try
        {
            if (parsed instanceof String)
                return new Constants.Value(fromString((String) parsed));
            else
                return new Constants.Value(getSerializer().serialize(((Number) parsed).floatValue()));
        }
        catch (ClassCastException exc)
        {
            throw new MarshalException(String.format(
                    "Expected a float value, but got a %s: %s", parsed.getClass().getSimpleName(), parsed));
        }
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        Float value = getSerializer().deserialize(buffer);
        if (value == null)
            return "\"\"";
        // JSON does not support NaN, Infinity and -Infinity values. Most of the parser convert them into null.
        if (value.isNaN() || value.isInfinite())
            return "null";
        return value.toString();
    }

    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.FLOAT;
    }

    public TypeSerializer<Float> getSerializer()
    {
        return FloatSerializer.instance;
    }

    @Override
    public ArgumentDeserializer getArgumentDeserializer()
    {
        return new NumberArgumentDeserializer<MutableFloat>(new MutableFloat())
        {
            @Override
            protected void setMutableValue(MutableFloat mutable, ByteBuffer buffer)
            {
                mutable.setValue(ByteBufferUtil.toFloat(buffer));
            }
        };
    }

    @Override
    public int valueLengthIfFixed()
    {
        return 4;
    }

    @Override
    public ByteBuffer add(Number left, Number right)
    {
        return ByteBufferUtil.bytes(left.floatValue() + right.floatValue());
    }

    @Override
    public ByteBuffer substract(Number left, Number right)
    {
        return ByteBufferUtil.bytes(left.floatValue() - right.floatValue());
    }

    @Override
    public ByteBuffer multiply(Number left, Number right)
    {
        return ByteBufferUtil.bytes(left.floatValue() * right.floatValue());
    }

    @Override
    public ByteBuffer divide(Number left, Number right)
    {
        return ByteBufferUtil.bytes(left.floatValue() / right.floatValue());
    }

    @Override
    public ByteBuffer mod(Number left, Number right)
    {
        return ByteBufferUtil.bytes(left.floatValue() % right.floatValue());
    }

    @Override
    public ByteBuffer negate(Number input)
    {
        return ByteBufferUtil.bytes(-input.floatValue());
    }

    @Override
    public ByteBuffer abs(Number input)
    {
        return ByteBufferUtil.bytes(Math.abs(input.floatValue()));
    }

    @Override
    public ByteBuffer exp(Number input)
    {
        return ByteBufferUtil.bytes((float) Math.exp(input.floatValue()));
    }

    @Override
    public ByteBuffer log(Number input)
    {
        return ByteBufferUtil.bytes((float) Math.log(input.floatValue()));
    }

    @Override
    public ByteBuffer log10(Number input)
    {
        return ByteBufferUtil.bytes((float) Math.log10(input.floatValue()));
    }

    @Override
    public ByteBuffer round(Number input)
    {
        return ByteBufferUtil.bytes((float) Math.round(input.floatValue()));
    }

    @Override
    public ByteBuffer getMaskedValue()
    {
        return MASKED_VALUE;
    }
}
