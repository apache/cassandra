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

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.cql3.functions.ArgumentDeserializer;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.serializers.CounterSerializer;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;

public class CounterColumnType extends NumberType<Long>
{
    public static final CounterColumnType instance = new CounterColumnType();

    private static final ByteBuffer MASKED_VALUE = instance.decompose(0L);

    CounterColumnType() {super(ComparisonType.NOT_COMPARABLE);} // singleton

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

    public boolean isCounter()
    {
        return true;
    }

    public <V> Long compose(V value, ValueAccessor<V> accessor)
    {
        return CounterContext.instance().total(value, accessor);
    }

    @Override
    public ByteBuffer decompose(Long value)
    {
        return ByteBufferUtil.bytes(value);
    }

    @Override
    public <V> void validateCellValue(V cellValue, ValueAccessor<V> accessor) throws MarshalException
    {
        CounterContext.instance().validateContext(cellValue, accessor);
    }

    public <V> String getString(V value, ValueAccessor<V> accessor)
    {
        return accessor.toHex(value);
    }

    public ByteBuffer fromString(String source)
    {
        return ByteBufferUtil.hexToBytes(source);
    }

    @Override
    public Term fromJSONObject(Object parsed)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toJSONString(ByteBuffer buffer, ProtocolVersion protocolVersion)
    {
        return CounterSerializer.instance.deserialize(buffer).toString();
    }

    public CQL3Type asCQL3Type()
    {
        return CQL3Type.Native.COUNTER;
    }

    public TypeSerializer<Long> getSerializer()
    {
        return CounterSerializer.instance;
    }

    @Override
    public ArgumentDeserializer getArgumentDeserializer()
    {
        return LongType.instance.getArgumentDeserializer();
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
