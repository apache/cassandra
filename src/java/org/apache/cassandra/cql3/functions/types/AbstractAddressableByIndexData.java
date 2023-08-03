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
package org.apache.cassandra.cql3.functions.types;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.reflect.TypeToken;

import org.apache.cassandra.transport.ProtocolVersion;

abstract class AbstractAddressableByIndexData<T extends SettableByIndexData<T>>
extends AbstractGettableByIndexData implements SettableByIndexData<T>
{

    final ByteBuffer[] values;

    AbstractAddressableByIndexData(ProtocolVersion protocolVersion, int size)
    {
        super(protocolVersion);
        this.values = new ByteBuffer[size];
    }

    @SuppressWarnings("unchecked")
    T setValue(int i, ByteBuffer value)
    {
        values[i] = value;
        return (T) this;
    }

    @Override
    protected ByteBuffer getValue(int i)
    {
        return values[i];
    }

    @Override
    public T setBool(int i, boolean v)
    {
        TypeCodec<Boolean> codec = codecFor(i, Boolean.class);
        ByteBuffer bb;
        if (codec instanceof TypeCodec.PrimitiveBooleanCodec)
            bb = ((TypeCodec.PrimitiveBooleanCodec) codec).serializeNoBoxing(v, protocolVersion);
        else bb = codec.serialize(v, protocolVersion);
        return setValue(i, bb);
    }

    @Override
    public T setByte(int i, byte v)
    {
        TypeCodec<Byte> codec = codecFor(i, Byte.class);
        ByteBuffer bb;
        if (codec instanceof TypeCodec.PrimitiveByteCodec)
            bb = ((TypeCodec.PrimitiveByteCodec) codec).serializeNoBoxing(v, protocolVersion);
        else bb = codec.serialize(v, protocolVersion);
        return setValue(i, bb);
    }

    @Override
    public T setShort(int i, short v)
    {
        TypeCodec<Short> codec = codecFor(i, Short.class);
        ByteBuffer bb;
        if (codec instanceof TypeCodec.PrimitiveShortCodec)
            bb = ((TypeCodec.PrimitiveShortCodec) codec).serializeNoBoxing(v, protocolVersion);
        else bb = codec.serialize(v, protocolVersion);
        return setValue(i, bb);
    }

    @Override
    public T setInt(int i, int v)
    {
        TypeCodec<Integer> codec = codecFor(i, Integer.class);
        ByteBuffer bb;
        if (codec instanceof TypeCodec.PrimitiveIntCodec)
            bb = ((TypeCodec.PrimitiveIntCodec) codec).serializeNoBoxing(v, protocolVersion);
        else bb = codec.serialize(v, protocolVersion);
        return setValue(i, bb);
    }

    @Override
    public T setLong(int i, long v)
    {
        TypeCodec<Long> codec = codecFor(i, Long.class);
        ByteBuffer bb;
        if (codec instanceof TypeCodec.PrimitiveLongCodec)
            bb = ((TypeCodec.PrimitiveLongCodec) codec).serializeNoBoxing(v, protocolVersion);
        else bb = codec.serialize(v, protocolVersion);
        return setValue(i, bb);
    }

    @Override
    public T setTimestamp(int i, Date v)
    {
        return setValue(i, codecFor(i, Date.class).serialize(v, protocolVersion));
    }

    @Override
    public T setDate(int i, LocalDate v)
    {
        return setValue(i, codecFor(i, LocalDate.class).serialize(v, protocolVersion));
    }

    @Override
    public T setTime(int i, long v)
    {
        TypeCodec<Long> codec = codecFor(i, Long.class);
        ByteBuffer bb;
        if (codec instanceof TypeCodec.PrimitiveLongCodec)
            bb = ((TypeCodec.PrimitiveLongCodec) codec).serializeNoBoxing(v, protocolVersion);
        else bb = codec.serialize(v, protocolVersion);
        return setValue(i, bb);
    }

    @Override
    public T setFloat(int i, float v)
    {
        TypeCodec<Float> codec = codecFor(i, Float.class);
        ByteBuffer bb;
        if (codec instanceof TypeCodec.PrimitiveFloatCodec)
            bb = ((TypeCodec.PrimitiveFloatCodec) codec).serializeNoBoxing(v, protocolVersion);
        else bb = codec.serialize(v, protocolVersion);
        return setValue(i, bb);
    }

    @Override
    public T setDouble(int i, double v)
    {
        TypeCodec<Double> codec = codecFor(i, Double.class);
        ByteBuffer bb;
        if (codec instanceof TypeCodec.PrimitiveDoubleCodec)
            bb = ((TypeCodec.PrimitiveDoubleCodec) codec).serializeNoBoxing(v, protocolVersion);
        else bb = codec.serialize(v, protocolVersion);
        return setValue(i, bb);
    }

    @Override
    public T setString(int i, String v)
    {
        return setValue(i, codecFor(i, String.class).serialize(v, protocolVersion));
    }

    @Override
    public T setBytes(int i, ByteBuffer v)
    {
        return setValue(i, codecFor(i, ByteBuffer.class).serialize(v, protocolVersion));
    }

    @Override
    public T setBytesUnsafe(int i, ByteBuffer v)
    {
        return setValue(i, v == null ? null : v.duplicate());
    }

    @Override
    public T setVarint(int i, BigInteger v)
    {
        return setValue(i, codecFor(i, BigInteger.class).serialize(v, protocolVersion));
    }

    @Override
    public T setDecimal(int i, BigDecimal v)
    {
        return setValue(i, codecFor(i, BigDecimal.class).serialize(v, protocolVersion));
    }

    @Override
    public T setUUID(int i, UUID v)
    {
        return setValue(i, codecFor(i, UUID.class).serialize(v, protocolVersion));
    }

    @Override
    public T setInet(int i, InetAddress v)
    {
        return setValue(i, codecFor(i, InetAddress.class).serialize(v, protocolVersion));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E> T setList(int i, List<E> v)
    {
        return setValue(i, codecFor(i).serialize(v, protocolVersion));
    }

    @Override
    public <E> T setList(int i, List<E> v, Class<E> elementsClass)
    {
        return setValue(i, codecFor(i, TypeTokens.listOf(elementsClass)).serialize(v, protocolVersion));
    }

    @Override
    public <E> T setList(int i, List<E> v, TypeToken<E> elementsType)
    {
        return setValue(i, codecFor(i, TypeTokens.listOf(elementsType)).serialize(v, protocolVersion));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> T setMap(int i, Map<K, V> v)
    {
        return setValue(i, codecFor(i).serialize(v, protocolVersion));
    }

    @Override
    public <K, V> T setMap(int i, Map<K, V> v, Class<K> keysClass, Class<V> valuesClass)
    {
        return setValue(
        i, codecFor(i, TypeTokens.mapOf(keysClass, valuesClass)).serialize(v, protocolVersion));
    }

    @Override
    public <K, V> T setMap(int i, Map<K, V> v, TypeToken<K> keysType, TypeToken<V> valuesType)
    {
        return setValue(
        i, codecFor(i, TypeTokens.mapOf(keysType, valuesType)).serialize(v, protocolVersion));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E> T setSet(int i, Set<E> v)
    {
        return setValue(i, codecFor(i).serialize(v, protocolVersion));
    }

    @Override
    public <E> T setSet(int i, Set<E> v, Class<E> elementsClass)
    {
        return setValue(i, codecFor(i, TypeTokens.setOf(elementsClass)).serialize(v, protocolVersion));
    }

    @Override
    public <E> T setSet(int i, Set<E> v, TypeToken<E> elementsType)
    {
        return setValue(i, codecFor(i, TypeTokens.setOf(elementsType)).serialize(v, protocolVersion));
    }

    @Override
    public <E> T setVector(int i, List<E> v)
    {
        return setValue(i, codecFor(i).serialize(v, protocolVersion));
    }

    @Override
    public T setUDTValue(int i, UDTValue v)
    {
        return setValue(i, codecFor(i, UDTValue.class).serialize(v, protocolVersion));
    }

    @Override
    public T setTupleValue(int i, TupleValue v)
    {
        return setValue(i, codecFor(i, TupleValue.class).serialize(v, protocolVersion));
    }

    @Override
    public <V> T set(int i, V v, Class<V> targetClass)
    {
        return set(i, v, codecFor(i, targetClass));
    }

    @Override
    public <V> T set(int i, V v, TypeToken<V> targetType)
    {
        return set(i, v, codecFor(i, targetType));
    }

    @Override
    public <V> T set(int i, V v, TypeCodec<V> codec)
    {
        checkType(i, codec.getCqlType().getName());
        return setValue(i, codec.serialize(v, protocolVersion));
    }

    @Override
    public T setToNull(int i)
    {
        return setValue(i, null);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof AbstractAddressableByIndexData)) return false;

        AbstractAddressableByIndexData<?> that = (AbstractAddressableByIndexData<?>) o;
        if (values.length != that.values.length) return false;

        if (this.protocolVersion != that.protocolVersion) return false;

        // Deserializing each value is slightly inefficient, but comparing
        // the bytes could in theory be wrong (for varint for instance, 2 values
        // can have different binary representation but be the same value due to
        // leading zeros). So we don't take any risk.
        for (int i = 0; i < values.length; i++)
        {
            DataType thisType = getType(i);
            DataType thatType = that.getType(i);
            if (!thisType.equals(thatType)) return false;

            Object thisValue = this.codecFor(i).deserialize(this.values[i], this.protocolVersion);
            Object thatValue = that.codecFor(i).deserialize(that.values[i], that.protocolVersion);
            if (!Objects.equals(thisValue, thatValue)) return false;
        }
        return true;
    }

    @Override
    public int hashCode()
    {
        // Same as equals
        int hash = 31;
        for (int i = 0; i < values.length; i++)
            hash +=
            values[i] == null ? 1 : codecFor(i).deserialize(values[i], protocolVersion).hashCode();
        return hash;
    }
}
