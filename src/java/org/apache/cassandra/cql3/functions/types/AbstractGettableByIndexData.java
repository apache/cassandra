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
import org.apache.cassandra.cql3.functions.types.exceptions.InvalidTypeException;

abstract class AbstractGettableByIndexData implements GettableByIndexData
{

    protected final ProtocolVersion protocolVersion;

    AbstractGettableByIndexData(ProtocolVersion protocolVersion)
    {
        this.protocolVersion = protocolVersion;
    }

    /**
     * Returns the type for the value at index {@code i}.
     *
     * @param i the index of the type to fetch.
     * @return the type of the value at index {@code i}.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index.
     */
    protected abstract DataType getType(int i);

    /**
     * Returns the name corresponding to the value at index {@code i}.
     *
     * @param i the index of the name to fetch.
     * @return the name corresponding to the value at index {@code i}.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index.
     */
    protected abstract String getName(int i);

    /**
     * Returns the value at index {@code i}.
     *
     * @param i the index to fetch.
     * @return the value at index {@code i}.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index.
     */
    protected abstract ByteBuffer getValue(int i);

    protected abstract CodecRegistry getCodecRegistry();

    protected <T> TypeCodec<T> codecFor(int i)
    {
        return getCodecRegistry().codecFor(getType(i));
    }

    protected <T> TypeCodec<T> codecFor(int i, Class<T> javaClass)
    {
        return getCodecRegistry().codecFor(getType(i), javaClass);
    }

    protected <T> TypeCodec<T> codecFor(int i, TypeToken<T> javaType)
    {
        return getCodecRegistry().codecFor(getType(i), javaType);
    }

    protected <T> TypeCodec<T> codecFor(int i, T value)
    {
        return getCodecRegistry().codecFor(getType(i), value);
    }

    void checkType(int i, DataType.Name actual)
    {
        DataType.Name expected = getType(i).getName();
        if (!actual.isCompatibleWith(expected))
            throw new InvalidTypeException(
            String.format("Value %s is of type %s, not %s", getName(i), expected, actual));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isNull(int i)
    {
        return getValue(i) == null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean getBool(int i)
    {
        ByteBuffer value = getValue(i);
        TypeCodec<Boolean> codec = codecFor(i, Boolean.class);
        if (codec instanceof TypeCodec.PrimitiveBooleanCodec)
            return ((TypeCodec.PrimitiveBooleanCodec) codec).deserializeNoBoxing(value, protocolVersion);
        else return codec.deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte getByte(int i)
    {
        ByteBuffer value = getValue(i);
        TypeCodec<Byte> codec = codecFor(i, Byte.class);
        if (codec instanceof TypeCodec.PrimitiveByteCodec)
            return ((TypeCodec.PrimitiveByteCodec) codec).deserializeNoBoxing(value, protocolVersion);
        else return codec.deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public short getShort(int i)
    {
        ByteBuffer value = getValue(i);
        TypeCodec<Short> codec = codecFor(i, Short.class);
        if (codec instanceof TypeCodec.PrimitiveShortCodec)
            return ((TypeCodec.PrimitiveShortCodec) codec).deserializeNoBoxing(value, protocolVersion);
        else return codec.deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getInt(int i)
    {
        ByteBuffer value = getValue(i);
        TypeCodec<Integer> codec = codecFor(i, Integer.class);
        if (codec instanceof TypeCodec.PrimitiveIntCodec)
            return ((TypeCodec.PrimitiveIntCodec) codec).deserializeNoBoxing(value, protocolVersion);
        else return codec.deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getLong(int i)
    {
        ByteBuffer value = getValue(i);
        TypeCodec<Long> codec = codecFor(i, Long.class);
        if (codec instanceof TypeCodec.PrimitiveLongCodec)
            return ((TypeCodec.PrimitiveLongCodec) codec).deserializeNoBoxing(value, protocolVersion);
        else return codec.deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date getTimestamp(int i)
    {
        ByteBuffer value = getValue(i);
        return codecFor(i, Date.class).deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LocalDate getDate(int i)
    {
        ByteBuffer value = getValue(i);
        return codecFor(i, LocalDate.class).deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getTime(int i)
    {
        ByteBuffer value = getValue(i);
        TypeCodec<Long> codec = codecFor(i, Long.class);
        if (codec instanceof TypeCodec.PrimitiveLongCodec)
            return ((TypeCodec.PrimitiveLongCodec) codec).deserializeNoBoxing(value, protocolVersion);
        else return codec.deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float getFloat(int i)
    {
        ByteBuffer value = getValue(i);
        TypeCodec<Float> codec = codecFor(i, Float.class);
        if (codec instanceof TypeCodec.PrimitiveFloatCodec)
            return ((TypeCodec.PrimitiveFloatCodec) codec).deserializeNoBoxing(value, protocolVersion);
        else return codec.deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getDouble(int i)
    {
        ByteBuffer value = getValue(i);
        TypeCodec<Double> codec = codecFor(i, Double.class);
        if (codec instanceof TypeCodec.PrimitiveDoubleCodec)
            return ((TypeCodec.PrimitiveDoubleCodec) codec).deserializeNoBoxing(value, protocolVersion);
        else return codec.deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ByteBuffer getBytesUnsafe(int i)
    {
        ByteBuffer value = getValue(i);
        if (value == null) return null;
        return value.duplicate();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ByteBuffer getBytes(int i)
    {
        ByteBuffer value = getValue(i);
        return codecFor(i, ByteBuffer.class).deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getString(int i)
    {
        ByteBuffer value = getValue(i);
        return codecFor(i, String.class).deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BigInteger getVarint(int i)
    {
        ByteBuffer value = getValue(i);
        return codecFor(i, BigInteger.class).deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BigDecimal getDecimal(int i)
    {
        ByteBuffer value = getValue(i);
        return codecFor(i, BigDecimal.class).deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UUID getUUID(int i)
    {
        ByteBuffer value = getValue(i);
        return codecFor(i, UUID.class).deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InetAddress getInet(int i)
    {
        ByteBuffer value = getValue(i);
        return codecFor(i, InetAddress.class).deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> List<T> getList(int i, Class<T> elementsClass)
    {
        return getList(i, TypeToken.of(elementsClass));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> List<T> getList(int i, TypeToken<T> elementsType)
    {
        ByteBuffer value = getValue(i);
        TypeToken<List<T>> javaType = TypeTokens.listOf(elementsType);
        return codecFor(i, javaType).deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> Set<T> getSet(int i, Class<T> elementsClass)
    {
        return getSet(i, TypeToken.of(elementsClass));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> Set<T> getSet(int i, TypeToken<T> elementsType)
    {
        ByteBuffer value = getValue(i);
        TypeToken<Set<T>> javaType = TypeTokens.setOf(elementsType);
        return codecFor(i, javaType).deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <K, V> Map<K, V> getMap(int i, Class<K> keysClass, Class<V> valuesClass)
    {
        return getMap(i, TypeToken.of(keysClass), TypeToken.of(valuesClass));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <K, V> Map<K, V> getMap(int i, TypeToken<K> keysType, TypeToken<V> valuesType)
    {
        ByteBuffer value = getValue(i);
        TypeToken<Map<K, V>> javaType = TypeTokens.mapOf(keysType, valuesType);
        return codecFor(i, javaType).deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> List<T> getVector(int i, Class<T> elementsClass)
    {
        return getList(i, TypeToken.of(elementsClass));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> List<T> getVector(int i, TypeToken<T> elementsType)
    {
        ByteBuffer value = getValue(i);
        TypeToken<List<T>> javaType = TypeTokens.listOf(elementsType);
        return codecFor(i, javaType).deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public UDTValue getUDTValue(int i)
    {
        ByteBuffer value = getValue(i);
        return codecFor(i, UDTValue.class).deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public TupleValue getTupleValue(int i)
    {
        ByteBuffer value = getValue(i);
        return codecFor(i, TupleValue.class).deserialize(value, protocolVersion);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getObject(int i)
    {
        return get(i, codecFor(i));
    }

    @Override
    public <T> T get(int i, Class<T> targetClass)
    {
        return get(i, codecFor(i, targetClass));
    }

    @Override
    public <T> T get(int i, TypeToken<T> targetType)
    {
        return get(i, codecFor(i, targetType));
    }

    @Override
    public <T> T get(int i, TypeCodec<T> codec)
    {
        checkType(i, codec.getCqlType().getName());
        ByteBuffer value = getValue(i);
        return codec.deserialize(value, protocolVersion);
    }
}
