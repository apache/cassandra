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

// We don't want to expose this one: it's less useful externally and it's a bit ugly to expose
// anyway (but it's convenient).
abstract class AbstractData<T extends SettableData<T>> extends AbstractGettableData
implements SettableData<T>
{

    private final T wrapped;
    final ByteBuffer[] values;

    // Ugly, we could probably clean that: it is currently needed however because we sometimes
    // want wrapped to be 'this' (UDTValue), and sometimes some other object (in BoundStatement).
    @SuppressWarnings("unchecked")
    protected AbstractData(ProtocolVersion protocolVersion, int size)
    {
        super(protocolVersion);
        this.wrapped = (T) this;
        this.values = new ByteBuffer[size];
    }

    protected AbstractData(ProtocolVersion protocolVersion, T wrapped, int size)
    {
        this(protocolVersion, wrapped, new ByteBuffer[size]);
    }

    protected AbstractData(ProtocolVersion protocolVersion, T wrapped, ByteBuffer[] values)
    {
        super(protocolVersion);
        this.wrapped = wrapped;
        this.values = values;
    }

    protected abstract int[] getAllIndexesOf(String name);

    private T setValue(int i, ByteBuffer value)
    {
        values[i] = value;
        return wrapped;
    }

    @Override
    protected ByteBuffer getValue(int i)
    {
        return values[i];
    }

    @Override
    protected int getIndexOf(String name)
    {
        return getAllIndexesOf(name)[0];
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
    public T setBool(String name, boolean v)
    {
        for (int i : getAllIndexesOf(name))
        {
            setBool(i, v);
        }
        return wrapped;
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
    public T setByte(String name, byte v)
    {
        for (int i : getAllIndexesOf(name))
        {
            setByte(i, v);
        }
        return wrapped;
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
    public T setShort(String name, short v)
    {
        for (int i : getAllIndexesOf(name))
        {
            setShort(i, v);
        }
        return wrapped;
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
    public T setInt(String name, int v)
    {
        for (int i : getAllIndexesOf(name))
        {
            setInt(i, v);
        }
        return wrapped;
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
    public T setLong(String name, long v)
    {
        for (int i : getAllIndexesOf(name))
        {
            setLong(i, v);
        }
        return wrapped;
    }

    @Override
    public T setTimestamp(int i, Date v)
    {
        return setValue(i, codecFor(i, Date.class).serialize(v, protocolVersion));
    }

    @Override
    public T setTimestamp(String name, Date v)
    {
        for (int i : getAllIndexesOf(name))
        {
            setTimestamp(i, v);
        }
        return wrapped;
    }

    @Override
    public T setDate(int i, LocalDate v)
    {
        return setValue(i, codecFor(i, LocalDate.class).serialize(v, protocolVersion));
    }

    @Override
    public T setDate(String name, LocalDate v)
    {
        for (int i : getAllIndexesOf(name))
        {
            setDate(i, v);
        }
        return wrapped;
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
    public T setTime(String name, long v)
    {
        for (int i : getAllIndexesOf(name))
        {
            setTime(i, v);
        }
        return wrapped;
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
    public T setFloat(String name, float v)
    {
        for (int i : getAllIndexesOf(name))
        {
            setFloat(i, v);
        }
        return wrapped;
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
    public T setDouble(String name, double v)
    {
        for (int i : getAllIndexesOf(name))
        {
            setDouble(i, v);
        }
        return wrapped;
    }

    @Override
    public T setString(int i, String v)
    {
        return setValue(i, codecFor(i, String.class).serialize(v, protocolVersion));
    }

    @Override
    public T setString(String name, String v)
    {
        for (int i : getAllIndexesOf(name))
        {
            setString(i, v);
        }
        return wrapped;
    }

    @Override
    public T setBytes(int i, ByteBuffer v)
    {
        return setValue(i, codecFor(i, ByteBuffer.class).serialize(v, protocolVersion));
    }

    @Override
    public T setBytes(String name, ByteBuffer v)
    {
        for (int i : getAllIndexesOf(name))
        {
            setBytes(i, v);
        }
        return wrapped;
    }

    @Override
    public T setBytesUnsafe(int i, ByteBuffer v)
    {
        return setValue(i, v == null ? null : v.duplicate());
    }

    @Override
    public T setBytesUnsafe(String name, ByteBuffer v)
    {
        ByteBuffer value = v == null ? null : v.duplicate();
        for (int i : getAllIndexesOf(name))
        {
            setValue(i, value);
        }
        return wrapped;
    }

    @Override
    public T setVarint(int i, BigInteger v)
    {
        return setValue(i, codecFor(i, BigInteger.class).serialize(v, protocolVersion));
    }

    @Override
    public T setVarint(String name, BigInteger v)
    {
        for (int i : getAllIndexesOf(name))
        {
            setVarint(i, v);
        }
        return wrapped;
    }

    @Override
    public T setDecimal(int i, BigDecimal v)
    {
        return setValue(i, codecFor(i, BigDecimal.class).serialize(v, protocolVersion));
    }

    @Override
    public T setDecimal(String name, BigDecimal v)
    {
        for (int i : getAllIndexesOf(name))
        {
            setDecimal(i, v);
        }
        return wrapped;
    }

    @Override
    public T setUUID(int i, UUID v)
    {
        return setValue(i, codecFor(i, UUID.class).serialize(v, protocolVersion));
    }

    @Override
    public T setUUID(String name, UUID v)
    {
        for (int i : getAllIndexesOf(name))
        {
            setUUID(i, v);
        }
        return wrapped;
    }

    @Override
    public T setInet(int i, InetAddress v)
    {
        return setValue(i, codecFor(i, InetAddress.class).serialize(v, protocolVersion));
    }

    @Override
    public T setInet(String name, InetAddress v)
    {
        for (int i : getAllIndexesOf(name))
        {
            setInet(i, v);
        }
        return wrapped;
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
    public <E> T setList(String name, List<E> v)
    {
        for (int i : getAllIndexesOf(name))
        {
            setList(i, v);
        }
        return wrapped;
    }

    @Override
    public <E> T setList(String name, List<E> v, Class<E> elementsClass)
    {
        for (int i : getAllIndexesOf(name))
        {
            setList(i, v, elementsClass);
        }
        return wrapped;
    }

    @Override
    public <E> T setList(String name, List<E> v, TypeToken<E> elementsType)
    {
        for (int i : getAllIndexesOf(name))
        {
            setList(i, v, elementsType);
        }
        return wrapped;
    }

    @SuppressWarnings("unchecked")
    @Override
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
    public <K, V> T setMap(String name, Map<K, V> v)
    {
        for (int i : getAllIndexesOf(name))
        {
            setMap(i, v);
        }
        return wrapped;
    }

    @Override
    public <K, V> T setMap(String name, Map<K, V> v, Class<K> keysClass, Class<V> valuesClass)
    {
        for (int i : getAllIndexesOf(name))
        {
            setMap(i, v, keysClass, valuesClass);
        }
        return wrapped;
    }

    @Override
    public <K, V> T setMap(String name, Map<K, V> v, TypeToken<K> keysType, TypeToken<V> valuesType)
    {
        for (int i : getAllIndexesOf(name))
        {
            setMap(i, v, keysType, valuesType);
        }
        return wrapped;
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
    public <E> T setSet(String name, Set<E> v)
    {
        for (int i : getAllIndexesOf(name))
        {
            setSet(i, v);
        }
        return wrapped;
    }

    @Override
    public <E> T setSet(String name, Set<E> v, Class<E> elementsClass)
    {
        for (int i : getAllIndexesOf(name))
        {
            setSet(i, v, elementsClass);
        }
        return wrapped;
    }

    @Override
    public <E> T setSet(String name, Set<E> v, TypeToken<E> elementsType)
    {
        for (int i : getAllIndexesOf(name))
        {
            setSet(i, v, elementsType);
        }
        return wrapped;
    }

    @Override
    public <E> T setVector(int i, List<E> v)
    {
        return setValue(i, codecFor(i).serialize(v, protocolVersion));
    }

    @Override
    public <E> T setVector(String name, List<E> v)
    {
        for (int i : getAllIndexesOf(name))
        {
            setVector(i, v);
        }
        return wrapped;
    }

    @Override
    public T setUDTValue(int i, UDTValue v)
    {
        return setValue(i, codecFor(i, UDTValue.class).serialize(v, protocolVersion));
    }

    @Override
    public T setUDTValue(String name, UDTValue v)
    {
        for (int i : getAllIndexesOf(name))
        {
            setUDTValue(i, v);
        }
        return wrapped;
    }

    @Override
    public T setTupleValue(int i, TupleValue v)
    {
        return setValue(i, codecFor(i, TupleValue.class).serialize(v, protocolVersion));
    }

    @Override
    public T setTupleValue(String name, TupleValue v)
    {
        for (int i : getAllIndexesOf(name))
        {
            setTupleValue(i, v);
        }
        return wrapped;
    }

    @Override
    public <V> T set(int i, V v, Class<V> targetClass)
    {
        return set(i, v, codecFor(i, targetClass));
    }

    @Override
    public <V> T set(String name, V v, Class<V> targetClass)
    {
        for (int i : getAllIndexesOf(name))
        {
            set(i, v, targetClass);
        }
        return wrapped;
    }

    @Override
    public <V> T set(int i, V v, TypeToken<V> targetType)
    {
        return set(i, v, codecFor(i, targetType));
    }

    @Override
    public <V> T set(String name, V v, TypeToken<V> targetType)
    {
        for (int i : getAllIndexesOf(name))
        {
            set(i, v, targetType);
        }
        return wrapped;
    }

    @Override
    public <V> T set(int i, V v, TypeCodec<V> codec)
    {
        checkType(i, codec.getCqlType().getName());
        return setValue(i, codec.serialize(v, protocolVersion));
    }

    @Override
    public <V> T set(String name, V v, TypeCodec<V> codec)
    {
        for (int i : getAllIndexesOf(name))
        {
            set(i, v, codec);
        }
        return wrapped;
    }

    @Override
    public T setToNull(int i)
    {
        return setValue(i, null);
    }

    @Override
    public T setToNull(String name)
    {
        for (int i : getAllIndexesOf(name))
        {
            setToNull(i);
        }
        return wrapped;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof AbstractData)) return false;

        AbstractData<?> that = (AbstractData<?>) o;
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
