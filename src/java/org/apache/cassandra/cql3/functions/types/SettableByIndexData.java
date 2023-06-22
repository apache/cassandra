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

import org.apache.cassandra.cql3.functions.types.exceptions.CodecNotFoundException;
import org.apache.cassandra.cql3.functions.types.exceptions.InvalidTypeException;

/**
 * Collection of (typed) CQL values that can be set by index (starting at zero).
 */
public interface SettableByIndexData<T extends SettableByIndexData<T>>
{

    /**
     * Sets the {@code i}th value to the provided boolean.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (for CQL type {@code boolean}, this will be the built-in codec).
     *
     * @param i the index of the value to set.
     * @param v the value to set. To set the value to NULL, use {@link #setToNull(int)} or {@code
     *          set(i, v, Boolean.class)}
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the value to the
     *                                   underlying CQL type.
     */
    public T setBool(int i, boolean v);

    /**
     * Set the {@code i}th value to the provided byte.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (for CQL type {@code tinyint}, this will be the built-in codec).
     *
     * @param i the index of the value to set.
     * @param v the value to set. To set the value to NULL, use {@link #setToNull(int)} or {@code
     *          set(i, v, Byte.class)}
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the value to the
     *                                   underlying CQL type.
     */
    public T setByte(int i, byte v);

    /**
     * Set the {@code i}th value to the provided short.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (for CQL type {@code smallint}, this will be the built-in codec).
     *
     * @param i the index of the value to set.
     * @param v the value to set. To set the value to NULL, use {@link #setToNull(int)} or {@code
     *          set(i, v, Short.class)}
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the value to the
     *                                   underlying CQL type.
     */
    public T setShort(int i, short v);

    /**
     * Set the {@code i}th value to the provided integer.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (for CQL type {@code int}, this will be the built-in codec).
     *
     * @param i the index of the value to set.
     * @param v the value to set. To set the value to NULL, use {@link #setToNull(int)} or {@code
     *          set(i, v, Integer.class)}
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the value to the
     *                                   underlying CQL type.
     */
    public T setInt(int i, int v);

    /**
     * Sets the {@code i}th value to the provided long.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (for CQL type {@code bigint}, this will be the built-in codec).
     *
     * @param i the index of the value to set.
     * @param v the value to set. To set the value to NULL, use {@link #setToNull(int)} or {@code
     *          set(i, v, Long.class)}
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the value to the
     *                                   underlying CQL type.
     */
    public T setLong(int i, long v);

    /**
     * Set the {@code i}th value to the provided date.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (for CQL type {@code timestamp}, this will be the built-in codec).
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the value to the
     *                                   underlying CQL type.
     */
    public T setTimestamp(int i, Date v);

    /**
     * Set the {@code i}th value to the provided date (without time).
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (for CQL type {@code date}, this will be the built-in codec).
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the value to the
     *                                   underlying CQL type.
     */
    public T setDate(int i, LocalDate v);

    /**
     * Set the {@code i}th value to the provided time as a long in nanoseconds since midnight.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (for CQL type {@code time}, this will be the built-in codec).
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the value to the
     *                                   underlying CQL type.
     */
    public T setTime(int i, long v);

    /**
     * Sets the {@code i}th value to the provided float.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (for CQL type {@code float}, this will be the built-in codec).
     *
     * @param i the index of the value to set.
     * @param v the value to set. To set the value to NULL, use {@link #setToNull(int)} or {@code
     *          set(i, v, Float.class)}
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the value to the
     *                                   underlying CQL type.
     */
    public T setFloat(int i, float v);

    /**
     * Sets the {@code i}th value to the provided double.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (for CQL type {@code double}, this will be the built-in codec).
     *
     * @param i the index of the value to set.
     * @param v the value to set. To set the value to NULL, use {@link #setToNull(int)} or {@code
     *          set(i, v, Double.class)}.
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the value to the
     *                                   underlying CQL type.
     */
    public T setDouble(int i, double v);

    /**
     * Sets the {@code i}th value to the provided string.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (for CQL types {@code text}, {@code varchar} and {@code ascii}, this will
     * be the built-in codec).
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the value to the
     *                                   underlying CQL type.
     */
    public T setString(int i, String v);

    /**
     * Sets the {@code i}th value to the provided byte buffer.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (for CQL type {@code blob}, this will be the built-in codec).
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the value to the
     *                                   underlying CQL type.
     */
    public T setBytes(int i, ByteBuffer v);

    /**
     * Sets the {@code i}th value to the provided byte buffer.
     *
     * <p>This method does not use any codec; it sets the value in its binary form directly. If you
     * insert data that is not compatible with the underlying CQL type, you will get an {@code
     * InvalidQueryException} at execute time.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     */
    public T setBytesUnsafe(int i, ByteBuffer v);

    /**
     * Sets the {@code i}th value to the provided big integer.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (for CQL type {@code varint}, this will be the built-in codec).
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the value to the
     *                                   underlying CQL type.
     */
    public T setVarint(int i, BigInteger v);

    /**
     * Sets the {@code i}th value to the provided big decimal.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (for CQL type {@code decimal}, this will be the built-in codec).
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the value to the
     *                                   underlying CQL type.
     */
    public T setDecimal(int i, BigDecimal v);

    /**
     * Sets the {@code i}th value to the provided UUID.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (for CQL types {@code uuid} and {@code timeuuid}, this will be the built-in
     * codec).
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the value to the
     *                                   underlying CQL type.
     */
    public T setUUID(int i, UUID v);

    /**
     * Sets the {@code i}th value to the provided inet address.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (for CQL type {@code inet}, this will be the built-in codec).
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the value to the
     *                                   underlying CQL type.
     */
    public T setInet(int i, InetAddress v);

    /**
     * Sets the {@code i}th value to the provided list.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (the type of the elements in the Java list is not considered). If two or
     * more codecs target that CQL type, the one that was first registered will be used. For this
     * reason, it is generally preferable to use the more deterministic methods {@link #setList(int,
     * List, Class)} or {@link #setList(int, List, TypeToken)}.
     *
     * @param i the index of the value to set.
     * @param v the value to set. Note that {@code null} values inside collections are not supported
     *          by CQL.
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws NullPointerException      if {@code v} contains null values. Nulls are not supported in
     *                                   collections by CQL.
     * @throws CodecNotFoundException    if there is no registered codec to convert the value to the
     *                                   underlying CQL type.
     */
    public <E> T setList(int i, List<E> v);

    /**
     * Sets the {@code i}th value to the provided list, which elements are of the provided Java class.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion of lists
     * of the given Java type to the underlying CQL type.
     *
     * <p>If the type of the elements is generic, use {@link #setList(int, List, TypeToken)}.
     *
     * @param i             the index of the value to set.
     * @param v             the value to set. Note that {@code null} values inside collections are not supported
     *                      by CQL.
     * @param elementsClass the class for the elements of the list.
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws NullPointerException      if {@code v} contains null values. Nulls are not supported in
     *                                   collections by CQL.
     * @throws CodecNotFoundException    if there is no registered codec to convert the value to the
     *                                   underlying CQL type.
     */
    public <E> T setList(int i, List<E> v, Class<E> elementsClass);

    /**
     * Sets the {@code i}th value to the provided list, which elements are of the provided Java type.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion of lists
     * of the given Java type to the underlying CQL type.
     *
     * @param i            the index of the value to set.
     * @param v            the value to set. Note that {@code null} values inside collections are not supported
     *                     by CQL.
     * @param elementsType the type for the elements of the list.
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws NullPointerException      if {@code v} contains null values. Nulls are not supported in
     *                                   collections by CQL.
     * @throws CodecNotFoundException    if there is no registered codec to convert the value to the
     *                                   underlying CQL type.
     */
    public <E> T setList(int i, List<E> v, TypeToken<E> elementsType);

    /**
     * Sets the {@code i}th value to the provided map.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (the type of the elements in the Java map is not considered). If two or
     * more codecs target that CQL type, the one that was first registered will be used. For this
     * reason, it is generally preferable to use the more deterministic methods {@link #setMap(int,
     * Map, Class, Class)} or {@link #setMap(int, Map, TypeToken, TypeToken)}.
     *
     * @param i the index of the value to set.
     * @param v the value to set. Note that {@code null} values inside collections are not supported
     *          by CQL.
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws NullPointerException      if {@code v} contains null values. Nulls are not supported in
     *                                   collections by CQL.
     * @throws CodecNotFoundException    if there is no registered codec to convert the value to the
     *                                   underlying CQL type.
     */
    public <K, V> T setMap(int i, Map<K, V> v);

    /**
     * Sets the {@code i}th value to the provided map, which keys and values are of the provided Java
     * classes.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion of lists
     * of the given Java types to the underlying CQL type.
     *
     * <p>If the type of the keys or values is generic, use {@link #setMap(int, Map, TypeToken,
     * TypeToken)}.
     *
     * @param i           the index of the value to set.
     * @param v           the value to set. Note that {@code null} values inside collections are not supported
     *                    by CQL.
     * @param keysClass   the class for the keys of the map.
     * @param valuesClass the class for the values of the map.
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws NullPointerException      if {@code v} contains null values. Nulls are not supported in
     *                                   collections by CQL.
     * @throws CodecNotFoundException    if there is no registered codec to convert the value to the
     *                                   underlying CQL type.
     */
    public <K, V> T setMap(int i, Map<K, V> v, Class<K> keysClass, Class<V> valuesClass);

    /**
     * Sets the {@code i}th value to the provided map, which keys and values are of the provided Java
     * types.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion of lists
     * of the given Java types to the underlying CQL type.
     *
     * @param i          the index of the value to set.
     * @param v          the value to set. Note that {@code null} values inside collections are not supported
     *                   by CQL.
     * @param keysType   the type for the keys of the map.
     * @param valuesType the type for the values of the map.
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws NullPointerException      if {@code v} contains null values. Nulls are not supported in
     *                                   collections by CQL.
     * @throws CodecNotFoundException    if there is no registered codec to convert the value to the
     *                                   underlying CQL type.
     */
    public <K, V> T setMap(int i, Map<K, V> v, TypeToken<K> keysType, TypeToken<V> valuesType);

    /**
     * Sets the {@code i}th value to the provided set.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (the type of the elements in the Java set is not considered). If two or
     * more codecs target that CQL type, the one that was first registered will be used. For this
     * reason, it is generally preferable to use the more deterministic methods {@link #setSet(int,
     * Set, Class)} or {@link #setSet(int, Set, TypeToken)}.
     *
     * @param i the index of the value to set.
     * @param v the value to set. Note that {@code null} values inside collections are not supported
     *          by CQL.
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws NullPointerException      if {@code v} contains null values. Nulls are not supported in
     *                                   collections by CQL.
     * @throws CodecNotFoundException    if there is no registered codec to convert the value to the
     *                                   underlying CQL type.
     */
    public <E> T setSet(int i, Set<E> v);

    /**
     * Sets the {@code i}th value to the provided set, which elements are of the provided Java class.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion of sets
     * of the given Java type to the underlying CQL type.
     *
     * <p>If the type of the elements is generic, use {@link #setSet(int, Set, TypeToken)}.
     *
     * @param i             the index of the value to set.
     * @param v             the value to set. Note that {@code null} values inside collections are not supported
     *                      by CQL.
     * @param elementsClass the class for the elements of the set.
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws NullPointerException      if {@code v} contains null values. Nulls are not supported in
     *                                   collections by CQL.
     * @throws CodecNotFoundException    if there is no registered codec to convert the value to the
     *                                   underlying CQL type.
     */
    public <E> T setSet(int i, Set<E> v, Class<E> elementsClass);

    /**
     * Sets the {@code i}th value to the provided set, which elements are of the provided Java type.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion of sets
     * of the given Java type to the underlying CQL type.
     *
     * @param i            the index of the value to set.
     * @param v            the value to set. Note that {@code null} values inside collections are not supported
     *                     by CQL.
     * @param elementsType the type for the elements of the set.
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws NullPointerException      if {@code v} contains null values. Nulls are not supported in
     *                                   collections by CQL.
     * @throws CodecNotFoundException    if there is no registered codec to convert the value to the
     *                                   underlying CQL type.
     */
    public <E> T setSet(int i, Set<E> v, TypeToken<E> elementsType);

    /**
     * Sets the {@code i}th value to the provided vector value.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion of {@code v} to the
     * underlying CQL type.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the value to the underlying CQL type.
     */
    public <E> T setVector(int i, List<E> v);

    /**
     * Sets the {@code i}th value to the provided UDT value.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion of
     * {@code UDTValue} to the underlying CQL type.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the value to the
     *                                   underlying CQL type.
     */
    public T setUDTValue(int i, UDTValue v);

    /**
     * Sets the {@code i}th value to the provided tuple value.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion of
     * {@code TupleValue} to the underlying CQL type.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the value to the
     *                                   underlying CQL type.
     */
    public T setTupleValue(int i, TupleValue v);

    /**
     * Sets the {@code i}th value to {@code null}.
     *
     * <p>This is mainly intended for CQL types which map to native Java types.
     *
     * @param i the index of the value to set.
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     */
    public T setToNull(int i);

    /**
     * Sets the {@code i}th value to the provided value of the provided Java class.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion of the
     * provided Java class to the underlying CQL type.
     *
     * <p>If the Java type is generic, use {@link #set(int, Object, TypeToken)} instead.
     *
     * @param i           the index of the value to set.
     * @param v           the value to set; may be {@code null}.
     * @param targetClass The Java class to convert to; must not be {@code null};
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the value to the
     *                                   underlying CQL type.
     */
    <V> T set(int i, V v, Class<V> targetClass);

    /**
     * Sets the {@code i}th value to the provided value of the provided Java type.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion of the
     * provided Java type to the underlying CQL type.
     *
     * @param i          the index of the value to set.
     * @param v          the value to set; may be {@code null}.
     * @param targetType The Java type to convert to; must not be {@code null};
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the value to the
     *                                   underlying CQL type.
     */
    <V> T set(int i, V v, TypeToken<V> targetType);

    /**
     * Sets the {@code i}th value to the provided value, converted using the given {@link TypeCodec}.
     *
     * <p>This method entirely bypasses the {@link CodecRegistry} and forces the driver to use the
     * given codec instead. This can be useful if the codec would collide with a previously registered
     * one, or if you want to use the codec just once without registering it.
     *
     * <p>It is the caller's responsibility to ensure that the given codec {@link
     * TypeCodec#accepts(DataType) accepts} the underlying CQL type; failing to do so may result in
     * {@link InvalidTypeException}s being thrown.
     *
     * @param i     the index of the value to set.
     * @param v     the value to set; may be {@code null}.
     * @param codec The {@link TypeCodec} to use to serialize the value; may not be {@code null}.
     * @return this object.
     * @throws InvalidTypeException      if the given codec does not {@link TypeCodec#accepts(DataType)
     *                                   accept} the underlying CQL type.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     */
    <V> T set(int i, V v, TypeCodec<V> codec);
}
