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
 * Collection of (typed) CQL values that can set by name.
 */
public interface SettableByNameData<T extends SettableData<T>>
{

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided boolean.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (for CQL type {@code boolean}, this will be the built-in codec).
     *
     * @param name the name of the value to set; if {@code name} is present multiple times, all its
     *             values are set.
     * @param v    the value to set. To set the value to NULL, use {@link #setToNull(String)} or {@code
     *             set(name, v, Boolean.class)}.
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the value to the
     *                                  underlying CQL type.
     */
    public T setBool(String name, boolean v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided byte.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (for CQL type {@code tinyint}, this will be the built-in codec).
     *
     * @param name the name of the value to set; if {@code name} is present multiple times, all its
     *             values are set.
     * @param v    the value to set. To set the value to NULL, use {@link #setToNull(String)} or {@code
     *             set(name, v, Byte.class)}.
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the value to the
     *                                  underlying CQL type.
     */
    public T setByte(String name, byte v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided short.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (for CQL type {@code smallint}, this will be the built-in codec).
     *
     * @param name the name of the value to set; if {@code name} is present multiple times, all its
     *             values are set.
     * @param v    the value to set. To set the value to NULL, use {@link #setToNull(String)} or {@code
     *             set(name, v, Short.class)}.
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the value to the
     *                                  underlying CQL type.
     */
    public T setShort(String name, short v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided integer.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (for CQL type {@code int}, this will be the built-in codec).
     *
     * @param name the name of the value to set; if {@code name} is present multiple times, all its
     *             values are set.
     * @param v    the value to set. To set the value to NULL, use {@link #setToNull(String)} or {@code
     *             set(name, v, Integer.class)}.
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the value to the
     *                                  underlying CQL type.
     */
    public T setInt(String name, int v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided long.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (for CQL type {@code bigint}, this will be the built-in codec).
     *
     * @param name the name of the value to set; if {@code name} is present multiple times, all its
     *             values are set.
     * @param v    the value to set. To set the value to NULL, use {@link #setToNull(String)} or {@code
     *             set(name, v, Long.class)}.
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the value to the
     *                                  underlying CQL type.
     */
    public T setLong(String name, long v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided date.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (for CQL type {@code timestamp}, this will be the built-in codec).
     *
     * @param name the name of the value to set; if {@code name} is present multiple times, all its
     *             values are set.
     * @param v    the value to set.
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the value to the
     *                                  underlying CQL type.
     */
    public T setTimestamp(String name, Date v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided date (without
     * time).
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (for CQL type {@code date}, this will be the built-in codec).
     *
     * @param name the name of the value to set; if {@code name} is present multiple times, all its
     *             values are set.
     * @param v    the value to set.
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the value to the
     *                                  underlying CQL type.
     */
    public T setDate(String name, LocalDate v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided time as a long in
     * nanoseconds since midnight.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (for CQL type {@code time}, this will be the built-in codec).
     *
     * @param name the name of the value to set; if {@code name} is present multiple times, all its
     *             values are set.
     * @param v    the value to set.
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the value to the
     *                                  underlying CQL type.
     */
    public T setTime(String name, long v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided float.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (for CQL type {@code float}, this will be the built-in codec).
     *
     * @param name the name of the value to set; if {@code name} is present multiple times, all its
     *             values are set.
     * @param v    the value to set. To set the value to NULL, use {@link #setToNull(String)} or {@code
     *             set(name, v, Float.class)}.
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the value to the
     *                                  underlying CQL type.
     */
    public T setFloat(String name, float v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided double.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (for CQL type {@code double}, this will be the built-in codec).
     *
     * @param name the name of the value to set; if {@code name} is present multiple times, all its
     *             values are set.
     * @param v    the value to set. To set the value to NULL, use {@link #setToNull(String)} or {@code
     *             set(name, v, Double.class)}.
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the value to the
     *                                  underlying CQL type.
     */
    public T setDouble(String name, double v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided string.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (for CQL types {@code text}, {@code varchar} and {@code ascii}, this will
     * be the built-in codec).
     *
     * @param name the name of the value to set; if {@code name} is present multiple times, all its
     *             values are set.
     * @param v    the value to set.
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the value to the
     *                                  underlying CQL type.
     */
    public T setString(String name, String v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided byte buffer.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (for CQL type {@code blob}, this will be the built-in codec).
     *
     * @param name the name of the value to set; if {@code name} is present multiple times, all its
     *             values are set.
     * @param v    the value to set.
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the value to the
     *                                  underlying CQL type.
     */
    public T setBytes(String name, ByteBuffer v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided byte buffer.
     *
     * <p>This method does not use any codec; it sets the value in its binary form directly. If you
     * insert data that is not compatible with the underlying CQL type, you will get an {@code
     * InvalidQueryException} at execute time.
     *
     * @param name the name of the value to set; if {@code name} is present multiple times, all its
     *             values are set.
     * @param v    the value to set.
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     */
    public T setBytesUnsafe(String name, ByteBuffer v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided big integer.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (for CQL type {@code varint}, this will be the built-in codec).
     *
     * @param name the name of the value to set; if {@code name} is present multiple times, all its
     *             values are set.
     * @param v    the value to set.
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the value to the
     *                                  underlying CQL type.
     */
    public T setVarint(String name, BigInteger v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided big decimal.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (for CQL type {@code decimal}, this will be the built-in codec).
     *
     * @param name the name of the value to set; if {@code name} is present multiple times, all its
     *             values are set.
     * @param v    the value to set.
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the value to the
     *                                  underlying CQL type.
     */
    public T setDecimal(String name, BigDecimal v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided UUID.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (for CQL types {@code uuid} and {@code timeuuid}, this will be the built-in
     * codec).
     *
     * @param name the name of the value to set; if {@code name} is present multiple times, all its
     *             values are set.
     * @param v    the value to set.
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the value to the
     *                                  underlying CQL type.
     */
    public T setUUID(String name, UUID v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided inet address.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (for CQL type {@code inet}, this will be the built-in codec).
     *
     * @param name the name of the value to set; if {@code name} is present multiple times, all its
     *             values are set.
     * @param v    the value to set.
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the value to the
     *                                  underlying CQL type.
     */
    public T setInet(String name, InetAddress v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided list.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (the type of the elements in the Java list is not considered). If two or
     * more codecs target that CQL type, the one that was first registered will be used. For this
     * reason, it is generally preferable to use the more deterministic methods {@link
     * #setList(String, List, Class)} or {@link #setList(String, List, TypeToken)}.
     *
     * @param name the name of the value to set; if {@code name} is present multiple times, all its
     *             values are set.
     * @param v    the value to set. Note that {@code null} values inside collections are not supported
     *             by CQL.
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws NullPointerException     if {@code v} contains null values. Nulls are not supported in
     *                                  collections by CQL.
     * @throws CodecNotFoundException   if there is no registered codec to convert the value to the
     *                                  underlying CQL type.
     */
    public <E> T setList(String name, List<E> v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided list, which
     * elements are of the provided Java class.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion of lists
     * of the given Java type to the underlying CQL type.
     *
     * <p>If the type of the elements is generic, use {@link #setList(String, List, TypeToken)}.
     *
     * @param name          the name of the value to set; if {@code name} is present multiple
     * @param v             the value to set. Note that {@code null} values inside collections are not supported
     *                      by CQL.
     * @param elementsClass the class for the elements of the list.
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws NullPointerException     if {@code v} contains null values. Nulls are not supported in
     *                                  collections by CQL.
     * @throws CodecNotFoundException   if there is no registered codec to convert the value to the
     *                                  underlying CQL type.
     */
    public <E> T setList(String name, List<E> v, Class<E> elementsClass);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided list, which
     * elements are of the provided Java type.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion of lists
     * of the given Java type to the underlying CQL type.
     *
     * @param name         the name of the value to set; if {@code name} is present multiple
     * @param v            the value to set. Note that {@code null} values inside collections are not supported
     *                     by CQL.
     * @param elementsType the type for the elements of the list.
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws NullPointerException     if {@code v} contains null values. Nulls are not supported in
     *                                  collections by CQL.
     * @throws CodecNotFoundException   if there is no registered codec to convert the value to the
     *                                  underlying CQL type.
     */
    public <E> T setList(String name, List<E> v, TypeToken<E> elementsType);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided map.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (the type of the elements in the Java map is not considered). If two or
     * more codecs target that CQL type, the one that was first registered will be used. For this
     * reason, it is generally preferable to use the more deterministic methods {@link #setMap(String,
     * Map, Class, Class)} or {@link #setMap(String, Map, TypeToken, TypeToken)}.
     *
     * @param name the name of the value to set; if {@code name} is present multiple times, all its
     *             values are set.
     * @param v    the value to set. Note that {@code null} values inside collections are not supported
     *             by CQL.
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws NullPointerException     if {@code v} contains null values. Nulls are not supported in
     *                                  collections by CQL.
     * @throws CodecNotFoundException   if there is no registered codec to convert the value to the
     *                                  underlying CQL type.
     */
    public <K, V> T setMap(String name, Map<K, V> v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided map, which keys
     * and values are of the provided Java classes.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion of lists
     * of the given Java types to the underlying CQL type.
     *
     * <p>If the type of the keys or values is generic, use {@link #setMap(String, Map, TypeToken,
     * TypeToken)}.
     *
     * @param name        the name of the value to set; if {@code name} is present multiple times, all its
     *                    values are set.
     * @param v           the value to set. Note that {@code null} values inside collections are not supported
     *                    by CQL.
     * @param keysClass   the class for the keys of the map.
     * @param valuesClass the class for the values of the map.
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws NullPointerException     if {@code v} contains null values. Nulls are not supported in
     *                                  collections by CQL.
     * @throws CodecNotFoundException   if there is no registered codec to convert the value to the
     *                                  underlying CQL type.
     */
    public <K, V> T setMap(String name, Map<K, V> v, Class<K> keysClass, Class<V> valuesClass);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided map, which keys
     * and values are of the provided Java types.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion of lists
     * of the given Java types to the underlying CQL type.
     *
     * @param name       the name of the value to set; if {@code name} is present multiple times, all its
     *                   values are set.
     * @param v          the value to set. Note that {@code null} values inside collections are not supported
     *                   by CQL.
     * @param keysType   the type for the keys of the map.
     * @param valuesType the type for the values of the map.
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws NullPointerException     if {@code v} contains null values. Nulls are not supported in
     *                                  collections by CQL.
     * @throws CodecNotFoundException   if there is no registered codec to convert the value to the
     *                                  underlying CQL type.
     */
    public <K, V> T setMap(String name, Map<K, V> v, TypeToken<K> keysType, TypeToken<V> valuesType);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided set.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion to the
     * underlying CQL type (the type of the elements in the Java set is not considered). If two or
     * more codecs target that CQL type, the one that was first registered will be used. For this
     * reason, it is generally preferable to use the more deterministic methods {@link #setSet(String,
     * Set, Class)} or {@link #setSet(String, Set, TypeToken)}.
     *
     * @param name the name of the value to set; if {@code name} is present multiple times, all its
     *             values are set.
     * @param v    the value to set. Note that {@code null} values inside collections are not supported
     *             by CQL.
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws NullPointerException     if {@code v} contains null values. Nulls are not supported in
     *                                  collections by CQL.
     * @throws CodecNotFoundException   if there is no registered codec to convert the value to the
     *                                  underlying CQL type.
     */
    public <E> T setSet(String name, Set<E> v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided set, which
     * elements are of the provided Java class.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion of sets
     * of the given Java type to the underlying CQL type.
     *
     * <p>If the type of the elements is generic, use {@link #setSet(String, Set, TypeToken)}.
     *
     * @param name          the name of the value to set; if {@code name} is present multiple
     * @param v             the value to set. Note that {@code null} values inside collections are not supported
     *                      by CQL.
     * @param elementsClass the class for the elements of the set.
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws NullPointerException     if {@code v} contains null values. Nulls are not supported in
     *                                  collections by CQL.
     * @throws CodecNotFoundException   if there is no registered codec to convert the value to the
     *                                  underlying CQL type.
     */
    public <E> T setSet(String name, Set<E> v, Class<E> elementsClass);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided set, which
     * elements are of the provided Java type.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion of sets
     * of the given Java type to the underlying CQL type.
     *
     * @param name         the name of the value to set; if {@code name} is present multiple
     * @param v            the value to set. Note that {@code null} values inside collections are not supported
     *                     by CQL.
     * @param elementsType the type for the elements of the set.
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws NullPointerException     if {@code v} contains null values. Nulls are not supported in
     *                                  collections by CQL.
     * @throws CodecNotFoundException   if there is no registered codec to convert the value to the
     *                                  underlying CQL type.
     */
    public <E> T setSet(String name, Set<E> v, TypeToken<E> elementsType);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided vector value.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion of {@code v} to the
     * underlying CQL type.
     *
     * @param name the name of the value to set; if {@code name} is present multiple times, all its values are set.
     * @param v    the value to set.
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the value to the underlying CQL type.
     */
    public <E> T setVector(String name, List<E> v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided UDT value.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion of
     * {@code UDTValue} to the underlying CQL type.
     *
     * @param name the name of the value to set; if {@code name} is present multiple times, all its
     *             values are set.
     * @param v    the value to set.
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the value to the
     *                                  underlying CQL type.
     */
    public T setUDTValue(String name, UDTValue v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided tuple value.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion of
     * {@code TupleValue} to the underlying CQL type.
     *
     * @param name the name of the value to set; if {@code name} is present multiple times, all its
     *             values are set.
     * @param v    the value to set.
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the value to the
     *                                  underlying CQL type.
     */
    public T setTupleValue(String name, TupleValue v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to {@code null}.
     *
     * <p>This is mainly intended for CQL types which map to native Java types.
     *
     * @param name the name of the value to set; if {@code name} is present multiple times, all its
     *             values are set.
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     */
    public T setToNull(String name);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided value of the
     * provided Java class.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion of the
     * provided Java class to the underlying CQL type.
     *
     * <p>If the Java type is generic, use {@link #set(String, Object, TypeToken)} instead.
     *
     * @param name        the name of the value to set; if {@code name} is present multiple times, all its
     *                    values are set.
     * @param v           the value to set; may be {@code null}.
     * @param targetClass The Java class to convert to; must not be {@code null};
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the value to the
     *                                  underlying CQL type.
     */
    <V> T set(String name, V v, Class<V> targetClass);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided value of the
     * provided Java type.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to handle the conversion of the
     * provided Java type to the underlying CQL type.
     *
     * @param name       the name of the value to set; if {@code name} is present multiple times, all its
     *                   values are set.
     * @param v          the value to set; may be {@code null}.
     * @param targetType The Java type to convert to; must not be {@code null};
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the value to the
     *                                  underlying CQL type.
     */
    <V> T set(String name, V v, TypeToken<V> targetType);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided value, converted
     * using the given {@link TypeCodec}.
     *
     * <p>This method entirely bypasses the {@link CodecRegistry} and forces the driver to use the
     * given codec instead. This can be useful if the codec would collide with a previously registered
     * one, or if you want to use the codec just once without registering it.
     *
     * <p>It is the caller's responsibility to ensure that the given codec {@link
     * TypeCodec#accepts(DataType) accepts} the underlying CQL type; failing to do so may result in
     * {@link InvalidTypeException}s being thrown.
     *
     * @param name  the name of the value to set; if {@code name} is present multiple times, all its
     *              values are set.
     * @param v     the value to set; may be {@code null}.
     * @param codec The {@link TypeCodec} to use to serialize the value; may not be {@code null}.
     * @return this object.
     * @throws InvalidTypeException     if the given codec does not {@link TypeCodec#accepts(DataType)
     *                                  accept} the underlying CQL type.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     */
    <V> T set(String name, V v, TypeCodec<V> codec);
}
