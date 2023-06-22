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
 * Collection of (typed) CQL values that can be retrieved by index (starting at zero).
 */
public interface GettableByIndexData
{

    /**
     * Returns whether the {@code i}th value is NULL.
     *
     * @param i the index ({@code 0 <= i < size()}) of the value to check.
     * @return whether the {@code i}th value is NULL.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     */
    public boolean isNull(int i);

    /**
     * Returns the {@code i}th value as a boolean.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a Java {@code boolean} (for CQL type {@code boolean}, this will be the built-in codec).
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the boolean value of the {@code i}th element. If the value is NULL, {@code false} is
     * returned. If you need to distinguish NULL and false values, check first with {@link
     * #isNull(int)} or use {@code get(i, Boolean.class)}.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the element's CQL
     *                                   type to a boolean.
     */
    public boolean getBool(int i);

    /**
     * Returns the {@code i}th value as a byte.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a Java {@code byte} (for CQL type {@code tinyint}, this will be the built-in codec).
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as a byte. If the value is NULL, {@code 0} is
     * returned. If you need to distinguish NULL and 0, check first with {@link #isNull(int)} or
     * use {@code get(i, Byte.class)}.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the element's CQL
     *                                   type to a byte.
     */
    public byte getByte(int i);

    /**
     * Returns the {@code i}th value as a short.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a Java {@code short} (for CQL type {@code smallint}, this will be the built-in codec).
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as a short. If the value is NULL, {@code 0} is
     * returned. If you need to distinguish NULL and 0, check first with {@link #isNull(int)} or
     * use {@code get(i, Short.class)}.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the element's CQL
     *                                   type to a short.
     */
    public short getShort(int i);

    /**
     * Returns the {@code i}th value as an integer.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a Java {@code int} (for CQL type {@code int}, this will be the built-in codec).
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as an integer. If the value is NULL, {@code 0} is
     * returned. If you need to distinguish NULL and 0, check first with {@link #isNull(int)} or
     * use {@code get(i, Integer.class)}.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the element's CQL
     *                                   type to an int.
     */
    public int getInt(int i);

    /**
     * Returns the {@code i}th value as a long.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a Java {@code byte} (for CQL types {@code bigint} and {@code counter}, this will be the
     * built-in codec).
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as a long. If the value is NULL, {@code 0L} is
     * returned. If you need to distinguish NULL and 0L, check first with {@link #isNull(int)} or
     * use {@code get(i, Long.class)}.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the element's CQL
     *                                   type to a long.
     */
    public long getLong(int i);

    /**
     * Returns the {@code i}th value as a date.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a {@code Date} (for CQL type {@code timestamp}, this will be the built-in codec).
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as a data. If the value is NULL, {@code null} is
     * returned.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the element's CQL
     *                                   type to a {@code Date}.
     */
    public Date getTimestamp(int i);

    /**
     * Returns the {@code i}th value as a date (without time).
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a {@link LocalDate} (for CQL type {@code date}, this will be the built-in codec).
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as an date. If the value is NULL, {@code null} is
     * returned.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the element's CQL
     *                                   type to a {@code LocalDate}.
     */
    public LocalDate getDate(int i);

    /**
     * Returns the {@code i}th value as a long in nanoseconds since midnight.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a Java {@code long} (for CQL type {@code time}, this will be the built-in codec).
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as a long. If the value is NULL, {@code 0L} is
     * returned.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the element's CQL
     *                                   type to a long.
     */
    public long getTime(int i);

    /**
     * Returns the {@code i}th value as a float.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a Java {@code float} (for CQL type {@code float}, this will be the built-in codec).
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as a float. If the value is NULL, {@code 0.0f} is
     * returned. If you need to distinguish NULL and 0.0f, check first with {@link #isNull(int)}
     * or use {@code get(i, Float.class)}.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the element's CQL
     *                                   type to a float.
     */
    public float getFloat(int i);

    /**
     * Returns the {@code i}th value as a double.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a Java {@code double} (for CQL type {@code double}, this will be the built-in codec).
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as a double. If the value is NULL, {@code 0.0} is
     * returned. If you need to distinguish NULL and 0.0, check first with {@link #isNull(int)} or
     * use {@code get(i, Double.class)}.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the element's CQL
     *                                   type to a double.
     */
    public double getDouble(int i);

    /**
     * Returns the {@code i}th value as a {@code ByteBuffer}.
     *
     * <p>This method does not use any codec; it returns a copy of the binary representation of the
     * value. It is up to the caller to convert the returned value appropriately.
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as a ByteBuffer. If the value is NULL, {@code
     * null} is returned.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     */
    public ByteBuffer getBytesUnsafe(int i);

    /**
     * Returns the {@code i}th value as a byte array.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a Java {@code ByteBuffer} (for CQL type {@code blob}, this will be the built-in codec).
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as a byte array. If the value is NULL, {@code
     * null} is returned.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the element's CQL
     *                                   type to a {@code ByteBuffer}.
     */
    public ByteBuffer getBytes(int i);

    /**
     * Returns the {@code i}th value as a string.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a Java string (for CQL types {@code text}, {@code varchar} and {@code ascii}, this will
     * be the built-in codec).
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as a string. If the value is NULL, {@code null} is
     * returned.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the element's CQL
     *                                   type to a string.
     */
    public String getString(int i);

    /**
     * Returns the {@code i}th value as a variable length integer.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a {@code BigInteger} (for CQL type {@code varint}, this will be the built-in codec).
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as a variable length integer. If the value is
     * NULL, {@code null} is returned.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the element's CQL
     *                                   type to a {@code BigInteger}.
     */
    public BigInteger getVarint(int i);

    /**
     * Returns the {@code i}th value as a variable length decimal.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a {@code BigDecimal} (for CQL type {@code decimal}, this will be the built-in codec).
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as a variable length decimal. If the value is
     * NULL, {@code null} is returned.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the element's CQL
     *                                   type to a {@code BigDecimal}.
     */
    public BigDecimal getDecimal(int i);

    /**
     * Returns the {@code i}th value as a UUID.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a {@code UUID} (for CQL types {@code uuid} and {@code timeuuid}, this will be the
     * built-in codec).
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as a UUID. If the value is NULL, {@code null} is
     * returned.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the element's CQL
     *                                   type to a {@code UUID}.
     */
    public UUID getUUID(int i);

    /**
     * Returns the {@code i}th value as an InetAddress.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to an {@code InetAddress} (for CQL type {@code inet}, this will be the built-in codec).
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as an InetAddress. If the value is NULL, {@code
     * null} is returned.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the element's CQL
     *                                   type to a {@code InetAddress}.
     */
    public InetAddress getInet(int i);

    /**
     * Returns the {@code i}th value as a list.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a list of the specified type.
     *
     * <p>If the type of the elements is generic, use {@link #getList(int, TypeToken)}.
     *
     * <p>Implementation note: the actual {@link List} implementation will depend on the {@link
     * TypeCodec codec} being used; therefore, callers should make no assumptions concerning its
     * mutability nor its thread-safety. Furthermore, the behavior of this method in respect to CQL
     * {@code NULL} values is also codec-dependent. By default, the driver will return mutable
     * instances, and a CQL {@code NULL} will be mapped to an empty collection (note that Cassandra
     * makes no distinction between {@code NULL} and an empty collection).
     *
     * @param i             the index ({@code 0 <= i < size()}) to retrieve.
     * @param elementsClass the class for the elements of the list to retrieve.
     * @return the value of the {@code i}th element as a list of {@code T} objects.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the element's CQL
     *                                   type to a list.
     */
    public <T> List<T> getList(int i, Class<T> elementsClass);

    /**
     * Returns the {@code i}th value as a list.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a list of the specified type.
     *
     * <p>Use this variant with nested collections, which produce a generic element type:
     *
     * <pre>
     * {@code List<List<String>> l = row.getList(1, new TypeToken<List<String>>() {});}
     * </pre>
     *
     * <p>Implementation note: the actual {@link List} implementation will depend on the {@link
     * TypeCodec codec} being used; therefore, callers should make no assumptions concerning its
     * mutability nor its thread-safety. Furthermore, the behavior of this method in respect to CQL
     * {@code NULL} values is also codec-dependent. By default, the driver will return mutable
     * instances, and a CQL {@code NULL} will mapped to an empty collection (note that Cassandra makes
     * no distinction between {@code NULL} and an empty collection).
     *
     * @param i            the index ({@code 0 <= i < size()}) to retrieve.
     * @param elementsType the type of the elements of the list to retrieve.
     * @return the value of the {@code i}th element as a list of {@code T} objects.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the element's CQL
     *                                   type to a list.
     */
    public <T> List<T> getList(int i, TypeToken<T> elementsType);

    /**
     * Returns the {@code i}th value as a set.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a set of the specified type.
     *
     * <p>If the type of the elements is generic, use {@link #getSet(int, TypeToken)}.
     *
     * <p>Implementation note: the actual {@link Set} implementation will depend on the {@link
     * TypeCodec codec} being used; therefore, callers should make no assumptions concerning its
     * mutability nor its thread-safety. Furthermore, the behavior of this method in respect to CQL
     * {@code NULL} values is also codec-dependent. By default, the driver will return mutable
     * instances, and a CQL {@code NULL} will mapped to an empty collection (note that Cassandra makes
     * no distinction between {@code NULL} and an empty collection).
     *
     * @param i             the index ({@code 0 <= i < size()}) to retrieve.
     * @param elementsClass the class for the elements of the set to retrieve.
     * @return the value of the {@code i}th element as a set of {@code T} objects.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the element's CQL
     *                                   type to a set.
     */
    public <T> Set<T> getSet(int i, Class<T> elementsClass);

    /**
     * Returns the {@code i}th value as a set.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a set of the specified type.
     *
     * <p>Use this variant with nested collections, which produce a generic element type:
     *
     * <pre>
     * {@code Set<List<String>> l = row.getSet(1, new TypeToken<List<String>>() {});}
     * </pre>
     *
     * <p>Implementation note: the actual {@link Set} implementation will depend on the {@link
     * TypeCodec codec} being used; therefore, callers should make no assumptions concerning its
     * mutability nor its thread-safety. Furthermore, the behavior of this method in respect to CQL
     * {@code NULL} values is also codec-dependent. By default, the driver will return mutable
     * instances, and a CQL {@code NULL} will mapped to an empty collection (note that Cassandra makes
     * no distinction between {@code NULL} and an empty collection).
     *
     * @param i            the index ({@code 0 <= i < size()}) to retrieve.
     * @param elementsType the type for the elements of the set to retrieve.
     * @return the value of the {@code i}th element as a set of {@code T} objects.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the element's CQL
     *                                   type to a set.
     */
    public <T> Set<T> getSet(int i, TypeToken<T> elementsType);

    /**
     * Returns the {@code i}th value as a map.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a map of the specified types.
     *
     * <p>If the type of the keys and/or values is generic, use {@link #getMap(int, TypeToken,
     * TypeToken)}.
     *
     * <p>Implementation note: the actual {@link Map} implementation will depend on the {@link
     * TypeCodec codec} being used; therefore, callers should make no assumptions concerning its
     * mutability nor its thread-safety. Furthermore, the behavior of this method in respect to CQL
     * {@code NULL} values is also codec-dependent. By default, the driver will return mutable
     * instances, and a CQL {@code NULL} will mapped to an empty collection (note that Cassandra makes
     * no distinction between {@code NULL} and an empty collection).
     *
     * @param i           the index ({@code 0 <= i < size()}) to retrieve.
     * @param keysClass   the class for the keys of the map to retrieve.
     * @param valuesClass the class for the values of the map to retrieve.
     * @return the value of the {@code i}th element as a map of {@code K} to {@code V} objects.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the element's CQL
     *                                   type to a map.
     */
    public <K, V> Map<K, V> getMap(int i, Class<K> keysClass, Class<V> valuesClass);

    /**
     * Returns the {@code i}th value as a map.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a map of the specified types.
     *
     * <p>Use this variant with nested collections, which produce a generic element type:
     *
     * <pre>
     * {@code Map<Int, List<String>> l = row.getMap(1, TypeToken.of(Integer.class), new TypeToken<List<String>>() {});}
     * </pre>
     *
     * <p>Implementation note: the actual {@link Map} implementation will depend on the {@link
     * TypeCodec codec} being used; therefore, callers should make no assumptions concerning its
     * mutability nor its thread-safety. Furthermore, the behavior of this method in respect to CQL
     * {@code NULL} values is also codec-dependent. By default, the driver will return mutable
     * instances, and a CQL {@code NULL} will mapped to an empty collection (note that Cassandra makes
     * no distinction between {@code NULL} and an empty collection).
     *
     * @param i          the index ({@code 0 <= i < size()}) to retrieve.
     * @param keysType   the type for the keys of the map to retrieve.
     * @param valuesType the type for the values of the map to retrieve.
     * @return the value of the {@code i}th element as a map of {@code K} to {@code V} objects.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the element's CQL
     *                                   type to a map.
     */
    public <K, V> Map<K, V> getMap(int i, TypeToken<K> keysType, TypeToken<V> valuesType);

    /**
     * Returns the {@code i}th value as a vector, represented as a Java list.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a vector of the specified type.
     *
     * <p>If the type of the elements is generic, use {@link #getVector(int, TypeToken)}.
     *
     * <p>Implementation note: the actual {@link List} implementation representing the vector will depend on the {@link
     * TypeCodec codec} being used; therefore, callers should make no assumptions concerning its mutability nor its
     * thread-safety.
     *
     * @param i             the index ({@code 0 <= i < size()}) to retrieve.
     * @param elementsClass the class for the elements of the list to retrieve.
     * @return the value of the {@code i}th element as a list of {@code T} objects.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the element's CQL type to a vector.
     */
    public <T> List<T> getVector(int i, Class<T> elementsClass);

    /**
     * Returns the {@code i}th value as a vector, represented as a Java list.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a vector of the specified type.
     *
     * <p>Use this variant with nested types, which produce a generic element type:
     *
     * <pre>
     * {@code List<List<String>> l = row.getVector(1, new TypeToken<List<String>>() {});}
     * </pre>
     *
     * <p>Implementation note: the actual {@link List} implementation representing the vector will depend on the {@link
     * TypeCodec codec} being used; therefore, callers should make no assumptions concerning its mutability nor its
     * thread-safety.
     *
     * @param i            the index ({@code 0 <= i < size()}) to retrieve.
     * @param elementsType the type of the elements of the vector to retrieve.
     * @return the value of the {@code i}th element as a vector of {@code T} objects.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the element's CQL type to a vector.
     */
    public <T> List<T> getVector(int i, TypeToken<T> elementsType);

    /**
     * Return the {@code i}th value as a UDT value.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a {@code UDTValue} (if the CQL type is a UDT, the registry will generate a codec
     * automatically).
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as a UDT value. If the value is NULL, then {@code
     * null} will be returned.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the element's CQL
     *                                   type to a {@code UDTValue}.
     */
    public UDTValue getUDTValue(int i);

    /**
     * Return the {@code i}th value as a tuple value.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a {@code TupleValue} (if the CQL type is a tuple, the registry will generate a codec
     * automatically).
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as a tuple value. If the value is NULL, then
     * {@code null} will be returned.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the element's CQL
     *                                   type to a {@code TupleValue}.
     */
    public TupleValue getTupleValue(int i);

    /**
     * Returns the {@code i}th value as the Java type matching its CQL type.
     *
     * <p>This method uses the {@link CodecRegistry} to find the first codec that handles the
     * underlying CQL type. The Java type of the returned object will be determined by the codec that
     * was selected.
     *
     * <p>Use this method to dynamically inspect elements when types aren't known in advance, for
     * instance if you're writing a generic row logger. If you know the target Java type, it is
     * generally preferable to use typed getters, such as the ones for built-in types ({@link
     * #getBool(int)}, {@link #getInt(int)}, etc.), or {@link #get(int, Class)} and {@link #get(int,
     * TypeToken)} for custom types.
     *
     * @param i the index to retrieve.
     * @return the value of the {@code i}th value as the Java type matching its CQL type.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @see CodecRegistry#codecFor(DataType)
     */
    public Object getObject(int i);

    /**
     * Returns the {@code i}th value converted to the given Java type.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to the given Java type.
     *
     * <p>If the target type is generic, use {@link #get(int, TypeToken)}.
     *
     * <p>Implementation note: the actual object returned by this method will depend on the {@link
     * TypeCodec codec} being used; therefore, callers should make no assumptions concerning its
     * mutability nor its thread-safety. Furthermore, the behavior of this method in respect to CQL
     * {@code NULL} values is also codec-dependent; by default, a CQL {@code NULL} value translates to
     * {@code null} for simple CQL types, UDTs and tuples, and to empty collections for all CQL
     * collection types.
     *
     * @param i           the index to retrieve.
     * @param targetClass The Java type the value should be converted to.
     * @return the value of the {@code i}th value converted to the given Java type.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the element's CQL
     *                                   type to {@code targetClass}.
     */
    <T> T get(int i, Class<T> targetClass);

    /**
     * Returns the {@code i}th value converted to the given Java type.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to the given Java type.
     *
     * <p>Implementation note: the actual object returned by this method will depend on the {@link
     * TypeCodec codec} being used; therefore, callers should make no assumptions concerning its
     * mutability nor its thread-safety. Furthermore, the behavior of this method in respect to CQL
     * {@code NULL} values is also codec-dependent; by default, a CQL {@code NULL} value translates to
     * {@code null} for simple CQL types, UDTs and tuples, and to empty collections for all CQL
     * collection types.
     *
     * @param i          the index to retrieve.
     * @param targetType The Java type the value should be converted to.
     * @return the value of the {@code i}th value converted to the given Java type.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws CodecNotFoundException    if there is no registered codec to convert the element's CQL
     *                                   type to {@code targetType}.
     */
    <T> T get(int i, TypeToken<T> targetType);

    /**
     * Returns the {@code i}th value converted using the given {@link TypeCodec}.
     *
     * <p>This method entirely bypasses the {@link CodecRegistry} and forces the driver to use the
     * given codec instead. This can be useful if the codec would collide with a previously registered
     * one, or if you want to use the codec just once without registering it.
     *
     * <p>It is the caller's responsibility to ensure that the given codec {@link
     * TypeCodec#accepts(DataType) accepts} the underlying CQL type; failing to do so may result in
     * {@link InvalidTypeException}s being thrown.
     *
     * <p>Implementation note: the actual object returned by this method will depend on the {@link
     * TypeCodec codec} being used; therefore, callers should make no assumptions concerning its
     * mutability nor its thread-safety. Furthermore, the behavior of this method in respect to CQL
     * {@code NULL} values is also codec-dependent; by default, a CQL {@code NULL} value translates to
     * {@code null} for simple CQL types, UDTs and tuples, and to empty collections for all CQL
     * collection types.
     *
     * @param i     the index to retrieve.
     * @param codec The {@link TypeCodec} to use to deserialize the value; may not be {@code null}.
     * @return the value of the {@code i}th value converted using the given {@link TypeCodec}.
     * @throws InvalidTypeException      if the given codec does not {@link TypeCodec#accepts(DataType)
     *                                   accept} the underlying CQL type.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     */
    <T> T get(int i, TypeCodec<T> codec);
}
