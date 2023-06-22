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
 * Collection of (typed) CQL values that can be retrieved by name.
 */
public interface GettableByNameData
{

    /**
     * Returns whether the value for {@code name} is NULL.
     *
     * @param name the name to check.
     * @return whether the value for {@code name} is NULL.
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     */
    public boolean isNull(String name);

    /**
     * Returns the value for {@code name} as a boolean.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a Java {@code boolean} (for CQL type {@code boolean}, this will be the built-in codec).
     *
     * @param name the name to retrieve.
     * @return the boolean value for {@code name}. If the value is NULL, {@code false} is returned. If
     * you need to distinguish NULL and false values, check first with {@link #isNull(String)} or
     * use {@code get(name, Boolean.class)}.
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the underlying CQL
     *                                  type to a boolean.
     */
    public boolean getBool(String name);

    /**
     * Returns the value for {@code name} as a byte.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a Java {@code byte} (for CQL type {@code tinyint}, this will be the built-in codec).
     *
     * @param name the name to retrieve.
     * @return the value for {@code name} as a byte. If the value is NULL, {@code 0} is returned. If
     * you need to distinguish NULL and 0, check first with {@link #isNull(String)} or use {@code
     * get(name, Byte.class)}. {@code 0} is returned.
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the underlying CQL
     *                                  type to a byte.
     */
    public byte getByte(String name);

    /**
     * Returns the value for {@code name} as a short.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a Java {@code short} (for CQL type {@code smallint}, this will be the built-in codec).
     *
     * @param name the name to retrieve.
     * @return the value for {@code name} as a short. If the value is NULL, {@code 0} is returned. If
     * you need to distinguish NULL and 0, check first with {@link #isNull(String)} or use {@code
     * get(name, Short.class)}. {@code 0} is returned.
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the underlying CQL
     *                                  type to a short.
     */
    public short getShort(String name);

    /**
     * Returns the value for {@code name} as an integer.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a Java {@code int} (for CQL type {@code int}, this will be the built-in codec).
     *
     * @param name the name to retrieve.
     * @return the value for {@code name} as an integer. If the value is NULL, {@code 0} is returned.
     * If you need to distinguish NULL and 0, check first with {@link #isNull(String)} or use
     * {@code get(name, Integer.class)}. {@code 0} is returned.
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the underlying CQL
     *                                  type to an int.
     */
    public int getInt(String name);

    /**
     * Returns the value for {@code name} as a long.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a Java {@code byte} (for CQL types {@code bigint} and {@code counter}, this will be the
     * built-in codec).
     *
     * @param name the name to retrieve.
     * @return the value for {@code name} as a long. If the value is NULL, {@code 0L} is returned. If
     * you need to distinguish NULL and 0L, check first with {@link #isNull(String)} or use {@code
     * get(name, Long.class)}.
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the underlying CQL
     *                                  type to a long.
     */
    public long getLong(String name);

    /**
     * Returns the value for {@code name} as a date.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a {@code Date} (for CQL type {@code timestamp}, this will be the built-in codec).
     *
     * @param name the name to retrieve.
     * @return the value for {@code name} as a date. If the value is NULL, {@code null} is returned.
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the underlying CQL
     *                                  type to a {@code Date}.
     */
    public Date getTimestamp(String name);

    /**
     * Returns the value for {@code name} as a date (without time).
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a {@link LocalDate} (for CQL type {@code date}, this will be the built-in codec).
     *
     * @param name the name to retrieve.
     * @return the value for {@code name} as a date. If the value is NULL, {@code null} is returned.
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the underlying CQL
     *                                  type to a {@code LocalDate}.
     */
    public LocalDate getDate(String name);

    /**
     * Returns the value for {@code name} as a long in nanoseconds since midnight.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a Java {@code long} (for CQL type {@code time}, this will be the built-in codec).
     *
     * @param name the name to retrieve.
     * @return the value for {@code name} as a long. If the value is NULL, {@code 0L} is returned.
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the underlying CQL
     *                                  type to a long.
     */
    public long getTime(String name);

    /**
     * Returns the value for {@code name} as a float.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a Java {@code float} (for CQL type {@code float}, this will be the built-in codec).
     *
     * @param name the name to retrieve.
     * @return the value for {@code name} as a float. If the value is NULL, {@code 0.0f} is returned.
     * If you need to distinguish NULL and 0.0f, check first with {@link #isNull(String)} or use
     * {@code get(name, Float.class)}.
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the underlying CQL
     *                                  type to a float.
     */
    public float getFloat(String name);

    /**
     * Returns the value for {@code name} as a double.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a Java {@code double} (for CQL type {@code double}, this will be the built-in codec).
     *
     * @param name the name to retrieve.
     * @return the value for {@code name} as a double. If the value is NULL, {@code 0.0} is returned.
     * If you need to distinguish NULL and 0.0, check first with {@link #isNull(String)} or use
     * {@code get(name, Double.class)}.
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the underlying CQL
     *                                  type to a double.
     */
    public double getDouble(String name);

    /**
     * Returns the value for {@code name} as a ByteBuffer.
     *
     * <p>This method does not use any codec; it returns a copy of the binary representation of the
     * value. It is up to the caller to convert the returned value appropriately.
     *
     * <p>Note: this method always return the bytes composing the value, even if the column is not of
     * type BLOB. That is, this method never throw an InvalidTypeException. However, if the type is
     * not BLOB, it is up to the caller to handle the returned value correctly.
     *
     * @param name the name to retrieve.
     * @return the value for {@code name} as a ByteBuffer. If the value is NULL, {@code null} is
     * returned.
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     */
    public ByteBuffer getBytesUnsafe(String name);

    /**
     * Returns the value for {@code name} as a byte array.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a Java {@code ByteBuffer} (for CQL type {@code blob}, this will be the built-in codec).
     *
     * @param name the name to retrieve.
     * @return the value for {@code name} as a byte array. If the value is NULL, {@code null} is
     * returned.
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the underlying CQL
     *                                  type to a {@code ByteBuffer}.
     */
    public ByteBuffer getBytes(String name);

    /**
     * Returns the value for {@code name} as a string.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a Java string (for CQL types {@code text}, {@code varchar} and {@code ascii}, this will
     * be the built-in codec).
     *
     * @param name the name to retrieve.
     * @return the value for {@code name} as a string. If the value is NULL, {@code null} is returned.
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the underlying CQL
     *                                  type to a string.
     */
    public String getString(String name);

    /**
     * Returns the value for {@code name} as a variable length integer.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a {@code BigInteger} (for CQL type {@code varint}, this will be the built-in codec).
     *
     * @param name the name to retrieve.
     * @return the value for {@code name} as a variable length integer. If the value is NULL, {@code
     * null} is returned.
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the underlying CQL
     *                                  type to a {@code BigInteger}.
     */
    public BigInteger getVarint(String name);

    /**
     * Returns the value for {@code name} as a variable length decimal.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a {@code BigDecimal} (for CQL type {@code decimal}, this will be the built-in codec).
     *
     * @param name the name to retrieve.
     * @return the value for {@code name} as a variable length decimal. If the value is NULL, {@code
     * null} is returned.
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the underlying CQL
     *                                  type to a {@code BigDecimal}.
     */
    public BigDecimal getDecimal(String name);

    /**
     * Returns the value for {@code name} as a UUID.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a {@code UUID} (for CQL types {@code uuid} and {@code timeuuid}, this will be the
     * built-in codec).
     *
     * @param name the name to retrieve.
     * @return the value for {@code name} as a UUID. If the value is NULL, {@code null} is returned.
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the underlying CQL
     *                                  type to a {@code UUID}.
     */
    public UUID getUUID(String name);

    /**
     * Returns the value for {@code name} as an InetAddress.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to an {@code InetAddress} (for CQL type {@code inet}, this will be the built-in codec).
     *
     * @param name the name to retrieve.
     * @return the value for {@code name} as an InetAddress. If the value is NULL, {@code null} is
     * returned.
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the underlying CQL
     *                                  type to a {@code InetAddress}.
     */
    public InetAddress getInet(String name);

    /**
     * Returns the value for {@code name} as a list.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a list of the specified type.
     *
     * <p>If the type of the elements is generic, use {@link #getList(String, TypeToken)}.
     *
     * <p>Implementation note: the actual {@link List} implementation will depend on the {@link
     * TypeCodec codec} being used; therefore, callers should make no assumptions concerning its
     * mutability nor its thread-safety. Furthermore, the behavior of this method in respect to CQL
     * {@code NULL} values is also codec-dependent. By default, the driver will return mutable
     * instances, and a CQL {@code NULL} will mapped to an empty collection (note that Cassandra makes
     * no distinction between {@code NULL} and an empty collection).
     *
     * @param name          the name to retrieve.
     * @param elementsClass the class for the elements of the list to retrieve.
     * @return the value of the {@code i}th element as a list of {@code T} objects.
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the underlying CQL
     *                                  type to a list.
     */
    public <T> List<T> getList(String name, Class<T> elementsClass);

    /**
     * Returns the value for {@code name} as a list.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a list of the specified type.
     *
     * <p>Use this variant with nested collections, which produce a generic element type:
     *
     * <pre>
     * {@code List<List<String>> l = row.getList("theColumn", new TypeToken<List<String>>() {});}
     * </pre>
     *
     * <p>Implementation note: the actual {@link List} implementation will depend on the {@link
     * TypeCodec codec} being used; therefore, callers should make no assumptions concerning its
     * mutability nor its thread-safety. Furthermore, the behavior of this method in respect to CQL
     * {@code NULL} values is also codec-dependent. By default, the driver will return mutable
     * instances, and a CQL {@code NULL} will mapped to an empty collection (note that Cassandra makes
     * no distinction between {@code NULL} and an empty collection).
     *
     * @param name         the name to retrieve.
     * @param elementsType the type for the elements of the list to retrieve.
     * @return the value of the {@code i}th element as a list of {@code T} objects.
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the underlying CQL
     *                                  type to a list.
     */
    public <T> List<T> getList(String name, TypeToken<T> elementsType);

    /**
     * Returns the value for {@code name} as a set.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a set of the specified type.
     *
     * <p>If the type of the elements is generic, use {@link #getSet(String, TypeToken)}.
     *
     * <p>Implementation note: the actual {@link Set} implementation will depend on the {@link
     * TypeCodec codec} being used; therefore, callers should make no assumptions concerning its
     * mutability nor its thread-safety. Furthermore, the behavior of this method in respect to CQL
     * {@code NULL} values is also codec-dependent. By default, the driver will return mutable
     * instances, and a CQL {@code NULL} will mapped to an empty collection (note that Cassandra makes
     * no distinction between {@code NULL} and an empty collection).
     *
     * @param name          the name to retrieve.
     * @param elementsClass the class for the elements of the set to retrieve.
     * @return the value of the {@code i}th element as a set of {@code T} objects.
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the underlying CQL
     *                                  type to a set.
     */
    public <T> Set<T> getSet(String name, Class<T> elementsClass);

    /**
     * Returns the value for {@code name} as a set.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a set of the specified type.
     *
     * <p>Use this variant with nested collections, which produce a generic element type:
     *
     * <pre>
     * {@code Set<List<String>> l = row.getSet("theColumn", new TypeToken<List<String>>() {});}
     * </pre>
     *
     * <p>Implementation note: the actual {@link Set} implementation will depend on the {@link
     * TypeCodec codec} being used; therefore, callers should make no assumptions concerning its
     * mutability nor its thread-safety. Furthermore, the behavior of this method in respect to CQL
     * {@code NULL} values is also codec-dependent. By default, the driver will return mutable
     * instances, and a CQL {@code NULL} will mapped to an empty collection (note that Cassandra makes
     * no distinction between {@code NULL} and an empty collection).
     *
     * @param name         the name to retrieve.
     * @param elementsType the type for the elements of the set to retrieve.
     * @return the value of the {@code i}th element as a set of {@code T} objects.
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the underlying CQL
     *                                  type to a set.
     */
    public <T> Set<T> getSet(String name, TypeToken<T> elementsType);

    /**
     * Returns the value for {@code name} as a map.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a map of the specified types.
     *
     * <p>If the type of the keys and/or values is generic, use {@link #getMap(String, TypeToken,
     * TypeToken)}.
     *
     * <p>Implementation note: the actual {@link Map} implementation will depend on the {@link
     * TypeCodec codec} being used; therefore, callers should make no assumptions concerning its
     * mutability nor its thread-safety. Furthermore, the behavior of this method in respect to CQL
     * {@code NULL} values is also codec-dependent. By default, the driver will return mutable
     * instances, and a CQL {@code NULL} will mapped to an empty collection (note that Cassandra makes
     * no distinction between {@code NULL} and an empty collection).
     *
     * @param name        the name to retrieve.
     * @param keysClass   the class for the keys of the map to retrieve.
     * @param valuesClass the class for the values of the map to retrieve.
     * @return the value of {@code name} as a map of {@code K} to {@code V} objects.
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the underlying CQL
     *                                  type to a map.
     */
    public <K, V> Map<K, V> getMap(String name, Class<K> keysClass, Class<V> valuesClass);

    /**
     * Returns the value for {@code name} as a map.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a map of the specified types.
     *
     * <p>Use this variant with nested collections, which produce a generic element type:
     *
     * <pre>
     * {@code Map<Int, List<String>> l = row.getMap("theColumn", TypeToken.of(Integer.class), new TypeToken<List<String>>() {});}
     * </pre>
     *
     * <p>Implementation note: the actual {@link Map} implementation will depend on the {@link
     * TypeCodec codec} being used; therefore, callers should make no assumptions concerning its
     * mutability nor its thread-safety. Furthermore, the behavior of this method in respect to CQL
     * {@code NULL} values is also codec-dependent. By default, the driver will return mutable
     * instances, and a CQL {@code NULL} will mapped to an empty collection (note that Cassandra makes
     * no distinction between {@code NULL} and an empty collection).
     *
     * @param name       the name to retrieve.
     * @param keysType   the class for the keys of the map to retrieve.
     * @param valuesType the class for the values of the map to retrieve.
     * @return the value of {@code name} as a map of {@code K} to {@code V} objects.
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the underlying CQL
     *                                  type to a map.
     */
    public <K, V> Map<K, V> getMap(String name, TypeToken<K> keysType, TypeToken<V> valuesType);

    /**
     * Returns the value for {@code name} as a vector, represented as a Java list.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a vector of the specified type.
     *
     * <p>If the type of the elements is generic, use {@link #getVector(String, TypeToken)}.
     *
     * <p>Implementation note: the actual {@link List} implementation representing the vector will depend on the {@link
     * TypeCodec codec} being used; therefore, callers should make no assumptions concerning its mutability nor its
     * thread-safety.
     *
     * @param name          the name to retrieve.
     * @param elementsClass the class for the elements of the vector to retrieve.
     * @return the value of the {@code i}th element as a vector of {@code T} objects.
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the underlying CQL type to a vector.
     */
    public <T> List<T> getVector(String name, Class<T> elementsClass);

    /**
     * Returns the value for {@code name} as a vector, represented as a Java list.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a vector of the specified type.
     *
     * <p>Use this variant with nested types, which produce a generic element type:
     *
     * <pre>
     * {@code List<List<String>> l = row.getVector("theColumn", new TypeToken<List<String>>() {});}
     * </pre>
     *
     * <p>Implementation note: the actual {@link List} implementation representing the vector will depend on the {@link
     * TypeCodec codec} being used; therefore, callers should make no assumptions concerning its mutability nor its
     * thread-safety.
     *
     * @param name         the name to retrieve.
     * @param elementsType the type for the elements of the vector to retrieve.
     * @return the value of the {@code i}th element as a vector of {@code T} objects.
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the underlying CQL type to a vector.
     */
    public <T> List<T> getVector(String name, TypeToken<T> elementsType);

    /**
     * Return the value for {@code name} as a UDT value.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a {@code UDTValue} (if the CQL type is a UDT, the registry will generate a codec
     * automatically).
     *
     * @param name the name to retrieve.
     * @return the value of {@code name} as a UDT value. If the value is NULL, then {@code null} will
     * be returned.
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the underlying CQL
     *                                  type to a {@code UDTValue}.
     */
    public UDTValue getUDTValue(String name);

    /**
     * Return the value for {@code name} as a tuple value.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to a {@code TupleValue} (if the CQL type is a tuple, the registry will generate a codec
     * automatically).
     *
     * @param name the name to retrieve.
     * @return the value of {@code name} as a tuple value. If the value is NULL, then {@code null}
     * will be returned.
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the underlying CQL
     *                                  type to a {@code TupleValue}.
     */
    public TupleValue getTupleValue(String name);

    /**
     * Returns the value for {@code name} as the Java type matching its CQL type.
     *
     * <p>This method uses the {@link CodecRegistry} to find the first codec that handles the
     * underlying CQL type. The Java type of the returned object will be determined by the codec that
     * was selected.
     *
     * <p>Use this method to dynamically inspect elements when types aren't known in advance, for
     * instance if you're writing a generic row logger. If you know the target Java type, it is
     * generally preferable to use typed getters, such as the ones for built-in types ({@link
     * #getBool(String)}, {@link #getInt(String)}, etc.), or {@link #get(String, Class)} and {@link
     * #get(String, TypeToken)} for custom types.
     *
     * @param name the name to retrieve.
     * @return the value of {@code name} as the Java type matching its CQL type. If the value is NULL
     * and is a simple type, UDT or tuple, {@code null} is returned. If it is NULL and is a
     * collection type, an empty (immutable) collection is returned.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @see CodecRegistry#codecFor(DataType)
     */
    Object getObject(String name);

    /**
     * Returns the value for {@code name} converted to the given Java type.
     *
     * <p>This method uses the {@link CodecRegistry} to find a codec to convert the underlying CQL
     * type to the given Java type.
     *
     * <p>If the target type is generic, use {@link #get(String, TypeToken)}.
     *
     * <p>Implementation note: the actual object returned by this method will depend on the {@link
     * TypeCodec codec} being used; therefore, callers should make no assumptions concerning its
     * mutability nor its thread-safety. Furthermore, the behavior of this method in respect to CQL
     * {@code NULL} values is also codec-dependent; by default, a CQL {@code NULL} value translates to
     * {@code null} for simple CQL types, UDTs and tuples, and to empty collections for all CQL
     * collection types.
     *
     * @param name        the name to retrieve.
     * @param targetClass The Java type the value should be converted to.
     * @return the value for {@code name} value converted to the given Java type.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the underlying CQL
     *                                  type to {@code targetClass}.
     */
    <T> T get(String name, Class<T> targetClass);

    /**
     * Returns the value for {@code name} converted to the given Java type.
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
     * @param name       the name to retrieve.
     * @param targetType The Java type the value should be converted to.
     * @return the value for {@code name} value converted to the given Java type.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws CodecNotFoundException   if there is no registered codec to convert the underlying CQL
     *                                  type to {@code targetType}.
     */
    <T> T get(String name, TypeToken<T> targetType);

    /**
     * Returns the value for {@code name} converted using the given {@link TypeCodec}.
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
     * @param name  the name to retrieve.
     * @param codec The {@link TypeCodec} to use to deserialize the value; may not be {@code null}.
     * @return the value of the {@code i}th value converted using the given {@link TypeCodec}.
     * @throws InvalidTypeException      if the given codec does not {@link TypeCodec#accepts(DataType)
     *                                   accept} the underlying CQL type.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     */
    <T> T get(String name, TypeCodec<T> codec);
}
