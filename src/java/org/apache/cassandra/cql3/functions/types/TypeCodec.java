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

import java.io.DataInput;
import java.io.IOException;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.*;
import java.util.regex.Pattern;

import com.google.common.io.ByteStreams;
import com.google.common.reflect.TypeToken;

import org.apache.cassandra.cql3.functions.types.exceptions.InvalidTypeException;
import org.apache.cassandra.cql3.functions.types.utils.Bytes;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.vint.VIntCoding;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.cassandra.cql3.functions.types.DataType.*;

/**
 * A Codec that can serialize and deserialize to and from a given {@link #getCqlType() CQL type} and
 * a given {@link #getJavaType() Java Type}.
 *
 * <p>
 *
 * <h3>Serializing and deserializing</h3>
 *
 * <p>Two methods handle the serialization and deserialization of Java types into CQL types
 * according to the native protocol specifications:
 *
 * <ol>
 * <li>{@link #serialize(Object, ProtocolVersion)}: used to serialize from the codec's Java type
 * to a {@link ByteBuffer} instance corresponding to the codec's CQL type;
 * <li>{@link #deserialize(ByteBuffer, ProtocolVersion)}: used to deserialize a {@link ByteBuffer}
 * instance corresponding to the codec's CQL type to the codec's Java type.
 * </ol>
 *
 * <p>
 *
 * <h3>Formatting and parsing</h3>
 *
 * <p>Two methods handle the formatting and parsing of Java types into CQL strings:
 *
 * <ol>
 * <li>{@link #format(Object)}: formats the Java type handled by the codec as a CQL string;
 * <li>{@link #parse(String)}; parses a CQL string into the Java type handled by the codec.
 * </ol>
 *
 * <p>
 *
 * <h3>Inspection</h3>
 *
 * <p>Codecs also have the following inspection methods:
 *
 * <p>
 *
 * <ol>
 * <li>{@link #accepts(DataType)}: returns true if the codec can deserialize the given CQL type;
 * <li>{@link #accepts(TypeToken)}: returns true if the codec can serialize the given Java type;
 * <li>{@link #accepts(Object)}; returns true if the codec can serialize the given object.
 * </ol>
 *
 * <p>
 *
 * <h3>Implementation notes</h3>
 *
 * <p>
 *
 * <ol>
 * <li>TypeCodec implementations <em>must</em> be thread-safe.
 * <li>TypeCodec implementations <em>must</em> perform fast and never block.
 * <li>TypeCodec implementations <em>must</em> support all native protocol versions; it is not
 * possible to use different codecs for the same types but under different protocol versions.
 * <li>TypeCodec implementations must comply with the native protocol specifications; failing to
 * do so will result in unexpected results and could cause the driver to crash.
 * <li>TypeCodec implementations <em>should</em> be stateless and immutable.
 * <li>TypeCodec implementations <em>should</em> interpret {@code null} values and empty
 * ByteBuffers (i.e. <code>{@link ByteBuffer#remaining()} == 0</code>) in a
 * <em>reasonable</em> way; usually, {@code NULL} CQL values should map to {@code null}
 * references, but exceptions exist; e.g. for varchar types, a {@code NULL} CQL value maps to
 * a {@code null} reference, whereas an empty buffer maps to an empty String. For collection
 * types, it is also admitted that {@code NULL} CQL values map to empty Java collections
 * instead of {@code null} references. In any case, the codec's behavior in respect to {@code
 * null} values and empty ByteBuffers should be clearly documented.
 * <li>TypeCodec implementations that wish to handle Java primitive types <em>must</em> be
 * instantiated with the wrapper Java class instead, and implement the appropriate interface
 * (e.g. {@link TypeCodec.PrimitiveBooleanCodec} for primitive {@code
 * boolean} types; there is one such interface for each Java primitive type).
 * <li>When deserializing, TypeCodec implementations should not consume {@link ByteBuffer}
 * instances by performing relative read operations that modify their current position; codecs
 * should instead prefer absolute read methods, or, if necessary, they should {@link
 * ByteBuffer#duplicate() duplicate} their byte buffers prior to reading them.
 * </ol>
 *
 * @param <T> The codec's Java type
 */
public abstract class TypeCodec<T>
{
    private final static int VARIABLE_LENGTH = -1;

    /**
     * Return the default codec for the CQL type {@code boolean}. The returned codec maps the CQL type
     * {@code boolean} into the Java type {@link Boolean}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code boolean}.
     */
    public static PrimitiveBooleanCodec cboolean()
    {
        return BooleanCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code tinyint}. The returned codec maps the CQL type
     * {@code tinyint} into the Java type {@link Byte}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code tinyint}.
     */
    public static PrimitiveByteCodec tinyInt()
    {
        return TinyIntCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code smallint}. The returned codec maps the CQL
     * type {@code smallint} into the Java type {@link Short}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code smallint}.
     */
    public static PrimitiveShortCodec smallInt()
    {
        return SmallIntCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code int}. The returned codec maps the CQL type
     * {@code int} into the Java type {@link Integer}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code int}.
     */
    public static PrimitiveIntCodec cint()
    {
        return IntCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code bigint}. The returned codec maps the CQL type
     * {@code bigint} into the Java type {@link Long}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code bigint}.
     */
    public static PrimitiveLongCodec bigint()
    {
        return BigintCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code counter}. The returned codec maps the CQL type
     * {@code counter} into the Java type {@link Long}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code counter}.
     */
    public static PrimitiveLongCodec counter()
    {
        return CounterCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code float}. The returned codec maps the CQL type
     * {@code float} into the Java type {@link Float}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code float}.
     */
    public static PrimitiveFloatCodec cfloat()
    {
        return FloatCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code double}. The returned codec maps the CQL type
     * {@code double} into the Java type {@link Double}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code double}.
     */
    public static PrimitiveDoubleCodec cdouble()
    {
        return DoubleCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code varint}. The returned codec maps the CQL type
     * {@code varint} into the Java type {@link BigInteger}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code varint}.
     */
    public static TypeCodec<BigInteger> varint()
    {
        return VarintCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code decimal}. The returned codec maps the CQL type
     * {@code decimal} into the Java type {@link BigDecimal}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code decimal}.
     */
    public static TypeCodec<BigDecimal> decimal()
    {
        return DecimalCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code ascii}. The returned codec maps the CQL type
     * {@code ascii} into the Java type {@link String}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code ascii}.
     */
    public static TypeCodec<String> ascii()
    {
        return AsciiCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code varchar}. The returned codec maps the CQL type
     * {@code varchar} into the Java type {@link String}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code varchar}.
     */
    public static TypeCodec<String> varchar()
    {
        return VarcharCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code blob}. The returned codec maps the CQL type
     * {@code blob} into the Java type {@link ByteBuffer}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code blob}.
     */
    public static TypeCodec<ByteBuffer> blob()
    {
        return BlobCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code date}. The returned codec maps the CQL type
     * {@code date} into the Java type {@link LocalDate}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code date}.
     */
    public static TypeCodec<LocalDate> date()
    {
        return DateCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code time}. The returned codec maps the CQL type
     * {@code time} into the Java type {@link Long}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code time}.
     */
    public static PrimitiveLongCodec time()
    {
        return TimeCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code timestamp}. The returned codec maps the CQL
     * type {@code timestamp} into the Java type {@link Date}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code timestamp}.
     */
    public static TypeCodec<Date> timestamp()
    {
        return TimestampCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code uuid}. The returned codec maps the CQL type
     * {@code uuid} into the Java type {@link UUID}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code uuid}.
     */
    public static TypeCodec<UUID> uuid()
    {
        return UUIDCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code timeuuid}. The returned codec maps the CQL
     * type {@code timeuuid} into the Java type {@link UUID}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code timeuuid}.
     */
    public static TypeCodec<UUID> timeUUID()
    {
        return TimeUUIDCodec.instance;
    }

    /**
     * Return the default codec for the CQL type {@code inet}. The returned codec maps the CQL type
     * {@code inet} into the Java type {@link InetAddress}. The returned instance is a singleton.
     *
     * @return the default codec for CQL type {@code inet}.
     */
    public static TypeCodec<InetAddress> inet()
    {
        return InetCodec.instance;
    }

    /**
     * Return a newly-created codec for the CQL type {@code list} whose element type is determined by
     * the given element codec. The returned codec maps the CQL type {@code list} into the Java type
     * {@link List}. This method does not cache returned instances and returns a newly-allocated
     * object at each invocation.
     *
     * @param elementCodec the codec that will handle elements of this list.
     * @return A newly-created codec for CQL type {@code list}.
     */
    public static <T> TypeCodec<List<T>> list(TypeCodec<T> elementCodec)
    {
        return new ListCodec<>(elementCodec);
    }

    /**
     * Return a newly-created codec for the CQL type {@code set} whose element type is determined by
     * the given element codec. The returned codec maps the CQL type {@code set} into the Java type
     * {@link Set}. This method does not cache returned instances and returns a newly-allocated object
     * at each invocation.
     *
     * @param elementCodec the codec that will handle elements of this set.
     * @return A newly-created codec for CQL type {@code set}.
     */
    public static <T> TypeCodec<Set<T>> set(TypeCodec<T> elementCodec)
    {
        return new SetCodec<>(elementCodec);
    }

    /**
     * Return a newly-created codec for the CQL type {@code map} whose key type and value type are
     * determined by the given codecs. The returned codec maps the CQL type {@code map} into the Java
     * type {@link Map}. This method does not cache returned instances and returns a newly-allocated
     * object at each invocation.
     *
     * @param keyCodec   the codec that will handle keys of this map.
     * @param valueCodec the codec that will handle values of this map.
     * @return A newly-created codec for CQL type {@code map}.
     */
    public static <K, V> TypeCodec<Map<K, V>> map(TypeCodec<K> keyCodec, TypeCodec<V> valueCodec)
    {
        return new MapCodec<>(keyCodec, valueCodec);
    }

    /**
     * Return a newly-created codec for the given CQL vector type. The returned codec maps the vector
     * type into the Java type {@link List}. This method does not cache returned instances and
     * returns a newly-allocated object at each invocation.
     *
     * @param type the vector type this codec should handle.
     * @return A newly-created codec for the given CQL tuple type.
     */
    public static <E> TypeCodec<List<E>> vector(VectorType type, TypeCodec<E> valueCodec)
    {
        return VectorCodec.of(type, valueCodec);
    }

    /**
     * Return a newly-created codec for the given user-defined CQL type. The returned codec maps the
     * user-defined type into the Java type {@link UDTValue}. This method does not cache returned
     * instances and returns a newly-allocated object at each invocation.
     *
     * @param type the user-defined type this codec should handle.
     * @return A newly-created codec for the given user-defined CQL type.
     */
    public static TypeCodec<UDTValue> userType(UserType type)
    {
        return new UDTCodec(type);
    }

    /**
     * Return a newly-created codec for the given CQL tuple type. The returned codec maps the tuple
     * type into the Java type {@link TupleValue}. This method does not cache returned instances and
     * returns a newly-allocated object at each invocation.
     *
     * @param type the tuple type this codec should handle.
     * @return A newly-created codec for the given CQL tuple type.
     */
    public static TypeCodec<TupleValue> tuple(TupleType type)
    {
        return new TupleCodec(type);
    }

    /**
     * Return a newly-created codec for the given CQL custom type.
     *
     * <p>The returned codec maps the custom type into the Java type {@link ByteBuffer}, thus
     * providing a (very lightweight) support for Cassandra types that do not have a CQL equivalent.
     *
     * <p>Note that the returned codec assumes that CQL literals for the given custom type are
     * expressed in binary form as well, e.g. {@code 0xcafebabe}. If this is not the case, <em>the
     * returned codec might be unable to {@link #parse(String) parse} and {@link #format(Object)
     * format} literals for this type</em>. This is notoriously true for types inheriting from {@code
     * org.apache.cassandra.db.marshal.AbstractCompositeType}, whose CQL literals are actually
     * expressed as quoted strings.
     *
     * <p>This method does not cache returned instances and returns a newly-allocated object at each
     * invocation.
     *
     * @param type the custom type this codec should handle.
     * @return A newly-created codec for the given CQL custom type.
     */
    public static TypeCodec<ByteBuffer> custom(DataType.CustomType type)
    {
        return new CustomCodec(type);
    }

    /**
     * Returns the default codec for the {@link DataType#duration() Duration type}.
     *
     * <p>This codec maps duration types to the driver's built-in {@link Duration} class, thus
     * providing a more user-friendly mapping than the low-level mapping provided by regular {@link
     * #custom(DataType.CustomType) custom type codecs}.
     *
     * <p>The returned instance is a singleton.
     *
     * @return the default codec for the Duration type.
     */
    public static TypeCodec<Duration> duration()
    {
        return DurationCodec.instance;
    }

    private final TypeToken<T> javaType;

    final DataType cqlType;

    /**
     * This constructor can only be used for non parameterized types. For parameterized ones, please
     * use {@link #TypeCodec(DataType, TypeToken)} instead.
     *
     * @param javaClass The Java class this codec serializes from and deserializes to.
     */
    protected TypeCodec(DataType cqlType, Class<T> javaClass)
    {
        this(cqlType, TypeToken.of(javaClass));
    }

    protected TypeCodec(DataType cqlType, TypeToken<T> javaType)
    {
        checkNotNull(cqlType, "cqlType cannot be null");
        checkNotNull(javaType, "javaType cannot be null");
        checkArgument(
        !javaType.isPrimitive(),
        "Cannot create a codec for a primitive Java type (%s), please use the wrapper type instead",
        javaType);
        this.cqlType = cqlType;
        this.javaType = javaType;
    }

    /**
     * Return the Java type that this codec deserializes to and serializes from.
     *
     * @return The Java type this codec deserializes to and serializes from.
     */
    public TypeToken<T> getJavaType()
    {
        return javaType;
    }

    /**
     * Return the CQL type that this codec deserializes from and serializes to.
     *
     * @return The Java type this codec deserializes from and serializes to.
     */
    public DataType getCqlType()
    {
        return cqlType;
    }

    /**
     * @return the length of values for this type if all values are of fixed length, {@link #VARIABLE_LENGTH} otherwise
     */
    public int serializedSize()
    {
        return VARIABLE_LENGTH;
    }

    /**
     * Checks if all values are of fixed length.
     *
     * @return {@code true} if all values are of fixed length, {@code false} otherwise.
     */
    public final boolean isSerializedSizeFixed()
    {
        return serializedSize() != VARIABLE_LENGTH;
    }

    /**
     * Serialize the given value according to the CQL type handled by this codec.
     *
     * <p>Implementation notes:
     *
     * <ol>
     * <li>Null values should be gracefully handled and no exception should be raised; these should
     * be considered as the equivalent of a NULL CQL value;
     * <li>Codecs for CQL collection types should not permit null elements;
     * <li>Codecs for CQL collection types should treat a {@code null} input as the equivalent of an
     * empty collection.
     * </ol>
     *
     * @param value           An instance of T; may be {@code null}.
     * @param protocolVersion the protocol version to use when serializing {@code bytes}. In most
     *                        cases, the proper value to provide for this argument is the value returned by {@code
     *                        ProtocolOptions#getProtocolVersion} (which is the protocol version in use by the driver).
     * @return A {@link ByteBuffer} instance containing the serialized form of T
     * @throws InvalidTypeException if the given value does not have the expected type
     */
    public abstract ByteBuffer serialize(T value, ProtocolVersion protocolVersion)
    throws InvalidTypeException;

    /**
     * Deserialize the given {@link ByteBuffer} instance according to the CQL type handled by this
     * codec.
     *
     * <p>Implementation notes:
     *
     * <ol>
     * <li>Null or empty buffers should be gracefully handled and no exception should be raised;
     * these should be considered as the equivalent of a NULL CQL value and, in most cases,
     * should map to {@code null} or a default value for the corresponding Java type, if
     * applicable;
     * <li>Codecs for CQL collection types should clearly document whether they return immutable
     * collections or not (note that the driver's default collection codecs return
     * <em>mutable</em> collections);
     * <li>Codecs for CQL collection types should avoid returning {@code null}; they should return
     * empty collections instead (the driver's default collection codecs all comply with this
     * rule).
     * <li>The provided {@link ByteBuffer} should never be consumed by read operations that modify
     * its current position; if necessary, {@link ByteBuffer#duplicate()} duplicate} it before
     * consuming.
     * </ol>
     *
     * @param bytes           A {@link ByteBuffer} instance containing the serialized form of T; may be {@code
     *                        null} or empty.
     * @param protocolVersion the protocol version to use when serializing {@code bytes}. In most
     *                        cases, the proper value to provide for this argument is the value returned by {@code
     *                        ProtocolOptions#getProtocolVersion} (which is the protocol version in use by the driver).
     * @return An instance of T
     * @throws InvalidTypeException if the given {@link ByteBuffer} instance cannot be deserialized
     */
    public abstract T deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)
    throws InvalidTypeException;

    /**
     * Parse the given CQL literal into an instance of the Java type handled by this codec.
     *
     * <p>Implementors should take care of unquoting and unescaping the given CQL string where
     * applicable. Null values and empty Strings should be accepted, as well as the string {@code
     * "NULL"}; in most cases, implementations should interpret these inputs has equivalent to a
     * {@code null} reference.
     *
     * <p>Implementing this method is not strictly mandatory: internally, the driver only uses it to
     * parse the INITCOND when building the metadata of an aggregate function (and in most cases it
     * will use a built-in codec, unless the INITCOND has a custom type).
     *
     * @param value The CQL string to parse, may be {@code null} or empty.
     * @return An instance of T; may be {@code null} on a {@code null input}.
     * @throws InvalidTypeException if the given value cannot be parsed into the expected type
     */
    public abstract T parse(String value) throws InvalidTypeException;

    /**
     * Format the given value as a valid CQL literal according to the CQL type handled by this codec.
     *
     * <p>Implementors should take care of quoting and escaping the resulting CQL literal where
     * applicable. Null values should be accepted; in most cases, implementations should return the
     * CQL keyword {@code "NULL"} for {@code null} inputs.
     *
     * <p>Implementing this method is not strictly mandatory. It is used:
     *
     * <ol>
     * <li>in the query builder, when values are inlined in the query string (see {@code
     * querybuilder.BuiltStatement} for a detailed explanation of when
     * this happens);
     * <li>in the {@code QueryLogger}, if parameter logging is enabled;
     * <li>to format the INITCOND in {@code AggregateMetadata#asCQLQuery(boolean)};
     * <li>in the {@code toString()} implementation of some objects ({@link UDTValue}, {@link
     * TupleValue}, and the internal representation of a {@code ROWS} response), which may
     * appear in driver logs.
     * </ol>
     * <p>
     * If you choose not to implement this method, you should not throw an exception but instead
     * return a constant string (for example "XxxCodec.format not implemented").
     *
     * @param value An instance of T; may be {@code null}.
     * @return CQL string
     * @throws InvalidTypeException if the given value does not have the expected type
     */
    public abstract String format(T value) throws InvalidTypeException;

    /**
     * Return {@code true} if this codec is capable of serializing the given {@code javaType}.
     *
     * <p>The implementation is <em>invariant</em> with respect to the passed argument (through the
     * usage of {@link TypeToken#equals(Object)} and <em>it's strongly recommended not to modify this
     * behavior</em>. This means that a codec will only ever return {@code true} for the
     * <em>exact</em> Java type that it has been created for.
     *
     * <p>If the argument represents a Java primitive type, its wrapper type is considered instead.
     *
     * @param javaType The Java type this codec should serialize from and deserialize to; cannot be
     *                 {@code null}.
     * @return {@code true} if the codec is capable of serializing the given {@code javaType}, and
     * {@code false} otherwise.
     * @throws NullPointerException if {@code javaType} is {@code null}.
     */
    public boolean accepts(TypeToken<?> javaType)
    {
        checkNotNull(javaType, "Parameter javaType cannot be null");
        return this.javaType.equals(javaType.wrap());
    }

    /**
     * Return {@code true} if this codec is capable of serializing the given {@code javaType}.
     *
     * <p>This implementation simply calls {@link #accepts(TypeToken)}.
     *
     * @param javaType The Java type this codec should serialize from and deserialize to; cannot be
     *                 {@code null}.
     * @return {@code true} if the codec is capable of serializing the given {@code javaType}, and
     * {@code false} otherwise.
     * @throws NullPointerException if {@code javaType} is {@code null}.
     */
    public boolean accepts(Class<?> javaType)
    {
        checkNotNull(javaType, "Parameter javaType cannot be null");
        return accepts(TypeToken.of(javaType));
    }

    /**
     * Return {@code true} if this codec is capable of deserializing the given {@code cqlType}.
     *
     * @param cqlType The CQL type this codec should deserialize from and serialize to; cannot be
     *                {@code null}.
     * @return {@code true} if the codec is capable of deserializing the given {@code cqlType}, and
     * {@code false} otherwise.
     * @throws NullPointerException if {@code cqlType} is {@code null}.
     */
    public boolean accepts(DataType cqlType)
    {
        checkNotNull(cqlType, "Parameter cqlType cannot be null");
        return this.cqlType.equals(cqlType);
    }

    /**
     * Return {@code true} if this codec is capable of serializing the given object. Note that the
     * object's Java type is inferred from the object' runtime (raw) type, contrary to {@link
     * #accepts(TypeToken)} which is capable of handling generic types.
     *
     * <p>This method is intended mostly to be used by the QueryBuilder when no type information is
     * available when the codec is used.
     *
     * <p>Implementation notes:
     *
     * <ol>
     * <li>The default implementation is <em>covariant</em> with respect to the passed argument
     * (through the usage of {@code TypeToken#isAssignableFrom(TypeToken)} or {@link
     * TypeToken#isSupertypeOf(Type)}) and <em>it's strongly recommended not to modify this
     * behavior</em>. This means that, by default, a codec will accept <em>any subtype</em> of
     * the Java type that it has been created for.
     * <li>The base implementation provided here can only handle non-parameterized types; codecs
     * handling parameterized types, such as collection types, must override this method and
     * perform some sort of "manual" inspection of the actual type parameters.
     * <li>Similarly, codecs that only accept a partial subset of all possible values must override
     * this method and manually inspect the object to check if it complies or not with the
     * codec's limitations.
     * </ol>
     *
     * @param value The Java type this codec should serialize from and deserialize to; cannot be
     *              {@code null}.
     * @return {@code true} if the codec is capable of serializing the given {@code javaType}, and
     * {@code false} otherwise.
     * @throws NullPointerException if {@code value} is {@code null}.
     */
    public boolean accepts(Object value)
    {
        checkNotNull(value, "Parameter value cannot be null");
        return this.javaType.isSupertypeOf(TypeToken.of(value.getClass()));
    }

    @Override
    public String toString()
    {
        return String.format("%s [%s <-> %s]", this.getClass().getSimpleName(), cqlType, javaType);
    }

    /**
     * A codec that is capable of handling primitive booleans, thus avoiding the overhead of boxing
     * and unboxing such primitives.
     */
    public abstract static class PrimitiveBooleanCodec extends TypeCodec<Boolean>
    {

        PrimitiveBooleanCodec(DataType cqlType)
        {
            super(cqlType, Boolean.class);
        }

        public abstract ByteBuffer serializeNoBoxing(boolean v, ProtocolVersion protocolVersion);

        public abstract boolean deserializeNoBoxing(ByteBuffer v, ProtocolVersion protocolVersion);

        @Override
        public ByteBuffer serialize(Boolean value, ProtocolVersion protocolVersion)
        {
            return value == null ? null : serializeNoBoxing(value, protocolVersion);
        }

        @Override
        public Boolean deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)
        {
            return bytes == null || bytes.remaining() == 0
                   ? null
                   : deserializeNoBoxing(bytes, protocolVersion);
        }
    }

    /**
     * A codec that is capable of handling primitive bytes, thus avoiding the overhead of boxing and
     * unboxing such primitives.
     */
    public abstract static class PrimitiveByteCodec extends TypeCodec<Byte>
    {

        PrimitiveByteCodec(DataType cqlType)
        {
            super(cqlType, Byte.class);
        }

        public abstract ByteBuffer serializeNoBoxing(byte v, ProtocolVersion protocolVersion);

        public abstract byte deserializeNoBoxing(ByteBuffer v, ProtocolVersion protocolVersion);

        @Override
        public ByteBuffer serialize(Byte value, ProtocolVersion protocolVersion)
        {
            return value == null ? null : serializeNoBoxing(value, protocolVersion);
        }

        @Override
        public Byte deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)
        {
            return bytes == null || bytes.remaining() == 0
                   ? null
                   : deserializeNoBoxing(bytes, protocolVersion);
        }
    }

    /**
     * A codec that is capable of handling primitive shorts, thus avoiding the overhead of boxing and
     * unboxing such primitives.
     */
    public abstract static class PrimitiveShortCodec extends TypeCodec<Short>
    {

        PrimitiveShortCodec(DataType cqlType)
        {
            super(cqlType, Short.class);
        }

        public abstract ByteBuffer serializeNoBoxing(short v, ProtocolVersion protocolVersion);

        public abstract short deserializeNoBoxing(ByteBuffer v, ProtocolVersion protocolVersion);

        @Override
        public ByteBuffer serialize(Short value, ProtocolVersion protocolVersion)
        {
            return value == null ? null : serializeNoBoxing(value, protocolVersion);
        }

        @Override
        public Short deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)
        {
            return bytes == null || bytes.remaining() == 0
                   ? null
                   : deserializeNoBoxing(bytes, protocolVersion);
        }
    }

    /**
     * A codec that is capable of handling primitive ints, thus avoiding the overhead of boxing and
     * unboxing such primitives.
     */
    public abstract static class PrimitiveIntCodec extends TypeCodec<Integer>
    {

        PrimitiveIntCodec(DataType cqlType)
        {
            super(cqlType, Integer.class);
        }

        public abstract ByteBuffer serializeNoBoxing(int v, ProtocolVersion protocolVersion);

        public abstract int deserializeNoBoxing(ByteBuffer v, ProtocolVersion protocolVersion);

        @Override
        public ByteBuffer serialize(Integer value, ProtocolVersion protocolVersion)
        {
            return value == null ? null : serializeNoBoxing(value, protocolVersion);
        }

        @Override
        public Integer deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)
        {
            return bytes == null || bytes.remaining() == 0
                   ? null
                   : deserializeNoBoxing(bytes, protocolVersion);
        }
    }

    /**
     * A codec that is capable of handling primitive longs, thus avoiding the overhead of boxing and
     * unboxing such primitives.
     */
    public abstract static class PrimitiveLongCodec extends TypeCodec<Long>
    {

        PrimitiveLongCodec(DataType cqlType)
        {
            super(cqlType, Long.class);
        }

        public abstract ByteBuffer serializeNoBoxing(long v, ProtocolVersion protocolVersion);

        public abstract long deserializeNoBoxing(ByteBuffer v, ProtocolVersion protocolVersion);

        @Override
        public ByteBuffer serialize(Long value, ProtocolVersion protocolVersion)
        {
            return value == null ? null : serializeNoBoxing(value, protocolVersion);
        }

        @Override
        public Long deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)
        {
            return bytes == null || bytes.remaining() == 0
                   ? null
                   : deserializeNoBoxing(bytes, protocolVersion);
        }
    }

    /**
     * A codec that is capable of handling primitive floats, thus avoiding the overhead of boxing and
     * unboxing such primitives.
     */
    public abstract static class PrimitiveFloatCodec extends TypeCodec<Float>
    {

        PrimitiveFloatCodec(DataType cqlType)
        {
            super(cqlType, Float.class);
        }

        public abstract ByteBuffer serializeNoBoxing(float v, ProtocolVersion protocolVersion);

        public abstract float deserializeNoBoxing(ByteBuffer v, ProtocolVersion protocolVersion);

        @Override
        public ByteBuffer serialize(Float value, ProtocolVersion protocolVersion)
        {
            return value == null ? null : serializeNoBoxing(value, protocolVersion);
        }

        @Override
        public Float deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)
        {
            return bytes == null || bytes.remaining() == 0
                   ? null
                   : deserializeNoBoxing(bytes, protocolVersion);
        }
    }

    /**
     * A codec that is capable of handling primitive doubles, thus avoiding the overhead of boxing and
     * unboxing such primitives.
     */
    public abstract static class PrimitiveDoubleCodec extends TypeCodec<Double>
    {

        PrimitiveDoubleCodec(DataType cqlType)
        {
            super(cqlType, Double.class);
        }

        public abstract ByteBuffer serializeNoBoxing(double v, ProtocolVersion protocolVersion);

        public abstract double deserializeNoBoxing(ByteBuffer v, ProtocolVersion protocolVersion);

        @Override
        public ByteBuffer serialize(Double value, ProtocolVersion protocolVersion)
        {
            return value == null ? null : serializeNoBoxing(value, protocolVersion);
        }

        @Override
        public Double deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)
        {
            return bytes == null || bytes.remaining() == 0
                   ? null
                   : deserializeNoBoxing(bytes, protocolVersion);
        }
    }

    /**
     * Base class for codecs handling CQL string types such as {@link DataType#varchar()}, {@link
     * DataType#text()} or {@link DataType#ascii()}.
     */
    private abstract static class StringCodec extends TypeCodec<String>
    {

        private final Charset charset;

        private StringCodec(DataType cqlType, Charset charset)
        {
            super(cqlType, String.class);
            this.charset = charset;
        }

        @Override
        public String parse(String value)
        {
            if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")) return null;
            if (!ParseUtils.isQuoted(value))
                throw new InvalidTypeException("text or varchar values must be enclosed by single quotes");

            return ParseUtils.unquote(value);
        }

        @Override
        public String format(String value)
        {
            if (value == null) return "NULL";
            return ParseUtils.quote(value);
        }

        @Override
        public ByteBuffer serialize(String value, ProtocolVersion protocolVersion)
        {
            return value == null ? null : ByteBuffer.wrap(value.getBytes(charset));
        }

        /**
         * {@inheritDoc}
         *
         * <p>Implementation note: this method treats {@code null}s and empty buffers differently: the
         * formers are mapped to {@code null}s while the latters are mapped to empty strings.
         */
        @Override
        public String deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)
        {
            if (bytes == null) return null;
            if (bytes.remaining() == 0) return "";
            return new String(Bytes.getArray(bytes), charset);
        }
    }

    /**
     * This codec maps a CQL {@link DataType#varchar()} to a Java {@link String}. Note that this codec
     * also handles {@link DataType#text()}, which is merely an alias for {@link DataType#varchar()}.
     */
    private static class VarcharCodec extends StringCodec
    {

        private static final VarcharCodec instance = new VarcharCodec();

        private VarcharCodec()
        {
            super(DataType.varchar(), Charset.forName("UTF-8"));
        }
    }

    /**
     * This codec maps a CQL {@link DataType#ascii()} to a Java {@link String}.
     */
    private static class AsciiCodec extends StringCodec
    {

        private static final AsciiCodec instance = new AsciiCodec();

        private static final Pattern ASCII_PATTERN = Pattern.compile("^\\p{ASCII}*$");

        private AsciiCodec()
        {
            super(DataType.ascii(), Charset.forName("US-ASCII"));
        }

        @Override
        public ByteBuffer serialize(String value, ProtocolVersion protocolVersion)
        {
            if (value != null && !ASCII_PATTERN.matcher(value).matches())
            {
                throw new InvalidTypeException(String.format("%s is not a valid ASCII String", value));
            }
            return super.serialize(value, protocolVersion);
        }

        @Override
        public String format(String value)
        {
            if (value != null && !ASCII_PATTERN.matcher(value).matches())
            {
                throw new InvalidTypeException(String.format("%s is not a valid ASCII String", value));
            }
            return super.format(value);
        }
    }

    /**
     * Base class for codecs handling CQL 8-byte integer types such as {@link DataType#bigint()},
     * {@link DataType#counter()} or {@link DataType#time()}.
     */
    private abstract static class LongCodec extends PrimitiveLongCodec
    {

        private LongCodec(DataType cqlType)
        {
            super(cqlType);
        }

        @Override
        public int serializedSize()
        {
            return 8;
        }

        @Override
        public Long parse(String value)
        {
            try
            {
                return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")
                       ? null
                       : Long.parseLong(value);
            }
            catch (NumberFormatException e)
            {
                throw new InvalidTypeException(
                String.format("Cannot parse 64-bits long value from \"%s\"", value));
            }
        }

        @Override
        public String format(Long value)
        {
            if (value == null) return "NULL";
            return Long.toString(value);
        }

        @Override
        public ByteBuffer serializeNoBoxing(long value, ProtocolVersion protocolVersion)
        {
            ByteBuffer bb = ByteBuffer.allocate(8);
            bb.putLong(0, value);
            return bb;
        }

        @Override
        public long deserializeNoBoxing(ByteBuffer bytes, ProtocolVersion protocolVersion)
        {
            if (bytes == null || bytes.remaining() == 0) return 0;
            if (bytes.remaining() != 8)
                throw new InvalidTypeException(
                "Invalid 64-bits long value, expecting 8 bytes but got " + bytes.remaining());

            return bytes.getLong(bytes.position());
        }
    }

    /**
     * This codec maps a CQL {@link DataType#bigint()} to a Java {@link Long}.
     */
    private static class BigintCodec extends LongCodec
    {

        private static final BigintCodec instance = new BigintCodec();

        private BigintCodec()
        {
            super(DataType.bigint());
        }
    }

    /**
     * This codec maps a CQL {@link DataType#counter()} to a Java {@link Long}.
     */
    private static class CounterCodec extends LongCodec
    {

        private static final CounterCodec instance = new CounterCodec();

        private CounterCodec()
        {
            super(DataType.counter());
        }
    }

    /**
     * This codec maps a CQL {@link DataType#blob()} to a Java {@link ByteBuffer}.
     */
    private static class BlobCodec extends TypeCodec<ByteBuffer>
    {

        private static final BlobCodec instance = new BlobCodec();

        private BlobCodec()
        {
            super(DataType.blob(), ByteBuffer.class);
        }

        @Override
        public ByteBuffer parse(String value)
        {
            return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")
                   ? null
                   : Bytes.fromHexString(value);
        }

        @Override
        public String format(ByteBuffer value)
        {
            if (value == null) return "NULL";
            return Bytes.toHexString(value);
        }

        @Override
        public ByteBuffer serialize(ByteBuffer value, ProtocolVersion protocolVersion)
        {
            return value == null ? null : value.duplicate();
        }

        @Override
        public ByteBuffer deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)
        {
            return bytes == null ? null : bytes.duplicate();
        }
    }

    /**
     * This codec maps a CQL {@link DataType#custom(String) custom} type to a Java {@link ByteBuffer}.
     * Note that no instance of this codec is part of the default set of codecs used by the Java
     * driver; instances of this codec must be manually registered.
     */
    private static class CustomCodec extends TypeCodec<ByteBuffer>
    {

        private CustomCodec(DataType custom)
        {
            super(custom, ByteBuffer.class);
            assert custom.getName() == Name.CUSTOM;
        }

        @Override
        public ByteBuffer parse(String value)
        {
            return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")
                   ? null
                   : Bytes.fromHexString(value);
        }

        @Override
        public String format(ByteBuffer value)
        {
            if (value == null) return "NULL";
            return Bytes.toHexString(value);
        }

        @Override
        public ByteBuffer serialize(ByteBuffer value, ProtocolVersion protocolVersion)
        {
            return value == null ? null : value.duplicate();
        }

        @Override
        public ByteBuffer deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)
        {
            return bytes == null ? null : bytes.duplicate();
        }
    }

    /**
     * This codec maps a CQL {@link DataType#cboolean()} to a Java {@link Boolean}.
     */
    private static class BooleanCodec extends PrimitiveBooleanCodec
    {

        private static final ByteBuffer TRUE = ByteBuffer.wrap(new byte[]{ 1 });
        private static final ByteBuffer FALSE = ByteBuffer.wrap(new byte[]{ 0 });

        private static final BooleanCodec instance = new BooleanCodec();

        private BooleanCodec()
        {
            super(DataType.cboolean());
        }

        @Override
        public int serializedSize()
        {
            return 1;
        }

        @Override
        public Boolean parse(String value)
        {
            if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")) return null;
            if (value.equalsIgnoreCase(Boolean.FALSE.toString())) return false;
            if (value.equalsIgnoreCase(Boolean.TRUE.toString())) return true;

            throw new InvalidTypeException(
            String.format("Cannot parse boolean value from \"%s\"", value));
        }

        @Override
        public String format(Boolean value)
        {
            if (value == null) return "NULL";
            return value ? "true" : "false";
        }

        @Override
        public ByteBuffer serializeNoBoxing(boolean value, ProtocolVersion protocolVersion)
        {
            return value ? TRUE.duplicate() : FALSE.duplicate();
        }

        @Override
        public boolean deserializeNoBoxing(ByteBuffer bytes, ProtocolVersion protocolVersion)
        {
            if (bytes == null || bytes.remaining() == 0) return false;
            if (bytes.remaining() != 1)
                throw new InvalidTypeException(
                "Invalid boolean value, expecting 1 byte but got " + bytes.remaining());

            return bytes.get(bytes.position()) != 0;
        }
    }

    /**
     * This codec maps a CQL {@link DataType#decimal()} to a Java {@link BigDecimal}.
     */
    private static class DecimalCodec extends TypeCodec<BigDecimal>
    {

        private static final DecimalCodec instance = new DecimalCodec();

        private DecimalCodec()
        {
            super(DataType.decimal(), BigDecimal.class);
        }

        @Override
        public BigDecimal parse(String value)
        {
            try
            {
                return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")
                       ? null
                       : new BigDecimal(value);
            }
            catch (NumberFormatException e)
            {
                throw new InvalidTypeException(
                String.format("Cannot parse decimal value from \"%s\"", value));
            }
        }

        @Override
        public String format(BigDecimal value)
        {
            if (value == null) return "NULL";
            return value.toString();
        }

        @Override
        public ByteBuffer serialize(BigDecimal value, ProtocolVersion protocolVersion)
        {
            if (value == null) return null;
            BigInteger bi = value.unscaledValue();
            int scale = value.scale();
            byte[] bibytes = bi.toByteArray();

            ByteBuffer bytes = ByteBuffer.allocate(4 + bibytes.length);
            bytes.putInt(scale);
            bytes.put(bibytes);
            bytes.rewind();
            return bytes;
        }

        @Override
        public BigDecimal deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)
        {
            if (bytes == null || bytes.remaining() == 0) return null;
            if (bytes.remaining() < 4)
                throw new InvalidTypeException(
                "Invalid decimal value, expecting at least 4 bytes but got " + bytes.remaining());

            bytes = bytes.duplicate();
            int scale = bytes.getInt();
            byte[] bibytes = new byte[bytes.remaining()];
            bytes.get(bibytes);

            BigInteger bi = new BigInteger(bibytes);
            return new BigDecimal(bi, scale);
        }
    }

    /**
     * This codec maps a CQL {@link DataType#cdouble()} to a Java {@link Double}.
     */
    private static class DoubleCodec extends PrimitiveDoubleCodec
    {

        private static final DoubleCodec instance = new DoubleCodec();

        private DoubleCodec()
        {
            super(DataType.cdouble());
        }

        @Override
        public int serializedSize()
        {
            return 8;
        }

        @Override
        public Double parse(String value)
        {
            try
            {
                return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")
                       ? null
                       : Double.parseDouble(value);
            }
            catch (NumberFormatException e)
            {
                throw new InvalidTypeException(
                String.format("Cannot parse 64-bits double value from \"%s\"", value));
            }
        }

        @Override
        public String format(Double value)
        {
            if (value == null) return "NULL";
            return Double.toString(value);
        }

        @Override
        public ByteBuffer serializeNoBoxing(double value, ProtocolVersion protocolVersion)
        {
            ByteBuffer bb = ByteBuffer.allocate(8);
            bb.putDouble(0, value);
            return bb;
        }

        @Override
        public double deserializeNoBoxing(ByteBuffer bytes, ProtocolVersion protocolVersion)
        {
            if (bytes == null || bytes.remaining() == 0) return 0;
            if (bytes.remaining() != 8)
                throw new InvalidTypeException(
                "Invalid 64-bits double value, expecting 8 bytes but got " + bytes.remaining());

            return bytes.getDouble(bytes.position());
        }
    }

    /**
     * This codec maps a CQL {@link DataType#cfloat()} to a Java {@link Float}.
     */
    private static class FloatCodec extends PrimitiveFloatCodec
    {

        private static final FloatCodec instance = new FloatCodec();

        private FloatCodec()
        {
            super(DataType.cfloat());
        }

        @Override
        public int serializedSize()
        {
            return 4;
        }

        @Override
        public Float parse(String value)
        {
            try
            {
                return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")
                       ? null
                       : Float.parseFloat(value);
            }
            catch (NumberFormatException e)
            {
                throw new InvalidTypeException(
                String.format("Cannot parse 32-bits float value from \"%s\"", value));
            }
        }

        @Override
        public String format(Float value)
        {
            if (value == null) return "NULL";
            return Float.toString(value);
        }

        @Override
        public ByteBuffer serializeNoBoxing(float value, ProtocolVersion protocolVersion)
        {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putFloat(0, value);
            return bb;
        }

        @Override
        public float deserializeNoBoxing(ByteBuffer bytes, ProtocolVersion protocolVersion)
        {
            if (bytes == null || bytes.remaining() == 0) return 0;
            if (bytes.remaining() != 4)
                throw new InvalidTypeException(
                "Invalid 32-bits float value, expecting 4 bytes but got " + bytes.remaining());

            return bytes.getFloat(bytes.position());
        }
    }

    /**
     * This codec maps a CQL {@link DataType#inet()} to a Java {@link InetAddress}.
     */
    private static class InetCodec extends TypeCodec<InetAddress>
    {

        private static final InetCodec instance = new InetCodec();

        private InetCodec()
        {
            super(DataType.inet(), InetAddress.class);
        }

        @Override
        public InetAddress parse(String value)
        {
            if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")) return null;

            value = value.trim();
            if (!ParseUtils.isQuoted(value))
                throw new InvalidTypeException(
                String.format("inet values must be enclosed in single quotes (\"%s\")", value));
            try
            {
                return InetAddress.getByName(value.substring(1, value.length() - 1));
            }
            catch (Exception e)
            {
                throw new InvalidTypeException(String.format("Cannot parse inet value from \"%s\"", value));
            }
        }

        @Override
        public String format(InetAddress value)
        {
            if (value == null) return "NULL";
            return '\'' + value.getHostAddress() + '\'';
        }

        @Override
        public ByteBuffer serialize(InetAddress value, ProtocolVersion protocolVersion)
        {
            return value == null ? null : ByteBuffer.wrap(value.getAddress());
        }

        @Override
        public InetAddress deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)
        {
            if (bytes == null || bytes.remaining() == 0) return null;
            try
            {
                return InetAddress.getByAddress(Bytes.getArray(bytes));
            }
            catch (UnknownHostException e)
            {
                throw new InvalidTypeException(
                "Invalid bytes for inet value, got " + bytes.remaining() + " bytes");
            }
        }
    }

    /**
     * This codec maps a CQL {@link DataType#tinyint()} to a Java {@link Byte}.
     */
    private static class TinyIntCodec extends PrimitiveByteCodec
    {

        private static final TinyIntCodec instance = new TinyIntCodec();

        private TinyIntCodec()
        {
            super(tinyint());
        }

        @Override
        public Byte parse(String value)
        {
            try
            {
                return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")
                       ? null
                       : Byte.parseByte(value);
            }
            catch (NumberFormatException e)
            {
                throw new InvalidTypeException(
                String.format("Cannot parse 8-bits int value from \"%s\"", value));
            }
        }

        @Override
        public String format(Byte value)
        {
            if (value == null) return "NULL";
            return Byte.toString(value);
        }

        @Override
        public ByteBuffer serializeNoBoxing(byte value, ProtocolVersion protocolVersion)
        {
            ByteBuffer bb = ByteBuffer.allocate(1);
            bb.put(0, value);
            return bb;
        }

        @Override
        public byte deserializeNoBoxing(ByteBuffer bytes, ProtocolVersion protocolVersion)
        {
            if (bytes == null || bytes.remaining() == 0) return 0;
            if (bytes.remaining() != 1)
                throw new InvalidTypeException(
                "Invalid 8-bits integer value, expecting 1 byte but got " + bytes.remaining());

            return bytes.get(bytes.position());
        }
    }

    /**
     * This codec maps a CQL {@link DataType#smallint()} to a Java {@link Short}.
     */
    private static class SmallIntCodec extends PrimitiveShortCodec
    {

        private static final SmallIntCodec instance = new SmallIntCodec();

        private SmallIntCodec()
        {
            super(smallint());
        }

        @Override
        public Short parse(String value)
        {
            try
            {
                return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")
                       ? null
                       : Short.parseShort(value);
            }
            catch (NumberFormatException e)
            {
                throw new InvalidTypeException(
                String.format("Cannot parse 16-bits int value from \"%s\"", value));
            }
        }

        @Override
        public String format(Short value)
        {
            if (value == null) return "NULL";
            return Short.toString(value);
        }

        @Override
        public ByteBuffer serializeNoBoxing(short value, ProtocolVersion protocolVersion)
        {
            ByteBuffer bb = ByteBuffer.allocate(2);
            bb.putShort(0, value);
            return bb;
        }

        @Override
        public short deserializeNoBoxing(ByteBuffer bytes, ProtocolVersion protocolVersion)
        {
            if (bytes == null || bytes.remaining() == 0) return 0;
            if (bytes.remaining() != 2)
                throw new InvalidTypeException(
                "Invalid 16-bits integer value, expecting 2 bytes but got " + bytes.remaining());

            return bytes.getShort(bytes.position());
        }
    }

    /**
     * This codec maps a CQL {@link DataType#cint()} to a Java {@link Integer}.
     */
    private static class IntCodec extends PrimitiveIntCodec
    {

        private static final IntCodec instance = new IntCodec();

        private IntCodec()
        {
            super(DataType.cint());
        }

        @Override
        public int serializedSize()
        {
            return 4;
        }

        @Override
        public Integer parse(String value)
        {
            try
            {
                return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")
                       ? null
                       : Integer.parseInt(value);
            }
            catch (NumberFormatException e)
            {
                throw new InvalidTypeException(
                String.format("Cannot parse 32-bits int value from \"%s\"", value));
            }
        }

        @Override
        public String format(Integer value)
        {
            if (value == null) return "NULL";
            return Integer.toString(value);
        }

        @Override
        public ByteBuffer serializeNoBoxing(int value, ProtocolVersion protocolVersion)
        {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(0, value);
            return bb;
        }

        @Override
        public int deserializeNoBoxing(ByteBuffer bytes, ProtocolVersion protocolVersion)
        {
            if (bytes == null || bytes.remaining() == 0) return 0;
            if (bytes.remaining() != 4)
                throw new InvalidTypeException(
                "Invalid 32-bits integer value, expecting 4 bytes but got " + bytes.remaining());

            return bytes.getInt(bytes.position());
        }
    }

    /**
     * This codec maps a CQL {@link DataType#timestamp()} to a Java {@link Date}.
     */
    private static class TimestampCodec extends TypeCodec<Date>
    {

        private static final TimestampCodec instance = new TimestampCodec();

        private TimestampCodec()
        {
            super(DataType.timestamp(), Date.class);
        }

        @Override
        public int serializedSize()
        {
            return 8;
        }

        @Override
        public Date parse(String value)
        {
            if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")) return null;
            // strip enclosing single quotes, if any
            if (ParseUtils.isQuoted(value)) value = ParseUtils.unquote(value);

            if (ParseUtils.isLongLiteral(value))
            {
                try
                {
                    return new Date(Long.parseLong(value));
                }
                catch (NumberFormatException e)
                {
                    throw new InvalidTypeException(
                    String.format("Cannot parse timestamp value from \"%s\"", value));
                }
            }

            try
            {
                return ParseUtils.parseDate(value);
            }
            catch (ParseException e)
            {
                throw new InvalidTypeException(
                String.format("Cannot parse timestamp value from \"%s\"", value));
            }
        }

        @Override
        public String format(Date value)
        {
            if (value == null) return "NULL";
            return Long.toString(value.getTime());
        }

        @Override
        public ByteBuffer serialize(Date value, ProtocolVersion protocolVersion)
        {
            return value == null
                   ? null
                   : BigintCodec.instance.serializeNoBoxing(value.getTime(), protocolVersion);
        }

        @Override
        public Date deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)
        {
            return bytes == null || bytes.remaining() == 0
                   ? null
                   : new Date(BigintCodec.instance.deserializeNoBoxing(bytes, protocolVersion));
        }
    }

    /**
     * This codec maps a CQL {@link DataType#date()} to the custom {@link LocalDate} class.
     */
    private static class DateCodec extends TypeCodec<LocalDate>
    {

        private static final DateCodec instance = new DateCodec();

        private static final String pattern = "yyyy-MM-dd";

        private DateCodec()
        {
            super(DataType.date(), LocalDate.class);
        }

        @Override
        public int serializedSize()
        {
            return 8;
        }

        @Override
        public LocalDate parse(String value)
        {
            if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")) return null;

            // single quotes are optional for long literals, mandatory for date patterns
            // strip enclosing single quotes, if any
            if (ParseUtils.isQuoted(value)) value = ParseUtils.unquote(value);

            if (ParseUtils.isLongLiteral(value))
            {
                long unsigned;
                try
                {
                    unsigned = Long.parseLong(value);
                }
                catch (NumberFormatException e)
                {
                    throw new InvalidTypeException(
                    String.format("Cannot parse date value from \"%s\"", value), e);
                }
                try
                {
                    int days = CodecUtils.fromCqlDateToDaysSinceEpoch(unsigned);
                    return LocalDate.fromDaysSinceEpoch(days);
                }
                catch (IllegalArgumentException e)
                {
                    throw new InvalidTypeException(
                    String.format("Cannot parse date value from \"%s\"", value), e);
                }
            }

            try
            {
                Date date = ParseUtils.parseDate(value, pattern);
                return LocalDate.fromMillisSinceEpoch(date.getTime());
            }
            catch (ParseException e)
            {
                throw new InvalidTypeException(
                String.format("Cannot parse date value from \"%s\"", value), e);
            }
        }

        @Override
        public String format(LocalDate value)
        {
            if (value == null) return "NULL";
            return ParseUtils.quote(value.toString());
        }

        @Override
        public ByteBuffer serialize(LocalDate value, ProtocolVersion protocolVersion)
        {
            if (value == null) return null;
            int unsigned = CodecUtils.fromSignedToUnsignedInt(value.getDaysSinceEpoch());
            return IntCodec.instance.serializeNoBoxing(unsigned, protocolVersion);
        }

        @Override
        public LocalDate deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)
        {
            if (bytes == null || bytes.remaining() == 0) return null;
            int unsigned = IntCodec.instance.deserializeNoBoxing(bytes, protocolVersion);
            int signed = CodecUtils.fromUnsignedToSignedInt(unsigned);
            return LocalDate.fromDaysSinceEpoch(signed);
        }
    }

    /**
     * This codec maps a CQL {@link DataType#time()} to a Java {@link Long}.
     */
    private static class TimeCodec extends LongCodec
    {

        private static final TimeCodec instance = new TimeCodec();

        private TimeCodec()
        {
            super(DataType.time());
        }

        @Override
        public Long parse(String value)
        {
            if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")) return null;

            // enclosing single quotes required, even for long literals
            if (!ParseUtils.isQuoted(value))
                throw new InvalidTypeException("time values must be enclosed by single quotes");
            value = value.substring(1, value.length() - 1);

            if (ParseUtils.isLongLiteral(value))
            {
                try
                {
                    return Long.parseLong(value);
                }
                catch (NumberFormatException e)
                {
                    throw new InvalidTypeException(
                    String.format("Cannot parse time value from \"%s\"", value), e);
                }
            }

            try
            {
                return ParseUtils.parseTime(value);
            }
            catch (ParseException e)
            {
                throw new InvalidTypeException(
                String.format("Cannot parse time value from \"%s\"", value), e);
            }
        }

        @Override
        public String format(Long value)
        {
            if (value == null) return "NULL";
            return ParseUtils.quote(ParseUtils.formatTime(value));
        }
    }

    /**
     * Base class for codecs handling CQL UUID types such as {@link DataType#uuid()} and {@link
     * DataType#timeuuid()}.
     */
    private abstract static class AbstractUUIDCodec extends TypeCodec<UUID>
    {

        private AbstractUUIDCodec(DataType cqlType)
        {
            super(cqlType, UUID.class);
        }

        @Override
        public int serializedSize()
        {
            return 16;
        }

        @Override
        public UUID parse(String value)
        {
            try
            {
                return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")
                       ? null
                       : UUID.fromString(value);
            }
            catch (IllegalArgumentException e)
            {
                throw new InvalidTypeException(
                String.format("Cannot parse UUID value from \"%s\"", value), e);
            }
        }

        @Override
        public String format(UUID value)
        {
            if (value == null) return "NULL";
            return value.toString();
        }

        @Override
        public ByteBuffer serialize(UUID value, ProtocolVersion protocolVersion)
        {
            if (value == null) return null;
            ByteBuffer bb = ByteBuffer.allocate(16);
            bb.putLong(0, value.getMostSignificantBits());
            bb.putLong(8, value.getLeastSignificantBits());
            return bb;
        }

        @Override
        public UUID deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)
        {
            return bytes == null || bytes.remaining() == 0
                   ? null
                   : new UUID(bytes.getLong(bytes.position()), bytes.getLong(bytes.position() + 8));
        }
    }

    /**
     * This codec maps a CQL {@link DataType#uuid()} to a Java {@link UUID}.
     */
    private static class UUIDCodec extends AbstractUUIDCodec
    {

        private static final UUIDCodec instance = new UUIDCodec();

        private UUIDCodec()
        {
            super(DataType.uuid());
        }
    }

    /**
     * This codec maps a CQL {@link DataType#timeuuid()} to a Java {@link UUID}.
     */
    private static class TimeUUIDCodec extends AbstractUUIDCodec
    {

        private static final TimeUUIDCodec instance = new TimeUUIDCodec();

        private TimeUUIDCodec()
        {
            super(timeuuid());
        }

        @Override
        public String format(UUID value)
        {
            if (value == null) return "NULL";
            if (value.version() != 1)
                throw new InvalidTypeException(
                String.format("%s is not a Type 1 (time-based) UUID", value));
            return super.format(value);
        }

        @Override
        public ByteBuffer serialize(UUID value, ProtocolVersion protocolVersion)
        {
            if (value == null) return null;
            if (value.version() != 1)
                throw new InvalidTypeException(
                String.format("%s is not a Type 1 (time-based) UUID", value));
            return super.serialize(value, protocolVersion);
        }
    }

    /**
     * This codec maps a CQL {@link DataType#varint()} to a Java {@link BigInteger}.
     */
    private static class VarintCodec extends TypeCodec<BigInteger>
    {

        private static final VarintCodec instance = new VarintCodec();

        private VarintCodec()
        {
            super(DataType.varint(), BigInteger.class);
        }

        @Override
        public BigInteger parse(String value)
        {
            try
            {
                return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")
                       ? null
                       : new BigInteger(value);
            }
            catch (NumberFormatException e)
            {
                throw new InvalidTypeException(
                String.format("Cannot parse varint value from \"%s\"", value), e);
            }
        }

        @Override
        public String format(BigInteger value)
        {
            if (value == null) return "NULL";
            return value.toString();
        }

        @Override
        public ByteBuffer serialize(BigInteger value, ProtocolVersion protocolVersion)
        {
            return value == null ? null : ByteBuffer.wrap(value.toByteArray());
        }

        @Override
        public BigInteger deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)
        {
            return bytes == null || bytes.remaining() == 0 ? null : new BigInteger(Bytes.getArray(bytes));
        }
    }

    /**
     * Base class for codecs mapping CQL {@link DataType#list(DataType) lists} and {@link
     * DataType#set(DataType) sets} to Java collections.
     */
    public abstract static class AbstractCollectionCodec<E, C extends Collection<E>>
    extends TypeCodec<C>
    {

        final TypeCodec<E> eltCodec;

        AbstractCollectionCodec(
        CollectionType cqlType, TypeToken<C> javaType, TypeCodec<E> eltCodec)
        {
            super(cqlType, javaType);
            checkArgument(
            cqlType.getName() == Name.LIST || cqlType.getName() == Name.SET,
            "Expecting list or set type, got %s",
            cqlType);
            this.eltCodec = eltCodec;
        }

        @Override
        public ByteBuffer serialize(C value, ProtocolVersion protocolVersion)
        {
            if (value == null) return null;
            int i = 0;
            ByteBuffer[] bbs = new ByteBuffer[value.size()];
            for (E elt : value)
            {
                if (elt == null)
                {
                    throw new NullPointerException("Collection elements cannot be null");
                }
                ByteBuffer bb;
                try
                {
                    bb = eltCodec.serialize(elt, protocolVersion);
                }
                catch (ClassCastException e)
                {
                    throw new InvalidTypeException(
                    String.format(
                    "Invalid type for %s element, expecting %s but got %s",
                    cqlType, eltCodec.getJavaType(), elt.getClass()),
                    e);
                }
                bbs[i++] = bb;
            }
            return CodecUtils.pack(bbs, value.size(), protocolVersion);
        }

        @Override
        public C deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)
        {
            if (bytes == null || bytes.remaining() == 0) return newInstance(0);
            try
            {
                ByteBuffer input = bytes.duplicate();
                int size = CodecUtils.readSize(input, protocolVersion);
                C coll = newInstance(size);
                for (int i = 0; i < size; i++)
                {
                    ByteBuffer databb = CodecUtils.readValue(input, protocolVersion);
                    coll.add(eltCodec.deserialize(databb, protocolVersion));
                }
                return coll;
            }
            catch (BufferUnderflowException e)
            {
                throw new InvalidTypeException("Not enough bytes to deserialize collection", e);
            }
        }

        @Override
        public String format(C value)
        {
            if (value == null) return "NULL";
            StringBuilder sb = new StringBuilder();
            sb.append(getOpeningChar());
            int i = 0;
            for (E v : value)
            {
                if (i++ != 0) sb.append(',');
                sb.append(eltCodec.format(v));
            }
            sb.append(getClosingChar());
            return sb.toString();
        }

        @Override
        public C parse(String value)
        {
            if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")) return null;

            int idx = ParseUtils.skipSpaces(value, 0);
            if (value.charAt(idx++) != getOpeningChar())
                throw new InvalidTypeException(
                String.format(
                "Cannot parse collection value from \"%s\", at character %d expecting '%s' but got '%c'",
                value, idx, getOpeningChar(), value.charAt(idx)));

            idx = ParseUtils.skipSpaces(value, idx);

            if (value.charAt(idx) == getClosingChar()) return newInstance(0);

            C l = newInstance(10);
            while (idx < value.length())
            {
                int n;
                try
                {
                    n = ParseUtils.skipCQLValue(value, idx);
                }
                catch (IllegalArgumentException e)
                {
                    throw new InvalidTypeException(
                    String.format(
                    "Cannot parse collection value from \"%s\", invalid CQL value at character %d",
                    value, idx),
                    e);
                }

                l.add(eltCodec.parse(value.substring(idx, n)));
                idx = n;

                idx = ParseUtils.skipSpaces(value, idx);
                if (value.charAt(idx) == getClosingChar()) return l;
                if (value.charAt(idx++) != ',')
                    throw new InvalidTypeException(
                    String.format(
                    "Cannot parse collection value from \"%s\", at character %d expecting ',' but got '%c'",
                    value, idx, value.charAt(idx)));

                idx = ParseUtils.skipSpaces(value, idx);
            }
            throw new InvalidTypeException(
            String.format(
            "Malformed collection value \"%s\", missing closing '%s'", value, getClosingChar()));
        }

        @Override
        public boolean accepts(Object value)
        {
            if (getJavaType().getRawType().isAssignableFrom(value.getClass()))
            {
                // runtime type ok, now check element type
                Collection<?> coll = (Collection<?>) value;
                if (coll.isEmpty()) return true;
                Object elt = coll.iterator().next();
                return eltCodec.accepts(elt);
            }
            return false;
        }

        /**
         * Return a new instance of {@code C} with the given estimated size.
         *
         * @param size The estimated size of the collection to create.
         * @return new instance of {@code C} with the given estimated size.
         */
        protected abstract C newInstance(int size);

        /**
         * Return the opening character to use when formatting values as CQL literals.
         *
         * @return The opening character to use when formatting values as CQL literals.
         */
        private char getOpeningChar()
        {
            return cqlType.getName() == Name.LIST ? '[' : '{';
        }

        /**
         * Return the closing character to use when formatting values as CQL literals.
         *
         * @return The closing character to use when formatting values as CQL literals.
         */
        private char getClosingChar()
        {
            return cqlType.getName() == Name.LIST ? ']' : '}';
        }
    }

    /**
     * This codec maps a CQL {@link DataType#list(DataType) list type} to a Java {@link List}.
     * Implementation note: this codec returns mutable, non thread-safe {@link ArrayList} instances.
     */
    private static class ListCodec<T> extends AbstractCollectionCodec<T, List<T>>
    {

        private ListCodec(TypeCodec<T> eltCodec)
        {
            super(
            DataType.list(eltCodec.getCqlType()),
            TypeTokens.listOf(eltCodec.getJavaType()),
            eltCodec);
        }

        @Override
        protected List<T> newInstance(int size)
        {
            return new ArrayList<>(size);
        }
    }

    /**
     * This codec maps a CQL {@link DataType#set(DataType) set type} to a Java {@link Set}.
     * Implementation note: this codec returns mutable, non thread-safe {@link LinkedHashSet}
     * instances.
     */
    private static class SetCodec<T> extends AbstractCollectionCodec<T, Set<T>>
    {

        private SetCodec(TypeCodec<T> eltCodec)
        {
            super(DataType.set(eltCodec.cqlType), TypeTokens.setOf(eltCodec.getJavaType()), eltCodec);
        }

        @Override
        protected Set<T> newInstance(int size)
        {
            return new LinkedHashSet<>(size);
        }
    }

    /**
     * Base class for codecs mapping CQL {@link DataType#map(DataType, DataType) maps} to a Java
     * {@link Map}.
     */
    public abstract static class AbstractMapCodec<K, V> extends TypeCodec<Map<K, V>>
    {

        final TypeCodec<K> keyCodec;

        final TypeCodec<V> valueCodec;

        AbstractMapCodec(TypeCodec<K> keyCodec, TypeCodec<V> valueCodec)
        {
            super(
            DataType.map(keyCodec.getCqlType(), valueCodec.getCqlType()),
            TypeTokens.mapOf(keyCodec.getJavaType(), valueCodec.getJavaType()));
            this.keyCodec = keyCodec;
            this.valueCodec = valueCodec;
        }

        @Override
        public boolean accepts(Object value)
        {
            if (value instanceof Map)
            {
                // runtime type ok, now check key and value types
                Map<?, ?> map = (Map<?, ?>) value;
                if (map.isEmpty()) return true;
                Map.Entry<?, ?> entry = map.entrySet().iterator().next();
                return keyCodec.accepts(entry.getKey()) && valueCodec.accepts(entry.getValue());
            }
            return false;
        }

        @Override
        public Map<K, V> parse(String value)
        {
            if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")) return null;

            int idx = ParseUtils.skipSpaces(value, 0);
            if (value.charAt(idx++) != '{')
                throw new InvalidTypeException(
                String.format(
                "cannot parse map value from \"%s\", at character %d expecting '{' but got '%c'",
                value, idx, value.charAt(idx)));

            idx = ParseUtils.skipSpaces(value, idx);

            if (value.charAt(idx) == '}') return newInstance(0);

            Map<K, V> m = new HashMap<>();
            while (idx < value.length())
            {
                int n;
                try
                {
                    n = ParseUtils.skipCQLValue(value, idx);
                }
                catch (IllegalArgumentException e)
                {
                    throw new InvalidTypeException(
                    String.format(
                    "Cannot parse map value from \"%s\", invalid CQL value at character %d",
                    value, idx),
                    e);
                }

                K k = keyCodec.parse(value.substring(idx, n));
                idx = n;

                idx = ParseUtils.skipSpaces(value, idx);
                if (value.charAt(idx++) != ':')
                    throw new InvalidTypeException(
                    String.format(
                    "Cannot parse map value from \"%s\", at character %d expecting ':' but got '%c'",
                    value, idx, value.charAt(idx)));
                idx = ParseUtils.skipSpaces(value, idx);

                try
                {
                    n = ParseUtils.skipCQLValue(value, idx);
                }
                catch (IllegalArgumentException e)
                {
                    throw new InvalidTypeException(
                    String.format(
                    "Cannot parse map value from \"%s\", invalid CQL value at character %d",
                    value, idx),
                    e);
                }

                V v = valueCodec.parse(value.substring(idx, n));
                idx = n;

                m.put(k, v);

                idx = ParseUtils.skipSpaces(value, idx);
                if (value.charAt(idx) == '}') return m;
                if (value.charAt(idx++) != ',')
                    throw new InvalidTypeException(
                    String.format(
                    "Cannot parse map value from \"%s\", at character %d expecting ',' but got '%c'",
                    value, idx, value.charAt(idx)));

                idx = ParseUtils.skipSpaces(value, idx);
            }
            throw new InvalidTypeException(
            String.format("Malformed map value \"%s\", missing closing '}'", value));
        }

        @Override
        public String format(Map<K, V> value)
        {
            if (value == null) return "NULL";
            StringBuilder sb = new StringBuilder();
            sb.append('{');
            int i = 0;
            for (Map.Entry<K, V> e : value.entrySet())
            {
                if (i++ != 0) sb.append(',');
                sb.append(keyCodec.format(e.getKey()));
                sb.append(':');
                sb.append(valueCodec.format(e.getValue()));
            }
            sb.append('}');
            return sb.toString();
        }

        @Override
        public ByteBuffer serialize(Map<K, V> value, ProtocolVersion protocolVersion)
        {
            if (value == null) return null;
            int i = 0;
            ByteBuffer[] bbs = new ByteBuffer[2 * value.size()];
            for (Map.Entry<K, V> entry : value.entrySet())
            {
                ByteBuffer bbk;
                K key = entry.getKey();
                if (key == null)
                {
                    throw new NullPointerException("Map keys cannot be null");
                }
                try
                {
                    bbk = keyCodec.serialize(key, protocolVersion);
                }
                catch (ClassCastException e)
                {
                    throw new InvalidTypeException(
                    String.format(
                    "Invalid type for map key, expecting %s but got %s",
                    keyCodec.getJavaType(), key.getClass()),
                    e);
                }
                ByteBuffer bbv;
                V v = entry.getValue();
                if (v == null)
                {
                    throw new NullPointerException("Map values cannot be null");
                }
                try
                {
                    bbv = valueCodec.serialize(v, protocolVersion);
                }
                catch (ClassCastException e)
                {
                    throw new InvalidTypeException(
                    String.format(
                    "Invalid type for map value, expecting %s but got %s",
                    valueCodec.getJavaType(), v.getClass()),
                    e);
                }
                bbs[i++] = bbk;
                bbs[i++] = bbv;
            }
            return CodecUtils.pack(bbs, value.size(), protocolVersion);
        }

        @Override
        public Map<K, V> deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)
        {
            if (bytes == null || bytes.remaining() == 0) return newInstance(0);
            try
            {
                ByteBuffer input = bytes.duplicate();
                int n = CodecUtils.readSize(input, protocolVersion);
                Map<K, V> m = newInstance(n);
                for (int i = 0; i < n; i++)
                {
                    ByteBuffer kbb = CodecUtils.readValue(input, protocolVersion);
                    ByteBuffer vbb = CodecUtils.readValue(input, protocolVersion);
                    m.put(
                    keyCodec.deserialize(kbb, protocolVersion),
                    valueCodec.deserialize(vbb, protocolVersion));
                }
                return m;
            }
            catch (BufferUnderflowException e)
            {
                throw new InvalidTypeException("Not enough bytes to deserialize a map", e);
            }
        }

        /**
         * Return a new {@link Map} instance with the given estimated size.
         *
         * @param size The estimated size of the collection to create.
         * @return A new {@link Map} instance with the given estimated size.
         */
        protected abstract Map<K, V> newInstance(int size);
    }

    /**
     * This codec maps a CQL {@link DataType#map(DataType, DataType) map type} to a Java {@link Map}.
     * Implementation note: this codec returns mutable, non thread-safe {@link LinkedHashMap}
     * instances.
     */
    private static class MapCodec<K, V> extends AbstractMapCodec<K, V>
    {

        private MapCodec(TypeCodec<K> keyCodec, TypeCodec<V> valueCodec)
        {
            super(keyCodec, valueCodec);
        }

        @Override
        protected Map<K, V> newInstance(int size)
        {
            return new LinkedHashMap<>(size);
        }
    }

    /**
     * Base class for codecs mapping CQL {@link UserType user-defined types} (UDTs) to Java objects.
     * It can serve as a base class for codecs dealing with direct UDT-to-Pojo mappings.
     *
     * @param <T> The Java type that the UDT will be mapped to.
     */
    public abstract static class AbstractUDTCodec<T> extends TypeCodec<T>
    {

        protected final UserType definition;

        AbstractUDTCodec(UserType definition, Class<T> javaClass)
        {
            this(definition, TypeToken.of(javaClass));
        }

        AbstractUDTCodec(UserType definition, TypeToken<T> javaType)
        {
            super(definition, javaType);
            this.definition = definition;
        }

        @Override
        public ByteBuffer serialize(T value, ProtocolVersion protocolVersion)
        {
            if (value == null) return null;
            int size = 0;
            int length = definition.size();
            ByteBuffer[] elements = new ByteBuffer[length];
            int i = 0;
            for (UserType.Field field : definition)
            {
                elements[i] =
                serializeField(value, Metadata.quoteIfNecessary(field.getName()), protocolVersion);
                size += 4 + (elements[i] == null ? 0 : elements[i].remaining());
                i++;
            }
            ByteBuffer result = ByteBuffer.allocate(size);
            for (ByteBuffer bb : elements)
            {
                if (bb == null)
                {
                    result.putInt(-1);
                }
                else
                {
                    result.putInt(bb.remaining());
                    result.put(bb.duplicate());
                }
            }
            return (ByteBuffer) result.flip();
        }

        @Override
        public T deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)
        {
            if (bytes == null) return null;
            // empty byte buffers will result in empty values
            try
            {
                ByteBuffer input = bytes.duplicate();
                T value = newInstance();
                for (UserType.Field field : definition)
                {
                    if (!input.hasRemaining()) break;
                    int n = input.getInt();
                    ByteBuffer element = n < 0 ? null : CodecUtils.readBytes(input, n);
                    value =
                    deserializeAndSetField(
                    element, value, Metadata.quoteIfNecessary(field.getName()), protocolVersion);
                }
                return value;
            }
            catch (BufferUnderflowException e)
            {
                throw new InvalidTypeException("Not enough bytes to deserialize a UDT", e);
            }
        }

        @Override
        public String format(T value)
        {
            if (value == null) return "NULL";
            StringBuilder sb = new StringBuilder("{");
            int i = 0;
            for (UserType.Field field : definition)
            {
                if (i > 0) sb.append(',');
                sb.append(Metadata.quoteIfNecessary(field.getName()));
                sb.append(':');
                sb.append(formatField(value, Metadata.quoteIfNecessary(field.getName())));
                i += 1;
            }
            sb.append('}');
            return sb.toString();
        }

        @Override
        public T parse(String value)
        {
            if (value == null || value.isEmpty() || value.equals("NULL")) return null;

            T v = newInstance();

            int idx = ParseUtils.skipSpaces(value, 0);
            if (value.charAt(idx++) != '{')
                throw new InvalidTypeException(
                String.format(
                "Cannot parse UDT value from \"%s\", at character %d expecting '{' but got '%c'",
                value, idx, value.charAt(idx)));

            idx = ParseUtils.skipSpaces(value, idx);

            if (value.charAt(idx) == '}') return v;

            while (idx < value.length())
            {

                int n;
                try
                {
                    n = ParseUtils.skipCQLId(value, idx);
                }
                catch (IllegalArgumentException e)
                {
                    throw new InvalidTypeException(
                    String.format(
                    "Cannot parse UDT value from \"%s\", cannot parse a CQL identifier at character %d",
                    value, idx),
                    e);
                }
                String name = value.substring(idx, n);
                idx = n;

                if (!definition.contains(name))
                    throw new InvalidTypeException(
                    String.format("Unknown field %s in value \"%s\"", name, value));

                idx = ParseUtils.skipSpaces(value, idx);
                if (value.charAt(idx++) != ':')
                    throw new InvalidTypeException(
                    String.format(
                    "Cannot parse UDT value from \"%s\", at character %d expecting ':' but got '%c'",
                    value, idx, value.charAt(idx)));
                idx = ParseUtils.skipSpaces(value, idx);

                try
                {
                    n = ParseUtils.skipCQLValue(value, idx);
                }
                catch (IllegalArgumentException e)
                {
                    throw new InvalidTypeException(
                    String.format(
                    "Cannot parse UDT value from \"%s\", invalid CQL value at character %d",
                    value, idx),
                    e);
                }

                String input = value.substring(idx, n);
                v = parseAndSetField(input, v, name);
                idx = n;

                idx = ParseUtils.skipSpaces(value, idx);
                if (value.charAt(idx) == '}') return v;
                if (value.charAt(idx) != ',')
                    throw new InvalidTypeException(
                    String.format(
                    "Cannot parse UDT value from \"%s\", at character %d expecting ',' but got '%c'",
                    value, idx, value.charAt(idx)));
                ++idx; // skip ','

                idx = ParseUtils.skipSpaces(value, idx);
            }
            throw new InvalidTypeException(
            String.format("Malformed UDT value \"%s\", missing closing '}'", value));
        }

        /**
         * Return a new instance of {@code T}.
         *
         * @return A new instance of {@code T}.
         */
        protected abstract T newInstance();

        /**
         * Serialize an individual field in an object, as part of serializing the whole object to a CQL
         * UDT (see {@link #serialize(Object, ProtocolVersion)}).
         *
         * @param source          The object to read the field from.
         * @param fieldName       The name of the field. Note that if it is case-sensitive or contains special
         *                        characters, it will be double-quoted (i.e. the string will contain actual quote
         *                        characters, as in {@code "\"foobar\""}).
         * @param protocolVersion The protocol version to use.
         * @return The serialized field, or {@code null} if that field should be ignored.
         */
        protected abstract ByteBuffer serializeField(
        T source, String fieldName, ProtocolVersion protocolVersion);

        /**
         * Deserialize an individual field and set it on an object, as part of deserializing the whole
         * object from a CQL UDT (see {@link #deserialize(ByteBuffer, ProtocolVersion)}).
         *
         * @param input           The serialized form of the field.
         * @param target          The object to set the field on.
         * @param fieldName       The name of the field. Note that if it is case-sensitive or contains special
         *                        characters, it will be double-quoted (i.e. the string will contain actual quote
         *                        characters, as in {@code "\"foobar\""}).
         * @param protocolVersion The protocol version to use.
         * @return The target object with the field set. In most cases this should be the same as {@code
         * target}, but if you're dealing with immutable types you'll need to return a different
         * instance.
         */
        protected abstract T deserializeAndSetField(
        ByteBuffer input, T target, String fieldName, ProtocolVersion protocolVersion);

        /**
         * Format an individual field in an object as a CQL literal, as part of formatting the whole
         * object (see {@link #format(Object)}).
         *
         * @param source    The object to read the field from.
         * @param fieldName The name of the field. Note that if it is case-sensitive or contains special
         *                  characters, it will be double-quoted (i.e. the string will contain actual quote
         *                  characters, as in {@code "\"foobar\""}).
         * @return The formatted value.
         */
        protected abstract String formatField(T source, String fieldName);

        /**
         * Parse an individual field and set it on an object, as part of parsing the whole object (see
         * {@link #parse(String)}).
         *
         * @param input     The String to parse the field from.
         * @param target    The value to write to.
         * @param fieldName The name of the field. Note that if it is case-sensitive or contains special
         *                  characters, it will be double-quoted (i.e. the string will contain actual quote
         *                  characters, as in {@code "\"foobar\""}).
         * @return The target object with the field set. In most cases this should be the same as {@code
         * target}, but if you're dealing with immutable types you'll need to return a different
         * instance.
         */
        protected abstract T parseAndSetField(String input, T target, String fieldName);
    }

    /**
     * This codec maps a CQL {@link UserType} to a {@link UDTValue}.
     */
    private static class UDTCodec extends AbstractUDTCodec<UDTValue>
    {

        private UDTCodec(UserType definition)
        {
            super(definition, UDTValue.class);
        }

        @Override
        public boolean accepts(Object value)
        {
            return super.accepts(value) && ((UDTValue) value).getType().equals(definition);
        }

        @Override
        protected UDTValue newInstance()
        {
            return definition.newValue();
        }

        @Override
        protected ByteBuffer serializeField(
        UDTValue source, String fieldName, ProtocolVersion protocolVersion)
        {
            return source.getBytesUnsafe(fieldName);
        }

        @Override
        protected UDTValue deserializeAndSetField(
        ByteBuffer input, UDTValue target, String fieldName, ProtocolVersion protocolVersion)
        {
            return target.setBytesUnsafe(fieldName, input);
        }

        @Override
        protected String formatField(UDTValue source, String fieldName)
        {
            DataType elementType = definition.getFieldType(fieldName);
            TypeCodec<Object> codec = definition.getCodecRegistry().codecFor(elementType);
            return codec.format(source.get(fieldName, codec.getJavaType()));
        }

        @Override
        protected UDTValue parseAndSetField(String input, UDTValue target, String fieldName)
        {
            DataType elementType = definition.getFieldType(fieldName);
            TypeCodec<Object> codec = definition.getCodecRegistry().codecFor(elementType);
            target.set(fieldName, codec.parse(input), codec.getJavaType());
            return target;
        }
    }

    /**
     * Base class for codecs mapping CQL {@link TupleType tuples} to Java objects. It can serve as a
     * base class for codecs dealing with direct tuple-to-Pojo mappings.
     *
     * @param <T> The Java type that this codec handles.
     */
    public abstract static class AbstractTupleCodec<T> extends TypeCodec<T>
    {

        protected final TupleType definition;

        AbstractTupleCodec(TupleType definition, Class<T> javaClass)
        {
            this(definition, TypeToken.of(javaClass));
        }

        AbstractTupleCodec(TupleType definition, TypeToken<T> javaType)
        {
            super(definition, javaType);
            this.definition = definition;
        }

        @Override
        public boolean accepts(DataType cqlType)
        {
            // a tuple codec should accept tuple values of a different type,
            // provided that the latter is contained in this codec's type.
            return super.accepts(cqlType) && definition.contains((TupleType) cqlType);
        }

        @Override
        public ByteBuffer serialize(T value, ProtocolVersion protocolVersion)
        {
            if (value == null) return null;
            int size = 0;
            int length = definition.getComponentTypes().size();
            ByteBuffer[] elements = new ByteBuffer[length];
            for (int i = 0; i < length; i++)
            {
                elements[i] = serializeField(value, i, protocolVersion);
                size += 4 + (elements[i] == null ? 0 : elements[i].remaining());
            }
            ByteBuffer result = ByteBuffer.allocate(size);
            for (ByteBuffer bb : elements)
            {
                if (bb == null)
                {
                    result.putInt(-1);
                }
                else
                {
                    result.putInt(bb.remaining());
                    result.put(bb.duplicate());
                }
            }
            return (ByteBuffer) result.flip();
        }

        @Override
        public T deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)
        {
            if (bytes == null) return null;
            // empty byte buffers will result in empty values
            try
            {
                ByteBuffer input = bytes.duplicate();
                T value = newInstance();
                int i = 0;
                while (input.hasRemaining() && i < definition.getComponentTypes().size())
                {
                    int n = input.getInt();
                    ByteBuffer element = n < 0 ? null : CodecUtils.readBytes(input, n);
                    value = deserializeAndSetField(element, value, i++, protocolVersion);
                }
                return value;
            }
            catch (BufferUnderflowException e)
            {
                throw new InvalidTypeException("Not enough bytes to deserialize a tuple", e);
            }
        }

        @Override
        public String format(T value)
        {
            if (value == null) return "NULL";
            StringBuilder sb = new StringBuilder("(");
            int length = definition.getComponentTypes().size();
            for (int i = 0; i < length; i++)
            {
                if (i > 0) sb.append(',');
                sb.append(formatField(value, i));
            }
            sb.append(')');
            return sb.toString();
        }

        @Override
        public T parse(String value)
        {
            if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")) return null;

            T v = newInstance();

            int idx = ParseUtils.skipSpaces(value, 0);
            if (value.charAt(idx++) != '(')
                throw new InvalidTypeException(
                String.format(
                "Cannot parse tuple value from \"%s\", at character %d expecting '(' but got '%c'",
                value, idx, value.charAt(idx)));

            idx = ParseUtils.skipSpaces(value, idx);

            if (value.charAt(idx) == ')') return v;

            int i = 0;
            while (idx < value.length())
            {
                int n;
                try
                {
                    n = ParseUtils.skipCQLValue(value, idx);
                }
                catch (IllegalArgumentException e)
                {
                    throw new InvalidTypeException(
                    String.format(
                    "Cannot parse tuple value from \"%s\", invalid CQL value at character %d",
                    value, idx),
                    e);
                }

                String input = value.substring(idx, n);
                v = parseAndSetField(input, v, i);
                idx = n;
                i += 1;

                idx = ParseUtils.skipSpaces(value, idx);
                if (value.charAt(idx) == ')') return v;
                if (value.charAt(idx) != ',')
                    throw new InvalidTypeException(
                    String.format(
                    "Cannot parse tuple value from \"%s\", at character %d expecting ',' but got '%c'",
                    value, idx, value.charAt(idx)));
                ++idx; // skip ','

                idx = ParseUtils.skipSpaces(value, idx);
            }
            throw new InvalidTypeException(
            String.format("Malformed tuple value \"%s\", missing closing ')'", value));
        }

        /**
         * Return a new instance of {@code T}.
         *
         * @return A new instance of {@code T}.
         */
        protected abstract T newInstance();

        /**
         * Serialize an individual field in an object, as part of serializing the whole object to a CQL
         * tuple (see {@link #serialize(Object, ProtocolVersion)}).
         *
         * @param source          The object to read the field from.
         * @param index           The index of the field.
         * @param protocolVersion The protocol version to use.
         * @return The serialized field, or {@code null} if that field should be ignored.
         */
        protected abstract ByteBuffer serializeField(
        T source, int index, ProtocolVersion protocolVersion);

        /**
         * Deserialize an individual field and set it on an object, as part of deserializing the whole
         * object from a CQL tuple (see {@link #deserialize(ByteBuffer, ProtocolVersion)}).
         *
         * @param input           The serialized form of the field.
         * @param target          The object to set the field on.
         * @param index           The index of the field.
         * @param protocolVersion The protocol version to use.
         * @return The target object with the field set. In most cases this should be the same as {@code
         * target}, but if you're dealing with immutable types you'll need to return a different
         * instance.
         */
        protected abstract T deserializeAndSetField(
        ByteBuffer input, T target, int index, ProtocolVersion protocolVersion);

        /**
         * Format an individual field in an object as a CQL literal, as part of formatting the whole
         * object (see {@link #format(Object)}).
         *
         * @param source The object to read the field from.
         * @param index  The index of the field.
         * @return The formatted value.
         */
        protected abstract String formatField(T source, int index);

        /**
         * Parse an individual field and set it on an object, as part of parsing the whole object (see
         * {@link #parse(String)}).
         *
         * @param input  The String to parse the field from.
         * @param target The value to write to.
         * @param index  The index of the field.
         * @return The target object with the field set. In most cases this should be the same as {@code
         * target}, but if you're dealing with immutable types you'll need to return a different
         * instance.
         */
        protected abstract T parseAndSetField(String input, T target, int index);
    }

    /**
     * This codec maps a CQL {@link TupleType tuple} to a {@link TupleValue}.
     */
    private static class TupleCodec extends AbstractTupleCodec<TupleValue>
    {

        private TupleCodec(TupleType definition)
        {
            super(definition, TupleValue.class);
        }

        @Override
        public boolean accepts(Object value)
        {
            // a tuple codec should accept tuple values of a different type,
            // provided that the latter is contained in this codec's type.
            return super.accepts(value) && definition.contains(((TupleValue) value).getType());
        }

        @Override
        protected TupleValue newInstance()
        {
            return definition.newValue();
        }

        @Override
        protected ByteBuffer serializeField(
        TupleValue source, int index, ProtocolVersion protocolVersion)
        {
            if (index >= source.values.length) return null;
            return source.getBytesUnsafe(index);
        }

        @Override
        protected TupleValue deserializeAndSetField(
        ByteBuffer input, TupleValue target, int index, ProtocolVersion protocolVersion)
        {
            if (index >= target.values.length) return target;
            return target.setBytesUnsafe(index, input);
        }

        @Override
        protected String formatField(TupleValue value, int index)
        {
            DataType elementType = definition.getComponentTypes().get(index);
            TypeCodec<Object> codec = definition.getCodecRegistry().codecFor(elementType);
            return codec.format(value.get(index, codec.getJavaType()));
        }

        @Override
        protected TupleValue parseAndSetField(String input, TupleValue target, int index)
        {
            DataType elementType = definition.getComponentTypes().get(index);
            TypeCodec<Object> codec = definition.getCodecRegistry().codecFor(elementType);
            target.set(index, codec.parse(input), codec.getJavaType());
            return target;
        }
    }

    private static class DurationCodec extends TypeCodec<Duration>
    {

        private static final DurationCodec instance = new DurationCodec();

        private DurationCodec()
        {
            super(DataType.duration(), Duration.class);
        }

        @Override
        public ByteBuffer serialize(Duration duration, ProtocolVersion protocolVersion)
        throws InvalidTypeException
        {
            if (duration == null) return null;
            long months = duration.getMonths();
            long days = duration.getDays();
            long nanoseconds = duration.getNanoseconds();
            int size =
            VIntCoding.computeVIntSize(months)
            + VIntCoding.computeVIntSize(days)
            + VIntCoding.computeVIntSize(nanoseconds);
            ByteBuffer bb = ByteBuffer.allocate(size);
            VIntCoding.writeVInt(months, bb);
            VIntCoding.writeVInt(days, bb);
            VIntCoding.writeVInt(nanoseconds, bb);
            bb.flip();
            return bb;
        }

        @Override
        public Duration deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion)
        throws InvalidTypeException
        {
            if (bytes == null || bytes.remaining() == 0)
            {
                return null;
            }
            else
            {
                DataInput in = ByteStreams.newDataInput(Bytes.getArray(bytes));
                try
                {
                    int months = VIntCoding.readVInt32(in);
                    int days = VIntCoding.readVInt32(in);
                    long nanoseconds = VIntCoding.readVInt(in);
                    return Duration.newInstance(months, days, nanoseconds);
                }
                catch (IOException e)
                {
                    // cannot happen
                    throw new AssertionError();
                }
            }
        }

        @Override
        public Duration parse(String value) throws InvalidTypeException
        {
            if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")) return null;
            return Duration.from(value);
        }

        @Override
        public String format(Duration value) throws InvalidTypeException
        {
            if (value == null) return "NULL";
            return value.toString();
        }
    }
}
