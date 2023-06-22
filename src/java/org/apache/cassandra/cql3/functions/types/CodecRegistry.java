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

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.*;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.functions.types.exceptions.CodecNotFoundException;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.cassandra.cql3.functions.types.DataType.Name.*;

/**
 * A registry for {@link TypeCodec}s. When the driver needs to serialize or deserialize a Java type
 * to/from CQL, it will lookup in the registry for a suitable codec. The registry is initialized
 * with default codecs that handle basic conversions (e.g. CQL {@code text} to {@code
 * java.lang.String}), and users can add their own. Complex codecs can also be generated on-the-fly
 * from simpler ones (more details below).
 *
 * <h3>Creating a registry </h3>
 * <p>
 * By default, the driver uses {@code CodecRegistry#DEFAULT_INSTANCE}, a shareable, JVM-wide
 * instance initialized with built-in codecs for all the base CQL types. The only reason to create
 * your own instances is if you have multiple {@code Cluster} objects that use different sets of
 * codecs. In that case, use {@code
 * Cluster.Builder#withCodecRegistry(CodecRegistry)} to associate the
 * registry with the cluster:
 *
 * <pre>{@code
 * CodecRegistry myCodecRegistry = new CodecRegistry();
 * myCodecRegistry.register(myCodec1, myCodec2, myCodec3);
 * Cluster cluster = Cluster.builder().withCodecRegistry(myCodecRegistry).build();
 *
 * // To retrieve the registry later:
 * CodecRegistry registry = cluster.getConfiguration().getCodecRegistry();
 * }</pre>
 * <p>
 * {@code CodecRegistry} instances are thread-safe.
 *
 * <p>It is possible to turn on log messages by setting the {@code
 * CodecRegistry} logger level to {@code TRACE}. Beware that the registry
 * can be very verbose at this log level.
 *
 * <h3>Registering and using custom codecs </h3>
 * <p>
 * To create a custom codec, write a class that extends {@link TypeCodec}, create an instance, and
 * pass it to one of the {@link #register(TypeCodec) register} methods; for example, one could
 * create a codec that maps CQL timestamps to JDK8's {@code java.time.LocalDate}:
 *
 * <pre>{@code
 * class LocalDateCodec extends TypeCodec<java.time.LocalDate> {
 *    ...
 * }
 * myCodecRegistry.register(new LocalDateCodec());
 * }</pre>
 * <p>
 * The conversion will be available to:
 *
 * <ul>
 * <li>all driver types that implement {@link GettableByIndexData}, {@link GettableByNameData},
 * {@link SettableByIndexData} and/or {@link SettableByNameData}. Namely: {@code Row}, {@code
 * BoundStatement}, {@link UDTValue} and {@link TupleValue};
 * <li>{@code SimpleStatement#SimpleStatement(String, Object...) simple statements};
 * <li>statements created with the {@code querybuilder.QueryBuilder Query
 * builder}.
 * </ul>
 *
 * <p>Example:
 *
 * <pre>{@code
 * Row row = session.executeQuery("select date from some_table where pk = 1").one();
 * java.time.LocalDate date = row.get(0, java.time.LocalDate.class); // uses LocalDateCodec registered above
 * }</pre>
 * <p>
 * You can also bypass the codec registry by passing a standalone codec instance to methods such as
 * {@link GettableByIndexData#get(int, TypeCodec)}.
 *
 * <h3>Codec generation </h3>
 * <p>
 * When a {@code CodecRegistry} cannot find a suitable codec among existing ones, it will attempt to
 * create it on-the-fly. It can manage:
 *
 * <ul>
 * <li>collections (lists, sets and maps) of known types. For example, if you registered a codec
 * for JDK8's {@code java.time.LocalDate} like in the example above, you get {@code
 * List<LocalDate>>} and {@code Set<LocalDate>>} handled for free, as well as all {@code Map}
 * types whose keys and/or values are {@code java.time.LocalDate}. This works recursively for
 * nested collections;
 * <li>{@link UserType user types}, mapped to {@link UDTValue} objects. Custom codecs are
 * available recursively to the UDT's fields, so if one of your fields is a {@code timestamp}
 * you can use your {@code LocalDateCodec} to retrieve it as a {@code java.time.LocalDate};
 * <li>{@link TupleType tuple types}, mapped to {@link TupleValue} (with the same rules for nested
 * fields);
 * <li>{@link DataType.CustomType custom types}, mapped to {@code
 * ByteBuffer}.
 * </ul>
 * <p>
 * If the codec registry encounters a mapping that it can't handle automatically, a {@link
 * CodecNotFoundException} is thrown; you'll need to register a custom codec for it.
 *
 * <h3>Performance and caching </h3>
 * <p>
 * Whenever possible, the registry will cache the result of a codec lookup for a specific type
 * mapping, including any generated codec. For example, if you registered {@code LocalDateCodec} and
 * ask the registry for a codec to convert a CQL {@code list<timestamp>} to a Java {@code
 * List<LocalDate>}:
 *
 * <ol>
 * <li>the first lookup will generate a {@code TypeCodec<List<LocalDate>>} from {@code
 * LocalDateCodec}, and put it in the cache;
 * <li>the second lookup will hit the cache directly, and reuse the previously generated instance.
 * </ol>
 * <p>
 * The javadoc for each {@link #codecFor(DataType) codecFor} variant specifies whether the result
 * can be cached or not.
 *
 * <h3>Codec order </h3>
 * <p>
 * When the registry looks up a codec, the rules of precedence are:
 *
 * <ul>
 * <li>if a result was previously cached for that mapping, it is returned;
 * <li>otherwise, the registry checks the list of built-in codecs – the default ones – and the
 * ones that were explicitly registered (in the order that they were registered). It calls
 * each codec's {@code accepts} methods to determine if it can handle the mapping, and if so
 * returns it;
 * <li>otherwise, the registry tries to generate a codec, according to the rules outlined above.
 * </ul>
 * <p>
 * It is currently impossible to override an existing codec. If you try to do so, {@link
 * #register(TypeCodec)} will log a warning and ignore it.
 */
public final class CodecRegistry
{

    private static final Logger logger = LoggerFactory.getLogger(CodecRegistry.class);

    private static final Map<DataType.Name, TypeCodec<?>> BUILT_IN_CODECS_MAP =
    new EnumMap<>(DataType.Name.class);

    static
    {
        BUILT_IN_CODECS_MAP.put(ASCII, TypeCodec.ascii());
        BUILT_IN_CODECS_MAP.put(BIGINT, TypeCodec.bigint());
        BUILT_IN_CODECS_MAP.put(BLOB, TypeCodec.blob());
        BUILT_IN_CODECS_MAP.put(BOOLEAN, TypeCodec.cboolean());
        BUILT_IN_CODECS_MAP.put(COUNTER, TypeCodec.counter());
        BUILT_IN_CODECS_MAP.put(DECIMAL, TypeCodec.decimal());
        BUILT_IN_CODECS_MAP.put(DOUBLE, TypeCodec.cdouble());
        BUILT_IN_CODECS_MAP.put(FLOAT, TypeCodec.cfloat());
        BUILT_IN_CODECS_MAP.put(INET, TypeCodec.inet());
        BUILT_IN_CODECS_MAP.put(INT, TypeCodec.cint());
        BUILT_IN_CODECS_MAP.put(TEXT, TypeCodec.varchar());
        BUILT_IN_CODECS_MAP.put(TIMESTAMP, TypeCodec.timestamp());
        BUILT_IN_CODECS_MAP.put(UUID, TypeCodec.uuid());
        BUILT_IN_CODECS_MAP.put(VARCHAR, TypeCodec.varchar());
        BUILT_IN_CODECS_MAP.put(VARINT, TypeCodec.varint());
        BUILT_IN_CODECS_MAP.put(TIMEUUID, TypeCodec.timeUUID());
        BUILT_IN_CODECS_MAP.put(SMALLINT, TypeCodec.smallInt());
        BUILT_IN_CODECS_MAP.put(TINYINT, TypeCodec.tinyInt());
        BUILT_IN_CODECS_MAP.put(DATE, TypeCodec.date());
        BUILT_IN_CODECS_MAP.put(TIME, TypeCodec.time());
        BUILT_IN_CODECS_MAP.put(DURATION, TypeCodec.duration());
    }

    // roughly sorted by popularity
    private static final TypeCodec<?>[] BUILT_IN_CODECS =
    new TypeCodec<?>[]{
    TypeCodec
    .varchar(), // must be declared before AsciiCodec so it gets chosen when CQL type not
    // available
    TypeCodec
    .uuid(), // must be declared before TimeUUIDCodec so it gets chosen when CQL type not
    // available
    TypeCodec.timeUUID(),
    TypeCodec.timestamp(),
    TypeCodec.cint(),
    TypeCodec.bigint(),
    TypeCodec.blob(),
    TypeCodec.cdouble(),
    TypeCodec.cfloat(),
    TypeCodec.decimal(),
    TypeCodec.varint(),
    TypeCodec.inet(),
    TypeCodec.cboolean(),
    TypeCodec.smallInt(),
    TypeCodec.tinyInt(),
    TypeCodec.date(),
    TypeCodec.time(),
    TypeCodec.duration(),
    TypeCodec.counter(),
    TypeCodec.ascii()
    };

    /**
     * Cache key for the codecs cache.
     */
    private static final class CacheKey
    {

        private final DataType cqlType;

        private final TypeToken<?> javaType;

        CacheKey(DataType cqlType, TypeToken<?> javaType)
        {
            this.javaType = javaType;
            this.cqlType = cqlType;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CacheKey cacheKey = (CacheKey) o;
            return Objects.equals(cqlType, cacheKey.cqlType)
                   && Objects.equals(javaType, cacheKey.javaType);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(cqlType, javaType);
        }
    }

    /**
     * Cache loader for the codecs cache.
     */
    private class TypeCodecCacheLoader extends CacheLoader<CacheKey, TypeCodec<?>>
    {
        @Override
        public TypeCodec<?> load(CacheKey cacheKey)
        {
            checkNotNull(cacheKey.cqlType, "Parameter cqlType cannot be null");
            if (logger.isTraceEnabled())
                logger.trace(
                "Loading codec into cache: [{} <-> {}]",
                CodecRegistry.toString(cacheKey.cqlType),
                CodecRegistry.toString(cacheKey.javaType));
            for (TypeCodec<?> codec : codecs)
            {
                if (codec.accepts(cacheKey.cqlType)
                    && (cacheKey.javaType == null || codec.accepts(cacheKey.javaType)))
                {
                    logger.trace("Already existing codec found: {}", codec);
                    return codec;
                }
            }
            return createCodec(cacheKey.cqlType, cacheKey.javaType);
        }
    }

    /**
     * A complexity-based weigher for the codecs cache. Weights are computed mainly according to the
     * CQL type:
     *
     * <ol>
     * <li>Manually-registered codecs always weigh 0;
     * <li>Codecs for primitive types weigh 0;
     * <li>Codecs for collections weigh the total weight of their inner types + the weight of their
     * level of deepness;
     * <li>Codecs for UDTs and tuples weigh the total weight of their inner types + the weight of
     * their level of deepness, but cannot weigh less than 1;
     * <li>Codecs for custom (non-CQL) types weigh 1.
     * </ol>
     * <p>
     * A consequence of this algorithm is that codecs for primitive types and codecs for all "shallow"
     * collections thereof are never evicted.
     */
    private class TypeCodecWeigher implements Weigher<CacheKey, TypeCodec<?>>
    {

        @Override
        public int weigh(CacheKey key, TypeCodec<?> value)
        {
            return codecs.contains(value) ? 0 : weigh(value.cqlType, 0);
        }

        private int weigh(DataType cqlType, int level)
        {
            switch (cqlType.getName())
            {
                case LIST:
                case SET:
                case MAP:
                {
                    int weight = level;
                    for (DataType eltType : cqlType.getTypeArguments())
                    {
                        weight += weigh(eltType, level + 1);
                    }
                    return weight;
                }
                case VECTOR:
                {
                    int weight = level;
                    DataType eltType = cqlType.getTypeArguments().get(0);
                    if (eltType != null)
                    {
                        weight += weigh(eltType, level + 1);
                    }
                    return weight == 0 ? 1 : weight;
                }
                case UDT:
                {
                    int weight = level;
                    for (UserType.Field field : ((UserType) cqlType))
                    {
                        weight += weigh(field.getType(), level + 1);
                    }
                    return weight == 0 ? 1 : weight;
                }
                case TUPLE:
                {
                    int weight = level;
                    for (DataType componentType : ((TupleType) cqlType).getComponentTypes())
                    {
                        weight += weigh(componentType, level + 1);
                    }
                    return weight == 0 ? 1 : weight;
                }
                case CUSTOM:
                    return 1;
                default:
                    return 0;
            }
        }
    }

    /**
     * Simple removal listener for the codec cache (can be used for debugging purposes by setting the
     * {@code CodecRegistry} logger level to {@code TRACE}.
     */
    private static class TypeCodecRemovalListener implements RemovalListener<CacheKey, TypeCodec<?>>
    {
        @Override
        public void onRemoval(RemovalNotification<CacheKey, TypeCodec<?>> notification)
        {
            logger.trace(
            "Evicting codec from cache: {} (cause: {})",
            notification.getValue(),
            notification.getCause());
        }
    }

    /**
     * The list of user-registered codecs.
     */
    private final CopyOnWriteArrayList<TypeCodec<?>> codecs;

    /**
     * A LoadingCache to serve requests for codecs whenever possible. The cache can be used as long as
     * at least the CQL type is known.
     */
    private final LoadingCache<CacheKey, TypeCodec<?>> cache;

    /**
     * Creates a new instance initialized with built-in codecs for all the base CQL types.
     */
    public CodecRegistry()
    {
        this.codecs = new CopyOnWriteArrayList<>();
        this.cache = defaultCacheBuilder().build(new TypeCodecCacheLoader());
    }

    private CacheBuilder<CacheKey, TypeCodec<?>> defaultCacheBuilder()
    {
        CacheBuilder<CacheKey, TypeCodec<?>> builder =
        CacheBuilder.newBuilder()
                    // lists, sets and maps of 20 primitive types = 20 + 20 + 20*20 = 440 codecs,
                    // so let's start with roughly 1/4 of that
                    .initialCapacity(100)
                    .maximumWeight(1000)
                    .weigher(new TypeCodecWeigher());
        if (logger.isTraceEnabled())
            // do not bother adding a listener if it will be ineffective
            builder = builder.removalListener(new TypeCodecRemovalListener());
        return builder;
    }

    /**
     * Register the given codec with this registry.
     *
     * <p>This method will log a warning and ignore the codec if it collides with a previously
     * registered one. Note that this check is not done in a completely thread-safe manner; codecs
     * should typically be registered at application startup, not in a highly concurrent context (if a
     * race condition occurs, the worst possible outcome is that no warning gets logged, and the codec
     * gets registered but will never actually be used).
     *
     * @param newCodec The codec to add to the registry.
     * @return this CodecRegistry (for method chaining).
     */
    public CodecRegistry register(TypeCodec<?> newCodec)
    {
        for (TypeCodec<?> oldCodec : BUILT_IN_CODECS)
        {
            if (oldCodec.accepts(newCodec.getCqlType()) && oldCodec.accepts(newCodec.getJavaType()))
            {
                logger.warn(
                "Ignoring codec {} because it collides with previously registered codec {}",
                newCodec,
                oldCodec);
                return this;
            }
        }
        for (TypeCodec<?> oldCodec : codecs)
        {
            if (oldCodec.accepts(newCodec.getCqlType()) && oldCodec.accepts(newCodec.getJavaType()))
            {
                logger.warn(
                "Ignoring codec {} because it collides with previously registered codec {}",
                newCodec,
                oldCodec);
                return this;
            }
        }
        CacheKey key = new CacheKey(newCodec.getCqlType(), newCodec.getJavaType());
        TypeCodec<?> existing = cache.getIfPresent(key);
        if (existing != null)
        {
            logger.warn(
            "Ignoring codec {} because it collides with previously generated codec {}",
            newCodec,
            existing);
            return this;
        }
        this.codecs.add(newCodec);
        return this;
    }

    /**
     * Register the given codecs with this registry.
     *
     * @param codecs The codecs to add to the registry.
     * @return this CodecRegistry (for method chaining).
     * @see #register(TypeCodec)
     */
    public CodecRegistry register(TypeCodec<?>... codecs)
    {
        for (TypeCodec<?> codec : codecs) register(codec);
        return this;
    }

    /**
     * Register the given codecs with this registry.
     *
     * @param codecs The codecs to add to the registry.
     * @return this CodecRegistry (for method chaining).
     * @see #register(TypeCodec)
     */
    public CodecRegistry register(Iterable<? extends TypeCodec<?>> codecs)
    {
        for (TypeCodec<?> codec : codecs) register(codec);
        return this;
    }

    /**
     * Returns a {@link TypeCodec codec} that accepts the given value.
     *
     * <p>This method takes an arbitrary Java object and tries to locate a suitable codec for it.
     * Codecs must perform a {@link TypeCodec#accepts(Object) runtime inspection} of the object to
     * determine if they can accept it or not, which, depending on the implementations, can be
     * expensive; besides, the resulting codec cannot be cached. Therefore there might be a
     * performance penalty when using this method.
     *
     * <p>Furthermore, this method returns the first matching codec, regardless of its accepted CQL
     * type. It should be reserved for situations where the target CQL type is not available or
     * unknown. In the Java driver, this happens mainly when serializing a value in a {@code
     * SimpleStatement#SimpleStatement(String, Object...) SimpleStatement} or in the {@code
     * querybuilder.QueryBuilder}, where no CQL type information is
     * available.
     *
     * <p>Codecs returned by this method are <em>NOT</em> cached (see the {@link CodecRegistry
     * top-level documentation} of this class for more explanations about caching).
     *
     * @param value The value the codec should accept; must not be {@code null}.
     * @return A suitable codec.
     * @throws CodecNotFoundException if a suitable codec cannot be found.
     */
    public <T> TypeCodec<T> codecFor(T value)
    {
        return findCodec(null, value);
    }

    /**
     * Returns a {@link TypeCodec codec} that accepts the given {@link DataType CQL type}.
     *
     * <p>This method returns the first matching codec, regardless of its accepted Java type. It
     * should be reserved for situations where the Java type is not available or unknown. In the Java
     * driver, this happens mainly when deserializing a value using the {@link
     * GettableByIndexData#getObject(int) getObject} method.
     *
     * <p>Codecs returned by this method are cached (see the {@link CodecRegistry top-level
     * documentation} of this class for more explanations about caching).
     *
     * @param cqlType The {@link DataType CQL type} the codec should accept; must not be {@code null}.
     * @return A suitable codec.
     * @throws CodecNotFoundException if a suitable codec cannot be found.
     */
    public <T> TypeCodec<T> codecFor(DataType cqlType) throws CodecNotFoundException
    {
        return lookupCodec(cqlType, null);
    }

    /**
     * Returns a {@link TypeCodec codec} that accepts the given {@link DataType CQL type} and the
     * given Java class.
     *
     * <p>This method can only handle raw (non-parameterized) Java types. For parameterized types, use
     * {@link #codecFor(DataType, TypeToken)} instead.
     *
     * <p>Codecs returned by this method are cached (see the {@link CodecRegistry top-level
     * documentation} of this class for more explanations about caching).
     *
     * @param cqlType  The {@link DataType CQL type} the codec should accept; must not be {@code null}.
     * @param javaType The Java type the codec should accept; can be {@code null}.
     * @return A suitable codec.
     * @throws CodecNotFoundException if a suitable codec cannot be found.
     */
    public <T> TypeCodec<T> codecFor(DataType cqlType, Class<T> javaType)
    throws CodecNotFoundException
    {
        return codecFor(cqlType, TypeToken.of(javaType));
    }

    /**
     * Returns a {@link TypeCodec codec} that accepts the given {@link DataType CQL type} and the
     * given Java type.
     *
     * <p>This method handles parameterized types thanks to Guava's {@link TypeToken} API.
     *
     * <p>Codecs returned by this method are cached (see the {@link CodecRegistry top-level
     * documentation} of this class for more explanations about caching).
     *
     * @param cqlType  The {@link DataType CQL type} the codec should accept; must not be {@code null}.
     * @param javaType The {@link TypeToken Java type} the codec should accept; can be {@code null}.
     * @return A suitable codec.
     * @throws CodecNotFoundException if a suitable codec cannot be found.
     */
    public <T> TypeCodec<T> codecFor(DataType cqlType, TypeToken<T> javaType)
    throws CodecNotFoundException
    {
        return lookupCodec(cqlType, javaType);
    }

    /**
     * Returns a {@link TypeCodec codec} that accepts the given {@link DataType CQL type} and the
     * given value.
     *
     * <p>This method takes an arbitrary Java object and tries to locate a suitable codec for it.
     * Codecs must perform a {@link TypeCodec#accepts(Object) runtime inspection} of the object to
     * determine if they can accept it or not, which, depending on the implementations, can be
     * expensive; besides, the resulting codec cannot be cached. Therefore there might be a
     * performance penalty when using this method.
     *
     * <p>Codecs returned by this method are <em>NOT</em> cached (see the {@link CodecRegistry
     * top-level documentation} of this class for more explanations about caching).
     *
     * @param cqlType The {@link DataType CQL type} the codec should accept; can be {@code null}.
     * @param value   The value the codec should accept; must not be {@code null}.
     * @return A suitable codec.
     * @throws CodecNotFoundException if a suitable codec cannot be found.
     */
    public <T> TypeCodec<T> codecFor(DataType cqlType, T value)
    {
        return findCodec(cqlType, value);
    }

    @SuppressWarnings("unchecked")
    private <T> TypeCodec<T> lookupCodec(DataType cqlType, TypeToken<T> javaType)
    {
        checkNotNull(cqlType, "Parameter cqlType cannot be null");
        TypeCodec<?> codec = BUILT_IN_CODECS_MAP.get(cqlType.getName());
        if (codec != null && (javaType == null || codec.accepts(javaType)))
        {
            logger.trace("Returning built-in codec {}", codec);
            return (TypeCodec<T>) codec;
        }
        if (logger.isTraceEnabled())
            logger.trace("Querying cache for codec [{} <-> {}]", toString(cqlType), toString(javaType));
        try
        {
            CacheKey cacheKey = new CacheKey(cqlType, javaType);
            codec = cache.get(cacheKey);
        }
        catch (UncheckedExecutionException e)
        {
            if (e.getCause() instanceof CodecNotFoundException)
            {
                throw (CodecNotFoundException) e.getCause();
            }
            throw new CodecNotFoundException(e.getCause());
        }
        catch (RuntimeException | ExecutionException e)
        {
            throw new CodecNotFoundException(e.getCause());
        }
        logger.trace("Returning cached codec {}", codec);
        return (TypeCodec<T>) codec;
    }

    @SuppressWarnings("unchecked")
    private <T> TypeCodec<T> findCodec(DataType cqlType, TypeToken<T> javaType)
    {
        checkNotNull(cqlType, "Parameter cqlType cannot be null");
        if (logger.isTraceEnabled())
            logger.trace("Looking for codec [{} <-> {}]", toString(cqlType), toString(javaType));

        // Look at the built-in codecs first
        for (TypeCodec<?> codec : BUILT_IN_CODECS)
        {
            if (codec.accepts(cqlType) && (javaType == null || codec.accepts(javaType)))
            {
                logger.trace("Built-in codec found: {}", codec);
                return (TypeCodec<T>) codec;
            }
        }

        // Look at the user-registered codecs next
        for (TypeCodec<?> codec : codecs)
        {
            if (codec.accepts(cqlType) && (javaType == null || codec.accepts(javaType)))
            {
                logger.trace("Already registered codec found: {}", codec);
                return (TypeCodec<T>) codec;
            }
        }
        return createCodec(cqlType, javaType);
    }

    @SuppressWarnings("unchecked")
    private <T> TypeCodec<T> findCodec(DataType cqlType, T value)
    {
        checkNotNull(value, "Parameter value cannot be null");
        if (logger.isTraceEnabled())
            logger.trace("Looking for codec [{} <-> {}]", toString(cqlType), value.getClass());

        // Look at the built-in codecs first
        for (TypeCodec<?> codec : BUILT_IN_CODECS)
        {
            if ((cqlType == null || codec.accepts(cqlType)) && codec.accepts(value))
            {
                logger.trace("Built-in codec found: {}", codec);
                return (TypeCodec<T>) codec;
            }
        }

        // Look at the user-registered codecs next
        for (TypeCodec<?> codec : codecs)
        {
            if ((cqlType == null || codec.accepts(cqlType)) && codec.accepts(value))
            {
                logger.trace("Already registered codec found: {}", codec);
                return (TypeCodec<T>) codec;
            }
        }
        return createCodec(cqlType, value);
    }

    private <T> TypeCodec<T> createCodec(DataType cqlType, TypeToken<T> javaType)
    {
        TypeCodec<T> codec = maybeCreateCodec(cqlType, javaType);
        if (codec == null) throw notFound(cqlType, javaType);
        // double-check that the created codec satisfies the initial request
        // this check can fail specially when creating codecs for collections
        // e.g. if B extends A and there is a codec registered for A and
        // we request a codec for List<B>, the registry would generate a codec for List<A>
        if (!codec.accepts(cqlType) || (javaType != null && !codec.accepts(javaType)))
            throw notFound(cqlType, javaType);
        logger.trace("Codec created: {}", codec);
        return codec;
    }

    private <T> TypeCodec<T> createCodec(DataType cqlType, T value)
    {
        TypeCodec<T> codec = maybeCreateCodec(cqlType, value);
        if (codec == null) throw notFound(cqlType, TypeToken.of(value.getClass()));
        // double-check that the created codec satisfies the initial request
        if ((cqlType != null && !codec.accepts(cqlType)) || !codec.accepts(value))
            throw notFound(cqlType, TypeToken.of(value.getClass()));
        logger.trace("Codec created: {}", codec);
        return codec;
    }

    @SuppressWarnings("unchecked")
    private <T> TypeCodec<T> maybeCreateCodec(DataType cqlType, TypeToken<T> javaType)
    {
        checkNotNull(cqlType);

        if (cqlType.getName() == LIST
            && (javaType == null || List.class.isAssignableFrom(javaType.getRawType())))
        {
            TypeToken<?> elementType = null;
            if (javaType != null && javaType.getType() instanceof ParameterizedType)
            {
                Type[] typeArguments = ((ParameterizedType) javaType.getType()).getActualTypeArguments();
                elementType = TypeToken.of(typeArguments[0]);
            }
            TypeCodec<?> eltCodec = findCodec(cqlType.getTypeArguments().get(0), elementType);
            return (TypeCodec<T>) TypeCodec.list(eltCodec);
        }

        if (cqlType.getName() == SET
            && (javaType == null || Set.class.isAssignableFrom(javaType.getRawType())))
        {
            TypeToken<?> elementType = null;
            if (javaType != null && javaType.getType() instanceof ParameterizedType)
            {
                Type[] typeArguments = ((ParameterizedType) javaType.getType()).getActualTypeArguments();
                elementType = TypeToken.of(typeArguments[0]);
            }
            TypeCodec<?> eltCodec = findCodec(cqlType.getTypeArguments().get(0), elementType);
            return (TypeCodec<T>) TypeCodec.set(eltCodec);
        }

        if (cqlType.getName() == MAP
            && (javaType == null || Map.class.isAssignableFrom(javaType.getRawType())))
        {
            TypeToken<?> keyType = null;
            TypeToken<?> valueType = null;
            if (javaType != null && javaType.getType() instanceof ParameterizedType)
            {
                Type[] typeArguments = ((ParameterizedType) javaType.getType()).getActualTypeArguments();
                keyType = TypeToken.of(typeArguments[0]);
                valueType = TypeToken.of(typeArguments[1]);
            }
            TypeCodec<?> keyCodec = findCodec(cqlType.getTypeArguments().get(0), keyType);
            TypeCodec<?> valueCodec = findCodec(cqlType.getTypeArguments().get(1), valueType);
            return (TypeCodec<T>) TypeCodec.map(keyCodec, valueCodec);
        }

        if (cqlType instanceof VectorType
            && (javaType == null || List.class.isAssignableFrom(javaType.getRawType())))
        {
            VectorType type = (VectorType) cqlType;
            return (TypeCodec<T>) TypeCodec.vector(type, findCodec(type.getSubtype(), null));
        }

        if (cqlType instanceof TupleType
            && (javaType == null || TupleValue.class.isAssignableFrom(javaType.getRawType())))
        {
            return (TypeCodec<T>) TypeCodec.tuple((TupleType) cqlType);
        }

        if (cqlType instanceof UserType
            && (javaType == null || UDTValue.class.isAssignableFrom(javaType.getRawType())))
        {
            return (TypeCodec<T>) TypeCodec.userType((UserType) cqlType);
        }

        if (cqlType instanceof DataType.CustomType
            && (javaType == null || ByteBuffer.class.isAssignableFrom(javaType.getRawType())))
        {
            return (TypeCodec<T>) TypeCodec.custom((DataType.CustomType) cqlType);
        }

        return null;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private <T> TypeCodec<T> maybeCreateCodec(DataType cqlType, T value)
    {
        checkNotNull(value);

        if ((cqlType == null || cqlType.getName() == LIST) && value instanceof List)
        {
            List list = (List) value;
            if (list.isEmpty())
            {
                DataType elementType =
                (cqlType == null || cqlType.getTypeArguments().isEmpty())
                ? DataType.blob()
                : cqlType.getTypeArguments().get(0);
                return (TypeCodec<T>) TypeCodec.list(findCodec(elementType, (TypeToken) null));
            }
            else
            {
                DataType elementType =
                (cqlType == null || cqlType.getTypeArguments().isEmpty())
                ? null
                : cqlType.getTypeArguments().get(0);
                return (TypeCodec<T>) TypeCodec.list(findCodec(elementType, list.iterator().next()));
            }
        }

        if ((cqlType == null || cqlType.getName() == SET) && value instanceof Set)
        {
            Set set = (Set) value;
            if (set.isEmpty())
            {
                DataType elementType =
                (cqlType == null || cqlType.getTypeArguments().isEmpty())
                ? DataType.blob()
                : cqlType.getTypeArguments().get(0);
                return (TypeCodec<T>) TypeCodec.set(findCodec(elementType, (TypeToken) null));
            }
            else
            {
                DataType elementType =
                (cqlType == null || cqlType.getTypeArguments().isEmpty())
                ? null
                : cqlType.getTypeArguments().get(0);
                return (TypeCodec<T>) TypeCodec.set(findCodec(elementType, set.iterator().next()));
            }
        }

        if ((cqlType == null || cqlType.getName() == MAP) && value instanceof Map)
        {
            Map map = (Map) value;
            if (map.isEmpty())
            {
                DataType keyType =
                (cqlType == null || cqlType.getTypeArguments().size() < 1)
                ? DataType.blob()
                : cqlType.getTypeArguments().get(0);
                DataType valueType =
                (cqlType == null || cqlType.getTypeArguments().size() < 2)
                ? DataType.blob()
                : cqlType.getTypeArguments().get(1);
                return (TypeCodec<T>) TypeCodec.map(
                findCodec(keyType, (TypeToken) null), findCodec(valueType, (TypeToken) null));
            }
            else
            {
                DataType keyType =
                (cqlType == null || cqlType.getTypeArguments().size() < 1)
                ? null
                : cqlType.getTypeArguments().get(0);
                DataType valueType =
                (cqlType == null || cqlType.getTypeArguments().size() < 2)
                ? null
                : cqlType.getTypeArguments().get(1);
                Map.Entry entry = (Map.Entry) map.entrySet().iterator().next();
                return (TypeCodec<T>)
                       TypeCodec.map(
                       findCodec(keyType, entry.getKey()), findCodec(valueType, entry.getValue()));
            }
        }

        if ((cqlType == null || cqlType.getName() == VECTOR) && value instanceof List)
        {
            VectorType type = (VectorType) cqlType;
            return (TypeCodec<T>) TypeCodec.vector(type, findCodec(type.getSubtype(), null));
        }

        if ((cqlType == null || cqlType.getName() == TUPLE)
            && value instanceof TupleValue)
        {
            return (TypeCodec<T>)
                   TypeCodec.tuple(cqlType == null ? ((TupleValue) value).getType() : (TupleType) cqlType);
        }

        if ((cqlType == null || cqlType.getName() == UDT) && value instanceof UDTValue)
        {
            return (TypeCodec<T>)
                   TypeCodec.userType(cqlType == null ? ((UDTValue) value).getType() : (UserType) cqlType);
        }

        if ((cqlType instanceof DataType.CustomType)
            && value instanceof ByteBuffer)
        {
            return (TypeCodec<T>) TypeCodec.custom((DataType.CustomType) cqlType);
        }

        return null;
    }

    private static CodecNotFoundException notFound(DataType cqlType, TypeToken<?> javaType)
    {
        String msg =
        String.format(
        "Codec not found for requested operation: [%s <-> %s]",
        toString(cqlType), toString(javaType));
        return new CodecNotFoundException(msg);
    }

    private static String toString(Object value)
    {
        return value == null ? "ANY" : value.toString();
    }
}
