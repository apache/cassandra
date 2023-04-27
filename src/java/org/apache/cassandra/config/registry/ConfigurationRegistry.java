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

package org.apache.cassandra.config.registry;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import org.apache.cassandra.exceptions.ConfigurationException;

/**
 * A registry of Cassandra's configuration properties that can be updated at runtime. The {@link org.apache.cassandra.config.Config}
 * class is the source of configuration fields, types and other metadata available to the registry. The registry is used to
 * handle configuration properties that are loaded from the configuration file, and are set via JMX or updated through
 * the settings virtual table.
 * <p>
 * You can use {@link #setValue(String, Object)} to update a property, in case the property is not present in the registry,
 * an exception will be thrown. If the property is present, the registry will try to convert given value to the property's
 * type, and if the conversion fails, an exception will be thrown. You can use the {@code String} as a value to be converted,
 * or you can use the property's type as a value. In the latter case, no conversion will be performed.
 * <p>
 */
public interface ConfigurationRegistry extends Iterable<ConfigurationRegistry.ConfigurationValue<?>>
{
    /**
     * Update configuration property with the given name to the given value. The value may be the same
     * as the property's value, or it may be represented as a string. In the latter case a corresponding
     * will be called to get the property's value matching type.
     *
     * @param name Property name.
     * @param value Value to set.
     */
    void setValue(String name, @Nullable Object value);

    /**
     * Get the value of the configuration property with the given key.
     * @param clazz Configuration property type calss
     * @param key Configuration property name.
     * @return Configuration property value.
     * @param <T> Configuration property type.
     */
    <T> ConfigurationValue<T> getValue(Class<T> clazz, String key);

    /**
     * Get the value of the configuration property with the given key.
     * @param clazz Configuration property type calss
     * @param key Configuration property name.
     * @return Configuration property value.
     * @param <T> Configuration property type.
     */
    <T> Optional<ConfigurationValue<T>> getValueOptional(Class<T> clazz, String key);

    interface ConfigurationValue<T>
    {
        T get();

        String getString();

        String key();

        ListenerRemover addListener(ConfigurationSourceListener.EventType changeType, BiConsumer<T, T> values);

        <U> ConfigurationValue<U> map(Mapper<? super T, ? extends U> mapper);

        default <U> ConfigurationValue<U> map(Mapper<? super T, ? extends U> mapper,
                                              BiFunction<? super Exception, ? super T, ? extends ConfigurationException> handler)
        {
            return map(new FlattenMappper<>(mapper, handler));
        }

        default boolean isEmpty()
        {
            return get() == null;
        }

        default T orElse(Supplier<? extends T> defaultValue)
        {
            return isEmpty() ? defaultValue.get() : get();
        }

        default <E extends ConfigurationException> T orElseThrow(Supplier<E> supplier) throws E
        {
            if (isEmpty())
                throw supplier.get();
            else
                return get();
        }

        default void ifPresent(Consumer<? super T> consumer)
        {
            if (isEmpty())
                return;
            consumer.accept(get());
        }
    }

    /**
     * The handler to remove a configuration value listener.
     */
    @FunctionalInterface
    interface ListenerRemover
    {
        void remove();
    }

    interface Mapper<T, R> extends Function<T, R>
    {
        /**
         * Applies this function to the given argument.
         * @param value the argument to map.
         * @return the mapping result
         * @throws Exception if an error occurs during the mapping.
         */
        R map(T value) throws Exception;

        /**
         * Handle an exception thrown by {@link #map(Object)}.
         *
         * @param ex the exception thrown by {@link #map(Object)}.
         * @param value the value passed to {@link #map(Object)}.
         * @return the exception to throw.
         */
        default ConfigurationException onFailure(Exception ex, T value)
        {
            return new ConfigurationException(String.format("Invalid configuration value '%s'. Cause: %s ",
                                                            value == null ? null : value.toString(), ex.getMessage()), false);
        }

        /** {@inheritDoc} */
        @Override
        default R apply(T value)
        {
            try
            {
                return map(value);
            }
            catch (Exception e)
            {
                throw onFailure(e, value);
            }
        }
    }

    /**
     * A mapper that flattens the exception thrown by the delegate mapper.
     */
    class FlattenMappper<T, R> implements Mapper<T, R>
    {
        private final Mapper<? super T, ? extends R> mapper;
        private final BiFunction<? super Exception, ? super T, ? extends ConfigurationException> handler;

        public FlattenMappper(Mapper<? super T, ? extends R> mapper,
                              BiFunction<? super Exception, ? super T, ? extends ConfigurationException> handler)
        {
            this.mapper = mapper;
            this.handler = handler;
        }

        @Override
        public R map(T value) throws Exception
        {
            return mapper.apply(value);
        }

        @Override
        public ConfigurationException onFailure(Exception ex, T value)
        {
            return handler.apply(ex, value);
        }
    }
}
