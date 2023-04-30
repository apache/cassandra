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

import org.apache.cassandra.exceptions.ConfigurationException;

public interface ConfigurationValue<T>
{
    T get();

    String key();

    void listen(ConfigurationSourceListener.EventType changeType, BiConsumer<T, T> values);
    default void listenOptional(ConfigurationSourceListener.EventType changeType, BiConsumer<Optional<T>, Optional<T>> values)
    {
        listen(changeType, (oldValue, newValue) -> values.accept(Optional.ofNullable(oldValue), Optional.ofNullable(newValue)));
    }

    <U> ConfigurationValue<U> map(Mapper<? super T, ? extends U> mapper);

    default <U> ConfigurationValue<U> map(Mapper<? super T, ? extends U> mapper,
                                          BiFunction<? super Exception, ? super T, ? extends ConfigurationException> handler)
    {
        return map(new FlattenMappper<>(mapper, handler));
    }

    T orElse(Supplier<? extends T> defaultValue);

    <E extends ConfigurationException> T orElseThrow(Supplier<E> supplier) throws E;

    void ifPresent(Consumer<? super T> consumer);

    default boolean isEmpty()
    {
        return get() == null;
    }


    interface Mapper<T, R> extends Function<T, R>
    {
        /**
         * Applies this function to the given argument.
         *
         * @param value the argument to map.
         * @return the mapping result
         * @throws Exception if an error occurs during the mapping.
         */
        R map(T value) throws Exception;

        /**
         * Handle an exception thrown by {@link #map(Object)}.
         *
         * @param ex    the exception thrown by {@link #map(Object)}.
         * @param value the value passed to {@link #map(Object)}.
         * @return the exception to throw.
         */
        default ConfigurationException onFailure(Exception ex, T value)
        {
            return new ConfigurationException(String.format("Invalid configuration value '%s'. Cause: %s ",
                                                            value == null ? null : value.toString(), ex.getMessage()), false);
        }

        /**
         * {@inheritDoc}
         */
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
