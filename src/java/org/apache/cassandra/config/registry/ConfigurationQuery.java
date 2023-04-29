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

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.cassandra.exceptions.ConfigurationException;

import static java.util.Optional.ofNullable;

/**
 *
 */
public class ConfigurationQuery implements Iterable<ConfigurationValue<?>>
{
    private static final Map<ConfigurationSource, ConfigurationQuery> instances = new HashMap<>();
    private final List<ListenerUnsubscriber> unsubscribers = new CopyOnWriteArrayList<>();
    private final Map<String, ConfigurationValue<?>> values = new ConcurrentHashMap<>();
    private final Function<ConfigurationException, ? extends RuntimeException> exceptionHandler;
    private final ConfigurationSource source;

    private ConfigurationQuery(ConfigurationSource source,
                               Function<ConfigurationException, ? extends RuntimeException> exceptionHandler)
    {
        this.source = source;
        this.exceptionHandler = exceptionHandler;
        unsubscribers.add(source.addSourceListener((name, eventType, oldValue, newValue) -> {
            ConfigurationValue<?> value = values.get(name);
            if (value == null)
                return;
            ((ValueChangeListener) value).update(eventType, oldValue, newValue);
        }));
    }

    private ConfigurationQuery(ConfigurationSource source)
    {
        this(source, e -> e);
    }

    public static ConfigurationQuery from(ConfigurationSource source)
    {
        return instances.computeIfAbsent(source, ConfigurationQuery::new);
    }

    public static ConfigurationQuery from(ConfigurationSource source,
                                          Function<ConfigurationException, ? extends RuntimeException> handler)
    {
        return instances.computeIfAbsent(source, s -> new ConfigurationQuery(s, handler));
    }

    /**
     * Get the value of the configuration property with the given key.
     * @param clazz Configuration property type calss
     * @param key Configuration property name.
     * @return Configuration property value.
     * @param <T> Configuration property type.
     */
    @SuppressWarnings("unchecked")
    public <T> ConfigurationValue<T> getValue(Class<T> clazz, String key)
    {
        return (ConfigurationValue<T>) values.computeIfAbsent(key, k -> new ConfigurationValueImpl<>(
            k,
            () -> getAndHandle(() -> source.get(clazz, k), exceptionHandler),
            clazz::cast
        ));
    }

    /**
     * Get the value of the configuration property and handle the exception thrown by the configuration source.
     * @param supplier Supplier of the configuration property value.
     * @param exceptionHandler Exception handler for the configuration property value.
     * @return Configuration property value.
     * @param <T> Configuration property type.
     */
    private static <T> T getAndHandle(Supplier<T> supplier, Function<ConfigurationException, ? extends RuntimeException> exceptionHandler)
    {
        try
        {
            return supplier.get();
        }
        catch (ConfigurationException e)
        {
            throw exceptionHandler.apply(e);
        }
    }

    @Override
    public Iterator<ConfigurationValue<?>> iterator()
    {
        return values.values().iterator();
    }

    public static void shutdown()
    {
        instances.forEach((source, registry) -> registry.unsubscribers.forEach(ListenerUnsubscriber::unsubscribe));
    }

    private class ConfigurationValueImpl<T> implements ConfigurationValue<T>, ValueChangeListener
    {
        private final String key;
        private final Supplier<T> value;
        private final Map<ConfigurationSourceListener.EventType, List<BiConsumer<T, T>>> valueListeners = new EnumMap<>(ConfigurationSourceListener.EventType.class);
        private final List<ValueChangeListener> dependents = new CopyOnWriteArrayList<>();
        private final TypeConverter<T> converter;

        public ConfigurationValueImpl(String key, Supplier<T> value, TypeConverter<T> converter)
        {
            this.key = key;
            this.value = value;
            this.converter = converter;
        }

        @Override
        public T get()
        {
            return value.get();
        }

        @Override
        public String key()
        {
            return key;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <U> ConfigurationValue<U> map(Mapper<? super T, ? extends U> mapper)
        {
            ConfigurationValueImpl<U> next = new ConfigurationValueImpl<>(key,
                                                                          () -> ofNullable(get()).map(mapper).orElse(null),
                                                                          // It is safe to cast the old value to T because
                                                                          // the mapper is called when type old value is T.
                                                                          oldValue -> mapper.apply((T) oldValue));
            dependents.add(next);
            return next;
        }

        @Override
        public T orElse(Supplier<? extends T> defaultValue)
        {
            T value0 = get();
            return value0 == null ? defaultValue.get() : value0;
        }

        @Override
        public <E extends ConfigurationException> T orElseThrow(Supplier<E> supplier) throws E
        {
            T value0 = get();
            if (value0 == null)
                throw supplier.get();
            else
                return value0;
        }

        @Override
        public void ifPresent(Consumer<? super T> consumer)
        {
            T value0 = get();
            if (value0 == null)
                return;
            consumer.accept(value0);
        }

        @Override
        public void listen(ConfigurationSourceListener.EventType changeType, BiConsumer<T, T> listener)
        {
            valueListeners.computeIfAbsent(changeType, t -> new CopyOnWriteArrayList<>()).add(listener);
            ConfigurationQuery.this.unsubscribers.add(() -> valueListeners.get(changeType).remove(listener));
        }

        @Override
        public void update(ConfigurationSourceListener.EventType changeType, Object oldValue, Object newValue)
        {
            dependents.forEach(listener -> listener.update(changeType, oldValue, newValue));
            ofNullable(valueListeners.get(changeType)).ifPresent(l -> l.forEach(listener -> listener.accept(converter.convertNullable(oldValue),
                                                                                                            converter.convertNullable(newValue))));
        }
    }

    interface ValueChangeListener
    {
        void update(ConfigurationSourceListener.EventType changeType, Object oldValue, Object newValue);
    }
}
