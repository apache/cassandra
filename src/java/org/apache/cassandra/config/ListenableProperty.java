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
package org.apache.cassandra.config;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;

import org.yaml.snakeyaml.introspector.Property;

/**
 *
 */
public class ListenableProperty<S, T> extends ForwardingProperty
{
    private final Map<EventType, List<Handler<S, T>>> handlers = new EnumMap<>(EventType.class);

    public ListenableProperty(Property property)
    {
        super(property.getName(), property);
        for (EventType eventType : EventType.values())
            handlers.put(eventType, new CopyOnWriteArrayList<>());
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized void set(Object source, Object newValue) throws Exception
    {
        T oldValue = (T) get(source);
        T value = Handler.compose(handlers.get(EventType.BEFORE)).handle((S) source, oldValue, (T) newValue);
        delegate().set(source, value);
        Handler.compose(handlers.get(EventType.AFTER)).handle((S) source, oldValue, value);
    }

    public synchronized Remover addBeforeHandler(Handler<S, T> handler)
    {
        handlers.computeIfAbsent(EventType.BEFORE, k -> new CopyOnWriteArrayList<>()).add(handler);
        return () -> handlers.get(EventType.BEFORE).remove(handler);
    }

    public synchronized Remover addAfterHandler(Handler<S, T> handler)
    {
        handlers.computeIfAbsent(EventType.AFTER, k -> new CopyOnWriteArrayList<>()).add(handler);
        return () -> handlers.get(EventType.AFTER).remove(handler);
    }

    private enum EventType
    {
        BEFORE, AFTER
    }

    /**
     * The handler to be notified before and after a configuration value is changed.
     * @param <S> the type of the object to mutate.
     * @param <V> the type of the value to mutate.
     */
    @FunctionalInterface
    public interface Handler<S, V>
    {
        V handle(S source, V oldValue, V newValue);

        static <S, V> Handler<S, V> consume(BiConsumer<? super V, ? super  V> consumer)
        {
            return (source, oldValue, newValue) -> {
                consumer.accept(oldValue, newValue);
                return newValue;
            };
        }

        static <S, V> Handler<S, V > compose(List<Handler < S, V >> handlers)
        {
            return (source, oldValue, newValue) -> {
                V value = newValue;
                for (Handler<S, V> handler : handlers)
                    value = handler.handle(source, oldValue, value);
                return value;
            };
        }
    }

    /**
     * The handler to remove a configuration value listener.
     */
    @FunctionalInterface
    public interface Remover extends Runnable
    {
        void remove();

        @Override
        default void run()
        {
            remove();
        }
    }
}
