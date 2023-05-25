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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;

import org.yaml.snakeyaml.introspector.Property;

/**
 *
 */
public class ListenableProperty<S, T> extends ForwardingProperty
{
    private final List<BeforeChangeListener<S, T>> beforeHandlers = new CopyOnWriteArrayList<>();
    private final List<AfterChangeListener<S, T>> afterHandlers = new CopyOnWriteArrayList<>();

    public ListenableProperty(Property property)
    {
        super(property.getName(), property);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void set(Object source, Object newValue) throws Exception
    {
        T oldValue = (T) get(source);
        T value = (T) newValue;
        for (BeforeChangeListener<S, T> handler : beforeHandlers)
            value = handler.before((S) source, getName(), oldValue, value);
        delegate().set(source, value);
        for (AfterChangeListener<S, T> handler : afterHandlers)
            handler.after((S) source, getName(), oldValue, value);
    }

    public Remover addBeforeListener(BeforeChangeListener<S, T> listener)
    {
        beforeHandlers.add(listener);
        return () -> beforeHandlers.remove(listener);
    }

    public Remover addAfterListener(AfterChangeListener<S, T> listener)
    {
        afterHandlers.add(listener);
        return () -> afterHandlers.remove(listener);
    }

    /**
     * The handler to be notified before and after a configuration value is changed.
     * @param <S> the type of the object to mutate.
     * @param <V> the type of the value to mutate.
     */
    @FunctionalInterface
    public interface BeforeChangeListener<S, V>
    {
        V before(S source, String name, V oldValue, V newValue);

        static <S, V> BeforeChangeListener<S, V> consume(BiConsumer<? super V, ? super  V> consumer)
        {
            return (source, name, oldValue, newValue) -> {
                consumer.accept(oldValue, newValue);
                return newValue;
            };
        }
    }

    /**
     * The listener to be notified after a configuration value is changed.
     * @param <S> the type of the object listened to.
     * @param <V> the type of the value.
     */
    @FunctionalInterface
    public interface AfterChangeListener<S, V>
    {
        void after(S source, String name, V oldValue, V newValue);

        static <S, V> AfterChangeListener<S, V> consume(BiConsumer<? super V, ? super  V> consumer)
        {
            return (source, name, oldValue, newValue) -> consumer.accept(oldValue, newValue);
        }
    }

    /**
     * The handler to remove a configuration value listeners.
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
