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

import org.yaml.snakeyaml.introspector.Property;

/**
 * A {@code ListenableProperty} represents wrapper of a {@code Property} for single member variable of a class
 * (or its accessor methods) that can be listened to for value updates of the property. The listeners are called
 * before and after the value is set on the object. The before change listener can modify the value to be set on
 * the object, which is useful for validation or transformation of the value before it is set on the object
 * e.g. converting {@code null} to a default value or throwing an exception if the value is invalid.
 */
public class ListenableProperty<S, T> extends ForwardingProperty
{
    private final List<Listener<S, T>> listeners = new CopyOnWriteArrayList<>();
    private final boolean hasMutableAnnotation;

    public ListenableProperty(Property property)
    {
        super(property.getName(), property);
        this.hasMutableAnnotation = property.getAnnotation(Mutable.class) != null;
    }

    public boolean hasMutableAnnotation()
    {
        return hasMutableAnnotation;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void set(Object source, Object newValue) throws Exception
    {
        T oldValue = (T) get(source);
        T value = (T) newValue;
        for (Listener<S, T> handler : listeners)
            value = handler.before((S) source, getName(), oldValue, value);
        delegate().set(source, value);
        for (Listener<S, T> handler : listeners)
            handler.after((S) source, getName(), oldValue, value);
    }

    public Remover addListener(Listener<S, T> listener)
    {
        listeners.add(listener);
        return () -> listeners.remove(listener);
    }

    /**
     * The handler to be notified before and after a configuration value is changed.
     * @param <S> the type of the object to mutate.
     * @param <V> the type of the value to mutate.
     */
    public interface Listener<S, V>
    {
        default V before(S source, String name, V oldValue, V newValue) { return newValue; }
        default void after(S source, String name, V oldValue, V newValue) {}
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
