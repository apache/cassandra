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

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import org.apache.cassandra.config.Config;

/**
 * A {@link ConfigurationSource} that reads from a {@link Config} object.
 */
public class ConfigurationRegistryImpl implements ConfigurationRegistry
{
    private final ConfigurationSource source;

    private ConfigurationRegistryImpl(ConfigurationSource source)
    {
        this.source = source;
    }

    @Override
    public void setValue(String name, @Nullable Object value)
    {

    }

    @Override
    public <T> ConfigurationValue<T> getValue(Class<T> clazz, String key)
    {
        return null;
    }

    @Override
    public <T> Optional<ConfigurationValue<T>> getValueOptional(Class<T> clazz, String key)
    {
        return Optional.empty();
    }

    @Override
    public Iterator<ConfigurationValue<?>> iterator()
    {
        return null;
    }

    private static class ConfigurationValueImpl<T> implements ConfigurationValue<T>
    {
        private final String key;
        private final Supplier<T> value;

        private final Map<ConfigurationSourceListener.EventType, List<BiConsumer<T, T>>> listeners = new EnumMap<>(ConfigurationSourceListener.EventType.class);

        public ConfigurationValueImpl(String key, Supplier<T> value)
        {
            this.key = key;
            this.value = value;
        }

        @Override
        public T get()
        {
            return value.get();
        }

        @Override
        public String getString()
        {
            return null;
        }

        @Override
        public String key()
        {
            return key;
        }

        @Override
        public <U> ConfigurationValue<U> map(Mapper<? super T, ? extends U> mapper)
        {
            return isEmpty() ?
                   new ConfigurationValueImpl<>(key, () -> null) :
                   new ConfigurationValueImpl<>(key, () -> mapper.apply(value.get()));
        }

        @Override
        public ListenerRemover addListener(ConfigurationSourceListener.EventType changeType, BiConsumer<T, T> listener)
        {
            listeners.computeIfAbsent(changeType, t -> new ArrayList<>()).add(listener);
            return () -> listeners.get(changeType).remove(listener);
        }
    }
}
