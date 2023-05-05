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

import java.util.function.Supplier;

import org.apache.cassandra.config.DataRateSpec;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.utils.Pair;

/**
 * A source of Cassandra's configuration properties that can be updated at runtime. As an example you can take
 * a look at the {@link org.apache.cassandra.config.Config} class of configuration fields available to the system.
 * This source is used to provide an access layer to the configuration properties, and to provide a way to
 * handle properties that are loaded from the {@code cassandra.yaml} configuration file, and are updated via JMX or
 * through the settings virtual table.
 * <p>
 * You can use {@link #set(String, Object)} to update a configuration property, in case the property is not present
 * in the source the {@link org.apache.cassandra.exceptions.PropertyNotFoundException} will be thrown. If the property
 * is present, the source will try to convert given value to a corresponding configuration property type, and if
 * the conversion fails, an exception will be thrown. You can use the {@code String} as a value to convert to,
 * or you can use the property's type as a value as a value input. In the latter case, no conversion will be performed.
 *
 * @see org.apache.cassandra.config.Config
 * @see org.apache.cassandra.exceptions.PropertyNotFoundException
 */
public interface ConfigurationSource extends Iterable<Pair<String, Supplier<Object>>>
{
    /**
     * Sets the value of the property with the given name.
     *
     * @param name the name of the property.
     * @param value the value of the property.
     */
    <T> void set(String name, T value);

    /**
     * Returns the type of the property with the given name.
     *
     * @param name the name of the property.
     * @return the type of the property.
     */
    Class<?> type(String name);

    Object getRaw(String name);

    /**
     * Returns the value of the property with the given name.
     *
     * @param clazz the class of the property.
     * @param name the name of the property.
     * @return the value of the property.
     */
    <T> T get(Class<T> clazz, String name);

    /**
     * Returns the value of the property with the given name.
     *
     * @param clazz the class of the property.
     * @param name the name of the property.
     * @param defaultValue the default value of the property.
     * @return the value of the property.
     */
    default <T> T get(Class<T> clazz, String name, T defaultValue)
    {
        T value = get(clazz, name);
        return value == null ? defaultValue : value;
    }

    default String getString(String name)
    {
        return get(String.class, name);
    }

    default String getString(String name, String defaultValue)
    {
        return get(String.class, name, defaultValue);
    }

    default Integer getInteger(String name)
    {
        return get(Integer.class, name);
    }

    default <T extends DataRateSpec<T>> T getDataRateSpec(Class<T> clazz, String name)
    {
        return get(clazz, name);
    }

    default <T extends DataRateSpec<T>> T getDataRateSpec(String name, T defaultValue)
    {
        T value = getDataRateSpec(defaultValue.getDeclaringClass(), name);
        return value == null ? defaultValue : value;
    }

    default <T extends DataStorageSpec<T>> T getDataStorageSpec(Class<T> clazz, String name)
    {
        return get(clazz, name);
    }

    default <T extends DataStorageSpec<T>> T getDataStorageSpec(String name, T defaultValue)
    {
        T value = getDataStorageSpec(defaultValue.getDeclaringClass(), name);
        return value == null ? defaultValue : value;
    }

    /**
     * Adds a listener to the configuration source.
     * @param listener the listener to add.
     */
    default ListenerRemover addSourceListener(ConfigurationSourceListener listener)
    {
        throw new UnsupportedOperationException();
    }
}
