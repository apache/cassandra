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
import javax.annotation.Nullable;

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
public interface ConfigurationRegistry extends Iterable<ConfigurationValue<?>>
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
}
