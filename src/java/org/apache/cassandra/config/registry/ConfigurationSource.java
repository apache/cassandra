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

public interface ConfigurationSource
{
    /**
     * Sets the value of the property with the given name.
     *
     * @param name the name of the property.
     * @param value the value of the property.
     */
    <T> void set(String name, T value);

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

    /**
     * Returns the value of the property with the given name.
     *
     * @param clazz the class of the property.
     * @param name the name of the property.
     * @return the value of the property.
     */
    default <T> Optional<T> getOptional(Class<T> clazz, String name) {
        return Optional.ofNullable(get(clazz, name));
    }
}
