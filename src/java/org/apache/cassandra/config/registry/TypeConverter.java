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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Converts configuration value from one type to another, you can use {@link org.apache.cassandra.config.StringConverters}
 * if your input type is String and you want to convert it to an appropriate configuration object type.
 *
 * @param <T> Type to convert to.
 *
 * @see Registry
 * @see org.apache.cassandra.config.StringConverters
 */
public interface TypeConverter<T>
{
    TypeConverter<String> DEFAULT = Object::toString;

    /**
     * Converts a value to the target type.
     * @param value Value to convert.
     * @return Converted value.
     */
    @Nullable T convert(@Nonnull Object value);

    default @Nullable T convertNullable(@Nullable Object value)
    {
        return value == null ? null : convert(value);
    }
}
