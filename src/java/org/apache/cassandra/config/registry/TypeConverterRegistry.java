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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.config.PropertyConverter;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.ConfigurationException;

public class TypeConverterRegistry
{
    private final Map<ConversionKey, TypeConverter<?, ?>> converters = new HashMap<>();

    public TypeConverterRegistry()
    {
        registerConverters(this);
    }

    private static void registerConverters(TypeConverterRegistry registry)
    {
        // Primitive types.
        addFromStringConverter(registry, Boolean.class, CassandraRelevantProperties.BOOLEAN_CONVERTER);
        addFromStringConverter(registry, Boolean.TYPE, CassandraRelevantProperties.BOOLEAN_CONVERTER);
        addFromStringConverter(registry, Double.class, CassandraRelevantProperties.DOUBLE_CONVERTER);
        addFromStringConverter(registry, Double.TYPE, CassandraRelevantProperties.DOUBLE_CONVERTER);
        addFromStringConverter(registry, Integer.class, CassandraRelevantProperties.INTEGER_CONVERTER);
        addFromStringConverter(registry, Integer.TYPE, CassandraRelevantProperties.INTEGER_CONVERTER);
        addFromStringConverter(registry, Long.class, CassandraRelevantProperties.LONG_CONVERTER);
        addFromStringConverter(registry, Long.TYPE, CassandraRelevantProperties.LONG_CONVERTER);
        addFromStringConverter(registry, String.class, CassandraRelevantProperties.STRING_CONVERTER);
        // Cassandra specific configuration types.
        addFromStringConverter(registry, DurationSpec.LongNanosecondsBound.class, DurationSpec.LongNanosecondsBound::new);
        addFromStringConverter(registry, DurationSpec.LongMillisecondsBound.class, DurationSpec.LongMillisecondsBound::new);
        addFromStringConverter(registry, DurationSpec.LongSecondsBound.class, DurationSpec.LongSecondsBound::new);
        addFromStringConverter(registry, DurationSpec.IntMinutesBound.class, DurationSpec.IntMinutesBound::new);
        addFromStringConverter(registry, DurationSpec.IntSecondsBound.class, DurationSpec.IntSecondsBound::new);
        addFromStringConverter(registry, DurationSpec.IntMillisecondsBound.class, DurationSpec.IntMillisecondsBound::new);
        addFromStringConverter(registry, DataStorageSpec.LongBytesBound.class, DataStorageSpec.LongBytesBound::new);
        addFromStringConverter(registry, DataStorageSpec.IntBytesBound.class, DataStorageSpec.IntBytesBound::new);
        addFromStringConverter(registry, DataStorageSpec.IntKibibytesBound.class, DataStorageSpec.IntKibibytesBound::new);
        addFromStringConverter(registry, DataStorageSpec.LongMebibytesBound.class, DataStorageSpec.LongMebibytesBound::new);
        addFromStringConverter(registry, DataStorageSpec.IntMebibytesBound.class, DataStorageSpec.IntMebibytesBound::new);
        // Cassandra Enum types.
        addFromStringConverter(registry, ConsistencyLevel.class, ConsistencyLevel::fromStringIgnoreCase);
    }

    public static <T> void addFromStringConverter(TypeConverterRegistry registry, Class<T> to, PropertyConverter<T> converter)
    {
        registry.addTypeConverter(String.class, to, new TypeConverter<String, T>()
        {
            @Nullable @Override public T convert(@Nullable String value)
            {
                if (to.isPrimitive() && StringUtils.isEmpty(value))
                    throw new ConfigurationException("Primitive type " + to + " cannot be null or empty");
                return value == null ? null : converter.convert(value);
            }
        });
    }

    public <F, T> void addTypeConverter(Class<F> from, Class<T> to, TypeConverter<F, T> converter)
    {
        converters.put(new ConversionKey(from, to), converter);
    }

    public <T> TypeConverter<?, T> converter(Class<?> from, Class<T> to)
    {
        return converter(from, to, null);
    }

    @SuppressWarnings("unchecked")
    public <T> TypeConverter<?, T> converter(Class<?> from, Class<T> to, TypeConverter<?, T> defaultConverter)
    {
        ConversionKey key = new ConversionKey(from, to);
        if (!converters.containsKey(key) && defaultConverter == null)
            throw new IllegalArgumentException(String.format("No converter registered to convert from '%s' to '%s'.", from, to));
        return (TypeConverter<?, T>) converters.getOrDefault(key, defaultConverter);
    }

    private static class ConversionKey
    {
        private final Class<?> from;
        private final Class<?> to;
        public ConversionKey(Class<?> from, Class<?> to)
        {
            this.from = from;
            this.to = to;
        }
        @Override public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ConversionKey that = (ConversionKey) o;
            return from.equals(that.from) && to.equals(that.to);
        }
        @Override public int hashCode()
        {
            return Objects.hash(from, to);
        }
    }
}
