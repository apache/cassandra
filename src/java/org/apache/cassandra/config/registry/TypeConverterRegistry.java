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

import org.apache.cassandra.config.DataRateSpec;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.ConfigurationException;

import static org.apache.commons.lang3.ClassUtils.primitiveToWrapper;

/**
 * A registry for {@link TypeConverter} instances.
 */
public class TypeConverterRegistry
{
    public static final TypeConverterRegistry instance = new TypeConverterRegistry();
    private final Map<ConverterKey, TypeConverter<?>> converters = new HashMap<>();

    private TypeConverterRegistry()
    {
        registerConverters(converters);
    }

    public <V> TypeConverter<V> get(Class<?> from, Class<V> to)
    {
        Class<?> fromShaded = primitiveToWrapper(from);
        TypeConverter<V> converter = get(fromShaded, to, null);
        if (converter == null)
            throw new ConfigurationException(String.format("No converter found from '%s' to '%s'", fromShaded.getCanonicalName(), to.getCanonicalName()));
        return converter;
    }

    @SuppressWarnings("unchecked")
    public <V> TypeConverter<V> get(Class<?> from, Class<V> to, TypeConverter<V> defaultConverter)
    {
        Class<?> fromShaded = primitiveToWrapper(from);
        if (fromShaded.equals(to))
            return to::cast;
        if (converters.get(key(fromShaded, to)) == null)
            return defaultConverter;
        return (TypeConverter<V>) converters.get(key(fromShaded, to));
    }

    private static void registerConverters(Map<ConverterKey, TypeConverter<?>> converters)
    {
        addForwardBackwardStringConverters(converters, Boolean.TYPE, s -> Boolean.parseBoolean((String) s), b -> Boolean.toString((Boolean) b));
        addForwardBackwardStringConverters(converters, Double.TYPE, s -> Double.parseDouble((String) s), d -> Double.toString((Double) d));
        addForwardBackwardStringConverters(converters, Integer.TYPE, s -> Integer.parseInt((String) s), i -> Integer.toString((Integer) i));
        addForwardBackwardStringConverters(converters, Long.TYPE, s -> Long.parseLong((String) s), l -> Long.toString((Long) l));
        addForwardBackwardStringConverters(converters, Boolean.class, s -> Boolean.parseBoolean((String) s), b -> Boolean.toString((Boolean) b));
        addForwardBackwardStringConverters(converters, Double.class, s -> Double.parseDouble((String) s), d -> Double.toString((Double) d));
        addForwardBackwardStringConverters(converters, Integer.class, s -> Integer.parseInt((String) s), i -> Integer.toString((Integer) i));
        addForwardBackwardStringConverters(converters, Long.class, s -> Long.parseLong((String) s), l -> Long.toString((Long) l));
        addForwardBackwardStringConverters(converters, String.class, s -> (String) s, s -> (String) s);
        addForwardBackwardStringConverters(converters, DurationSpec.LongNanosecondsBound.class, s -> new DurationSpec.LongNanosecondsBound((String) s), TypeConverter.TO_STRING);
        addForwardBackwardStringConverters(converters, DurationSpec.LongMillisecondsBound.class, s -> new DurationSpec.LongMillisecondsBound((String) s), TypeConverter.TO_STRING);
        addForwardBackwardStringConverters(converters, DurationSpec.LongSecondsBound.class, s -> new DurationSpec.LongSecondsBound((String) s), TypeConverter.TO_STRING);
        addForwardBackwardStringConverters(converters, DurationSpec.IntMinutesBound.class, s -> new DurationSpec.IntMinutesBound((String) s), TypeConverter.TO_STRING);
        addForwardBackwardStringConverters(converters, DurationSpec.IntSecondsBound.class, s -> new DurationSpec.IntSecondsBound((String) s), TypeConverter.TO_STRING);
        addForwardBackwardStringConverters(converters, DurationSpec.IntMillisecondsBound.class, s -> new DurationSpec.IntMillisecondsBound((String) s), TypeConverter.TO_STRING);
        addForwardBackwardStringConverters(converters, DataStorageSpec.LongBytesBound.class, s -> new DataStorageSpec.LongBytesBound((String) s), TypeConverter.TO_STRING);
        addForwardBackwardStringConverters(converters, DataStorageSpec.IntBytesBound.class, s -> new DataStorageSpec.IntBytesBound((String) s), TypeConverter.TO_STRING);
        addForwardBackwardStringConverters(converters, DataStorageSpec.IntKibibytesBound.class, s -> new DataStorageSpec.IntKibibytesBound((String) s), TypeConverter.TO_STRING);
        addForwardBackwardStringConverters(converters, DataStorageSpec.LongMebibytesBound.class, s -> new DataStorageSpec.LongMebibytesBound((String) s), TypeConverter.TO_STRING);
        addForwardBackwardStringConverters(converters, DataStorageSpec.IntMebibytesBound.class, s -> new DataStorageSpec.IntMebibytesBound((String) s), TypeConverter.TO_STRING);
        addForwardBackwardStringConverters(converters, ConsistencyLevel.class, s -> ConsistencyLevel.fromStringIgnoreCase((String) s), c -> ((ConsistencyLevel) c).name());
        addForwardBackwardStringConverters(converters, DataRateSpec.LongBytesPerSecondBound.class, s -> new DataRateSpec.LongBytesPerSecondBound((String) s), TypeConverter.TO_STRING);
    }

    private static <T> void addForwardBackwardStringConverters(Map<ConverterKey, TypeConverter<?>> converters, Class<T> type, TypeConverter<T> forward, TypeConverter<String> reverse)
    {
        converters.put(key(type, String.class), reverse);
        converters.put(key(String.class, type), forward);
    }

    private static ConverterKey key(Class<?> from, Class<?> to)
    {
        return new ConverterKey(from, to);
    }

    private static class ConverterKey
    {
        private final Class<?> from;
        private final Class<?> to;

        public ConverterKey(Class<?> from, Class<?> to)
        {
            this.from = from;
            this.to = to;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ConverterKey that = (ConverterKey) o;
            return Objects.equals(from, that.from) && Objects.equals(to, that.to);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(from, to);
        }
    }
}
