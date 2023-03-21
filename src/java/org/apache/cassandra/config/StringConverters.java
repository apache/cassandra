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

import org.apache.cassandra.config.registry.PrimitiveUnaryConverter;
import org.apache.cassandra.config.registry.TypeConverter;
import org.apache.cassandra.db.ConsistencyLevel;

public enum StringConverters
{
    PRIMITIVE_BOOLEAN(Boolean.TYPE, s -> Boolean.parseBoolean((String) s), b -> Boolean.toString((Boolean) b)),
    PRIMITIVE_DOUBLE(Double.TYPE, s -> Double.parseDouble((String) s),  d -> Double.toString((Double) d)),
    PRIMITIVE_INTEGER(Integer.TYPE, s -> Integer.parseInt((String) s), i -> Integer.toString((Integer) i)),
    PRIMITIVE_LONG(Long.TYPE, s -> Long.parseLong((String) s), l -> Long.toString((Long) l)),
    BOOLEAN(Boolean.class, s -> Boolean.parseBoolean((String) s), b -> Boolean.toString((Boolean) b)),
    DOUBLE(Double.class, s -> Double.parseDouble((String) s),  d -> Double.toString((Double) d)),
    INTEGER(Integer.class, s -> Integer.parseInt((String) s), i -> Integer.toString((Integer) i)),
    LONG(Long.class, s -> Long.parseLong((String) s), l -> Long.toString((Long) l)),
    STRING(String.class, s -> (String) s, s -> (String) s),
    // Cassandra specific configuration types.
    LONG_NANOSECONDS_BOUND(DurationSpec.LongNanosecondsBound.class, s -> new DurationSpec.LongNanosecondsBound((String) s), TypeConverter.DEFAULT),
    LONG_MILLISECONDS_BOUND(DurationSpec.LongMillisecondsBound.class, s -> new DurationSpec.LongMillisecondsBound((String) s), TypeConverter.DEFAULT),
    LONG_SECONDS_BOUND(DurationSpec.LongSecondsBound.class, s -> new DurationSpec.LongSecondsBound((String) s), TypeConverter.DEFAULT),
    INT_MINUSTES_BOUND(DurationSpec.IntMinutesBound.class, s -> new DurationSpec.IntMinutesBound((String) s), TypeConverter.DEFAULT),
    INT_SECONDS_BOUND(DurationSpec.IntSecondsBound.class, s -> new DurationSpec.IntSecondsBound((String) s), TypeConverter.DEFAULT),
    INT_MILLISECONDS_BOUND(DurationSpec.IntMillisecondsBound.class, s -> new DurationSpec.IntMillisecondsBound((String) s), TypeConverter.DEFAULT),
    LONG_BYTES_BOUND(DataStorageSpec.LongBytesBound.class, s -> new DataStorageSpec.LongBytesBound((String) s), TypeConverter.DEFAULT),
    INT_BYTES_BOUND(DataStorageSpec.IntBytesBound.class, s -> new DataStorageSpec.IntBytesBound((String) s), TypeConverter.DEFAULT),
    INT_KIBIBYTES_BOUND(DataStorageSpec.IntKibibytesBound.class, s -> new DataStorageSpec.IntKibibytesBound((String) s), TypeConverter.DEFAULT),
    LONG_MEBIBYTES_BOUND(DataStorageSpec.LongMebibytesBound.class, s -> new DataStorageSpec.LongMebibytesBound((String) s), TypeConverter.DEFAULT),
    INT_MEBIBYTES_BOUND(DataStorageSpec.IntMebibytesBound.class, s -> new DataStorageSpec.IntMebibytesBound((String) s), TypeConverter.DEFAULT),
    CONSYSTENCY_LEVEL(ConsistencyLevel.class, s -> ConsistencyLevel.fromStringIgnoreCase((String) s), c -> ((ConsistencyLevel) c).name());

    private final Class<?> type;
    private final TypeConverter<?> forward;
    private final TypeConverter<String> reverse;

    <T> StringConverters(Class<T> type, TypeConverter<T> forward, TypeConverter<String> reverse)
    {
        this.type = type;
        this.forward = forward.then(new PrimitiveUnaryConverter<>(type));
        this.reverse = reverse;
    }

    @SuppressWarnings("unchecked")
    public <T> T fromString(String value, Class<T> target)
    {
        if (target.equals(type))
            return (T) forward.convertNullable(value);
        throw new IllegalArgumentException(String.format("Invalid target type '%s' for converter '%s'", target, this));
    }

    public String toString(Object value)
    {
        return reverse.convertNullable(value);
    }

    public static StringConverters fromType(Class<?> type)
    {
        for (StringConverters converter : values())
            if (converter.type.equals(type))
                return converter;

        return null;
    }
}
