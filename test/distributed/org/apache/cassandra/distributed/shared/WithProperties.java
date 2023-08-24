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

package org.apache.cassandra.distributed.shared;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import com.google.common.base.Joiner;

public final class WithProperties implements AutoCloseable
{
    private final List<Runnable> rollback = new ArrayList<>();

    public WithProperties()
    {
    }

    public WithProperties with(String... kvs)
    {
        assert kvs.length % 2 == 0 : "Input must have an even amount of inputs but given " + kvs.length;
        for (int i = 0; i <= kvs.length - 2; i = i + 2)
            with(kvs[i], kvs[i + 1]);
        return this;
    }

    public WithProperties set(String prop, String value)
    {
        return set(prop, () -> System.setProperty(prop, value));
    }

    public WithProperties set(String prop, String... values)
    {
        return set(prop, Arrays.asList(values));
    }

    public WithProperties set(String prop, Collection<String> values)
    {
        return set(prop, Joiner.on(",").join(values));
    }

    public WithProperties set(String prop, boolean value)
    {
        return set(prop, () -> System.setProperty(prop, convertToString(value)));
    }

    public WithProperties set(String prop, long value)
    {
        return set(prop, () -> System.setProperty(prop, convertToString(value)));
    }

    private void with(String key, String value)
    {
        String previous = System.setProperty(key, value); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        rollback.add(previous == null ? () -> System.clearProperty(key) : () -> System.setProperty(key, previous)); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
    }

    private WithProperties set(String prop, Supplier<Object> prev)
    {
        String previous = convertToString(prev.get());
        rollback.add(previous == null ? () -> System.clearProperty(prop) : () -> System.setProperty(prop, previous));
        return this;
    }

    @Override
    public void close()
    {
        Collections.reverse(rollback);
        rollback.forEach(Runnable::run);
        rollback.clear();
    }

    public static String convertToString(@Nullable Object value)
    {
        if (value == null)
            return null;
        if (value instanceof String)
            return (String) value;
        if (value instanceof Boolean)
            return Boolean.toString((Boolean) value);
        if (value instanceof Long)
            return Long.toString((Long) value);
        if (value instanceof Integer)
            return Integer.toString((Integer) value);
        if (value instanceof Double)
            return Double.toString((Double) value);
        if (value instanceof Float)
            return Float.toString((Float) value);
        throw new IllegalArgumentException("Unknown type " + value.getClass());
    }
}
