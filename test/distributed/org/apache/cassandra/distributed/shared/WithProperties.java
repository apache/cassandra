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

import com.google.common.base.Joiner;

import org.apache.cassandra.config.CassandraRelevantProperties;

import static org.apache.cassandra.config.CassandraRelevantProperties.convertToString;

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

    public WithProperties set(CassandraRelevantProperties prop, String value)
    {
        return set(prop, () -> prop.setString(value));
    }

    public WithProperties set(CassandraRelevantProperties prop, String... values)
    {
        return set(prop, Arrays.asList(values));
    }

    public WithProperties set(CassandraRelevantProperties prop, Collection<String> values)
    {
        return set(prop, Joiner.on(",").join(values));
    }

    public WithProperties set(CassandraRelevantProperties prop, boolean value)
    {
        return set(prop, () -> prop.setBoolean(value));
    }

    public WithProperties set(CassandraRelevantProperties prop, long value)
    {
        return set(prop, () -> prop.setLong(value));
    }

    private void with(String key, String value)
    {
        String previous = System.setProperty(key, value); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        rollback.add(previous == null ? () -> System.clearProperty(key) : () -> System.setProperty(key, previous)); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
    }

    private WithProperties set(CassandraRelevantProperties prop, Supplier<Object> prev)
    {
        String previous = convertToString(prev.get());
        rollback.add(previous == null ? prop::clearValue : () -> prop.setString(previous));
        return this;
    }

    @Override
    public void close()
    {
        Collections.reverse(rollback);
        rollback.forEach(Runnable::run);
        rollback.clear();
    }
}
