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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public final class WithEnvironment implements AutoCloseable
{
    private final List<Environment> properties = new ArrayList<>();

    public WithEnvironment(String... kvs)
    {
        with(kvs);
    }

    public void with(String... kvs)
    {
        assert kvs.length % 2 == 0 : "Input must have an even amount of inputs but given " + kvs.length;
        for (int i = 0; i <= kvs.length - 2; i = i + 2)
        {
            with(kvs[i], kvs[i + 1]);
        }
    }

    public void with(String key, String value)
    {
        try
        {
            Map<String, String> writableEnv = getWritableEnv();
            String previous = writableEnv.put(key, value);
            properties.add(new Environment(key, previous));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to set environment variable", e);
        }
    }

    private static Map<String, String> getWritableEnv() throws NoSuchFieldException, IllegalAccessException
    {
        Map<String, String> env = System.getenv(); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        Class<?> cl = env.getClass();
        Field field = cl.getDeclaredField("m");
        field.setAccessible(true);
        return (Map<String, String>) field.get(env);
    }


    @Override
    public void close()
    {
        Collections.reverse(properties);
        properties.forEach(s -> {
            try
            {
                Map<String, String> writableEnv = getWritableEnv();
                if (s.value == null)
                    writableEnv.remove(s.key);
                else
                    writableEnv.put(s.key, s.value);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to set environment variable", e);
            }
        });
        properties.clear();
    }

    private static final class Environment
    {
        private final String key;
        private final String value;

        private Environment(String key, String value)
        {
            this.key = key;
            this.value = value;
        }
    }
}
