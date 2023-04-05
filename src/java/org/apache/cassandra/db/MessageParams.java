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

package org.apache.cassandra.db;

import java.util.EnumMap;
import java.util.Map;

import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.ParamType;

public class MessageParams
{
    private static final FastThreadLocal<Map<ParamType, Object>> local = new FastThreadLocal<>();

    private MessageParams()
    {
    }

    private static Map<ParamType, Object> get()
    {
        Map<ParamType, Object> instance = local.get();
        if (instance == null)
        {
            instance = new EnumMap<>(ParamType.class);
            local.set(instance);
        }

        return instance;
    }

    public static void add(ParamType key, Object value)
    {
        get().put(key, value);
    }

    public static <T> T get(ParamType key)
    {
        return (T) get().get(key);
    }

    public static void remove(ParamType key)
    {
        get().remove(key);
    }

    public static void reset()
    {
        get().clear();
    }

    public static <T> Message<T> addToMessage(Message<T> message)
    {
        return message.withParams(get());
    }
}
