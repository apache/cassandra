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

package org.apache.cassandra.service.accord.fastpath;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;

import accord.local.Node;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public interface FastPathStrategy
{
    enum Kind
    {
        SIMPLE, PARAMETERIZED, INHERIT_KEYSPACE;

        static final String KEY = "kind";
        private static final Map<String, Kind> LOOKUP;
        static
        {
            ImmutableMap.Builder<String, Kind> builder = ImmutableMap.builder();
            builder.put(SIMPLE.name(), SIMPLE);
            builder.put(PARAMETERIZED.name(), PARAMETERIZED);
            builder.put(INHERIT_KEYSPACE.name(), INHERIT_KEYSPACE);
            LOOKUP = builder.build();
        }

        public byte asByte()
        {
            return (byte) ordinal();
        }

        public static Kind fromByte(byte i)
        {
            return values()[i];
        }

        @Nullable
        public static Kind fromString(String s)
        {
            return LOOKUP.get(s.toUpperCase());
        }

        @Nullable
        private static Kind fromMap(Map<String, String> map)
        {
            String name = map.remove(KEY);
            return name != null ? fromString(name) : null;
        }
    }

    /**
     * @param nodes expected to be sorted deterministically
     * @param unavailable
     * @param dcMap
     * @return
     */
    Set<Node.Id> calculateFastPath(List<Node.Id> nodes, Set<Node.Id> unavailable, Map<Node.Id, String> dcMap);

    Kind kind();

    Map<String, String> asMap();

    String asCQL();

    static FastPathStrategy fromMap(Map<String, String> map)
    {
        if (map == null || map.isEmpty())
            return SimpleFastPathStrategy.instance;

        map = new HashMap<>(map);
        Kind kind = Kind.fromMap(map);
        if (kind == null)
            return map.isEmpty()
                   ? simple()
                   : ParameterizedFastPathStrategy.fromMap(map);

        switch (kind)
        {
            case SIMPLE:
                return simple();
            case PARAMETERIZED:
                return ParameterizedFastPathStrategy.fromMap(map);
            case INHERIT_KEYSPACE:
                return inheritKeyspace();
            default:
                throw new IllegalArgumentException("Unhandled strategy kind: " + kind);
        }
    }

    static FastPathStrategy tableStrategyFromString(String s)
    {
        s = s.toLowerCase().trim();
        if (s.equals("keyspace"))
            return InheritKeyspaceFastPathStrategy.instance;
        if (s.equals("simple"))
            return SimpleFastPathStrategy.instance;

        throw new ConfigurationException("Fast path strategy must either be 'keyspace', `default` or a map size and optional dcs {'size':n, 'dcs': dc0,dc1...");
    }

    static FastPathStrategy keyspaceStrategyFromString(String s)
    {
        s = s.toLowerCase().trim();
        if (s.equals("simple"))
            return SimpleFastPathStrategy.instance;

        throw new ConfigurationException("Fast path strategy must either be `default` or a map size and optional dcs {'size':n, 'dcs': dc0,dc1...");
    }

    static FastPathStrategy simple()
    {
        return SimpleFastPathStrategy.instance;
    }

    static FastPathStrategy inheritKeyspace()
    {
        return InheritKeyspaceFastPathStrategy.instance;
    }

    MetadataSerializer<FastPathStrategy> serializer = new MetadataSerializer<FastPathStrategy>()
    {
        public void serialize(FastPathStrategy strategy, DataOutputPlus out, Version version) throws IOException
        {
            Kind type = strategy.kind();
            out.write(type.asByte());
            if (type == Kind.PARAMETERIZED)
                ParameterizedFastPathStrategy.serializer.serialize((ParameterizedFastPathStrategy) strategy, out, version);
        }

        public FastPathStrategy deserialize(DataInputPlus in, Version version) throws IOException
        {
            Kind type =  Kind.fromByte(in.readByte());
            switch (type)
            {
                case SIMPLE:
                    return simple();
                case PARAMETERIZED:
                    return ParameterizedFastPathStrategy.serializer.deserialize(in, version);
                case INHERIT_KEYSPACE:
                    return inheritKeyspace();
                default:
                    throw new IllegalArgumentException("Unhandled type: " + type);
            }
        }

        public long serializedSize(FastPathStrategy strategy, Version version)
        {
            long size = TypeSizes.BYTE_SIZE;
            if (strategy.kind() == Kind.PARAMETERIZED)
                size += ParameterizedFastPathStrategy.serializer.serializedSize((ParameterizedFastPathStrategy) strategy, version);
            return size;
        }
    };
}
