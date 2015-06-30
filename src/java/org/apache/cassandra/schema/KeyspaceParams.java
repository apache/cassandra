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
package org.apache.cassandra.schema;

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.*;
import org.apache.cassandra.service.StorageService;

/**
 * An immutable class representing keyspace parameters (durability and replication).
 */
public final class KeyspaceParams
{
    public static final boolean DEFAULT_DURABLE_WRITES = true;

    public enum Option
    {
        DURABLE_WRITES,
        REPLICATION;

        @Override
        public String toString()
        {
            return name().toLowerCase();
        }
    }

    public final boolean durableWrites;
    public final Replication replication;

    public KeyspaceParams(boolean durableWrites, Replication replication)
    {
        this.durableWrites = durableWrites;
        this.replication = replication;
    }

    public static KeyspaceParams create(boolean durableWrites, Map<String, String> replication)
    {
        return new KeyspaceParams(durableWrites, Replication.fromMap(replication));
    }

    public static KeyspaceParams local()
    {
        return new KeyspaceParams(true, Replication.local());
    }

    public static KeyspaceParams simple(int replicationFactor)
    {
        return new KeyspaceParams(true, Replication.simple(replicationFactor));
    }

    public static KeyspaceParams simpleTransient(int replicationFactor)
    {
        return new KeyspaceParams(false, Replication.simple(replicationFactor));
    }

    public void validate(String name)
    {
        replication.validate(name);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof KeyspaceParams))
            return false;

        KeyspaceParams p = (KeyspaceParams) o;

        return durableWrites == p.durableWrites && replication.equals(p.replication);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(durableWrites, replication);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                      .add(Option.DURABLE_WRITES.toString(), durableWrites)
                      .add(Option.REPLICATION.toString(), replication)
                      .toString();
    }

    public static final class Replication
    {
        public static String CLASS = "class";

        public final Class<? extends AbstractReplicationStrategy> klass;
        public final ImmutableMap<String, String> options;

        private Replication(Class<? extends AbstractReplicationStrategy> klass, Map<String, String> options)
        {
            this.klass = klass;
            this.options = ImmutableMap.copyOf(options);
        }

        private static Replication local()
        {
            return new Replication(LocalStrategy.class, ImmutableMap.of());
        }

        private static Replication simple(int replicationFactor)
        {
            return new Replication(SimpleStrategy.class, ImmutableMap.of("replication_factor", Integer.toString(replicationFactor)));
        }

        public void validate(String name)
        {
            // Attempt to instantiate the ARS, which will throw a ConfigurationException if the options aren't valid.
            TokenMetadata tmd = StorageService.instance.getTokenMetadata();
            IEndpointSnitch eps = DatabaseDescriptor.getEndpointSnitch();
            AbstractReplicationStrategy.validateReplicationStrategy(name, klass, tmd, eps, options);
        }

        public static Replication fromMap(Map<String, String> map)
        {
            Map<String, String> options = new HashMap<>(map);
            String className = options.remove(CLASS);
            Class<? extends AbstractReplicationStrategy> klass = AbstractReplicationStrategy.getClass(className);
            return new Replication(klass, options);
        }

        public Map<String, String> asMap()
        {
            Map<String, String> map = new HashMap<>(options);
            map.put(CLASS, klass.getName());
            return map;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;

            if (!(o instanceof Replication))
                return false;

            Replication r = (Replication) o;

            return klass.equals(r.klass) && options.equals(r.options);
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(klass, options);
        }

        @Override
        public String toString()
        {
            Objects.ToStringHelper helper = Objects.toStringHelper(this);
            helper.add(CLASS, klass.getName());
            for (Map.Entry<String, String> entry : options.entrySet())
                helper.add(entry.getKey(), entry.getValue());
            return helper.toString();
        }
    }
}
