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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.*;
import org.apache.cassandra.service.StorageService;

public final class ReplicationParams
{
    public static final String CLASS = "class";

    public final Class<? extends AbstractReplicationStrategy> klass;
    public final ImmutableMap<String, String> options;

    private ReplicationParams(Class<? extends AbstractReplicationStrategy> klass, Map<String, String> options)
    {
        this.klass = klass;
        this.options = ImmutableMap.copyOf(options);
    }

    static ReplicationParams local()
    {
        return new ReplicationParams(LocalStrategy.class, ImmutableMap.of());
    }

    static ReplicationParams simple(int replicationFactor)
    {
        return new ReplicationParams(SimpleStrategy.class, ImmutableMap.of("replication_factor", Integer.toString(replicationFactor)));
    }

    static ReplicationParams nts(Object... args)
    {
        assert args.length % 2 == 0;

        Map<String, String> options = new HashMap<>();
        for (int i = 0; i < args.length; i += 2)
        {
            String dc = (String) args[i];
            Integer rf = (Integer) args[i + 1];
            options.put(dc, rf.toString());
        }

        return new ReplicationParams(NetworkTopologyStrategy.class, options);
    }

    public void validate(String name)
    {
        // Attempt to instantiate the ARS, which will throw a ConfigurationException if the options aren't valid.
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        IEndpointSnitch eps = DatabaseDescriptor.getEndpointSnitch();
        AbstractReplicationStrategy.validateReplicationStrategy(name, klass, tmd, eps, options);
    }

    public static ReplicationParams fromMap(Map<String, String> map)
    {
        Map<String, String> options = new HashMap<>(map);
        String className = options.remove(CLASS);
        Class<? extends AbstractReplicationStrategy> klass = AbstractReplicationStrategy.getClass(className);
        return new ReplicationParams(klass, options);
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

        if (!(o instanceof ReplicationParams))
            return false;

        ReplicationParams r = (ReplicationParams) o;

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
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        helper.add(CLASS, klass.getName());
        for (Map.Entry<String, String> entry : options.entrySet())
            helper.add(entry.getKey(), entry.getValue());
        return helper.toString();
    }
}
