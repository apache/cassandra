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

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CqlBuilder;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.MetaStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.db.TypeSizes.sizeof;

public final class ReplicationParams
{
    public static final Serializer serializer = new Serializer();
    public static final MessageSerializer messageSerializer = new MessageSerializer();

    public static final String CLASS = "class";

    public final Class<? extends AbstractReplicationStrategy> klass;
    public final ImmutableMap<String, String> options;

    private ReplicationParams(Class<? extends AbstractReplicationStrategy> klass, Map<String, String> options)
    {
        this.klass = klass;
        this.options = ImmutableMap.copyOf(options);
    }

    public static ReplicationParams local()
    {
        return new ReplicationParams(LocalStrategy.class, ImmutableMap.of());
    }

    public boolean isLocal()
    {
        return klass == LocalStrategy.class;
    }

    public boolean isMeta()
    {
        return klass == MetaStrategy.class;
    }

    /**
     * For backward-compatibility reasons we are persisting replication params for cluster metadata as non-meta
     * replication params. This means that when we are creating mutations, meta params will be written to as Network
     * Topology Strategy (in local DC, if no DCs are specified), and when schema is loaded from system tables, we will
     * create a meta strategy instance for cluster metadata keyspace.
     */
    public ReplicationParams asMeta()
    {
        assert !isMeta() : this;
        if (options.containsKey(SimpleStrategy.REPLICATION_FACTOR))
        {
            Map<String, String> dcRf = new HashMap<>();
            String rf = options.get(SimpleStrategy.REPLICATION_FACTOR);
            dcRf.put(DatabaseDescriptor.getLocalDataCenter(), rf);
            return new ReplicationParams(MetaStrategy.class, dcRf);
        }

        return new ReplicationParams(MetaStrategy.class, options);
    }

    /**
     * Counterpart of `asMeta`, see comment for asMeta for details.
     */
    public ReplicationParams asNonMeta()
    {
        assert isMeta() : this;
        if (options.containsKey(SimpleStrategy.REPLICATION_FACTOR))
            return new ReplicationParams(SimpleStrategy.class, options);

        return new ReplicationParams(NetworkTopologyStrategy.class, options);
    }

    @VisibleForTesting
    public static ReplicationParams simple(int replicationFactor)
    {
        return new ReplicationParams(SimpleStrategy.class, ImmutableMap.of("replication_factor", Integer.toString(replicationFactor)));
    }

    static ReplicationParams simple(String replicationFactor)
    {
        return new ReplicationParams(SimpleStrategy.class, ImmutableMap.of("replication_factor", replicationFactor));
    }

    public static ReplicationParams simpleMeta(int replicationFactor, String datacenter)
    {
        return simpleMeta(replicationFactor, Collections.singleton(datacenter));
    }

    public static ReplicationParams simpleMeta(int replicationFactor, Set<String> knownDatacenters)
    {
        if (replicationFactor <= 0)
            throw new IllegalStateException("Replication factor should be strictly positive");
        if (knownDatacenters.isEmpty())
            throw new IllegalStateException("No known datacenters");
        String dc = knownDatacenters.stream().min(Comparator.comparing(s -> s)).get();
        Map<String, Integer> dcRf = new HashMap<>();
        dcRf.put(dc, replicationFactor);
        return ntsMeta(dcRf);
    }

    public static ReplicationParams ntsMeta(Map<String, Integer> replicationFactor)
    {
        Map<String, String> rfAsString = new HashMap<>();
        int aggregate = 0;
        for (Map.Entry<String, Integer> e : replicationFactor.entrySet())
        {
            int rf = e.getValue();
            aggregate += rf;
            if (rf <= 0)
                throw new IllegalStateException("Replication factor should be strictly positive: " + rf);
            rfAsString.put(e.getKey(), Integer.toString(rf));
        }

        if (aggregate <= 0)
            throw new IllegalArgumentException("Aggregate replication factor should be strictly positive: " + replicationFactor);
        return new ReplicationParams(MetaStrategy.class, rfAsString);
    }

    // meta replication, i.e. the replication strategy used for topology decisions
    public static ReplicationParams meta(ClusterMetadata metadata)
    {
        ReplicationParams metaParams = metadata.schema.getKeyspaceMetadata(SchemaConstants.METADATA_KEYSPACE_NAME).params.replication;
        assert metaParams.isMeta() : metaParams;
        return metaParams;
    }

    static ReplicationParams nts(Object... args)
    {
        assert args.length % 2 == 0;

        Map<String, String> options = new HashMap<>();
        for (int i = 0; i < args.length; i += 2)
        {
            options.put((String) args[i], args[i + 1].toString());
        }

        return new ReplicationParams(NetworkTopologyStrategy.class, options);
    }

    public void validate(String name, ClientState state, ClusterMetadata metadata)
    {
        // Attempt to instantiate the ARS, which will throw a ConfigurationException if the options aren't valid.
        AbstractReplicationStrategy.validateReplicationStrategy(name, klass, metadata, options, state);
    }

    public static ReplicationParams fromMap(Map<String, String> map) {
        return fromMapWithDefaults(map, new HashMap<>());
    }

    public static ReplicationParams fromMapWithDefaults(Map<String, String> map, Map<String, String> previousOptions)
    {
        Map<String, String> options = new HashMap<>(map);
        String className = options.remove(CLASS);

        Class<? extends AbstractReplicationStrategy> klass = AbstractReplicationStrategy.getClass(className);
        AbstractReplicationStrategy.prepareReplicationStrategyOptions(klass, options, previousOptions);

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

    public void appendCqlTo(CqlBuilder builder)
    {
        String classname = "org.apache.cassandra.locator".equals(klass.getPackage().getName()) ? klass.getSimpleName()
                                                                                               : klass.getName();
        builder.append("{'class': ")
               .appendWithSingleQuotes(classname);

        options.forEach((k, v) -> {
            builder.append(", ")
                   .appendWithSingleQuotes(k)
                   .append(": ")
                   .appendWithSingleQuotes(v);
        });

        builder.append('}');
    }

    public static class Serializer implements MetadataSerializer<ReplicationParams>
    {
        public void serialize(ReplicationParams t, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUTF(t.klass.getCanonicalName());
            out.writeUnsignedVInt32(t.options.size());
            for (Map.Entry<String, String> option : t.options.entrySet())
            {
                out.writeUTF(option.getKey());
                out.writeUTF(option.getValue());
            }
        }

        public ReplicationParams deserialize(DataInputPlus in, Version version) throws IOException
        {
            String klassName = in.readUTF();
            int size = in.readUnsignedVInt32();
            Map<String, String> options = new HashMap<>(size);
            for (int i = 0; i < size; i++)
                options.put(in.readUTF(), in.readUTF());
            return new ReplicationParams(FBUtilities.classForName(klassName, "ReplicationStrategy"), options);
        }

        public long serializedSize(ReplicationParams t, Version version)
        {
            long size = sizeof(t.klass.getCanonicalName());
            size += TypeSizes.sizeofUnsignedVInt(t.options.size());
            for (Map.Entry<String, String> option : t.options.entrySet())
            {
                size += sizeof(option.getKey());
                size += sizeof(option.getValue());
            }
            return size;
        }
    }

    public static class MessageSerializer implements IVersionedSerializer<ReplicationParams>
    {
        public void serialize(ReplicationParams t, DataOutputPlus out, int version) throws IOException
        {
            out.writeUTF(t.klass.getCanonicalName());
            out.writeUnsignedVInt32(t.options.size());
            for (Map.Entry<String, String> option : t.options.entrySet())
            {
                out.writeUTF(option.getKey());
                out.writeUTF(option.getValue());
            }
        }

        public ReplicationParams deserialize(DataInputPlus in, int version) throws IOException
        {
            String klassName = in.readUTF();
            int size = in.readUnsignedVInt32();
            Map<String, String> options = new HashMap<>(size);
            for (int i=0; i<size; i++)
                options.put(in.readUTF(), in.readUTF());
            return new ReplicationParams(FBUtilities.classForName(klassName, "ReplicationStrategy"), options);
        }

        public long serializedSize(ReplicationParams t, int version)
        {
            long size = sizeof(t.klass.getCanonicalName());
            size += TypeSizes.sizeofUnsignedVInt(t.options.size());
            for (Map.Entry<String, String> option : t.options.entrySet())
            {
                size += sizeof(option.getKey());
                size += sizeof(option.getValue());
            }
            return size;
        }
    }
}
