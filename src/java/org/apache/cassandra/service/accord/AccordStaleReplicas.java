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

package org.apache.cassandra.service.accord;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

import javax.annotation.concurrent.Immutable;

import com.google.common.collect.ImmutableSet;

import accord.local.Node;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.serializers.TopologySerializers;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataValue;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.CollectionSerializers;

@Immutable
public class AccordStaleReplicas implements MetadataValue<AccordStaleReplicas>
{
    public static final AccordStaleReplicas EMPTY = new AccordStaleReplicas(ImmutableSet.of(), Epoch.EMPTY);

    private final Set<Node.Id> staleIds;
    private final Epoch lastModified;

    public AccordStaleReplicas(Set<Node.Id> staleIds, Epoch lastModified)
    {
        this.staleIds = staleIds;
        this.lastModified = lastModified;
    }

    @Override
    public AccordStaleReplicas withLastModified(Epoch epoch)
    {
        return new AccordStaleReplicas(staleIds, epoch);
    }

    @Override
    public Epoch lastModified()
    {
        return lastModified;
    }
    
    public AccordStaleReplicas withNodeIds(Set<Node.Id> ids)
    {
        ImmutableSet.Builder<Node.Id> builder = new ImmutableSet.Builder<>();
        Set<Node.Id> newIds = builder.addAll(staleIds).addAll(ids).build();
        return new AccordStaleReplicas(newIds, lastModified);
    }

    public AccordStaleReplicas without(Set<Node.Id> ids)
    {
        ImmutableSet.Builder<Node.Id> builder = new ImmutableSet.Builder<>();

        for (Node.Id staleId : staleIds)
            if (!ids.contains(staleId))
                builder.add(staleId);

        return new AccordStaleReplicas(builder.build(), lastModified);
    }

    public boolean contains(Node.Id nodeId)
    {
        return staleIds.contains(nodeId);
    }

    public Set<Node.Id> ids()
    {
        return staleIds;
    }

    @Override
    public String toString()
    {
        return "AccordStaleReplicas{staleIds=" + staleIds + ", lastModified=" + lastModified + '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AccordStaleReplicas that = (AccordStaleReplicas) o;
        return Objects.equals(staleIds, that.staleIds) && Objects.equals(lastModified, that.lastModified);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(staleIds, lastModified);
    }

    public static final MetadataSerializer<AccordStaleReplicas> serializer = new MetadataSerializer<>()
    {
        @Override
        public void serialize(AccordStaleReplicas replicas, DataOutputPlus out, Version version) throws IOException
        {
            CollectionSerializers.serializeCollection(replicas.staleIds, out, TopologySerializers.nodeId);
            Epoch.serializer.serialize(replicas.lastModified, out, version);
        }

        @Override
        public AccordStaleReplicas deserialize(DataInputPlus in, Version version) throws IOException
        {
            return new AccordStaleReplicas(CollectionSerializers.deserializeSet(in, TopologySerializers.nodeId),
                                           Epoch.serializer.deserialize(in, version));
        }

        @Override
        public long serializedSize(AccordStaleReplicas replicas, Version version)
        {
            return CollectionSerializers.serializedCollectionSize(replicas.staleIds, TopologySerializers.nodeId)
                   + Epoch.serializer.serializedSize(replicas.lastModified, version);
        }
    };
}
