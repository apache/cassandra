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

import javax.annotation.concurrent.Immutable;

import accord.local.Node;
import accord.utils.SortedArrays.SortedArrayList;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.serializers.TopologySerializers;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataValue;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.CollectionSerializers;

import static accord.topology.Topology.NO_IDS;

@Immutable
public class AccordStaleReplicas implements MetadataValue<AccordStaleReplicas>
{
    public static final AccordStaleReplicas EMPTY = new AccordStaleReplicas(NO_IDS, NO_IDS, Epoch.EMPTY);

    private final SortedArrayList<Node.Id> stale;
    private final SortedArrayList<Node.Id> hardRemoved;
    private final Epoch lastModified;

    public AccordStaleReplicas(SortedArrayList<Node.Id> stale, SortedArrayList<Node.Id> hardRemoved, Epoch lastModified)
    {
        this.stale = stale;
        this.hardRemoved = hardRemoved;
        this.lastModified = lastModified;
    }

    @Override
    public AccordStaleReplicas withLastModified(Epoch epoch)
    {
        return new AccordStaleReplicas(stale, hardRemoved, epoch);
    }

    @Override
    public Epoch lastModified()
    {
        return lastModified;
    }
    
    public AccordStaleReplicas withStale(SortedArrayList<Node.Id> stale)
    {
        return new AccordStaleReplicas(this.stale.with(stale), hardRemoved, lastModified);
    }

    public AccordStaleReplicas withHardRemoved(SortedArrayList<Node.Id> hardRemoved)
    {
        return new AccordStaleReplicas(stale, this.hardRemoved.with(hardRemoved), lastModified);
    }

    public AccordStaleReplicas withoutStale(SortedArrayList<Node.Id> withoutStale)
    {

        return new AccordStaleReplicas(this.stale.without(withoutStale), hardRemoved, lastModified);
    }

    public SortedArrayList<Node.Id> stale()
    {
        return stale;
    }

    public SortedArrayList<Node.Id> hardRemoved()
    {
        return hardRemoved;
    }

    @Override
    public String toString()
    {
        return "AccordStaleReplicas{staleIds=" + stale + ", hardRemoved=" + hardRemoved  + ", lastModified=" + lastModified + '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AccordStaleReplicas that = (AccordStaleReplicas) o;
        return Objects.equals(stale, that.stale) && Objects.equals(lastModified, that.lastModified);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(stale, lastModified);
    }

    public static final MetadataSerializer<AccordStaleReplicas> serializer = new MetadataSerializer<>()
    {
        private static final int HAS_STALE_IDS = 0x1;
        private static final int HAS_HARD_REMOVED_IDS = 0x2;

        @Override
        public void serialize(AccordStaleReplicas replicas, DataOutputPlus out, Version version) throws IOException
        {
            int flags = 0;
            if (!replicas.stale.isEmpty())
                flags |= HAS_STALE_IDS;
            if (!replicas.hardRemoved.isEmpty())
                flags |= HAS_HARD_REMOVED_IDS;
            out.writeUnsignedVInt32(flags);
            if (!replicas.stale.isEmpty())
                CollectionSerializers.serializeCollection(replicas.stale, out, TopologySerializers.nodeId);
            if (!replicas.hardRemoved.isEmpty())
                CollectionSerializers.serializeCollection(replicas.hardRemoved, out, TopologySerializers.nodeId);
            Epoch.serializer.serialize(replicas.lastModified, out, version);
        }

        @Override
        public AccordStaleReplicas deserialize(DataInputPlus in, Version version) throws IOException
        {
            int flags = in.readUnsignedVInt32();
            SortedArrayList<Node.Id> stale = NO_IDS, hardRemoved = NO_IDS;
            if ((flags & HAS_STALE_IDS) != 0)
                stale = CollectionSerializers.deserializeSortedArrayList(in, TopologySerializers.nodeId, Node.Id[]::new);
            if ((flags & HAS_HARD_REMOVED_IDS) != 0)
                hardRemoved = CollectionSerializers.deserializeSortedArrayList(in, TopologySerializers.nodeId, Node.Id[]::new);
            Epoch lastModified = Epoch.serializer.deserialize(in, version);
            return new AccordStaleReplicas(stale, hardRemoved, lastModified);
        }

        @Override
        public long serializedSize(AccordStaleReplicas replicas, Version version)
        {
            int flags = 0;
            if (!replicas.stale.isEmpty())
                flags |= HAS_STALE_IDS;
            if (!replicas.hardRemoved.isEmpty())
                flags |= HAS_HARD_REMOVED_IDS;
            long size = TypeSizes.sizeofUnsignedVInt(flags);
            if (!replicas.stale.isEmpty())
                size += CollectionSerializers.serializedCollectionSize(replicas.stale, TopologySerializers.nodeId);
            if (!replicas.hardRemoved.isEmpty())
                size += CollectionSerializers.serializedCollectionSize(replicas.hardRemoved, TopologySerializers.nodeId);
            size += Epoch.serializer.serializedSize(replicas.lastModified, version);
            return size;
        }
    };
}
