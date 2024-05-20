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

package org.apache.cassandra.tcm.ownership;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.MetaStrategy;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class DataPlacement
{
    private static final Serializer globalSerializer = new Serializer(IPartitioner.global());
    private static final Serializer metaKeyspaceSerializer = new Serializer(MetaStrategy.partitioner);
    public static Serializer serializerFor(ReplicationParams replication)
    {
        return replication.isMeta() ? metaKeyspaceSerializer : globalSerializer;
    }

    private static final DataPlacement EMPTY = new DataPlacement(ReplicaGroups.EMPTY, ReplicaGroups.EMPTY);

    // TODO make tree of just EndpointsForRange, navigable by EFR.range()
    // TODO combine peers into a single entity with one vote in any quorum
    //      (e.g. old & new peer must both respond to count one replica)
    public final ReplicaGroups reads;
    public final ReplicaGroups writes;

    public DataPlacement(ReplicaGroups reads,
                         ReplicaGroups writes)
    {
        this.reads = reads;
        this.writes = writes;
    }

    /**
     * A union of read and write endpoints for range, for watermark purposes
     */
    public Set<InetAddressAndPort> affectedReplicas(Range<Token> range)
    {
        Set<InetAddressAndPort> endpoints = new HashSet<>();
        for (Replica r : reads.matchRange(range).get())
            endpoints.add(r.endpoint());
        for (Replica r : writes.matchRange(range).get())
            endpoints.add(r.endpoint());
        return endpoints;
    }

    public DataPlacement combineReplicaGroups(DataPlacement other)
    {
        return new DataPlacement(ReplicaGroups.builder()
                                              .withReplicaGroups(reads.endpoints)
                                              .withReplicaGroups(other.reads.endpoints)
                                              .build(),
                                 ReplicaGroups.builder()
                                              .withReplicaGroups(writes.endpoints)
                                              .withReplicaGroups(other.writes.endpoints)
                                              .build());
    }

    public PlacementDeltas.PlacementDelta difference(DataPlacement next)
    {
        return new PlacementDeltas.PlacementDelta(reads.difference(next.reads),
                                                  writes.difference(next.writes));
    }

    public DataPlacement splitRangesForPlacement(List<Token> tokens)
    {
        return new DataPlacement(ReplicaGroups.splitRangesForPlacement(tokens, reads),
                                 ReplicaGroups.splitRangesForPlacement(tokens, writes));
    }

    public static DataPlacement empty()
    {
        return EMPTY;
    }

    public static Builder builder()
    {
        return new Builder(ReplicaGroups.builder(),
                           ReplicaGroups.builder());
    }

    public Builder unbuild()
    {
        return new Builder(reads.unbuild(), writes.unbuild());
    }
    public static class Builder
    {
        public final ReplicaGroups.Builder reads;
        public final ReplicaGroups.Builder writes;

        public Builder(ReplicaGroups.Builder reads, ReplicaGroups.Builder writes)
        {
            this.reads = reads;
            this.writes = writes;
        }

        public Builder withWriteReplica(Epoch epoch, Replica replica)
        {
            this.writes.withReplica(epoch, replica);
            return this;
        }

        public Builder withoutWriteReplica(Epoch epoch, Replica replica)
        {
            this.writes.withoutReplica(epoch, replica);
            return this;
        }

        public Builder withReadReplica(Epoch epoch, Replica replica)
        {
            this.reads.withReplica(epoch, replica);
            return this;
        }

        public Builder withoutReadReplica(Epoch epoch, Replica replica)
        {
            this.reads.withoutReplica(epoch, replica);
            return this;
        }

        public DataPlacement build()
        {
            return new DataPlacement(reads.build(), writes.build());
        }
    }

    public DataPlacement withCappedLastModified(Epoch lastModified)
    {
        return new DataPlacement(reads.withCappedLastModified(lastModified),
                                 writes.withCappedLastModified(lastModified));
    }

    @Override
    public String toString()
    {
        return "DataPlacement{" +
               "reads=" + reads +
               ", writes=" + writes +
               '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof DataPlacement)) return false;
        DataPlacement that = (DataPlacement) o;
        return Objects.equals(reads, that.reads) && Objects.equals(writes, that.writes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(reads, writes);
    }

    public static class Serializer implements MetadataSerializer<DataPlacement>
    {
        private final IPartitioner partitioner;
        private Serializer(IPartitioner partitioner)
        {
            this.partitioner = partitioner;
        }

        public void serialize(DataPlacement t, DataOutputPlus out, Version version) throws IOException
        {
            ReplicaGroups.serializer.serialize(t.reads, out, partitioner, version);
            ReplicaGroups.serializer.serialize(t.writes, out, partitioner, version);
        }

        public DataPlacement deserialize(DataInputPlus in, Version version) throws IOException
        {
            ReplicaGroups reads = ReplicaGroups.serializer.deserialize(in, partitioner, version);
            ReplicaGroups writes = ReplicaGroups.serializer.deserialize(in, partitioner, version);
            return new DataPlacement(reads, writes);
        }

        public long serializedSize(DataPlacement t, Version version)
        {
            return ReplicaGroups.serializer.serializedSize(t.reads, partitioner, version) +
                   ReplicaGroups.serializer.serializedSize(t.writes, partitioner, version);
        }
    }
}
