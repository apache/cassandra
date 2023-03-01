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
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class DataPlacement
{
    public static final Serializer serializer = new Serializer();

    private static final DataPlacement EMPTY = new DataPlacement(PlacementForRange.EMPTY, PlacementForRange.EMPTY);

    // TODO make tree of just EndpointsForRange, navigable by EFR.range()
    // TODO combine peers into a single entity with one vote in any quorum
    //      (e.g. old & new peer must both respond to count one replica)
    public final PlacementForRange reads;
    public final PlacementForRange writes;

    public DataPlacement(PlacementForRange reads,
                         PlacementForRange writes)
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
        for (Replica r : reads.matchRange(range))
            endpoints.add(r.endpoint());
        for (Replica r : writes.matchRange(range))
            endpoints.add(r.endpoint());
        return endpoints;
    }

    public DataPlacement combineReplicaGroups(DataPlacement other)
    {
        return new DataPlacement(PlacementForRange.builder()
                                                  .withReplicaGroups(reads.replicaGroups().values())
                                                  .withReplicaGroups(other.reads.replicaGroups.values())
                                                  .build(),
                                 PlacementForRange.builder()
                                                  .withReplicaGroups(writes.replicaGroups().values())
                                                  .withReplicaGroups(other.writes.replicaGroups.values())
                                                  .build());
    }

    public PlacementDeltas.PlacementDelta difference(DataPlacement next)
    {
        return new PlacementDeltas.PlacementDelta(reads.difference(next.reads),
                                                  writes.difference(next.writes));
    }

    public DataPlacement mergeRangesForPlacement(Set<Token> leavingTokens)
    {
        return new DataPlacement(PlacementForRange.mergeRangesForPlacement(leavingTokens, reads),
                                 PlacementForRange.mergeRangesForPlacement(leavingTokens, writes));
    }

    public DataPlacement splitRangesForPlacement(List<Token> tokens)
    {
        return new DataPlacement(PlacementForRange.splitRangesForPlacement(tokens, reads),
                                 PlacementForRange.splitRangesForPlacement(tokens, writes));
    }

    public static DataPlacement empty()
    {
        return EMPTY;
    }

    public static Builder builder()
    {
        return new Builder(PlacementForRange.builder(),
                           PlacementForRange.builder());
    }

    public Builder unbuild()
    {
        return new Builder(reads.unbuild(), writes.unbuild());
    }
    public static class Builder
    {
        public final PlacementForRange.Builder reads;
        public final PlacementForRange.Builder writes;

        public Builder(PlacementForRange.Builder reads, PlacementForRange.Builder writes)
        {
            this.reads = reads;
            this.writes = writes;
        }

        public Builder withWriteReplica(Replica replica)
        {
            this.writes.withReplica(replica);
            return this;
        }

        public Builder withoutWriteReplica(Replica replica)
        {
            this.writes.withoutReplica(replica);
            return this;
        }

        public Builder withReadReplica(Replica replica)
        {
            this.reads.withReplica(replica);
            return this;
        }

        public Builder withoutReadReplica(Replica replica)
        {
            this.reads.withoutReplica(replica);
            return this;
        }

        public DataPlacement build()
        {
            return new DataPlacement(reads.build(), writes.build());
        }
    }

    @Override
    public String toString()
    {
        return "DataPlacement{" +
               "reads=" + toString(reads.replicaGroups) +
               ", writes=" + toString(writes.replicaGroups) +
               '}';
    }

    public static String toString(Map<?, ?> predicted)
    {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<?, ?> e : predicted.entrySet())
        {
            sb.append(e.getKey()).append("=").append(e.getValue()).append(",\n");
        }

        return sb.toString();
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
        public void serialize(DataPlacement t, DataOutputPlus out, Version version) throws IOException
        {
            PlacementForRange.serializer.serialize(t.reads, out, version);
            PlacementForRange.serializer.serialize(t.writes, out, version);
        }

        public DataPlacement deserialize(DataInputPlus in, Version version) throws IOException
        {
            PlacementForRange reads = PlacementForRange.serializer.deserialize(in, version);
            PlacementForRange writes = PlacementForRange.serializer.deserialize(in, version);
            return new DataPlacement(reads, writes);
        }

        public long serializedSize(DataPlacement t, Version version)
        {
            return PlacementForRange.serializer.serializedSize(t.reads, version) +
                   PlacementForRange.serializer.serializedSize(t.writes, version);
        }
    }
}
