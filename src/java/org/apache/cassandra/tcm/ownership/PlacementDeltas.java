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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class PlacementDeltas extends ReplicationMap<PlacementDeltas.PlacementDelta>
{
    public static final Serializer serializer = new Serializer();
    private static final PlacementDeltas EMPTY = new PlacementDeltas(Collections.emptyMap());

    private PlacementDeltas(Map<ReplicationParams, PlacementDelta> map)
    {
        super(map);
    }

    protected PlacementDelta defaultValue()
    {
        return PlacementDelta.EMPTY;
    }

    protected PlacementDelta localOnly()
    {
        throw new IllegalStateException("Cannot apply diff to local placements.");
    }

    public PlacementDeltas invert()
    {
        Builder inverse = builder(size());
        asMap().forEach((params, delta) -> inverse.put(params, delta.invert()));
        return inverse.build();
    }

    @Override
    public String toString()
    {
        return "DeltaMap{" +
               "map=" + asMap() +
               '}';
    }

    public DataPlacements apply(DataPlacements placements)
    {
        DataPlacements.Builder builder = placements.unbuild();
        asMap().forEach((params, delta) -> {
            DataPlacement previous = placements.get(params);
            builder.with(params, delta.apply(previous));
        });
        return builder.build();
    }

    public static PlacementDeltas empty()
    {
        return EMPTY;
    }

    public static Builder builder()
    {
        return new Builder(new HashMap<>());
    }

    public static Builder builder(int expectedSize)
    {
        return new Builder(new HashMap<>(expectedSize));
    }

    public static Builder builder(Map<ReplicationParams, PlacementDelta> map)
    {
        return new Builder(map);
    }

    public static class PlacementDelta
    {
        public static PlacementDelta EMPTY = new PlacementDelta(Delta.empty(), Delta.empty());

        public final Delta reads;
        public final Delta writes;

        public PlacementDelta(Delta reads, Delta writes)
        {
            this.reads = reads;
            this.writes = writes;
        }

        public PlacementDelta onlyReads()
        {
            return new PlacementDelta(reads, Delta.empty());
        }

        public PlacementDelta onlyWrites()
        {
            return new PlacementDelta(Delta.empty(), writes);
        }

        public PlacementDelta onlyAdditions()
        {
            return new PlacementDelta(reads.onlyAdditions(), writes.onlyAdditions());
        }

        public PlacementDelta onlyRemovals()
        {
            return new PlacementDelta(reads.onlyRemovals(), writes.onlyRemovals());
        }

        public DataPlacement apply(DataPlacement placement)
        {
            DataPlacement.Builder builder = placement.unbuild();
            reads.additions.flattenValues().forEach(builder.reads::withReplica);
            writes.additions.flattenValues().forEach(builder.writes::withReplica);

            reads.removals.flattenValues().forEach(builder.reads::withoutReplica);
            writes.removals.flattenValues().forEach(builder.writes::withoutReplica);
            return builder.build();
        }

        public PlacementDelta merge(PlacementDelta other)
        {
            return new PlacementDelta(reads.merge(other.reads),
                                      writes.merge(other.writes));
        }

        public PlacementDelta invert()
        {
            return new PlacementDelta(reads.invert(), writes.invert());
        }

        public String toString()
        {
            return "PlacementDelta{" +
                   "reads=" + reads +
                   ", writes=" + writes +
                   '}';
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PlacementDelta other = (PlacementDelta) o;

            return Objects.equals(reads, other.reads) && Objects.equals(writes, other.writes);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(reads, writes);
        }
    }

    public static class Builder
    {
        private final Map<ReplicationParams, PlacementDelta> map;
        private Builder(Map<ReplicationParams, PlacementDelta> map)
        {
            this.map = map;
        }

        public Builder put(ReplicationParams params, PlacementDelta placement)
        {
            PlacementDelta delta = map.get(params);
            if (delta == null)
                map.put(params, placement);
            else
                map.put(params, delta.merge(placement));
            return this;
        }

        public PlacementDeltas build()
        {
            return new PlacementDeltas(map);
        }
    }

    public static class Serializer implements MetadataSerializer<PlacementDeltas>
    {
        public void serialize(PlacementDeltas t, DataOutputPlus out, Version version) throws IOException
        {
            out.writeInt(t.size());
            for (Map.Entry<ReplicationParams, PlacementDelta> e : t.asMap().entrySet())
            {
                ReplicationParams.serializer.serialize(e.getKey(), out, version);
                Delta.serializer.serialize(e.getValue().reads, out, version);
                Delta.serializer.serialize(e.getValue().writes, out, version);
            }

        }

        public PlacementDeltas deserialize(DataInputPlus in, Version version) throws IOException
        {
            int size = in.readInt();
            Builder builder = PlacementDeltas.builder(size);
            for (int i = 0; i < size; i++)
            {
                ReplicationParams replicationParams = ReplicationParams.serializer.deserialize(in, version);
                Delta reads = Delta.serializer.deserialize(in, version);
                Delta writes = Delta.serializer.deserialize(in, version);
                builder.put(replicationParams, new PlacementDelta(reads, writes));
            }
            return builder.build();
        }

        public long serializedSize(PlacementDeltas t, Version version)
        {
            long size = TypeSizes.INT_SIZE;
            for (Map.Entry<ReplicationParams, PlacementDelta> e : t.asMap().entrySet())
            {
                size += ReplicationParams.serializer.serializedSize(e.getKey(), version);
                size += Delta.serializer.serializedSize(e.getValue().reads, version);
                size += Delta.serializer.serializedSize(e.getValue().writes, version);
            }
            return size;
        }
    }
}
