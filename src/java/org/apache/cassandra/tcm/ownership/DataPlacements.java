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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataValue;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.db.TypeSizes.sizeof;

public class DataPlacements extends ReplicationMap<DataPlacement> implements MetadataValue<DataPlacements>
{
    public static Serializer serializer = new Serializer();

    public static final DataPlacements EMPTY = DataPlacements.builder(1).build();
    private static DataPlacement LOCAL_PLACEMENT;

    private final Epoch lastModified;

    private DataPlacements(Epoch lastModified, Map<ReplicationParams, DataPlacement> map)
    {
        super(map);
        this.lastModified = lastModified;
    }

    public DataPlacements replaceParams(Epoch lastModified, ReplicationParams oldParams, ReplicationParams newParams)
    {
        Map<ReplicationParams, DataPlacement> newMap = Maps.newHashMapWithExpectedSize(map.size());
        assert map.containsKey(oldParams) : String.format("Can't replace key %s, since map doesn't contain it: %s", oldParams, map);
        for (Map.Entry<ReplicationParams, DataPlacement> e : map.entrySet())
        {
            if (e.getKey().equals(oldParams))
                newMap.put(newParams, e.getValue());
            else
                newMap.put(e.getKey(), e.getValue());
        }

        return new DataPlacements(lastModified, newMap);
    }

    protected DataPlacement defaultValue()
    {
        return DataPlacement.empty();
    }

    public void withDistributed(BiConsumer<ReplicationParams, DataPlacement> consumer)
    {
        forEach(e -> {
            if (e.getKey().isLocal() || e.getKey().isMeta())
                return;

            consumer.accept(e.getKey(), e.getValue());
        });
    }

    protected DataPlacement localOnly()
    {
        // it's unlikely to happen, but perfectly safe to create multiple times, so no need to lock or statically init
        if (null == LOCAL_PLACEMENT)
        {
            // todo remove this entirely
            EndpointsForRange endpoints = EndpointsForRange.of(new Replica(FBUtilities.getBroadcastAddressAndPort(),
                                                                           DatabaseDescriptor.getPartitioner().getMinimumToken(),
                                                                           DatabaseDescriptor.getPartitioner().getMinimumToken(),
                                                                           true));
            ReplicaGroups placement = ReplicaGroups.builder(1)
                                                   .withReplicaGroup(VersionedEndpoints.forRange(Epoch.EMPTY, endpoints))
                                                   .build();
            LOCAL_PLACEMENT = new DataPlacement(placement, placement);
        }
        return LOCAL_PLACEMENT;
    }

    public DataPlacements combineReplicaGroups(DataPlacements end)
    {
        DataPlacements start = this;
        if (start.isEmpty())
            return end;
        Builder mapBuilder = DataPlacements.builder(start.size());
        start.asMap().forEach((params, placement) ->
                              mapBuilder.with(params, placement.combineReplicaGroups(end.get(params))));
        return mapBuilder.build();
    }

    @Override
    public DataPlacements withLastModified(Epoch epoch)
    {
        return new DataPlacements(epoch, capLastModified(epoch, map));
    }

    @Override
    public Epoch lastModified()
    {
        return lastModified;
    }

    @Override
    public String toString()
    {
        return "DataPlacements{" +
               "lastModified=" + lastModified +
               ", placementMap=" + asMap() +
               '}';
    }

    public static DataPlacements sortReplicaGroups(DataPlacements placements, Comparator<Replica> comparator)
    {
        Builder builder = DataPlacements.builder(placements.size());
        placements.forEach((params, placement) -> {
            if (params.isMeta() || params.isLocal())
                builder.with(params, placement);
            else
            {
                ReplicaGroups.Builder reads = ReplicaGroups.builder(placement.reads.size());
                placement.reads.endpoints.forEach((endpoints) -> {
                    reads.withReplicaGroup(VersionedEndpoints.forRange(endpoints.lastModified(),
                                                                       endpoints.get().sorted(comparator)));
                });
                ReplicaGroups.Builder writes = ReplicaGroups.builder(placement.writes.size());
                placement.writes.endpoints.forEach((endpoints) -> {
                    writes.withReplicaGroup(VersionedEndpoints.forRange(endpoints.lastModified(),
                                                                        endpoints.get().sorted(comparator)));
                });
                builder.with(params, new DataPlacement(reads.build(), writes.build()));
            }
        });
        return builder.build();
    }

    public DataPlacements applyDelta(Epoch epoch, PlacementDeltas deltas)
    {
        return deltas.apply(epoch, this);
    }

    public static DataPlacements empty()
    {
        return EMPTY;
    }

    public static Builder builder(int expectedSize)
    {
        return new Builder(new HashMap<>(expectedSize));
    }

    public static Builder builder(Map<ReplicationParams, DataPlacement> map)
    {
        return new Builder(map);
    }

    public Builder unbuild()
    {
        return new Builder(new HashMap<>(this.asMap()));
    }

    public static class Builder
    {
        private final Map<ReplicationParams, DataPlacement> map;
        private Builder(Map<ReplicationParams, DataPlacement> map)
        {
            this.map = map;
        }

        public Builder with(ReplicationParams params, DataPlacement placement)
        {
            map.put(params, placement);
            return this;
        }

        public Builder without(ReplicationParams params)
        {
            map.remove(params);
            return this;
        }

        public DataPlacements build()
        {
            return new DataPlacements(Epoch.EMPTY, map);
        }
    }

    public static class Serializer implements MetadataSerializer<DataPlacements>
    {
        public void serialize(DataPlacements t, DataOutputPlus out, Version version) throws IOException
        {
            Map<ReplicationParams, DataPlacement> map = t.asMap();
            out.writeInt(map.size());
            for (Map.Entry<ReplicationParams, DataPlacement> entry : map.entrySet())
            {
                ReplicationParams.serializer.serialize(entry.getKey(), out, version);
                DataPlacement.serializerFor(entry.getKey()).serialize(entry.getValue(), out, version);
            }
            Epoch.serializer.serialize(t.lastModified, out, version);
        }

        public DataPlacements deserialize(DataInputPlus in, Version version) throws IOException
        {
            int size = in.readInt();
            Map<ReplicationParams, DataPlacement> map = Maps.newHashMapWithExpectedSize(size);
            for (int i = 0; i < size; i++)
            {
                ReplicationParams params = ReplicationParams.serializer.deserialize(in, version);
                map.put(params, DataPlacement.serializerFor(params).deserialize(in, version));
            }
            Epoch lastModified = Epoch.serializer.deserialize(in, version);
            return new DataPlacements(lastModified, map);
        }

        public long serializedSize(DataPlacements t, Version version)
        {
            long size = sizeof(t.size());
            for (Map.Entry<ReplicationParams, DataPlacement> entry : t.asMap().entrySet())
            {
                size += ReplicationParams.serializer.serializedSize(entry.getKey(), version);
                size += DataPlacement.serializerFor(entry.getKey()).serializedSize(entry.getValue(), version);
            }
            size += Epoch.serializer.serializedSize(t.lastModified, version);
            return size;
        }
    }

    public static ImmutableMap<ReplicationParams, DataPlacement> capLastModified(Epoch lastModified, Map<ReplicationParams, DataPlacement> placements)
    {
        ImmutableMap.Builder<ReplicationParams, DataPlacement> builder = ImmutableMap.builder();
        placements.forEach((params, placement) -> builder.put(params, placement.withCappedLastModified(lastModified)));
        return builder.build();
    }

    public void dumpDiff(DataPlacements other)
    {
        if (!map.equals(other.map))
        {
            logger.warn("Maps differ: {} != {}", map, other.map);
            dumpDiff(logger,map, other.map);
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(DataPlacements.class);
    public static void dumpDiff(Logger logger, Map<ReplicationParams, DataPlacement> l, Map<ReplicationParams, DataPlacement> r)
    {
        for (ReplicationParams k : Sets.intersection(l.keySet(), r.keySet()))
        {
            DataPlacement lv = l.get(k);
            DataPlacement rv = r.get(k);
            if (!Objects.equals(lv, rv))
            {
                logger.warn("Values for key {} differ: {} != {}", k, lv, rv);
                logger.warn("Difference: {}", lv.difference(rv));
            }
        }
        for (ReplicationParams k : Sets.difference(l.keySet(), r.keySet()))
            logger.warn("Value for key {} is only present in the left set: {}", k, l.get(k));
        for (ReplicationParams k : Sets.difference(r.keySet(), l.keySet()))
            logger.warn("Value for key {} is only present in the right set: {}", k, r.get(k));

    }
}
