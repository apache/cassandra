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
import java.util.*;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.AbstractReplicaCollection;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.RangesByEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static org.apache.cassandra.db.TypeSizes.sizeof;

public class PlacementForRange
{
    public static final Serializer serializer = new Serializer();

    public static final PlacementForRange EMPTY = PlacementForRange.builder().build();

    final SortedMap<Range<Token>, VersionedEndpoints.ForRange> replicaGroups;

    public PlacementForRange(Map<Range<Token>, VersionedEndpoints.ForRange> replicaGroups)
    {
        this.replicaGroups = new TreeMap<>(replicaGroups);
    }

    @VisibleForTesting
    public Map<Range<Token>, VersionedEndpoints.ForRange> replicaGroups()
    {
        return Collections.unmodifiableMap(replicaGroups);
    }

    @VisibleForTesting
    public List<Range<Token>> ranges()
    {
        List<Range<Token>> ranges = new ArrayList<>(replicaGroups.keySet());
        ranges.sort(Range::compareTo);
        return ranges;
    }

    @VisibleForTesting
    public VersionedEndpoints.ForRange forRange(Range<Token> range)
    {
        // can't use range.isWrapAround() since range.unwrap() returns a wrapping range (right token is min value)
        assert range.right.compareTo(range.left) > 0 || range.right.equals(range.right.minValue());
        return replicaGroups.get(range);
    }

    /**
     * This method is intended to be used on read/write path, not forRange.
     */
    public VersionedEndpoints.ForRange matchRange(Range<Token> range)
    {
        EndpointsForRange.Builder builder = new EndpointsForRange.Builder(range);
        Epoch lastModified = Epoch.EMPTY;

        for (Map.Entry<Range<Token>, VersionedEndpoints.ForRange> entry : replicaGroups.entrySet())
        {
            if (entry.getKey().contains(range))
            {
                lastModified = Epoch.max(lastModified, entry.getValue().lastModified());
                builder.addAll(entry.getValue().get(), ReplicaCollection.Builder.Conflict.ALL);
            }
        }

        return VersionedEndpoints.forRange(lastModified, builder.build());
    }

    public VersionedEndpoints.ForRange forRange(Token token)
    {
        for (Map.Entry<Range<Token>, VersionedEndpoints.ForRange> entry : replicaGroups.entrySet())
        {
            if (entry.getKey().contains(token))
                return entry.getValue();
        }
        throw new IllegalStateException("Could not find range for token " + token + " in PlacementForRange: " + replicaGroups);
    }

    public VersionedEndpoints.ForToken forToken(Token token)
    {
        for (Map.Entry<Range<Token>, VersionedEndpoints.ForRange> entry : replicaGroups.entrySet())
        {
            if (entry.getKey().contains(token))
                return entry.getValue().forToken(token);
        }
        throw new IllegalStateException("Could not find range for token " + token + " in PlacementForRange: " + replicaGroups);
    }

    public Delta difference(PlacementForRange next)
    {
        RangesByEndpoint oldMap = this.byEndpoint();
        RangesByEndpoint newMap = next.byEndpoint();
        return new Delta(diff(oldMap, newMap), diff(newMap, oldMap));
    }

    @VisibleForTesting
    public RangesByEndpoint byEndpoint()
    {
        RangesByEndpoint.Builder builder = new RangesByEndpoint.Builder();
        for (Map.Entry<Range<Token>, VersionedEndpoints.ForRange> oldPlacement : this.replicaGroups.entrySet())
            oldPlacement.getValue().byEndpoint().forEach(builder::put);
        return builder.build();
    }

    private RangesByEndpoint diff(RangesByEndpoint left, RangesByEndpoint right)
    {
        RangesByEndpoint.Builder builder = new RangesByEndpoint.Builder();
        for (Map.Entry<InetAddressAndPort, RangesAtEndpoint> endPointRanges : left.entrySet())
        {
            InetAddressAndPort endpoint = endPointRanges.getKey();
            RangesAtEndpoint leftRanges = endPointRanges.getValue();
            RangesAtEndpoint rightRanges = right.get(endpoint);
            for (Replica leftReplica : leftRanges)
            {
                if (!rightRanges.contains(leftReplica))
                    builder.put(endpoint, leftReplica);
            }
        }
        return builder.build();
    }

    @Override
    public String toString()
    {
        return replicaGroups.toString();
    }

    @VisibleForTesting
    public Map<String, List<String>> toStringByEndpoint()
    {
        Map<String, List<String>> mappings = new HashMap<>();
        for (Map.Entry<InetAddressAndPort, RangesAtEndpoint> entry : byEndpoint().entrySet())
            mappings.put(entry.getKey().toString(), entry.getValue().asList(r -> r.range().toString()));
        return mappings;
    }

    @VisibleForTesting
    public List<String> toReplicaStringList()
    {
        return replicaGroups.values()
                            .stream()
                            .map(VersionedEndpoints.ForRange::get)
                            .flatMap(AbstractReplicaCollection::stream)
                            .map(Replica::toString)
                            .collect(Collectors.toList());
    }

    public Builder unbuild()
    {
        return new Builder(new HashMap<>(replicaGroups));
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(int expectedSize)
    {
        return new Builder(expectedSize);
    }

    @VisibleForTesting
    public static PlacementForRange splitRangesForPlacement(List<Token> tokens, PlacementForRange placement)
    {
        if (placement.replicaGroups.isEmpty())
            return placement;

        Builder newPlacement = PlacementForRange.builder();
        List<VersionedEndpoints.ForRange> eprs = new ArrayList<>(placement.replicaGroups.values());
        eprs.sort(Comparator.comparing(a -> a.range().left));
        Token min = eprs.get(0).range().left;
        Token max = eprs.get(eprs.size() - 1).range().right;

        // if any token is < the start or > the end of the ranges covered, error
        if (tokens.get(0).compareTo(min) < 0 || (!max.equals(min) && tokens.get(tokens.size()-1).compareTo(max) > 0))
            throw new IllegalArgumentException("New tokens exceed total bounds of current placement ranges " + tokens + " " + eprs);
        Iterator<VersionedEndpoints.ForRange> iter = eprs.iterator();
        VersionedEndpoints.ForRange current = iter.next();
        for (Token token : tokens)
        {
            // handle special case where one of the tokens is the min value
            if (token.equals(min))
                continue;

            assert current != null : tokens + " " + eprs;
            Range<Token> r = current.get().range();
            int cmp = token.compareTo(r.right);
            if (cmp == 0)
            {
                newPlacement.withReplicaGroup(current);
                if (iter.hasNext())
                    current = iter.next();
                else
                    current = null;
            }
            else if (cmp < 0 || r.right.isMinimum())
            {
                Range<Token> left = new Range<>(r.left, token);
                Range<Token> right = new Range<>(token, r.right);
                newPlacement.withReplicaGroup(VersionedEndpoints.forRange(current.lastModified(),
                                                                          EndpointsForRange.builder(left)
                                                                                           .addAll(current.get().asList(rep->rep.decorateSubrange(left)))
                                                                                           .build()));
                current = VersionedEndpoints.forRange(current.lastModified(),
                                                      EndpointsForRange.builder(right)
                                                                       .addAll(current.get().asList(rep->rep.decorateSubrange(right)))
                                                                       .build());
            }
        }

        if (current != null)
            newPlacement.withReplicaGroup(current);

        return newPlacement.build();
    }

    public static class Builder
    {
        private final Map<Range<Token>, VersionedEndpoints.ForRange> replicaGroups;

        private Builder()
        {
            this(new HashMap<>());
        }

        private Builder(int expectedSize)
        {
            this(new HashMap<>(expectedSize));
        }

        private Builder(Map<Range<Token>, VersionedEndpoints.ForRange> replicaGroups)
        {
            this.replicaGroups = replicaGroups;
        }

        public Builder withReplica(Epoch epoch, Replica replica)
        {
            VersionedEndpoints.ForRange group =
                replicaGroups.computeIfPresent(replica.range(),
                                               (t, v) -> {
                                                   EndpointsForRange old = v.get();
                                                   return VersionedEndpoints.forRange(epoch,
                                                                                      old.newBuilder(old.size() + 1)
                                                                                         .addAll(old)
                                                                                         .add(replica, ReplicaCollection.Builder.Conflict.NONE)
                                                                                         .build());
                                               });
;            if (group == null)
                replicaGroups.put(replica.range(), VersionedEndpoints.forRange(epoch, EndpointsForRange.of(replica)));
            return this;

        }

        public Builder withoutReplica(Epoch epoch, Replica replica)
        {
            Range<Token> range = replica.range();
            VersionedEndpoints.ForRange group = replicaGroups.get(range);
            if (group == null)
                throw new IllegalArgumentException(String.format("No group found for range of supplied replica %s (%s)",
                                                                 replica, range));
            EndpointsForRange without = group.get().without(Collections.singleton(replica.endpoint()));
            if (without.isEmpty())
                replicaGroups.remove(range);
            else
                replicaGroups.put(range, VersionedEndpoints.forRange(epoch, without));
            return this;
        }

        public Builder withReplicaGroup(VersionedEndpoints.ForRange replicas)
        {
            VersionedEndpoints.ForRange group =
                replicaGroups.computeIfPresent(replicas.range(),
                                               (t, v) -> {
                                                   EndpointsForRange old = v.get();
                                                   return VersionedEndpoints.forRange(Epoch.max(v.lastModified(), replicas.lastModified()),
                                                                                      replicas.get()
                                                                                              .newBuilder(replicas.size() + old.size())
                                                                                              .addAll(old)
                                                                                              .addAll(replicas.get(), ReplicaCollection.Builder.Conflict.ALL)
                                                                                              .build());
                                               });
            if (group == null)
                replicaGroups.put(replicas.range(), replicas);
            return this;
        }

        public Builder withReplicaGroups(Iterable<VersionedEndpoints.ForRange> replicas)
        {
            replicas.forEach(this::withReplicaGroup);
            return this;
        }

        public PlacementForRange build()
        {
            return new PlacementForRange(this.replicaGroups);
        }
    }

    public static class Serializer implements MetadataSerializer<PlacementForRange>
    {
        public void serialize(PlacementForRange t, DataOutputPlus out, Version version) throws IOException
        {
            out.writeInt(t.replicaGroups.size());

            for (Map.Entry<Range<Token>, VersionedEndpoints.ForRange> entry : t.replicaGroups.entrySet())
            {
                Range<Token> range = entry.getKey();
                VersionedEndpoints.ForRange efr = entry.getValue();
                if (version.isAtLeast(Version.V2))
                    Epoch.serializer.serialize(efr.lastModified(), out, version);
                Token.metadataSerializer.serialize(range.left, out, version);
                Token.metadataSerializer.serialize(range.right, out, version);
                out.writeInt(efr.size());
                for (int i = 0; i < efr.size(); i++)
                {
                    Replica r = efr.get().get(i);
                    Token.metadataSerializer.serialize(r.range().left, out, version);
                    Token.metadataSerializer.serialize(r.range().right, out, version);
                    InetAddressAndPort.MetadataSerializer.serializer.serialize(r.endpoint(), out, version);
                    out.writeBoolean(r.isFull());
                }
            }
        }

        public PlacementForRange deserialize(DataInputPlus in, Version version) throws IOException
        {
            int groupCount = in.readInt();
            Map<Range<Token>, VersionedEndpoints.ForRange> result = Maps.newHashMapWithExpectedSize(groupCount);
            IPartitioner partitioner = ClusterMetadata.current().partitioner;
            for (int i = 0; i < groupCount; i++)
            {
                Epoch lastModified;
                if (version.isAtLeast(Version.V2))
                    lastModified = Epoch.serializer.deserialize(in, version);
                else
                {
                    // During upgrade to V2, when reading from snapshot, we should start from the current version, rather than EMPTY
                    ClusterMetadata metadata = ClusterMetadata.currentNullable();
                    if (metadata != null)
                        lastModified = metadata.epoch;
                    else
                        lastModified = Epoch.EMPTY;
                }
                Range<Token> range = new Range<>(Token.metadataSerializer.deserialize(in, partitioner, version),
                                                 Token.metadataSerializer.deserialize(in, partitioner, version));
                int replicaCount = in.readInt();
                List<Replica> replicas = new ArrayList<>(replicaCount);
                for (int x = 0; x < replicaCount; x++)
                {
                    Range<Token> replicaRange = new Range<>(Token.metadataSerializer.deserialize(in, partitioner, version),
                                                            Token.metadataSerializer.deserialize(in, partitioner, version));
                    InetAddressAndPort replicaAddress = InetAddressAndPort.MetadataSerializer.serializer.deserialize(in, version);
                    boolean isFull = in.readBoolean();
                    replicas.add(new Replica(replicaAddress, replicaRange, isFull));

                }
                EndpointsForRange efr = EndpointsForRange.copyOf(replicas);
                result.put(range, VersionedEndpoints.forRange(lastModified, efr));
            }
            return new PlacementForRange(result);
        }

        public long serializedSize(PlacementForRange t, Version version)
        {
            int size = sizeof(t.replicaGroups.size());
            for (Map.Entry<Range<Token>, VersionedEndpoints.ForRange> entry : t.replicaGroups.entrySet())
            {
                Range<Token> range = entry.getKey();
                VersionedEndpoints.ForRange efr = entry.getValue();

                if (version.isAtLeast(Version.V2))
                    size += Epoch.serializer.serializedSize(efr.lastModified(), version);
                size += Token.metadataSerializer.serializedSize(range.left, version);
                size += Token.metadataSerializer.serializedSize(range.right, version);
                size += sizeof(efr.size());
                for (int i = 0; i < efr.size(); i++)
                {
                    Replica r = efr.get().get(i);
                    size += Token.metadataSerializer.serializedSize(r.range().left, version);
                    size += Token.metadataSerializer.serializedSize(r.range().right, version);
                    size += InetAddressAndPort.MetadataSerializer.serializer.serializedSize(r.endpoint(), version);
                    size += sizeof(r.isFull());
                }
            }
            return size;
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof PlacementForRange)) return false;
        PlacementForRange that = (PlacementForRange) o;
        return Objects.equals(replicaGroups, that.replicaGroups);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(replicaGroups);
    }
}
