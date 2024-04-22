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
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
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
import org.apache.cassandra.tcm.serialization.PartitionerAwareMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.AsymmetricOrdering;

import static org.apache.cassandra.db.TypeSizes.sizeof;

public class ReplicaGroups
{
    private static final AsymmetricOrdering<Range<Token>, Token> ordering = new AsymmetricOrdering<>()
    {
        @Override
        public int compare(Range<Token> left, Range<Token> right)
        {
            return left.compareTo(right);
        }

        @Override
        public int compareAsymmetric(Range<Token> range, Token token)
        {
            if (token.isMinimum() && !range.right.isMinimum())
                return -1;
            if (range.left.compareTo(token) >= 0)
                return 1;
            if (!range.right.isMinimum() && range.right.compareTo(token) < 0)
                return -1;
            return 0;
        }
    };

    public static final Serializer serializer = new Serializer();
    public static final ReplicaGroups EMPTY = ReplicaGroups.builder().build();

    public final ImmutableList<Range<Token>> ranges;
    public final ImmutableList<VersionedEndpoints.ForRange> endpoints;

    public ReplicaGroups(Map<Range<Token>, VersionedEndpoints.ForRange> replicaGroups)
    {
        ImmutableList.Builder<Range<Token>> rangesBuilder = ImmutableList.builderWithExpectedSize(replicaGroups.size());
        ImmutableList.Builder<VersionedEndpoints.ForRange> endpointsBuilder = ImmutableList.builderWithExpectedSize(replicaGroups.size());
        Range<Token> prev = null;
        for (Map.Entry<Range<Token>, VersionedEndpoints.ForRange> entry : ImmutableSortedMap.copyOf(replicaGroups, Comparator.comparing(o -> o.left)).entrySet())
        {
            if (prev != null && prev.right.compareTo(entry.getKey().left) > 0 )
                throw new IllegalArgumentException("Got overlapping ranges in replica groups: " + replicaGroups);
            prev = entry.getKey();
            rangesBuilder.add(entry.getKey());
            endpointsBuilder.add(entry.getValue());
        }
        this.ranges = rangesBuilder.build();
        this.endpoints = endpointsBuilder.build();
    }

    @VisibleForTesting
    public List<Range<Token>> ranges()
    {
        List<Range<Token>> ranges = new ArrayList<>(this.ranges);
        ranges.sort(Range::compareTo);
        return ranges;
    }

    @VisibleForTesting
    public VersionedEndpoints.ForRange forRange(Range<Token> range)
    {
        // can't use range.isWrapAround() since range.unwrap() returns a wrapping range (right token is min value)
        assert range.right.compareTo(range.left) > 0 || range.right.equals(range.right.minValue());
        // we're searching for an exact match to the input range here, can use standard binary search
        int pos = Collections.binarySearch(ranges, range, Comparator.comparing(o -> o.left));
        if (pos >= 0 && pos < ranges.size() && ranges.get(pos).equals(range))
            return endpoints.get(pos);
        return null;
    }

    /**
     * This method is intended to be used on read/write path, not forRange.
     */
    public VersionedEndpoints.ForRange matchRange(Range<Token> range)
    {
        EndpointsForRange.Builder builder = new EndpointsForRange.Builder(range);
        Epoch lastModified = Epoch.EMPTY;
        // find a range containing the *right* token for the given range - Range is start exclusive so if we looked for the
        // left one we could get the wrong range
        int pos = ordering.binarySearchAsymmetric(ranges, range.right, AsymmetricOrdering.Op.CEIL);
        if (pos >= 0 && pos < ranges.size() && ranges.get(pos).contains(range))
        {
            VersionedEndpoints.ForRange eps = endpoints.get(pos);
            lastModified = eps.lastModified();
            builder.addAll(eps.get(), ReplicaCollection.Builder.Conflict.ALL);
        }
        return VersionedEndpoints.forRange(lastModified, builder.build());
    }

    public VersionedEndpoints.ForRange forRange(Token token)
    {
        int pos = ordering.binarySearchAsymmetric(ranges, token, AsymmetricOrdering.Op.CEIL);
        if (pos >= 0 && pos < endpoints.size())
            return endpoints.get(pos);
        throw new IllegalStateException("Could not find range for token " + token + " in ReplicaGroups: " + this);
    }

    public VersionedEndpoints.ForToken forToken(Token token)
    {
        return forRange(token).forToken(token);
    }

    public Delta difference(ReplicaGroups next)
    {
        RangesByEndpoint oldMap = this.byEndpoint();
        RangesByEndpoint newMap = next.byEndpoint();
        return new Delta(diff(oldMap, newMap), diff(newMap, oldMap));
    }

    @VisibleForTesting
    public RangesByEndpoint byEndpoint()
    {
        RangesByEndpoint.Builder builder = new RangesByEndpoint.Builder();
        for (int i = 0; i < endpoints.size(); i++)
            endpoints.get(i).byEndpoint().forEach(builder::put);
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

    public ReplicaGroups withCappedLastModified(Epoch lastModified)
    {
        SortedMap<Range<Token>, VersionedEndpoints.ForRange> copy = new TreeMap<>();
        for (int i = 0; i < ranges.size(); i++)
        {
            Range<Token> range = ranges.get(i);
            VersionedEndpoints.ForRange forRange = endpoints.get(i);
            if (forRange.lastModified().isAfter(lastModified))
                forRange = forRange.withLastModified(lastModified);
            copy.put(range, forRange);
        }
        return new ReplicaGroups(copy);
    }


    public int size()
    {
        return ranges.size();
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    @VisibleForTesting
    public Map<Range<Token>, VersionedEndpoints.ForRange> asMap()
    {
        Map<Range<Token>, VersionedEndpoints.ForRange> map = new HashMap<>();
        for (int i = 0; i < size(); i++)
            map.put(ranges.get(i), endpoints.get(i));
        return map;
    }

    public void forEach(BiConsumer<Range<Token>, VersionedEndpoints.ForRange> consumer)
    {
        for (int i = 0; i < size(); i++)
            consumer.accept(ranges.get(i), endpoints.get(i));
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("ReplicaGroups{");
        forEach((range, eps) -> sb.append(range).append('=').append(eps).append(", "));
        return sb.append('}').toString();
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
        return endpoints.stream()
                        .map(VersionedEndpoints.ForRange::get)
                        .flatMap(AbstractReplicaCollection::stream)
                        .map(Replica::toString)
                        .collect(Collectors.toList());
    }

    public Builder unbuild()
    {
        return new Builder(asMap());
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
    public static ReplicaGroups splitRangesForPlacement(List<Token> tokens, ReplicaGroups placement)
    {
        if (placement.ranges.isEmpty())
            return placement;

        Builder newPlacement = ReplicaGroups.builder();
        List<VersionedEndpoints.ForRange> eprs = new ArrayList<>(placement.endpoints);
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
                                                                                      combine(old, replicas.get()));
                                               });
            if (group == null)
                replicaGroups.put(replicas.range(), replicas);
            return this;
        }

        /**
         * Combine two replica groups, assuming one is the current group and the other is the proposed.
         * During range movements this is used when calculating the maximal placement, which combines the current and
         * future replica groups. This special cases the merging of two replica groups to make sure that when a replica
         * moves from transient to full, it starts to act as a FULL write replica as early as possible.
         *
         * Where an endpoint is present in both groups, prefer the proposed iff it is a FULL replica. During a
         * multi-step operation (join/leave/move), we want any change from transient to full to happen as early
         * as possible so that a replica whose ownership is modified in this way becomes FULL for writes before it
         * becomes FULL for reads. This works as additions to write replica groups are applied before any other
         * placement changes (i.e. in START_[JOIN|LEAVE|MOVE]).
         *
         * @param prev Initial set of replicas for a given range
         * @param next Proposed set of replicas for the same range.
         * @return The union of the two groups
         */
        private EndpointsForRange combine(EndpointsForRange prev, EndpointsForRange next)
        {
            Map<InetAddressAndPort, Replica> e1 = prev.byEndpoint();
            Map<InetAddressAndPort, Replica> e2 = next.byEndpoint();
            EndpointsForRange.Builder combined = prev.newBuilder(prev.size() + next.size());
            e1.forEach((e, r1) -> {
                Replica r2 = e2.get(e);
                if (null == r2)          // not present in next
                    combined.add(r1);
                else if (r2.isFull())    // prefer replica from next, if it is moving from transient to full
                    combined.add(r2);
                else
                    combined.add(r1);    // replica is moving from full to transient, or staying the same
            });
            // any new replicas not in prev
            e2.forEach((e, r2) -> {
                if (!combined.contains(e))
                    combined.add(r2);
            });
            return combined.build();
        }

        public Builder withReplicaGroups(Iterable<VersionedEndpoints.ForRange> replicas)
        {
            replicas.forEach(this::withReplicaGroup);
            return this;
        }

        public ReplicaGroups build()
        {
            return new ReplicaGroups(this.replicaGroups);
        }
    }

    public static class Serializer implements PartitionerAwareMetadataSerializer<ReplicaGroups>
    {
        public void serialize(ReplicaGroups t, DataOutputPlus out, IPartitioner partitioner, Version version) throws IOException
        {
            out.writeInt(t.ranges.size());

            for (int i = 0; i < t.ranges.size(); i++)
            {
                Range<Token> range = t.ranges.get(i);
                VersionedEndpoints.ForRange efr = t.endpoints.get(i);
                if (version.isAtLeast(Version.V2))
                    Epoch.serializer.serialize(efr.lastModified(), out, version);
                Token.metadataSerializer.serialize(range.left, out, partitioner, version);
                Token.metadataSerializer.serialize(range.right, out, partitioner, version);
                out.writeInt(efr.size());
                for (int efrIdx = 0; efrIdx < efr.size(); efrIdx++)
                {
                    Replica r = efr.get().get(efrIdx);
                    Token.metadataSerializer.serialize(r.range().left, out, partitioner, version);
                    Token.metadataSerializer.serialize(r.range().right, out, partitioner, version);
                    InetAddressAndPort.MetadataSerializer.serializer.serialize(r.endpoint(), out, version);
                    out.writeBoolean(r.isFull());
                }
            }
        }

        public ReplicaGroups deserialize(DataInputPlus in, IPartitioner partitioner, Version version) throws IOException
        {
            int groupCount = in.readInt();
            Map<Range<Token>, VersionedEndpoints.ForRange> result = Maps.newHashMapWithExpectedSize(groupCount);
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
            return new ReplicaGroups(result);
        }

        public long serializedSize(ReplicaGroups t, IPartitioner partitioner, Version version)
        {
            long size = sizeof(t.ranges.size());
            for (int i = 0; i < t.ranges.size(); i++)
            {
                Range<Token> range = t.ranges.get(i);
                VersionedEndpoints.ForRange efr = t.endpoints.get(i);

                if (version.isAtLeast(Version.V2))
                    size += Epoch.serializer.serializedSize(efr.lastModified(), version);
                size += Token.metadataSerializer.serializedSize(range.left, partitioner, version);
                size += Token.metadataSerializer.serializedSize(range.right, partitioner, version);
                size += sizeof(efr.size());
                for (int efrIdx = 0; efrIdx < efr.size(); efrIdx++)
                {
                    Replica r = efr.get().get(efrIdx);
                    size += Token.metadataSerializer.serializedSize(r.range().left, partitioner, version);
                    size += Token.metadataSerializer.serializedSize(r.range().right, partitioner, version);
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
        if (!(o instanceof ReplicaGroups)) return false;
        ReplicaGroups that = (ReplicaGroups) o;
        return Objects.equals(ranges, that.ranges) && Objects.equals(endpoints, that.endpoints);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(ranges, endpoints);
    }
}
