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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.AbstractReplicaCollection;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.RangesByEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static org.apache.cassandra.db.TypeSizes.sizeof;

public class PlacementForRange
{
    public static final Serializer serializer = new Serializer();

    public static final PlacementForRange EMPTY = PlacementForRange.builder().build();

    final SortedMap<Range<Token>, EndpointsForRange> replicaGroups;

    public PlacementForRange(Map<Range<Token>, EndpointsForRange> replicaGroups)
    {
        this.replicaGroups = new TreeMap<>(replicaGroups);
    }

    @VisibleForTesting
    public Map<Range<Token>, EndpointsForRange> replicaGroups()
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
    public EndpointsForRange forRange(Range<Token> range) // TODO: rename to `forRangeRead`? e
    {
        // can't use range.isWrapAround() since range.unwrap() returns a wrapping range (right token is min value)
        assert range.right.compareTo(range.left) > 0 || range.right.equals(range.right.minValue());
        return replicaGroups.get(range);
    }

    /**
     * This method is intended to be used on read/write path, not forRange.
     */
    public EndpointsForRange matchRange(Range<Token> range)
    {
        EndpointsForRange.Builder builder = new EndpointsForRange.Builder(range);
        for (Map.Entry<Range<Token>, EndpointsForRange> entry : replicaGroups.entrySet())
        {
            if (entry.getKey().contains(range))
                builder.addAll(entry.getValue(), ReplicaCollection.Builder.Conflict.ALL);
        }
        return builder.build();
    }

    public EndpointsForRange forRange(Token token)
    {
        for (Map.Entry<Range<Token>, EndpointsForRange> entry : replicaGroups.entrySet())
        {
            if (entry.getKey().contains(token))
                return entry.getValue();
        }
        throw new IllegalStateException("Could not find range for token " + token + " in PlacementForRange: " + replicaGroups);
    }

    // TODO: this could be improved by searching it rather than iterating over it
    public EndpointsForToken forToken(Token token)
    {
        EndpointsForToken.Builder builder = new EndpointsForToken.Builder(token);
        for (Map.Entry<Range<Token>, EndpointsForRange> entry : replicaGroups.entrySet())
        {
            if (entry.getKey().contains(token))
                builder.addAll(entry.getValue().forToken(token), ReplicaCollection.Builder.Conflict.ALL);
        }
        return builder.build();
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
        for (Map.Entry<Range<Token>, EndpointsForRange> oldPlacement : this.replicaGroups.entrySet())
            oldPlacement.getValue().byEndpoint().forEach(builder::put);
        return builder.build();
    }

    // TODO do this without using a builder to collate and without using subtractSameReplication
    // i.e. by directly removing the replicas so that we don't need to care about the ordering of with/without.
    public PlacementForRange without(RangesByEndpoint toRemove)
    {
        Builder builder = builder();
        RangesByEndpoint currentByEndpoint = this.byEndpoint();
        for (Map.Entry<InetAddressAndPort, RangesAtEndpoint> endPointRanges : currentByEndpoint.entrySet())
        {
            InetAddressAndPort endpoint = endPointRanges.getKey();
            RangesAtEndpoint currentRanges = endPointRanges.getValue();
            RangesAtEndpoint removeRanges = toRemove.get(endpoint);
            for (Replica oldReplica : currentRanges)
            {
                RangesAtEndpoint toRetain = oldReplica.subtractSameReplication(removeRanges);
                toRetain.forEach(builder::withReplica);
            }
        }
        return builder.build();
    }

    // TODO do this without using a builder, i.e. by directly adding the replicas
    // (and directly removing them in without) so that we don't need to care
    // about the ordering of with/without.
    public PlacementForRange with(RangesByEndpoint toAdd)
    {
        Builder builder = builder();
        for (EndpointsForRange current : replicaGroups.values())
            builder.withReplicaGroup(current);
        for (Replica newReplica : toAdd.flattenValues())
            builder.withReplica(newReplica);
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
    public static PlacementForRange mergeRangesForPlacement(Set<Token> leavingTokens, PlacementForRange placement)
    {
        if (placement.replicaGroups.isEmpty())
            return placement;

        Builder newPlacement = PlacementForRange.builder();
        List<Map.Entry<Range<Token>, EndpointsForRange>> sortedEntries = new ArrayList<>(placement.replicaGroups.entrySet());
        sortedEntries.sort(Comparator.comparing(e -> e.getKey().left));
        Iterator<Map.Entry<Range<Token>, EndpointsForRange>> iter = sortedEntries.iterator();
        EndpointsForRange stashedReplicas = null;

        // TODO verify with transient replication. For example, if we have ranges [0,100] -> A, B and
        // [100,200] -> Atransient, B, we would currently not be able to merge those as A != Atransient
        while (iter.hasNext())
        {
            Map.Entry<Range<Token>, EndpointsForRange> entry = iter.next();
            Range<Token> currentRange = entry.getKey();
            EndpointsForRange currentReplicas = entry.getValue();
            // current range ends with one of the tokens being removed so we will want to merge it with the neighbouring
            // range, potentially with a number of subsequent ranges if there is an unbroken sequence of leaving tokens.
            if (leavingTokens.contains(currentRange.right))
            {
                // the previous range (if there was one) did not end in a leaving token, so the stash is empty
                if (stashedReplicas == null)
                    stashedReplicas = currentReplicas;
                    // we already have a stashed replica group. Its range must be contiguous with this one so extend the
                    // range, joining them. This asserts that the two sets of replicas match in terms of endpoints and
                    // full/transient status too.
                else
                    stashedReplicas = mergeReplicaGroups(stashedReplicas, currentReplicas);
            }
            // current range does not end in a leaving token
            else
            {
                // nothing stashed to merge, so add current replica group as it is
                if (stashedReplicas == null)
                    newPlacement.withReplicaGroup(currentReplicas);
                else
                {
                    // we have stashed the preceding replica set, so merge it with this one, add it to the new
                    // placement and clear the stash
                    newPlacement.withReplicaGroup(mergeReplicaGroups(stashedReplicas, currentReplicas));
                    stashedReplicas = null;
                }
            }
        }
        if (null != stashedReplicas)
            newPlacement.withReplicaGroup(stashedReplicas);

        return newPlacement.build();
    }

    private static EndpointsForRange mergeReplicaGroups(EndpointsForRange left, EndpointsForRange right)
    {
        assert sameReplicas(left, right);
        Range<Token> mergedRange = new Range<>(left.range().left, right.range().right);
        return EndpointsForRange.builder(mergedRange, left.size())
                                .addAll(left.asList(r -> new Replica(r.endpoint(), mergedRange, r.isFull())))
                                .build();
    }

    private static boolean sameReplicas(EndpointsForRange left, EndpointsForRange right)
    {
        if (left.size() != right.size())
            return false;

        Comparator<Replica> comparator = Comparator.comparing(Replica::endpoint);
        EndpointsForRange l = left.sorted(comparator);
        EndpointsForRange r = right.sorted(comparator);
        for (int i = 0; i < l.size(); i++)
        {
            Replica r1 = l.get(i);
            Replica r2 = r.get(i);
            if (!(r1.endpoint().equals(r2.endpoint())) || r1.isFull() != r2.isFull())
                return false;
        }
        return true;
    }

    @VisibleForTesting
    public static PlacementForRange splitRangesForPlacement(List<Token> tokens, PlacementForRange placement)
    {
        if (placement.replicaGroups.isEmpty())
            return placement;

        Builder newPlacement = PlacementForRange.builder();
        List<EndpointsForRange> eprs = new ArrayList<>(placement.replicaGroups.values());
        eprs.sort(Comparator.comparing(a -> a.range().left));
        Token min = eprs.get(0).range().left;
        Token max = eprs.get(eprs.size() - 1).range().right;

        // if any token is < the start or > the end of the ranges covered, error
        if (tokens.get(0).compareTo(min) < 0 || (!max.equals(min) && tokens.get(tokens.size()-1).compareTo(max) > 0))
            throw new IllegalArgumentException("New tokens exceed total bounds of current placement ranges " + tokens + " " + eprs);
        Iterator<EndpointsForRange> iter = eprs.iterator();
        EndpointsForRange current = iter.next();
        for (Token token : tokens)
        {
            // handle special case where one of the tokens is the min value
            if (token.equals(min))
                continue;

            assert current != null : tokens + " " + eprs;
            Range<Token> r = current.range();
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
                newPlacement.withReplicaGroup(EndpointsForRange.builder(left)
                                                               .addAll(current.asList(rep->rep.decorateSubrange(left)))
                                                               .build());
                current = EndpointsForRange.builder(right)
                                           .addAll(current.asList(rep->rep.decorateSubrange(right)))
                                           .build();
            }
        }

        if (current != null)
            newPlacement.withReplicaGroup(current);

        return newPlacement.build();
    }

    public static class Builder
    {
        private final Map<Range<Token>, EndpointsForRange> replicaGroups;

        private Builder()
        {
            this(new HashMap<>());
        }

        private Builder(int expectedSize)
        {
            this(new HashMap<>(expectedSize));
        }

        private Builder(Map<Range<Token>, EndpointsForRange> replicaGroups)
        {
            this.replicaGroups = replicaGroups;
        }

        public Builder withReplica(Replica replica)
        {
            EndpointsForRange group =
                replicaGroups.computeIfPresent(replica.range(),
                                               (t, old) -> old.newBuilder(old.size() + 1)
                                                              .addAll(old)
                                                              .add(replica, ReplicaCollection.Builder.Conflict.ALL)
                                                              .build());
            if (group == null)
                replicaGroups.put(replica.range(), EndpointsForRange.of(replica));
            return this;

        }

        public Builder withoutReplica(Replica replica)
        {
            Range<Token> range = replica.range();
            EndpointsForRange group = replicaGroups.get(range);
            if (group == null)
                throw new IllegalArgumentException(String.format("No group found for range of supplied replica %s (%s)",
                                                                 replica, range));
            EndpointsForRange without = group.without(Collections.singleton(replica.endpoint()));
            if (without.isEmpty())
                replicaGroups.remove(range);
            else
                replicaGroups.put(range, without);
            return this;
        }

        public Builder withReplicaGroup(EndpointsForRange replicas)
        {
            EndpointsForRange group =
                replicaGroups.computeIfPresent(replicas.range(),
                                               (t, old) -> replicas.newBuilder(replicas.size() + old.size())
                                                                   .addAll(old)
                                                                   .addAll(replicas, ReplicaCollection.Builder.Conflict.ALL)
                                                                   .build());
            if (group == null)
                replicaGroups.put(replicas.range(), replicas);
            return this;
        }

        public Builder withReplicaGroups(Iterable<EndpointsForRange> replicas)
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

            for (Map.Entry<Range<Token>, EndpointsForRange> entry : t.replicaGroups.entrySet())
            {
                Range<Token> range = entry.getKey();
                EndpointsForRange efr = entry.getValue();
                Token.metadataSerializer.serialize(range.left, out, version);
                Token.metadataSerializer.serialize(range.right, out, version);
                out.writeInt(efr.size());
                for (int i = 0; i < efr.size(); i++)
                {
                    Replica r = efr.get(i);
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
            Map<Range<Token>, EndpointsForRange> result = Maps.newHashMapWithExpectedSize(groupCount);
            for (int i = 0; i < groupCount; i++)
            {
                Range<Token> range = new Range<>(Token.metadataSerializer.deserialize(in, version),
                                                 Token.metadataSerializer.deserialize(in, version));
                int replicaCount = in.readInt();
                List<Replica> replicas = new ArrayList<>(replicaCount);
                for (int x = 0; x < replicaCount; x++)
                {
                    Range<Token> replicaRange = new Range<>(Token.metadataSerializer.deserialize(in, version),
                                                            Token.metadataSerializer.deserialize(in, version));
                    InetAddressAndPort replicaAddress = InetAddressAndPort.MetadataSerializer.serializer.deserialize(in, version);
                    boolean isFull = in.readBoolean();
                    replicas.add(new Replica(replicaAddress, replicaRange, isFull));

                }
                EndpointsForRange efr = EndpointsForRange.copyOf(replicas);
                result.put(range, efr);
            }
            return new PlacementForRange(result);
        }

        public long serializedSize(PlacementForRange t, Version version)
        {
            int size = sizeof(t.replicaGroups.size());
            for (Map.Entry<Range<Token>, EndpointsForRange> entry : t.replicaGroups.entrySet())
            {
                Range<Token> range = entry.getKey();
                EndpointsForRange efr = entry.getValue();

                size += Token.metadataSerializer.serializedSize(range.left, version);
                size += Token.metadataSerializer.serializedSize(range.right, version);
                size += sizeof(efr.size());
                for (int i = 0; i < efr.size(); i++)
                {
                    Replica r = efr.get(i);
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
