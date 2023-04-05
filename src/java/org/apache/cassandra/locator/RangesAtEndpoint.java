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

package org.apache.cassandra.locator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collector;

import static com.google.common.collect.Iterables.all;
import static org.apache.cassandra.locator.ReplicaCollection.Builder.Conflict.*;

/**
 * A ReplicaCollection for Ranges occurring at an endpoint. All Replica will be for the same endpoint,
 * and must be unique Ranges (though overlapping ranges are presently permitted, these should probably not be permitted to occur)
 */
public class RangesAtEndpoint extends AbstractReplicaCollection<RangesAtEndpoint>
{
    private static ReplicaMap<Range<Token>> rangeMap(ReplicaList list) { return new ReplicaMap<>(list, Replica::range); }
    private static final ReplicaMap<Range<Token>> EMPTY_MAP = rangeMap(EMPTY_LIST);

    private final InetAddressAndPort endpoint;

    // volatile not needed, as all of these caching collections have final members,
    // besides (transitively) those that cache objects that themselves have only final members
    private ReplicaMap<Range<Token>> byRange;
    private RangesAtEndpoint onlyFull;
    private RangesAtEndpoint onlyTransient;

    private RangesAtEndpoint(InetAddressAndPort endpoint, ReplicaList list, ReplicaMap<Range<Token>> byRange)
    {
        super(list);
        this.endpoint = endpoint;
        this.byRange = byRange;
        assert endpoint != null;
    }

    public InetAddressAndPort endpoint()
    {
        return endpoint;
    }

    @Override
    public Set<InetAddressAndPort> endpoints()
    {
        return Collections.unmodifiableSet(list.isEmpty()
                ? Collections.emptySet()
                : Collections.singleton(endpoint)
        );
    }

    /**
     * @return a set of all unique Ranges
     * This method is threadsafe, though it is not synchronised
     */
    public Set<Range<Token>> ranges()
    {
        return byRange().keySet();
    }

    /**
     * @return a map of all Ranges, to their owning Replica instance
     * This method is threadsafe, though it is not synchronised
     */
    public Map<Range<Token>, Replica> byRange()
    {
        ReplicaMap<Range<Token>> map = byRange;
        if (map == null)
            byRange = map = rangeMap(list);
        return map;
    }

    @Override
    protected RangesAtEndpoint snapshot(ReplicaList newList)
    {
        if (newList.isEmpty()) return empty(endpoint);
        ReplicaMap<Range<Token>> byRange = null;
        if (this.byRange != null && list.isSubList(newList))
            byRange = this.byRange.forSubList(newList);
        return new RangesAtEndpoint(endpoint, newList, byRange);
    }

    @Override
    public RangesAtEndpoint snapshot()
    {
        return this;
    }

    @Override
    public ReplicaCollection.Builder<RangesAtEndpoint> newBuilder(int initialCapacity)
    {
        return new Builder(endpoint, initialCapacity);
    }

    @Override
    public boolean contains(Replica replica)
    {
        return replica != null
                && Objects.equals(
                        byRange().get(replica.range()),
                        replica);
    }

    public RangesAtEndpoint onlyFull()
    {
        RangesAtEndpoint result = onlyFull;
        if (result == null)
            onlyFull = result = filter(Replica::isFull);
        return result;
    }

    public RangesAtEndpoint onlyTransient()
    {
        RangesAtEndpoint result = onlyTransient;
        if (result == null)
            onlyTransient = result = filter(Replica::isTransient);
        return result;
    }

    public boolean contains(Range<Token> range, boolean isFull)
    {
        Replica replica = byRange().get(range);
        return replica != null && replica.isFull() == isFull;
    }

    /**
     * @return if there are no wrap around ranges contained in this RangesAtEndpoint, return self;
     * otherwise, return a RangesAtEndpoint covering the same logical portions of the ring, but with those ranges unwrapped
     */
    public RangesAtEndpoint unwrap()
    {
        int wrapAroundCount = 0;
        for (Replica replica : this)
        {
            if (replica.range().isWrapAround())
                ++wrapAroundCount;
        }

        assert wrapAroundCount <= 1;
        if (wrapAroundCount == 0)
            return snapshot();

        RangesAtEndpoint.Builder builder = builder(endpoint, size() + wrapAroundCount);
        for (Replica replica : this)
        {
            if (!replica.range().isWrapAround())
            {
                builder.add(replica);
                continue;
            }
            for (Range<Token> range : replica.range().unwrap())
                builder.add(replica.decorateSubrange(range));
        }
        return builder.build();
    }

    public static Collector<Replica, Builder, RangesAtEndpoint> collector(InetAddressAndPort endpoint)
    {
        return collector(ImmutableSet.of(), () -> new Builder(endpoint));
    }

    public static class Builder extends RangesAtEndpoint implements ReplicaCollection.Builder<RangesAtEndpoint>
    {
        boolean built;
        public Builder(InetAddressAndPort endpoint) { this(endpoint, 0); }
        public Builder(InetAddressAndPort endpoint, int capacity) { this(endpoint, new ReplicaList(capacity)); }
        private Builder(InetAddressAndPort endpoint, ReplicaList list) { super(endpoint, list, rangeMap(list)); }

        public RangesAtEndpoint.Builder add(Replica replica)
        {
            return add(replica, Conflict.DUPLICATE);
        }

        public RangesAtEndpoint.Builder add(Replica replica, Conflict ignoreConflict)
        {
            if (built) throw new IllegalStateException();
            Preconditions.checkNotNull(replica);
            if (!Objects.equals(super.endpoint, replica.endpoint()))
                throw new IllegalArgumentException("Replica " + replica + " has incorrect endpoint (expected " + super.endpoint + ")");

            if (!super.byRange.internalPutIfAbsent(replica, list.size()))
            {
                switch (ignoreConflict)
                {
                    case DUPLICATE:
                        if (byRange().get(replica.range()).equals(replica))
                            break;
                    case NONE:
                        throw new IllegalArgumentException("Conflicting replica added (expected unique ranges): "
                                + replica + "; existing: " + byRange().get(replica.range()));
                    case ALL:
                }
                return this;
            }

            list.add(replica);
            return this;
        }

        @Override
        public RangesAtEndpoint snapshot()
        {
            return snapshot(list.subList(0, list.size()));
        }

        public RangesAtEndpoint build()
        {
            built = true;
            return new RangesAtEndpoint(super.endpoint, super.list, super.byRange);
        }
    }

    public static Builder builder(InetAddressAndPort endpoint)
    {
        return new Builder(endpoint);
    }
    public static Builder builder(InetAddressAndPort endpoint, int capacity)
    {
        return new Builder(endpoint, capacity);
    }

    public static RangesAtEndpoint empty(InetAddressAndPort endpoint)
    {
        return new RangesAtEndpoint(endpoint, EMPTY_LIST, EMPTY_MAP);
    }

    public static RangesAtEndpoint of(Replica replica)
    {
        ReplicaList one = new ReplicaList(1);
        one.add(replica);
        return new RangesAtEndpoint(replica.endpoint(), one, rangeMap(one));
    }

    public static RangesAtEndpoint of(Replica ... replicas)
    {
        return copyOf(Arrays.asList(replicas));
    }

    public static RangesAtEndpoint copyOf(List<Replica> replicas)
    {
        if (replicas.isEmpty())
            throw new IllegalArgumentException("Must specify a non-empty collection of replicas");
        return builder(replicas.get(0).endpoint(), replicas.size()).addAll(replicas).build();
    }


    /**
     * Use of this method to synthesize Replicas is almost always wrong. In repair it turns out the concerns of transient
     * vs non-transient are handled at a higher level, but eventually repair needs to ask streaming to actually move
     * the data and at that point it doesn't have a great handle on what the replicas are and it doesn't really matter.
     *
     * Streaming expects to be given Replicas with each replica indicating what type of data (transient or not transient)
     * should be sent.
     *
     * So in this one instance we can lie to streaming and pretend all the replicas are full and use a dummy address
     * and it doesn't matter because streaming doesn't rely on the address for anything other than debugging and full
     * is a valid value for transientness because streaming is selecting candidate tables from the repair/unrepaired
     * set already.
     * @param ranges
     * @return
     */
    @VisibleForTesting
    public static RangesAtEndpoint toDummyList(Collection<Range<Token>> ranges)
    {
        InetAddressAndPort dummy;
        try
        {
            dummy = InetAddressAndPort.getByNameOverrideDefaults("0.0.0.0", 0);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }

        //For repair we are less concerned with full vs transient since repair is already dealing with those concerns.
        //Always say full and then if the repair is incremental or not will determine what is streamed.
        return ranges.stream()
                .map(range -> new Replica(dummy, range, true))
                .collect(collector(dummy));
    }

    public static boolean isDummyList(RangesAtEndpoint ranges)
    {
        return all(ranges, range -> range.endpoint().getHostAddress(true).equals("0.0.0.0:0"));
    }

    /**
     * @return concatenate two DISJOINT collections together
     */
    public static RangesAtEndpoint concat(RangesAtEndpoint replicas, RangesAtEndpoint extraReplicas)
    {
        return AbstractReplicaCollection.concat(replicas, extraReplicas, NONE);
    }

}
