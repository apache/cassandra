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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static org.apache.cassandra.locator.ReplicaCollection.Mutable.Conflict.*;

/**
 * A ReplicaCollection for Ranges occurring at an endpoint. All Replica will be for the same endpoint,
 * and must be unique Ranges (though overlapping ranges are presently permitted, these should probably not be permitted to occur)
 */
public class RangesAtEndpoint extends AbstractReplicaCollection<RangesAtEndpoint>
{
    private static final Map<Range<Token>, Replica> EMPTY_MAP = Collections.unmodifiableMap(new LinkedHashMap<>());

    private final InetAddressAndPort endpoint;
    private volatile Map<Range<Token>, Replica> byRange;
    private volatile RangesAtEndpoint fullRanges;
    private volatile RangesAtEndpoint transRanges;

    private RangesAtEndpoint(InetAddressAndPort endpoint, List<Replica> list, boolean isSnapshot)
    {
        this(endpoint, list, isSnapshot, null);
    }
    private RangesAtEndpoint(InetAddressAndPort endpoint, List<Replica> list, boolean isSnapshot, Map<Range<Token>, Replica> byRange)
    {
        super(list, isSnapshot);
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

    public Set<Range<Token>> ranges()
    {
        return byRange().keySet();
    }

    public Map<Range<Token>, Replica> byRange()
    {
        Map<Range<Token>, Replica> map = byRange;
        if (map == null)
            byRange = map = buildByRange(list);
        return map;
    }

    @Override
    protected RangesAtEndpoint snapshot(List<Replica> subList)
    {
        if (subList.isEmpty()) return empty(endpoint);
        return new RangesAtEndpoint(endpoint, subList, true);
    }

    @Override
    public RangesAtEndpoint self()
    {
        return this;
    }

    @Override
    public ReplicaCollection.Mutable<RangesAtEndpoint> newMutable(int initialCapacity)
    {
        return new Mutable(endpoint, initialCapacity);
    }

    @Override
    public boolean contains(Replica replica)
    {
        return replica != null
                && Objects.equals(
                        byRange().get(replica.range()),
                        replica);
    }

    public RangesAtEndpoint full()
    {
        RangesAtEndpoint coll = fullRanges;
        if (fullRanges == null)
            fullRanges = coll = filter(Replica::isFull);
        return coll;
    }

    public RangesAtEndpoint trans()
    {
        RangesAtEndpoint coll = transRanges;
        if (transRanges == null)
            transRanges = coll = filter(Replica::isTransient);
        return coll;
    }

    public Collection<Range<Token>> fullRanges()
    {
        return full().ranges();
    }

    public Collection<Range<Token>> transientRanges()
    {
        return trans().ranges();
    }

    public boolean contains(Range<Token> range, boolean isFull)
    {
        Replica replica = byRange().get(range);
        return replica != null && replica.isFull() == isFull;
    }

    private static Map<Range<Token>, Replica> buildByRange(List<Replica> list)
    {
        // TODO: implement a delegating map that uses our superclass' list, and is immutable
        Map<Range<Token>, Replica> byRange = new LinkedHashMap<>(list.size());
        for (Replica replica : list)
        {
            Replica prev = byRange.put(replica.range(), replica);
            assert prev == null : "duplicate range in RangesAtEndpoint: " + prev + " and " + replica;
        }

        return Collections.unmodifiableMap(byRange);
    }

    public static Collector<Replica, Builder, RangesAtEndpoint> collector(InetAddressAndPort endpoint)
    {
        return collector(ImmutableSet.of(), () -> new Builder(endpoint));
    }

    public static class Mutable extends RangesAtEndpoint implements ReplicaCollection.Mutable<RangesAtEndpoint>
    {
        boolean hasSnapshot;
        public Mutable(InetAddressAndPort endpoint) { this(endpoint, 0); }
        public Mutable(InetAddressAndPort endpoint, int capacity) { super(endpoint, new ArrayList<>(capacity), false, new LinkedHashMap<>()); }

        public void add(Replica replica, Conflict ignoreConflict)
        {
            if (hasSnapshot) throw new IllegalStateException();
            Preconditions.checkNotNull(replica);
            if (!Objects.equals(super.endpoint, replica.endpoint()))
                throw new IllegalArgumentException("Replica " + replica + " has incorrect endpoint (expected " + super.endpoint + ")");

            Replica prev = super.byRange.put(replica.range(), replica);
            if (prev != null)
            {
                super.byRange.put(replica.range(), prev); // restore prev
                switch (ignoreConflict)
                {
                    case DUPLICATE:
                        if (prev.equals(replica))
                            break;
                    case NONE:
                        throw new IllegalArgumentException("Conflicting replica added (expected unique ranges): " + replica + "; existing: " + prev);
                    case ALL:
                }
                return;
            }

            list.add(replica);
        }

        @Override
        public Map<Range<Token>, Replica> byRange()
        {
            // our internal map is modifiable, but it is unsafe to modify the map externally
            // it would be possible to implement a safe modifiable map, but it is probably not valuable
            return Collections.unmodifiableMap(super.byRange());
        }

        public RangesAtEndpoint get(boolean isSnapshot)
        {
            return new RangesAtEndpoint(super.endpoint, super.list, isSnapshot, Collections.unmodifiableMap(super.byRange));
        }

        public RangesAtEndpoint asImmutableView()
        {
            return get(false);
        }

        public RangesAtEndpoint asSnapshot()
        {
            hasSnapshot = true;
            return get(true);
        }
    }

    public static class Builder extends ReplicaCollection.Builder<RangesAtEndpoint, Mutable, RangesAtEndpoint.Builder>
    {
        public Builder(InetAddressAndPort endpoint) { this(endpoint, 0); }
        public Builder(InetAddressAndPort endpoint, int capacity) { super(new Mutable(endpoint, capacity)); }
    }

    public static RangesAtEndpoint.Builder builder(InetAddressAndPort endpoint)
    {
        return new RangesAtEndpoint.Builder(endpoint);
    }

    public static RangesAtEndpoint.Builder builder(InetAddressAndPort endpoint, int capacity)
    {
        return new RangesAtEndpoint.Builder(endpoint, capacity);
    }

    public static RangesAtEndpoint empty(InetAddressAndPort endpoint)
    {
        return new RangesAtEndpoint(endpoint, EMPTY_LIST, true, EMPTY_MAP);
    }

    public static RangesAtEndpoint of(Replica replica)
    {
        ArrayList<Replica> one = new ArrayList<>(1);
        one.add(replica);
        return new RangesAtEndpoint(replica.endpoint(), one, true, Collections.unmodifiableMap(Collections.singletonMap(replica.range(), replica)));
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

    /**
     * @return concatenate two DISJOINT collections together
     */
    public static RangesAtEndpoint concat(RangesAtEndpoint replicas, RangesAtEndpoint extraReplicas)
    {
        return AbstractReplicaCollection.concat(replicas, extraReplicas, NONE);
    }

}
