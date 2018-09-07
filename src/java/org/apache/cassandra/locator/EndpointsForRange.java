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

import com.google.common.base.Preconditions;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

import java.util.Arrays;
import java.util.Collection;

import static com.google.common.collect.Iterables.all;

/**
 * A ReplicaCollection where all Replica are required to cover a range that fully contains the range() defined in the builder().
 * Endpoints are guaranteed to be unique; on construction, this is enforced unless optionally silenced (in which case
 * only the first occurrence makes the cut).
 */
public class EndpointsForRange extends Endpoints<EndpointsForRange>
{
    private final Range<Token> range;
    private EndpointsForRange(Range<Token> range, ReplicaList list, ReplicaMap<InetAddressAndPort> byEndpoint)
    {
        super(list, byEndpoint);
        this.range = range;
        assert range != null;
    }

    public Range<Token> range()
    {
        return range;
    }

    @Override
    public Builder newBuilder(int initialCapacity)
    {
        return new Builder(range, initialCapacity);
    }

    public EndpointsForToken forToken(Token token)
    {
        if (!range.contains(token))
            throw new IllegalArgumentException(token + " is not contained within " + range);
        return new EndpointsForToken(token, list, byEndpoint);
    }

    @Override
    public EndpointsForRange snapshot()
    {
        return this;
    }

    @Override
    EndpointsForRange snapshot(ReplicaList newList)
    {
        if (newList.isEmpty()) return empty(range);
        ReplicaMap<InetAddressAndPort> byEndpoint = null;
        if (this.byEndpoint != null && list.isSubList(newList))
            byEndpoint = this.byEndpoint.forSubList(newList);
        return new EndpointsForRange(range, newList, byEndpoint);
    }

    public static class Builder extends EndpointsForRange implements ReplicaCollection.Builder<EndpointsForRange>
    {
        boolean built;
        public Builder(Range<Token> range) { this(range, 0); }
        public Builder(Range<Token> range, int capacity) { this(range, new ReplicaList(capacity)); }
        private Builder(Range<Token> range, ReplicaList list) { super(range, list, endpointMap(list)); }

        public EndpointsForRange.Builder add(Replica replica, Conflict ignoreConflict)
        {
            if (built) throw new IllegalStateException();
            Preconditions.checkNotNull(replica);
            if (!replica.range().contains(super.range))
                throw new IllegalArgumentException("Replica " + replica + " does not contain " + super.range);

            if (!super.byEndpoint.internalPutIfAbsent(replica, list.size()))
            {
                switch (ignoreConflict)
                {
                    case DUPLICATE:
                        if (byEndpoint().get(replica.endpoint()).equals(replica))
                            break;
                    case NONE:
                        throw new IllegalArgumentException("Conflicting replica added (expected unique endpoints): "
                                + replica + "; existing: " + byEndpoint().get(replica.endpoint()));
                    case ALL:
                }
                return this;
            }

            list.add(replica);
            return this;
        }

        @Override
        public EndpointsForRange snapshot()
        {
            return snapshot(list.subList(0, list.size()));
        }

        public EndpointsForRange build()
        {
            built = true;
            return new EndpointsForRange(super.range, super.list, super.byEndpoint);
        }
    }

    public static Builder builder(Range<Token> range)
    {
        return new Builder(range);
    }
    public static Builder builder(Range<Token> range, int capacity)
    {
        return new Builder(range, capacity);
    }

    public static EndpointsForRange empty(Range<Token> range)
    {
        return new EndpointsForRange(range, EMPTY_LIST, EMPTY_MAP);
    }

    public static EndpointsForRange of(Replica replica)
    {
        // we only use ArrayList or ArrayList.SubList, to ensure callsites are bimorphic
        ReplicaList one = new ReplicaList(1);
        one.add(replica);
        // we can safely use singletonMap, as we only otherwise use LinkedHashMap
        return new EndpointsForRange(replica.range(), one, endpointMap(one));
    }

    public static EndpointsForRange of(Replica ... replicas)
    {
        return copyOf(Arrays.asList(replicas));
    }

    public static EndpointsForRange copyOf(Collection<Replica> replicas)
    {
        if (replicas.isEmpty())
            throw new IllegalArgumentException("Collection must be non-empty to copy");
        Range<Token> range = replicas.iterator().next().range();
        assert all(replicas, r -> range.equals(r.range()));
        return builder(range, replicas.size()).addAll(replicas).build();
    }
}
