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
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesByEndpoint;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

public class EndpointDelta implements Delta
{
    public static final Serializer serializer = new Serializer();

    private static final Delta EMPTY = new EndpointDelta(RangesByEndpoint.EMPTY, RangesByEndpoint.EMPTY);

    public final RangesByEndpoint removals;
    public final RangesByEndpoint additions;

    public EndpointDelta(RangesByEndpoint removals, RangesByEndpoint additions)
    {
        this.removals = removals;
        this.additions = additions;
    }

    /**
     * Merges this delta with `other`
     *
     * Note that if opposite operations (add a range in this, remove it in other for example) exist in
     * `this` and `other` the operations cancel eachother out and neither will be in the resulting delta.
     * @param other
     * @return
     */
    public Delta merge(Delta other)
    {
        throw new IllegalStateException("We only merge when constructing new deltas, EndpointDeltas are only constructed when deserializing existing ones");
    }

    public Delta invert()
    {
        return new EndpointDelta(additions, removals);
    }

    @Override
    public boolean isEmpty()
    {
        return additions.isEmpty() && removals.isEmpty();
    }

    @Override
    public EndpointDelta asEndpointDelta(Function<NodeId, InetAddressAndPort> endpointLookup)
    {
        return this;
    }

    @Override
    public RangesByEndpoint removals(Function<NodeId, InetAddressAndPort> endpointLookup)
    {
        return removals;
    }

    @Override
    public RangesByEndpoint additions(Function<NodeId, InetAddressAndPort> endpointLookup)
    {
        return additions;
    }

    @Override
    public Collection<Range<Token>> addedRanges()
    {
        Set<Range<Token>> ranges = new HashSet<>();
        additions.flattenValues().forEach(r -> ranges.add(r.range()));
        return ranges;
    }

    @Override
    public Collection<Range<Token>> removedRanges()
    {
        Set<Range<Token>> ranges = new HashSet<>();
        removals.flattenValues().forEach(r -> ranges.add(r.range()));
        return ranges;
    }

    @Override
    public Set<InetAddressAndPort> allEndpoints()
    {
        Set<InetAddressAndPort> endpoints = new HashSet<>(removals.keySet());
        endpoints.addAll(additions.keySet());
        return endpoints;
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EndpointDelta delta = (EndpointDelta) o;

        return Objects.equals(removals, delta.removals) && Objects.equals(additions, delta.additions);
    }

    public int hashCode()
    {
        return Objects.hash(removals, additions);
    }

    @Override
    public String toString()
    {
        return "Delta{" +
               "removals=" + removals +
               ", additions=" + additions +
               '}';
    }

    public static Delta empty()
    {
        return EMPTY;
    }

    public static final class Serializer implements MetadataSerializer<EndpointDelta>
    {
        public void serialize(EndpointDelta t, DataOutputPlus out, Version version) throws IOException
        {
            RangesByEndpoint.serializer.serialize(t.removals, out, version);
            RangesByEndpoint.serializer.serialize(t.additions, out, version);
        }

        public EndpointDelta deserialize(DataInputPlus in, Version version) throws IOException
        {
            return new EndpointDelta(RangesByEndpoint.serializer.deserialize(in, version),
                             RangesByEndpoint.serializer.deserialize(in, version));
        }

        public long serializedSize(EndpointDelta t, Version version)
        {
            return RangesByEndpoint.serializer.serializedSize(t.removals, version) +
                   RangesByEndpoint.serializer.serializedSize(t.additions, version);
        }
    }
}
