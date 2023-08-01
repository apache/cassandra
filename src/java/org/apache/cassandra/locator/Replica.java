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

import java.util.Objects;
import java.util.Set;

import com.google.common.base.Preconditions;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A Replica represents an owning node for a copy of a portion of the token ring.
 *
 * It consists of:
 *  - the logical token range that is being replicated (i.e. for the first logical replica only, this will be equal
 *      to one of its owned portions of the token ring; all other replicas will have this token range also)
 *  - an endpoint (IP and port)
 *  - whether the range is replicated in full, or transiently (CASSANDRA-14404)
 *
 * In general, it is preferred to use a Replica to a Range&lt;Token&gt;, particularly when users of the concept depend on
 * knowledge of the full/transient status of the copy.
 *
 * That means you should avoid unwrapping and rewrapping these things and think hard about subtraction
 * and such and what the result is WRT to transientness. Definitely avoid creating fake Replicas with misinformation
 * about endpoints, ranges, or transientness.
 */
public final class Replica implements Comparable<Replica>
{
    private final Range<Token> range;
    private final InetAddressAndPort endpoint;
    private final boolean full;

    public Replica(InetAddressAndPort endpoint, Range<Token> range, boolean full)
    {
        Preconditions.checkNotNull(endpoint);
        Preconditions.checkNotNull(range);
        this.endpoint = endpoint;
        this.range = range;
        this.full = full;
    }

    public Replica(InetAddressAndPort endpoint, Token start, Token end, boolean full)
    {
        this(endpoint, new Range<>(start, end), full);
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Replica replica = (Replica) o;
        return full == replica.full &&
               Objects.equals(endpoint, replica.endpoint) &&
               Objects.equals(range, replica.range);
    }

    @Override
    public int compareTo(Replica o)
    {
        int c = range.compareTo(o.range);
        if (c == 0)
            c = endpoint.compareTo(o.endpoint);
        if (c == 0)
            c =  Boolean.compare(full, o.full);
        return c;
    }

    public int hashCode()
    {
        return Objects.hash(endpoint, range, full);
    }

    @Override
    public String toString()
    {
        return (full ? "Full" : "Transient") + '(' + endpoint() + ',' + range + ')';
    }

    public final InetAddressAndPort endpoint()
    {
        return endpoint;
    }

    public boolean isSelf()
    {
        return endpoint.equals(FBUtilities.getBroadcastAddressAndPort());
    }

    public Range<Token> range()
    {
        return range;
    }

    public final boolean isFull()
    {
        return full;
    }

    public final boolean isTransient()
    {
        return !isFull();
    }

    /**
     * This is used exclusively in TokenMetadata to check if a portion of a range is already replicated
     * by an endpoint so that we only mark as pending the portion that is either not replicated sufficiently (transient
     * when we need full) or at all.
     *
     * If it's not replicated at all it needs to be pending because there is no data.
     * If it's replicated but only transiently and we need to replicate it fully it must be marked as pending until it
     * is available fully otherwise a read might treat this replica as full and not read from a full replica that has
     * the data.
     */
    public RangesAtEndpoint subtractSameReplication(RangesAtEndpoint toSubtract)
    {
        Set<Range<Token>> subtractedRanges = range().subtractAll(toSubtract.filter(r -> r.isFull() == isFull()).ranges());
        RangesAtEndpoint.Builder result = RangesAtEndpoint.builder(endpoint, subtractedRanges.size());
        for (Range<Token> range : subtractedRanges)
        {
            result.add(decorateSubrange(range));
        }
        return result.build();
    }

    /**
     * Don't use this method and ignore transient status unless you are explicitly handling it outside this method.
     *
     * This helper method is used by StorageService.calculateStreamAndFetchRanges to perform subtraction.
     * It ignores transient status because it's already being handled in calculateStreamAndFetchRanges.
     */
    public RangesAtEndpoint subtractIgnoreTransientStatus(Range<Token> subtract)
    {
        Set<Range<Token>> ranges = this.range.subtract(subtract);
        RangesAtEndpoint.Builder result = RangesAtEndpoint.builder(endpoint, ranges.size());
        for (Range<Token> subrange : ranges)
            result.add(decorateSubrange(subrange));
        return result.build();
    }

    public boolean contains(Range<Token> that)
    {
        return range().contains(that);
    }

    public boolean intersectsOnRange(Replica replica)
    {
        return range().intersects(replica.range());
    }

    public Replica decorateSubrange(Range<Token> subrange)
    {
        Preconditions.checkArgument(range.contains(subrange));
        return new Replica(endpoint(), subrange, isFull());
    }

    public static Replica fullReplica(InetAddressAndPort endpoint, Range<Token> range)
    {
        return new Replica(endpoint, range, true);
    }

    public static Replica fullReplica(InetAddressAndPort endpoint, Token start, Token end)
    {
        return fullReplica(endpoint, new Range<>(start, end));
    }

    public static Replica transientReplica(InetAddressAndPort endpoint, Range<Token> range)
    {
        return new Replica(endpoint, range, false);
    }

    public static Replica transientReplica(InetAddressAndPort endpoint, Token start, Token end)
    {
        return transientReplica(endpoint, new Range<>(start, end));
    }
}

