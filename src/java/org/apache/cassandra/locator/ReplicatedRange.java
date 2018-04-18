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

import java.util.Collection;
import java.util.Objects;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.Sets;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

public class ReplicatedRange
{
    private final Range<Token> range;
    private final boolean full;

    public ReplicatedRange(Range<Token> range, boolean full)
    {
        this.range = range;
        this.full = full;
    }

    public ReplicatedRange(Token left, Token right, boolean full)
    {
        this(new Range<>(left, right), full);
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplicatedRange that = (ReplicatedRange) o;
        return full == that.full &&
               Objects.equals(range, that.range);
    }

    public int hashCode()
    {

        return Objects.hash(range, full);
    }

    @Override
    public String toString()
    {
        return (full ? "Full" : "Transient") + range;
    }

    public Range<Token> getRange()
    {
        return range;
    }

    public boolean isFull()
    {
        return full;
    }

    public boolean isTransient()
    {
        return !full;
    }
    public Replica decorateEndpoint(InetAddressAndPort endpoint)
    {
        return full ? Replica.full(endpoint) : Replica.trans(endpoint);
    }

    public boolean contains(Range<Token> that)
    {
        return range.contains(that);
    }

    public ReplicatedRange decorateSubrange(Range<Token> that)
    {
        Preconditions.checkArgument(contains(that));
        return new ReplicatedRange(that, full);
    }

    public Set<ReplicatedRange> subtract(ReplicatedRange rhs)
    {
        assert isFull() && rhs.isFull();  // FIXME: this
        Set<Range<Token>> ranges = range.subtract(rhs.range);
        Set<ReplicatedRange> replicatedRanges = Sets.newHashSetWithExpectedSize(ranges.size());
        replicatedRanges.addAll(ReplicatedRanges.decorateRanges(ranges, isFull()));
        return replicatedRanges;
    }

    /**
     * TODO: explain why this works the way it does
     * TODO: double check this does the right thing when a full range becomes pending and vice versa
     */
    public static Collection<ReplicatedRange> subtractForPending(ReplicatedRange newRange, Collection<ReplicatedRange> oldRanges)
    {
        Collection<Range<Token>> filteredRanges = newRange.isFull()
                                                  ? ReplicatedRanges.asRanges(Collections2.filter(oldRanges, ReplicatedRange::isFull))
                                                  : ReplicatedRanges.asRanges(oldRanges);
        return ReplicatedRanges.decorateRanges(newRange.range.subtractAll(filteredRanges), newRange.isFull());
    }
}

