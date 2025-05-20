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

package org.apache.cassandra.repair;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;

/**
 * Groups ranges with identical endpoints/witness endpoints
 */
public class CommonRange
{
    public final ImmutableSet<InetAddressAndPort> endpoints;
    public final ImmutableSet<InetAddressAndPort> witnessEndpoints;
    public final Collection<Range<Token>> ranges;
    public final boolean hasSkippedReplicas;

    public CommonRange(Set<InetAddressAndPort> endpoints, Set<InetAddressAndPort> witnessEndpoints, Collection<Range<Token>> ranges)
    {
        this(endpoints, witnessEndpoints, ranges, false);
    }

    public CommonRange(Set<InetAddressAndPort> endpoints, Set<InetAddressAndPort> witnessEndpoints, Collection<Range<Token>> ranges, boolean hasSkippedReplicas)
    {
        Preconditions.checkArgument(endpoints != null && !endpoints.isEmpty(), "Endpoints can not be empty");
        Preconditions.checkArgument(witnessEndpoints != null, "Witness endpoints can not be null");
        Preconditions.checkArgument(endpoints.containsAll(witnessEndpoints), "witnessEndpoints must be a subset of endpoints");
        Preconditions.checkArgument(ranges != null && !ranges.isEmpty(), "Ranges can not be empty");

        this.endpoints = ImmutableSet.copyOf(endpoints);
        this.witnessEndpoints = ImmutableSet.copyOf(witnessEndpoints);
        this.ranges = new ArrayList<>(ranges);
        this.hasSkippedReplicas = hasSkippedReplicas;
    }

    public boolean matchesEndpoints(Set<InetAddressAndPort> endpoints, Set<InetAddressAndPort> transEndpoints)
    {
        // Use strict equality here, as worst thing that can happen is we generate one more stream
        return this.endpoints.equals(endpoints) && this.witnessEndpoints.equals(transEndpoints);
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CommonRange that = (CommonRange) o;

        return Objects.equals(endpoints, that.endpoints)
               && Objects.equals(witnessEndpoints, that.witnessEndpoints)
               && Objects.equals(ranges, that.ranges)
               && hasSkippedReplicas == that.hasSkippedReplicas;
    }

    public int hashCode()
    {
        return Objects.hash(endpoints, witnessEndpoints, ranges, hasSkippedReplicas);
    }

    public String toString()
    {
        return "CommonRange{" +
               "endpoints=" + endpoints +
               ", transEndpoints=" + witnessEndpoints +
               ", ranges=" + ranges +
               ", hasSkippedReplicas=" + hasSkippedReplicas +
               '}';
    }
}
