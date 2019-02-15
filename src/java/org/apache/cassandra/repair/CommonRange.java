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
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;

/**
 * Groups ranges with identical endpoints/transient endpoints
 */
public class CommonRange
{
    public final ImmutableSet<InetAddressAndPort> endpoints;
    public final ImmutableSet<InetAddressAndPort> transEndpoints;
    public final Collection<Range<Token>> ranges;

    public CommonRange(Set<InetAddressAndPort> endpoints, Set<InetAddressAndPort> transEndpoints, Collection<Range<Token>> ranges)
    {
        Preconditions.checkArgument(endpoints != null && !endpoints.isEmpty(), "Endpoints can not be empty");
        Preconditions.checkArgument(transEndpoints != null, "Transient endpoints can not be null");
        Preconditions.checkArgument(endpoints.containsAll(transEndpoints), "transEndpoints must be a subset of endpoints");
        Preconditions.checkArgument(ranges != null && !ranges.isEmpty(), "Ranges can not be empty");

        this.endpoints = ImmutableSet.copyOf(endpoints);
        this.transEndpoints = ImmutableSet.copyOf(transEndpoints);
        this.ranges = new ArrayList<>(ranges);
    }

    public boolean matchesEndpoints(Set<InetAddressAndPort> endpoints, Set<InetAddressAndPort> transEndpoints)
    {
        // Use strict equality here, as worst thing that can happen is we generate one more stream
        return this.endpoints.equals(endpoints) && this.transEndpoints.equals(transEndpoints);
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CommonRange that = (CommonRange) o;

        if (!endpoints.equals(that.endpoints)) return false;
        if (!transEndpoints.equals(that.transEndpoints)) return false;
        return ranges.equals(that.ranges);
    }

    public int hashCode()
    {
        int result = endpoints.hashCode();
        result = 31 * result + transEndpoints.hashCode();
        result = 31 * result + ranges.hashCode();
        return result;
    }

    public String toString()
    {
        return "CommonRange{" +
               "endpoints=" + endpoints +
               ", transEndpoints=" + transEndpoints +
               ", ranges=" + ranges +
               '}';
    }
}
