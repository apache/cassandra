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

import org.apache.cassandra.utils.Sortable;

import java.util.Comparator;

public class NoOpProximity extends BaseProximity
{
    @Override
    public <C extends ReplicaCollection<? extends C>> C sortedByProximity(final InetAddressAndPort address, C unsortedAddress)
    {
        // Optimization to avoid walking the list
        return unsortedAddress;
    }

    @Override
    public int compareEndpoints(InetAddressAndPort target, Replica r1, Replica r2)
    {
        // Making all endpoints equal ensures we won't change the original ordering (since
        // Collections.sort is guaranteed to be stable)
        return 0;
    }

    @Override
    public boolean supportCompareByEndpoint()
    {
        return true;
    }

    @Override
    public <C extends Sortable<? extends Endpoint, ? extends C>> Comparator<Endpoint> endpointComparator(InetAddressAndPort address, C addresses)
    {
        return this::compareByEndpoint;
    }

    private int compareByEndpoint(Endpoint a, Endpoint b)
    {
        // Making all endpoints equal ensures we won't change the original ordering (since
        // Collections.sort is guaranteed to be stable)
        return 0;
    }
}
