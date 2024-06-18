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

/**
 * A simple endpoint snitch implementation that treats Strategy order as proximity,
 * allowing non-read-repaired reads to prefer a single endpoint, which improves
 * cache locality.
 * @deprecated See CASSANDRA-19488
 */
@Deprecated(since = "CEP-21")
public class SimpleSnitch extends AbstractEndpointSnitch
{
    private static final NodeProximity sorter = new NoOpProximity();
    private static final SimpleLocationProvider provider = new SimpleLocationProvider();

    @Override
    public String getLocalRack()
    {
        return provider.initialLocation().rack;
    }

    @Override
    public String getLocalDatacenter()
    {
        return provider.initialLocation().datacenter;
    }

    @Override
    public <C extends ReplicaCollection<? extends C>> C sortedByProximity(InetAddressAndPort address, C unsortedAddress)
    {
        return sorter.sortedByProximity(address, unsortedAddress);
    }

    @Override
    public int compareEndpoints(InetAddressAndPort target, Replica r1, Replica r2)
    {
        return sorter.compareEndpoints(target, r1, r2);
    }

    @Override
    public boolean isWorthMergingForRangeQuery(ReplicaCollection<?> merged, ReplicaCollection<?> l1, ReplicaCollection<?> l2)
    {
        return sorter.isWorthMergingForRangeQuery(merged, l1, l2);
    }
}
