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

import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Location;

public class SnitchAdapter implements InitialLocationProvider, NodeProximity, NodeAddressConfig
{
    public final IEndpointSnitch snitch;

    public SnitchAdapter(IEndpointSnitch snitch)
    {
        this.snitch = snitch;
    }

    @Override
    public Location initialLocation()
    {
        return new Location(snitch.getLocalDatacenter(), snitch.getLocalRack());
    }

    @Override
    public void validate(ClusterMetadata metadata)
    {
        Set<String> datacenters = metadata.directory.allDatacenterRacks().keySet();
        Set<String> racks = new HashSet<>();
        for (String dc : datacenters)
            racks.addAll(metadata.directory.datacenterRacks(dc).keySet());
        if (!snitch.validate(datacenters, racks))
            throw new ConfigurationException("Initial location provider rejected registration location, " +
                                             "please check the system log for errors");
    }

    @Override
    public <C extends ReplicaCollection<? extends C>> C sortedByProximity(InetAddressAndPort address, C addresses)
    {
        return snitch.sortedByProximity(address, addresses);
    }

    @Override
    public int compareEndpoints(InetAddressAndPort target, Replica r1, Replica r2)
    {
        return snitch.compareEndpoints(target, r1, r2);
    }

    @Override
    public boolean isWorthMergingForRangeQuery(ReplicaCollection<?> merged, ReplicaCollection<?> l1, ReplicaCollection<?> l2)
    {
        return snitch.isWorthMergingForRangeQuery(merged, l1, l2);
    }

    @Override
    public void configureAddresses()
    {
        snitch.configureAddresses();
    }

    @Override
    public boolean preferLocalConnections()
    {
        return snitch.preferLocalConnections();
    }
}
