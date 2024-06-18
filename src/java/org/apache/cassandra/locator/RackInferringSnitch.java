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

import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A simple endpoint snitch implementation that assumes datacenter and rack information is encoded
 * in the 2nd and 3rd octets of the ip address, respectively.
 * As with all snitches post CEP-21, this retrieves Location for remote peers from ClusterMetadata.
 * Local location is derived from (broadcast) ip address and added to ClusterMetadata during node
 * registration. Every member of the cluster is required to do this, hence remote peers' Location
 * can always be retrieved, consistently.
 * @deprecated See CASSANDRA-19488
 */
@Deprecated(since = "CEP-21")
public class RackInferringSnitch extends AbstractNetworkTopologySnitch
{
    final Location local;

    public RackInferringSnitch()
    {
        InetAddressAndPort localAddress = FBUtilities.getBroadcastAddressAndPort();
        local = inferLocation(localAddress);
    }

    public static Location inferLocation(InetAddressAndPort address)
    {
        return new Location(Integer.toString(address.getAddress().getAddress()[1] & 0xFF, 10),
                             Integer.toString(address.getAddress().getAddress()[2] & 0xFF, 10));

    }

    @Override
    public String getRack(InetAddressAndPort endpoint)
    {
        return inferLocation(endpoint).rack;
    }

    @Override
    public String getDatacenter(InetAddressAndPort endpoint)
    {
        return inferLocation(endpoint).datacenter;
    }
}
