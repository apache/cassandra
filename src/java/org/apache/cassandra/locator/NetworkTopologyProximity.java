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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.tcm.membership.Location;

public class NetworkTopologyProximity extends BaseProximity
{
    public int compareEndpoints(InetAddressAndPort address, Replica r1, Replica r2)
    {
        InetAddressAndPort a1 = r1.endpoint();
        InetAddressAndPort a2 = r2.endpoint();
        if (address.equals(a1) && !address.equals(a2))
            return -1;
        if (address.equals(a2) && !address.equals(a1))
            return 1;

        Locator locator = DatabaseDescriptor.getLocator();
        Location l1 = locator.location(a1);
        Location l2 = locator.location(a2);
        Location location = locator.location(address);

        if (location.datacenter.equals(l1.datacenter) && !location.datacenter.equals(l2.datacenter))
            return -1;
        if (location.datacenter.equals(l2.datacenter) && !location.datacenter.equals(l1.datacenter))
            return 1;

        if (location.rack.equals(l1.rack) && !location.rack.equals(l2.rack))
            return -1;
        if (location.rack.equals(l2.rack) && !location.rack.equals(l1.rack))
            return 1;
        return 0;
    }
}
