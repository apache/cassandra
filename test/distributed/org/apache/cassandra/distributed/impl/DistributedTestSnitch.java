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

package org.apache.cassandra.distributed.impl;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.locator.AbstractNetworkTopologySnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.Pair;

public class DistributedTestSnitch extends AbstractNetworkTopologySnitch
{
    private static NetworkTopology mapping = null;

    public String getRack(InetAddress endpoint)
    {
        assert mapping != null : "network topology must be assigned before using snitch";
        return mapping.localRack(InetAddressAndPort.getByAddress(endpoint));
    }

    public String getRack(InetAddressAndPort endpoint)
    {
        assert mapping != null : "network topology must be assigned before using snitch";
        return mapping.localRack(endpoint);
    }

    public String getDatacenter(InetAddress endpoint)
    {
        assert mapping != null : "network topology must be assigned before using snitch";
        return mapping.localDC(InetAddressAndPort.getByAddress(endpoint));
    }

    public String getDatacenter(InetAddressAndPort endpoint)
    {
        assert mapping != null : "network topology must be assigned before using snitch";
        return mapping.localDC(endpoint);
    }

    static void assign(NetworkTopology newMapping)
    {
        mapping = new NetworkTopology(newMapping);
    }
}
