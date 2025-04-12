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

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.tcm.membership.Location;

/**
 * <p>
 * Used to determine if two IP's are in the same datacenter or on the same rack.
 * </p>
 * Based on a properties file in the following format:
 *
 * 10.0.0.13=DC1:RAC2
 * 10.21.119.14=DC3:RAC2
 * 10.20.114.15=DC2:RAC2
 * default=DC1:r1
 *
 * Post CEP-21, only the local rack and DC are loaded from file. Each peer in the cluster is required to register
 * itself with the Cluster Metadata Service and provide its Location (Rack + DC) before joining. During upgrades,
 * this is done automatically with location derived from gossip state (ultimately from system.local).
 * Once registered, the Rack & DC should not be changed but currently the only safeguards against this are the
 * StartupChecks which validate the snitch against system.local.
 * @deprecated See CASSANDRA-19488
 */
@Deprecated(since = "CEP-21")
public class PropertyFileSnitch extends AbstractNetworkTopologySnitch
{
    // Used only during initialization of a new node. This provides the location it will register in cluster metadata
    private final Location local;

    public PropertyFileSnitch() throws ConfigurationException
    {
        local = new TopologyFileLocationProvider().initialLocation();
    }

    @Override
    public String getLocalRack()
    {
        return local.rack;
    }

    @Override
    public String getLocalDatacenter()
    {
        return local.datacenter;
    }
}
