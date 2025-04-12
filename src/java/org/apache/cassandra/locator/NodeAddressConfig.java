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

public interface NodeAddressConfig
{
    void configureAddresses();
    boolean preferLocalConnections();

    NodeAddressConfig DEFAULT = new NodeAddressConfig()
    {
        @Override
        public void configureAddresses()
        {
            // Default is to use whatever addresses are specified in node config already, so this is a no-op
        }

        @Override
        public boolean preferLocalConnections()
        {
            // Previously the equivalent config was hard coded for Ec2MultiRegionSnitch and read from
            // cassandra-rackdc.properties in the case of GossipingPropertyFileSnitch. In legacy config
            // mode, where one of those IEndpointSnitch impls is still specified the original behaviour
            // is preserved. For modern config the option can be specified in the main yaml, distinct
            // from the NodeProximity and InitialLocationProvider options.
            return DatabaseDescriptor.preferLocalConnections();
        }
    };
}
