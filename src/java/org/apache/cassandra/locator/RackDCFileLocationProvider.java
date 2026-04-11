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
 * Emulates the previous behaviour of GossipingPropertyFileSnitch; reads cassandra-rackdc.properties
 * to identify the local datacenter and rack for one time use during node registration.
 *
 * Based on a properties file in the following format:
 *
 * dc=DC1
 * rack=RAC1
 */
public class RackDCFileLocationProvider implements InitialLocationProvider
{
    private final Location local;
    private static final Location DEFAULT = new Location("UNKNOWN_DC", "UNKNOWN_RACK");

    /**
     * Used via reflection by DatabaseDescriptor::createInitialLocationProvider
     */
    public RackDCFileLocationProvider()
    {
        this(loadConfiguration());
    }

    /**
     * Used in legacy compatibility mode by GossipingPropertyFileSnitch
     * @param properties
     */
    RackDCFileLocationProvider(SnitchProperties properties)
    {
        local = new Location(properties.get("dc", DEFAULT.datacenter).trim(),
                             properties.get("rack", DEFAULT.rack).trim());
    }

    @Override
    public Location initialLocation()
    {
        return local;
    }

    public static SnitchProperties loadConfiguration() throws ConfigurationException
    {
        final SnitchProperties properties = new SnitchProperties();
        if (!properties.contains("dc") || !properties.contains("rack"))
            throw new ConfigurationException("DC or rack not found in snitch properties, check your configuration in: " + SnitchProperties.RACKDC_PROPERTY_FILENAME);

        return properties;
    }


}
