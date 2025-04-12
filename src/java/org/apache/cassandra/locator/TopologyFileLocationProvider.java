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

import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Emulates the previous behaviour of PropertyFileSnitch; reads cassandra-topology.properties
 * to identify the local datacenter and rack for one time use during node registration.
 *
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
 */
public class TopologyFileLocationProvider implements InitialLocationProvider
{
    private static final Logger logger = LoggerFactory.getLogger(TopologyFileLocationProvider.class);

    public static final String PROPERTIES_FILENAME = "cassandra-topology.properties";
    // All the defaults
    private static final String DEFAULT_PROPERTY = "default";
    @VisibleForTesting
    public static final String DEFAULT_DC = "default";
    @VisibleForTesting
    public static final String DEFAULT_RACK = "default";

    // Used only during initialization of a new node. This provides the location it will register in cluster metadata
    private final Location local;

    public TopologyFileLocationProvider()
    {
        local = loadConfiguration();
    }

    @Override
    public Location initialLocation()
    {
        return local;
    }

    private Location makeLocation(String value)
    {
        if (value == null || value.isEmpty())
            return null;

        String[] parts = value.split(":");
        if (parts.length < 2)
        {
            return new Location(DEFAULT_DC, DEFAULT_RACK);
        }
        else
        {
            return new Location(parts[0].trim(), parts[1].trim());
        }
    }

    private Location loadConfiguration() throws ConfigurationException
    {
        Properties properties = new Properties();
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream(PROPERTIES_FILENAME))
        {
            properties.load(stream);
        }
        catch (Exception e)
        {
            throw new ConfigurationException("Unable to read " + PROPERTIES_FILENAME, e);
        }

        Location local = null;
        InetAddressAndPort broadcastAddress = FBUtilities.getBroadcastAddressAndPort();
        for (Map.Entry<Object, Object> entry : properties.entrySet())
        {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            if (DEFAULT_PROPERTY.equals(key))
                continue;

            String hostString = StringUtils.remove(key, '/');
            try
            {
                InetAddressAndPort host = InetAddressAndPort.getByName(hostString);
                if (host.equals(broadcastAddress))
                {
                    local = makeLocation(value);
                    break;
                }
            }
            catch (UnknownHostException e)
            {
                throw new ConfigurationException("Unknown host " + hostString, e);
            }

        }

        // This may be null, which is ok unless config doesn't contain the location of the local node
        Location defaultLocation = makeLocation(properties.getProperty(DEFAULT_PROPERTY));

        if (local == null)
        {
            if (defaultLocation == null)
            {
                throw new ConfigurationException(String.format("Snitch definitions at %s do not define a location for " +
                                                               "this node's broadcast address %s, nor does it provides a default",
                                                               PROPERTIES_FILENAME, broadcastAddress));
            }
            else
            {
                logger.debug("Broadcast address {} was not present in snitch config, using default location {}. " +
                             "This only matters on first boot, before registering with the cluster metadata service",
                             broadcastAddress, defaultLocation);
                local = defaultLocation;
            }
        }

        logger.debug("Loaded location {} for broadcast address {} from property file. " +
                     "This only matters on first boot, before registering with the cluster metadata service",
                     local, broadcastAddress);

        return local;
    }

}
