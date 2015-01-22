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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ResourceWatcher;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.commons.lang3.StringUtils;

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
 */
public class PropertyFileSnitch extends AbstractNetworkTopologySnitch
{
    private static final Logger logger = LoggerFactory.getLogger(PropertyFileSnitch.class);

    public static final String SNITCH_PROPERTIES_FILENAME = "cassandra-topology.properties";

    private static volatile Map<InetAddress, String[]> endpointMap;
    private static volatile String[] defaultDCRack;

    private volatile boolean gossipStarted;

    public PropertyFileSnitch() throws ConfigurationException
    {
        reloadConfiguration();

        try
        {
            FBUtilities.resourceToFile(SNITCH_PROPERTIES_FILENAME);
            Runnable runnable = new WrappedRunnable()
            {
                protected void runMayThrow() throws ConfigurationException
                {
                    reloadConfiguration();
                }
            };
            ResourceWatcher.watch(SNITCH_PROPERTIES_FILENAME, runnable, 60 * 1000);
        }
        catch (ConfigurationException ex)
        {
            logger.error("{} found, but does not look like a plain file. Will not watch it for changes", SNITCH_PROPERTIES_FILENAME);
        }
    }

    /**
     * Get the raw information about an end point
     *
     * @param endpoint endpoint to process
     * @return a array of string with the first index being the data center and the second being the rack
     */
    public String[] getEndpointInfo(InetAddress endpoint)
    {
        String[] rawEndpointInfo = getRawEndpointInfo(endpoint);
        if (rawEndpointInfo == null)
            throw new RuntimeException("Unknown host " + endpoint + " with no default configured");
        return rawEndpointInfo;
    }

    private String[] getRawEndpointInfo(InetAddress endpoint)
    {
        String[] value = endpointMap.get(endpoint);
        if (value == null)
        {
            logger.debug("Could not find end point information for {}, will use default", endpoint);
            return defaultDCRack;
        }
        return value;
    }

    /**
     * Return the data center for which an endpoint resides in
     *
     * @param endpoint the endpoint to process
     * @return string of data center
     */
    public String getDatacenter(InetAddress endpoint)
    {
        String[] info = getEndpointInfo(endpoint);
        assert info != null : "No location defined for endpoint " + endpoint;
        return info[0];
    }

    /**
     * Return the rack for which an endpoint resides in
     *
     * @param endpoint the endpoint to process
     * @return string of rack
     */
    public String getRack(InetAddress endpoint)
    {
        String[] info = getEndpointInfo(endpoint);
        assert info != null : "No location defined for endpoint " + endpoint;
        return info[1];
    }

    public void reloadConfiguration() throws ConfigurationException
    {
        HashMap<InetAddress, String[]> reloadedMap = new HashMap<InetAddress, String[]>();

        Properties properties = new Properties();
        InputStream stream = null;
        try
        {
            stream = getClass().getClassLoader().getResourceAsStream(SNITCH_PROPERTIES_FILENAME);
            properties.load(stream);
        }
        catch (Exception e)
        {
            throw new ConfigurationException("Unable to read " + SNITCH_PROPERTIES_FILENAME, e);
        }
        finally
        {
            FileUtils.closeQuietly(stream);
        }

        for (Map.Entry<Object, Object> entry : properties.entrySet())
        {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();

            if (key.equals("default"))
            {
                String[] newDefault = value.split(":");
                if (newDefault.length < 2)
                    defaultDCRack = new String[] { "default", "default" };
                else
                    defaultDCRack = new String[] { newDefault[0].trim(), newDefault[1].trim() };
            }
            else
            {
                InetAddress host;
                String hostString = key.replace("/", "");
                try
                {
                    host = InetAddress.getByName(hostString);
                }
                catch (UnknownHostException e)
                {
                    throw new ConfigurationException("Unknown host " + hostString, e);
                }
                String[] token = value.split(":");
                if (token.length < 2)
                    token = new String[] { "default", "default" };
                else
                    token = new String[] { token[0].trim(), token[1].trim() };
                reloadedMap.put(host, token);
            }
        }
        if (defaultDCRack == null && !reloadedMap.containsKey(FBUtilities.getBroadcastAddress()))
            throw new ConfigurationException(String.format("Snitch definitions at %s do not define a location for this node's broadcast address %s, nor does it provides a default",
                                                           SNITCH_PROPERTIES_FILENAME, FBUtilities.getBroadcastAddress()));

        if (logger.isDebugEnabled())
        {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<InetAddress, String[]> entry : reloadedMap.entrySet())
                sb.append(entry.getKey()).append(":").append(Arrays.toString(entry.getValue())).append(", ");
            logger.debug("Loaded network topology from property file: {}", StringUtils.removeEnd(sb.toString(), ", "));
        }

        endpointMap = reloadedMap;
        if (StorageService.instance != null) // null check tolerates circular dependency; see CASSANDRA-4145
            StorageService.instance.getTokenMetadata().invalidateCachedRings();

        if (gossipStarted)
            StorageService.instance.gossipSnitchInfo();
    }

    @Override
    public void gossiperStarting()
    {
        gossipStarted = true;
    }
}
