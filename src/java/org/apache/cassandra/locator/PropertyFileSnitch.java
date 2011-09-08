/**
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

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ResourceWatcher;
import org.apache.cassandra.utils.WrappedRunnable;

/**
 * Used to determine if two IP's are in the same datacenter or on the same rack.
 * <p/>
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

    private static final String RACK_PROPERTY_FILENAME = "cassandra-topology.properties";

    private static volatile Map<InetAddress, String[]> endpointMap;
    private static volatile String[] defaultDCRack;

    public PropertyFileSnitch() throws ConfigurationException
    {
        reloadConfiguration();
        try
        {
            FBUtilities.resourceToFile(RACK_PROPERTY_FILENAME);
            Runnable runnable = new WrappedRunnable()
            {
                protected void runMayThrow() throws ConfigurationException
                {
                    reloadConfiguration();
                }
            };
            ResourceWatcher.watch(RACK_PROPERTY_FILENAME, runnable, 60 * 1000);
        }
        catch (ConfigurationException ex)
        {
            logger.debug(RACK_PROPERTY_FILENAME + " found, but does not look like a plain file. Will not watch it for changes");
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
        return getEndpointInfo(endpoint)[0];
    }

    /**
     * Return the rack for which an endpoint resides in
     *
     * @param endpoint the endpoint to process
     * @return string of rack
     */
    public String getRack(InetAddress endpoint)
    {
        return getEndpointInfo(endpoint)[1];
    }

    public void reloadConfiguration() throws ConfigurationException
    {
        HashMap<InetAddress, String[]> reloadedMap = new HashMap<InetAddress, String[]>();

        Properties properties = new Properties();
        InputStream stream = null;
        try
        {
            stream = getClass().getClassLoader().getResourceAsStream(RACK_PROPERTY_FILENAME);
            properties.load(stream);
        }
        catch (IOException e)
        {
            throw new ConfigurationException("Unable to read " + RACK_PROPERTY_FILENAME, e);
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
                defaultDCRack = value.split(":");
                if (defaultDCRack.length < 2)
                    defaultDCRack = new String[] { "default", "default" };
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
                reloadedMap.put(host, token);
            }
        }

        logger.debug("loaded network topology {}", FBUtilities.toString(reloadedMap));
        endpointMap = reloadedMap;
        StorageService.instance.getTokenMetadata().invalidateCaches();
    }
}
