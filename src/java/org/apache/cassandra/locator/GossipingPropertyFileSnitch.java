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

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.InetAddress;
import java.util.Map;
import java.util.Properties;

public class GossipingPropertyFileSnitch extends AbstractNetworkTopologySnitch
{
    private static final Logger logger = LoggerFactory.getLogger(GossipingPropertyFileSnitch.class);

    public static final String RACKDC_PROPERTY_FILENAME = "cassandra-rackdc.properties";
    private PropertyFileSnitch psnitch;
    private String myDC;
    private String myRack;

    public GossipingPropertyFileSnitch() throws ConfigurationException
    {
        try
        {
            loadConfiguration();
        }
        catch (ConfigurationException e)
        {
            throw new RuntimeException("Unable to load " + RACKDC_PROPERTY_FILENAME + " : ", e);
        }
        try
        {
            psnitch = new PropertyFileSnitch();
            logger.info("Loaded " + PropertyFileSnitch.RACK_PROPERTY_FILENAME + " for compatibility");
        }
        catch (ConfigurationException e)
        {
            logger.info("Unable to load " + PropertyFileSnitch.RACK_PROPERTY_FILENAME + "; compatibility mode disabled");
        }
    }

    private void loadConfiguration() throws ConfigurationException
    {
        InputStream stream = GossipingPropertyFileSnitch.class.getClassLoader().getResourceAsStream(RACKDC_PROPERTY_FILENAME);
        Properties properties = new Properties();
        try
        {
            properties.load(stream);
        }
        catch (Exception e)
        {
            throw new ConfigurationException("Unable to read " + RACKDC_PROPERTY_FILENAME, e);
        }
        finally
        {
            FileUtils.closeQuietly(stream);
        }
        for (Map.Entry<Object, Object> entry : properties.entrySet())
        {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            if (key.equals("dc"))
                myDC = value;
            else if (key.equals("rack"))
                myRack = value;
        }
        if (myDC == null || myRack == null)
            throw new ConfigurationException("DC or rack not found in " + RACKDC_PROPERTY_FILENAME);
    }

    /**
     * Return the data center for which an endpoint resides in
     *
     * @param endpoint the endpoint to process
     * @return string of data center
     */
    public String getDatacenter(InetAddress endpoint)
    {
        EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        if (epState == null || epState.getApplicationState(ApplicationState.DC) == null)
        {
            if (psnitch == null)
                throw new RuntimeException("Could not retrieve DC for " + endpoint + " from gossip and PFS compatibility is disabled");
            else
                return psnitch.getDatacenter(endpoint);
        }
        return epState.getApplicationState(ApplicationState.DC).value;
    }

    /**
     * Return the rack for which an endpoint resides in
     *
     * @param endpoint the endpoint to process
     * @return string of rack
     */
    public String getRack(InetAddress endpoint)
    {
        EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        if (epState == null || epState.getApplicationState(ApplicationState.RACK) == null)
        {
            if (psnitch == null)
                throw new RuntimeException("Could not retrieve rack for " + endpoint + " from gossip and PFS compatibility is disabled");
            else
                return psnitch.getRack(endpoint);
        }
        return epState.getApplicationState(ApplicationState.RACK).value;
    }

    @Override
    public void gossiperStarting()
    {
        // Share info via gossip.
        logger.info("Adding ApplicationState DC=" + myDC + " rack=" + myRack);
        Gossiper.instance.addLocalApplicationState(ApplicationState.DC, StorageService.instance.valueFactory.datacenter(myDC));
        Gossiper.instance.addLocalApplicationState(ApplicationState.RACK, StorageService.instance.valueFactory.rack(myRack));
    }
}
