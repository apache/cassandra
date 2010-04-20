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

import java.io.FileReader;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.UnknownHostException;
import java.net.URL;
import java.util.Properties;
import java.util.StringTokenizer;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.net.InetAddress;

import org.apache.cassandra.config.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PropertyFileSnitch
 *
 * PropertyFileSnitch is used by Digg to determine if two IP's are in the same
 * datacenter or on the same rack.
 *
 */
public class PropertyFileSnitch extends RackInferringSnitch implements PropertyFileSnitchMBean {
    /**
     * A list of properties with keys being host:port and values being datacenter:rack
     */
    private Properties hostProperties = new Properties();

    /**
     * The default rack property file to be read.
     */
    private static String RACK_PROPERTY_FILENAME = "cassandra-rack.properties";

    /**
     * Reference to the logger.
     */
    private static Logger logger_ = LoggerFactory.getLogger(PropertyFileSnitch.class);

    public PropertyFileSnitch() throws ConfigurationException
    {
        reloadConfiguration();
        try
        {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            mbs.registerMBean(this, new ObjectName(MBEAN_OBJECT_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get the raw information about an end point
     * 
     * @param endpoint endpoint to process
     * 
     * @return a array of string with the first index being the data center and the second being the rack
     */
    public String[] getEndpointInfo(InetAddress endpoint) {
        String key = endpoint.toString();
        String value = hostProperties.getProperty(key);
        if (value == null)
        {
            logger_.error("Could not find end point information for {}, will use default.", key);
            value = hostProperties.getProperty("default");
        }
        StringTokenizer st = new StringTokenizer(value, ":");
        if (st.countTokens() < 2)
        {
            logger_.error("Value for " + key + " is invalid: " + value);
            return new String [] {"default", "default"};
        }
        return new String[] {st.nextToken(), st.nextToken()};
    }

    /**
     * Return the data center for which an endpoint resides in
     *  
     * @param endpoint the endpoint to process
     * @return string of data center
     */
    public String getDatacenter(InetAddress endpoint) {
        return getEndpointInfo(endpoint)[0];
    }

    /**
     * Return the rack for which an endpoint resides in
     *  
     * @param endpoint the endpoint to process
     * 
     * @return string of rack
     */
    public String getRack(InetAddress endpoint) {
        return getEndpointInfo(endpoint)[1];
    }

    public String displayConfiguration() {
        StringBuffer configurationString = new StringBuffer("Current rack configuration\n=================\n");
        for (Object key: hostProperties.keySet()) {
            String endpoint = (String) key;
            String value = hostProperties.getProperty(endpoint);
            configurationString.append(endpoint).append("=").append(value).append("\n");
        }
        return configurationString.toString();
    }

    public void reloadConfiguration() throws ConfigurationException
    {
        ClassLoader loader = PropertyFileSnitch.class.getClassLoader();
        URL scpurl = loader.getResource(RACK_PROPERTY_FILENAME);
        if (scpurl == null)
            throw new ConfigurationException("unable to locate " + RACK_PROPERTY_FILENAME);

        String rackPropertyFilename = scpurl.getFile();

        try
        {
            Properties localHostProperties = new Properties();
            localHostProperties.load(new FileReader(rackPropertyFilename));
            hostProperties = localHostProperties;
        }
        catch (IOException ioe)
        {
            throw new ConfigurationException("Could not process " + rackPropertyFilename, ioe);
        }
    }
}
