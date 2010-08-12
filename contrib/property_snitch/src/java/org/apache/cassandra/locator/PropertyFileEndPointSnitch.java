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

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.StringTokenizer;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.cassandra.locator.EndPointSnitch;
import java.net.InetAddress;
import org.apache.log4j.Logger;

/**
 * PropertyFileEndPointSnitch
 * 
 * PropertyFileEndPointSnitch is used by Digg to determine if two IP's are in the same
 * datacenter or on the same rack.
 * 
 * @author Sammy Yu <syu@sammyyu.net>
 * 
 */
public class PropertyFileEndPointSnitch extends EndPointSnitch implements PropertyFileEndPointSnitchMBean {
    /**
     * A list of properties with keys being host:port and values being datacenter:rack
     */
    private Properties hostProperties = new Properties();
    
    /**
     * The default rack property file to be read.
     */
    private static String DEFAULT_RACK_PROPERTY_FILE = "/etc/cassandra/rack.properties"; 

    /**
     * Whether to use the parent for detection of same node
     */
    private boolean runInBaseMode = false;
    
    /**
     * Reference to the logger.
     */
    private static Logger logger_ = Logger.getLogger(PropertyFileEndPointSnitch.class);     

    public PropertyFileEndPointSnitch() throws IOException {
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
     * @param endPoint endPoint to process
     * 
     * @return a array of string with the first index being the data center and the second being the rack
     */
    public String[] getEndPointInfo(InetAddress endPoint) {
        String key = endPoint.toString();
        String value = hostProperties.getProperty(key);
        if (value == null)
        {
            logger_.error("Could not find end point information for " + key + ", will use default.");
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
     * @param endPoint the endPoint to process
     * @return string of data center
     */
    public String getDataCenterForEndPoint(InetAddress endPoint) {
        return getEndPointInfo(endPoint)[0];
    }

    public String getLocation(InetAddress endPoint) {
        return getEndPointInfo(endPoint)[0];
    }
    /**
     * Return the rack for which an endpoint resides in
     *  
     * @param endPoint the endPoint to process
     * 
     * @return string of rack
     */
    public String getRackForEndPoint(InetAddress endPoint) {
        return getEndPointInfo(endPoint)[1];
    }

    @Override
    public boolean isInSameDataCenter(InetAddress host, InetAddress host2)
            throws UnknownHostException {
        if (runInBaseMode) 
        {
            return super.isInSameDataCenter(host, host2);
        }
        return getDataCenterForEndPoint(host).equals(getDataCenterForEndPoint(host2));
    }

    @Override
    public boolean isOnSameRack(InetAddress host, InetAddress host2)
            throws UnknownHostException {
        if (runInBaseMode) 
        {
            return super.isOnSameRack(host, host2);
        }
        if (!isInSameDataCenter(host, host2)) 
        {
            return false;
        }
        return getRackForEndPoint(host).equals(getRackForEndPoint(host2)); 
    }

    @Override
    public String displayConfiguration() {
        StringBuffer configurationString = new StringBuffer("Current rack configuration\n=================\n");
        for (Object key: hostProperties.keySet()) {
            String endpoint = (String) key;
            String value = hostProperties.getProperty(endpoint);
            configurationString.append(endpoint + "=" + value + "\n");
        }
        return configurationString.toString();
    }
    
    @Override
    public void reloadConfiguration() throws IOException {        
        String rackPropertyFilename = System.getProperty("rackFile", DEFAULT_RACK_PROPERTY_FILE);
        try 
        {
            Properties localHostProperties = new Properties();
            localHostProperties.load(new FileReader(rackPropertyFilename));
            hostProperties = localHostProperties;
            runInBaseMode = false;
        }
        catch (FileNotFoundException fnfe) {
            logger_.error("Could not find " + rackPropertyFilename + ", using default EndPointSnitch", fnfe);
            runInBaseMode = true;
        }
        catch (IOException ioe) {
            logger_.error("Could not process " + rackPropertyFilename, ioe);
            throw ioe;
        }
    }

}
