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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.log4j.Logger;

/**
 * PropertyFileEndPointSnitch
 * <p/>
 * PropertyFileEndPointSnitch is used by Digg to determine if two IP's are in the same
 * datacenter or on the same rack.
 */
public class PropertyFileEndPointSnitch extends EndPointSnitch implements PropertyFileEndPointSnitchMBean
{
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
    private static Map<InetAddress, String[]> endpointMap = new HashMap<InetAddress, String[]>();
    private static String[] defaultDCRack;

    public PropertyFileEndPointSnitch() throws IOException
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
     * @param endPoint endPoint to process
     * @return a array of string with the first index being the data center and the second being the rack
     */
    public String[] getEndPointInfo(InetAddress endPoint)
    {
        String[] value = endpointMap.get(endPoint);
        if (value == null)
        {
            if (logger_.isDebugEnabled())
                logger_.debug("Could not find end point information for " + endPoint + ", will use default.");
            return defaultDCRack;
        }
        return value;
    }

    /**
     * Return the data center for which an endpoint resides in
     *
     * @param endPoint the endPoint to process
     * @return string of data center
     */
    public String getDataCenterForEndPoint(InetAddress endPoint)
    {
        return getEndPointInfo(endPoint)[0];
    }

    public String getLocation(InetAddress endPoint)
    {
        return getEndPointInfo(endPoint)[0];
    }

    /**
     * Return the rack for which an endpoint resides in
     *
     * @param endPoint the endPoint to process
     * @return string of rack
     */
    public String getRackForEndPoint(InetAddress endPoint)
    {
        return getEndPointInfo(endPoint)[1];
    }

    @Override
    public boolean isInSameDataCenter(InetAddress host, InetAddress host2) throws UnknownHostException
    {
        if (runInBaseMode)
            return super.isInSameDataCenter(host, host2);
        return getDataCenterForEndPoint(host).equals(getDataCenterForEndPoint(host2));
    }

    @Override
    public boolean isOnSameRack(InetAddress host, InetAddress host2) throws UnknownHostException
    {
        if (runInBaseMode)
            return super.isOnSameRack(host, host2);
        if (!isInSameDataCenter(host, host2))
            return false;
        return getRackForEndPoint(host).equals(getRackForEndPoint(host2));
    }

    public String displayConfiguration()
    {
        StringBuffer configurationString = new StringBuffer("Current rack configuration\n=================\n");
        for (Map.Entry<InetAddress, String[]> entry : endpointMap.entrySet())
        {
            String[] dcRack = entry.getValue();
            configurationString.append(String.format("%s=%s:%s\n", entry.getKey(), dcRack[0], dcRack[1]));
        }
        return configurationString.toString();
    }

    public void reloadConfiguration() throws IOException
    {
        String rackPropertyFilename = System.getProperty("rackFile", DEFAULT_RACK_PROPERTY_FILE);
        endpointMap.clear();
        try
        {
            Properties properties = new Properties();
            properties.load(new FileReader(rackPropertyFilename));
            for (Map.Entry<Object, Object> entry : properties.entrySet())
            {
                String key = (String) entry.getKey();
                String value = (String) entry.getValue();

                if (key.equals("default"))
                {
                    defaultDCRack = value.split(":");
                    if (defaultDCRack.length < 2)
                        defaultDCRack = new String[]{ "default", "default" };
                }
                else
                {
                    InetAddress host = InetAddress.getByName(key.replace("/", ""));
                    String[] token = value.split(":");
                    if (token.length < 2)
                        token = new String[]{ "default", "default" };
                    endpointMap.put(host, token);
                }
            }
            runInBaseMode = false;
        }
        catch (FileNotFoundException fnfe)
        {
            logger_.error("Could not find " + rackPropertyFilename + ", using default EndPointSnitch", fnfe);
            runInBaseMode = true;
        }
        catch (IOException ioe)
        {
            logger_.error("Could not process " + rackPropertyFilename, ioe);
            throw ioe;
        }
    }
}
