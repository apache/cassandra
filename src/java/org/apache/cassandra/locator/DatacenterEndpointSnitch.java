package org.apache.cassandra.locator;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import javax.xml.parsers.ParserConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.XMLUtils;
import org.xml.sax.SAXException;

/**
 * DataCenterEndpointSnitch
 * <p/>
 * This class basically reads the configuration and sets the IP Ranges to a
 * hashMap which can be read later. this class also provides a way to compare 2
 * Endpoints and also get details from the same.
 */

public class DatacenterEndpointSnitch extends AbstractRackAwareSnitch
{
    /**
     * This Map will contain the information of the Endpoints and its Location
     * (Datacenter and RAC)
     */
    private Map<Byte, Map<Byte, String>> ipDC = new HashMap<Byte, Map<Byte, String>>();
    private Map<Byte, Map<Byte, String>> ipRAC = new HashMap<Byte, Map<Byte, String>>();
    private Map<String, Integer> dcRepFactor = new HashMap<String, Integer>();

    private XMLUtils xmlUtils;
    private Map<String, Integer> quorumDCMap = new HashMap<String, Integer>();
    /**
     * The default rack property file to be read.
     */
    private static String DEFAULT_RACK_CONFIG_FILE = "/etc/cassandra/DC-Config.xml";

    /**
     * Reference to the logger.
     */
    private static Logger logger_ = LoggerFactory.getLogger(DatacenterEndpointSnitch.class);

    /**
     * Constructor, intialize XML config and read the config in...
     */
    public DatacenterEndpointSnitch() throws IOException, ParserConfigurationException, SAXException
    {
        xmlUtils = new XMLUtils(DEFAULT_RACK_CONFIG_FILE);
        reloadConfiguration();
    }

    /**
     * Return the rack for which an endpoint resides in
     */
    public String getRackForEndpoint(InetAddress endpoint)
            throws UnknownHostException
    {
        byte[] ipQuads = getIPAddress(endpoint.getHostAddress());
        return ipRAC.get(ipQuads[1]).get(ipQuads[2]);
    }

    /**
     * This method will load the configuration from the xml file. Mandatory
     * fields are Atleast 1 DC and 1RAC configurations. Name of the DC/RAC, IP
     * Quadrents for RAC and DC.
     * <p/>
     * This method will not be called everytime
     */
    public void reloadConfiguration() throws IOException
    {
        try
        {
            String[] dcNames = xmlUtils.getNodeValues("/Endpoints/DataCenter/name");
            for (String dcName : dcNames)
            {
                // Parse the Datacenter Quaderant.
                String dcXPath = "/Endpoints/DataCenter[name='" + dcName + "']";
                String dcIPQuad = xmlUtils.getNodeValue(dcXPath + "/ip2ndQuad");
                String replicationFactor = xmlUtils.getNodeValue(dcXPath + "/replicationFactor");
                byte dcByte = intToByte(Integer.parseInt(dcIPQuad));
                // Parse the replication factor for a DC
                int dcReF = Integer.parseInt(replicationFactor);
                dcRepFactor.put(dcName, dcReF);
                quorumDCMap.put(dcName, (dcReF / 2 + 1));
                String[] racNames = xmlUtils.getNodeValues(dcXPath + "/rack/name");
                Map<Byte, String> dcRackMap = ipDC.get(dcByte);
                if (null == dcRackMap)
                {
                    dcRackMap = new HashMap<Byte, String>();
                }
                Map<Byte, String> rackDcMap = ipRAC.get(dcByte);
                if (null == rackDcMap)
                {
                    rackDcMap = new HashMap<Byte, String>();
                }
                for (String racName : racNames)
                {
                    // Parse the RAC ip Quaderant.
                    String racIPQuad = xmlUtils.getNodeValue(dcXPath + "/rack[name = '" + racName + "']/ip3rdQuad");
                    byte racByte = intToByte(Integer.parseInt(racIPQuad));
                    dcRackMap.put(racByte, dcName);
                    rackDcMap.put(racByte, racName);
                }
                ipDC.put(dcByte, dcRackMap);
                ipRAC.put(dcByte, rackDcMap);
            }
        }
        catch (Exception ioe)
        {
            throw new IOException("Could not process " + DEFAULT_RACK_CONFIG_FILE, ioe);
        }
    }

    /**
     * This methood will return ture if the hosts are in the same RAC else
     * false.
     */
    public boolean isOnSameRack(InetAddress host, InetAddress host2)
            throws UnknownHostException
    {
        /*
        * Look at the IP Address of the two hosts. Compare the 2nd and 3rd
        * octet. If they are the same then the hosts are in the same rack else
        * different racks.
        */
        byte[] ip = getIPAddress(host.getHostAddress());
        byte[] ip2 = getIPAddress(host2.getHostAddress());

        return ipRAC.get(ip[1]).get(ip[2])
                .equals(ipRAC.get(ip2[1]).get(ip2[2]));
    }

    /**
     * This methood will return ture if the hosts are in the same DC else false.
     */
    public boolean isInSameDataCenter(InetAddress host, InetAddress host2)
            throws UnknownHostException
    {
        /*
        * Look at the IP Address of the two hosts. Compare the 2nd and 3rd
        * octet and get the DC Name. If they are the same then the hosts are in
        * the same datacenter else different datacenter.
        */
        byte[] ip = getIPAddress(host.getHostAddress());
        byte[] ip2 = getIPAddress(host2.getHostAddress());

        return ipDC.get(ip[1]).get(ip[2]).equals(ipDC.get(ip2[1]).get(ip2[2]));
    }

    /**
     * Returns a DC replication map, the key will be the dc name and the value
     * will be the replication factor of that Datacenter.
     */
    public HashMap<String, Integer> getMapReplicationFactor()
    {
        return new HashMap<String, Integer>(dcRepFactor);
    }

    /**
     * Returns a DC replication map, the key will be the dc name and the value
     * will be the replication factor of that Datacenter.
     */
    public HashMap<String, Integer> getMapQuorumFactor()
    {
        return new HashMap<String, Integer>(quorumDCMap);
    }

    private byte[] getIPAddress(String host) throws UnknownHostException
    {
        InetAddress ia = InetAddress.getByName(host);
        return ia.getAddress();
    }

    public static byte intToByte(int n)
    {
        return (byte) (n & 0x000000ff);
    }

    public String getLocation(InetAddress endpoint) throws UnknownHostException
    {
        byte[] ipQuads = getIPAddress(endpoint.getHostAddress());
        return ipDC.get(ipQuads[1]).get(ipQuads[2]);
    }
}
