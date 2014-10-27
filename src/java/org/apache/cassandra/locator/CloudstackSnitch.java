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

import java.io.DataInputStream;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.File;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * A snitch that assumes a Cloudstack Zone follows the typical convention
 * <country>-<location>-<availability zone> and uses the country/location
 * tuple as a datacenter and the availability zone as a rack
 */

public class CloudstackSnitch extends AbstractNetworkTopologySnitch
{
    protected static final Logger logger = LoggerFactory.getLogger(CloudstackSnitch.class);
    protected static final String ZONE_NAME_QUERY_URI = "/latest/meta-data/availability-zone";

    private Map<InetAddress, Map<String, String>> savedEndpoints;

    private static final String DEFAULT_DC = "UNKNOWN-DC";
    private static final String DEFAULT_RACK = "UNKNOWN-RACK";
    private static final String[] LEASE_FILES = 
    {
        "file:///var/lib/dhcp/dhclient.eth0.leases",
        "file:///var/lib/dhclient/dhclient.eth0.leases"
    };

    protected String csZoneDc;
    protected String csZoneRack;

    public CloudstackSnitch() throws IOException, ConfigurationException
    {
        String endpoint = csMetadataEndpoint();
        String zone = csQueryMetadata(endpoint + ZONE_NAME_QUERY_URI);
        String zone_parts[] = zone.split("-");

        if (zone_parts.length != 3) 
        {
            throw new ConfigurationException("CloudstackSnitch cannot handle invalid zone format: " + zone);
        }
        csZoneDc = zone_parts[0] + "-" + zone_parts[1];
        csZoneRack = zone_parts[2];
    }

    public String getRack(InetAddress endpoint)
    {
        if (endpoint.equals(FBUtilities.getBroadcastAddress()))
            return csZoneRack;
        EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        if (state == null || state.getApplicationState(ApplicationState.RACK) == null) 
        {
            if (savedEndpoints == null)
                savedEndpoints = SystemKeyspace.loadDcRackInfo();
            if (savedEndpoints.containsKey(endpoint))
                return savedEndpoints.get(endpoint).get("rack");
            return DEFAULT_RACK;
        }
        return state.getApplicationState(ApplicationState.RACK).value;
    }

    public String getDatacenter(InetAddress endpoint)
    {
        if (endpoint.equals(FBUtilities.getBroadcastAddress()))
            return csZoneDc;
        EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        if (state == null || state.getApplicationState(ApplicationState.DC) == null) 
        {
            if (savedEndpoints == null)
                savedEndpoints = SystemKeyspace.loadDcRackInfo();
            if (savedEndpoints.containsKey(endpoint))
                return savedEndpoints.get(endpoint).get("data_center");
            return DEFAULT_DC;
        }
        return state.getApplicationState(ApplicationState.DC).value;
    }

    String csQueryMetadata(String url) throws ConfigurationException, IOException
    {
        HttpURLConnection conn = null;
        DataInputStream is = null;

        try 
        {
            conn = (HttpURLConnection) new URL(url).openConnection();
        } 
        catch (Exception e) 
        {
            throw new ConfigurationException("CloudstackSnitch cannot query wrong metadata URL: " + url);
        }
        try 
        {
            conn.setRequestMethod("GET");
            if (conn.getResponseCode() != 200) 
            {
                throw new ConfigurationException("CloudstackSnitch was unable to query metadata.");
            }

            int cl = conn.getContentLength();
            byte[] b = new byte[cl];
            is = new DataInputStream(new BufferedInputStream(conn.getInputStream()));
            is.readFully(b);
            return new String(b, StandardCharsets.UTF_8);
        } 
        finally 
        {
            FileUtils.close(is);
            conn.disconnect();
        }
    }

    String csMetadataEndpoint() throws ConfigurationException
    {
        for (String lease_uri: LEASE_FILES) 
        {
            try 
            {
                File lease_file = new File(new URI(lease_uri));
                if (lease_file.exists()) 
                {
                    return csEndpointFromLease(lease_file);
                }
            } 
            catch (Exception e) 
            {
                JVMStabilityInspector.inspectThrowable(e);
                continue;
            }

        }

        throw new ConfigurationException("No valid DHCP lease file could be found.");
    }

    String csEndpointFromLease(File lease) throws ConfigurationException, IOException
    {
        BufferedReader reader = null;

        String line = null;
        String endpoint = null;
        Pattern identifierPattern = Pattern.compile("^[ \t]*option dhcp-server-identifier (.*);$");

        try 
        {
            reader = new BufferedReader(new FileReader(lease));
            
            while ((line = reader.readLine()) != null) 
            {
                Matcher matcher = identifierPattern.matcher(line);

                if (matcher.find()) 
                {
                    endpoint = matcher.group(1);
                    break;
                }
            }
        } 
        catch (Exception e)  
        {
            throw new ConfigurationException("CloudstackSnitch cannot access lease file.");
        } 
        finally
        {
        	FileUtils.closeQuietly(reader);
        }

        if (endpoint == null) 
        {
            throw new ConfigurationException("No metadata server could be found in lease file.");
        }

        return "http://" + endpoint;
    }
}
