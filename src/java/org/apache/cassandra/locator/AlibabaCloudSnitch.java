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
import java.io.FilterInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  A snitch that assumes an ECS region is a DC and an ECS availability_zone
 *  is a rack. This information is available in the config for the node. the 
 *  format of the zone-id is like :cn-hangzhou-a where cn means china, hangzhou
 *  means the hangzhou region, a means the az id. We use cn-hangzhou as the dc,
 *  and f as the zone-id.
 */
public class AlibabaCloudSnitch extends AbstractNetworkTopologySnitch
{
    protected static final Logger logger = LoggerFactory.getLogger(AlibabaCloudSnitch.class);
    protected static final String ZONE_NAME_QUERY_URL = "http://100.100.100.200/latest/meta-data/zone-id";
    private static final String DEFAULT_DC = "UNKNOWN-DC";
    private static final String DEFAULT_RACK = "UNKNOWN-RACK";
    private Map<InetAddressAndPort, Map<String, String>> savedEndpoints; 
    protected String ecsZone;
    protected String ecsRegion;
    
    private static final int HTTP_CONNECT_TIMEOUT = 30000;
    
    
    public AlibabaCloudSnitch() throws MalformedURLException, IOException 
    {
        String response = alibabaApiCall(ZONE_NAME_QUERY_URL);
        String[] splits = response.split("/");
        String az = splits[splits.length - 1];

        // Split "us-central1-a" or "asia-east1-a" into "us-central1"/"a" and "asia-east1"/"a".
        splits = az.split("-");
        ecsZone = splits[splits.length - 1];

        int lastRegionIndex = az.lastIndexOf("-");
        ecsRegion = az.substring(0, lastRegionIndex);

        String datacenterSuffix = (new SnitchProperties()).get("dc_suffix", "");
        ecsRegion = ecsRegion.concat(datacenterSuffix);
        logger.info("AlibabaSnitch using region: {}, zone: {}.", ecsRegion, ecsZone);
    
    }
    
    String alibabaApiCall(String url) throws ConfigurationException, IOException, SocketTimeoutException
    {
        // Populate the region and zone by introspection, fail if 404 on metadata
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        DataInputStream d = null;
        try
        {
            conn.setConnectTimeout(HTTP_CONNECT_TIMEOUT);
            conn.setRequestMethod("GET");
            
            int code = conn.getResponseCode();
            if (code != HttpURLConnection.HTTP_OK)
                throw new ConfigurationException("AlibabaSnitch was unable to execute the API call. Not an ecs node? and the returun code is " + code);

            // Read the information. I wish I could say (String) conn.getContent() here...
            int cl = conn.getContentLength();
            byte[] b = new byte[cl];
            d = new DataInputStream((FilterInputStream) conn.getContent());
            d.readFully(b);
            return new String(b, StandardCharsets.UTF_8);
        }
        catch (SocketTimeoutException e)
        {
            throw new SocketTimeoutException("Timeout occurred reading a response from the Alibaba ECS metadata");
        }
        finally
        {
            FileUtils.close(d);
            conn.disconnect();
        }
    }
    
    @Override
    public String getRack(InetAddressAndPort endpoint)
    {
        if (endpoint.equals(FBUtilities.getBroadcastAddressAndPort()))
            return ecsZone;
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

    @Override
    public String getDatacenter(InetAddressAndPort endpoint) 
    {
        if (endpoint.equals(FBUtilities.getBroadcastAddressAndPort()))
            return ecsRegion;
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

}
