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
import java.net.InetAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A snitch that assumes an GCE region is a DC and an GCE availability_zone
 *  is a rack. This information is available in the config for the node.
 */
public class GoogleCloudSnitch extends AbstractNetworkTopologySnitch
{
    protected static final Logger logger = LoggerFactory.getLogger(GoogleCloudSnitch.class);
    protected static final String ZONE_NAME_QUERY_URL = "http://metadata.google.internal/computeMetadata/v1/instance/zone";
    private static final String DEFAULT_DC = "UNKNOWN-DC";
    private static final String DEFAULT_RACK = "UNKNOWN-RACK";
    private Map<InetAddress, Map<String, String>> savedEndpoints;
    protected String gceZone;
    protected String gceRegion;

    public GoogleCloudSnitch() throws IOException, ConfigurationException
    {
        String response = gceApiCall(ZONE_NAME_QUERY_URL);
        String[] splits = response.split("/");
        String az = splits[splits.length - 1];

        // Split "us-central1-a" or "asia-east1-a" into "us-central1"/"a" and "asia-east1"/"a".
        splits = az.split("-");
        gceZone = splits[splits.length - 1];

        int lastRegionIndex = az.lastIndexOf("-");
        gceRegion = az.substring(0, lastRegionIndex);

        String datacenterSuffix = (new SnitchProperties()).get("dc_suffix", "");
        gceRegion = gceRegion.concat(datacenterSuffix);
        logger.info("GCESnitch using region: {}, zone: {}.", gceRegion, gceZone);
    }

    String gceApiCall(String url) throws IOException, ConfigurationException
    {
        // Populate the region and zone by introspection, fail if 404 on metadata
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        DataInputStream d = null;
        try
        {
            conn.setRequestMethod("GET");
	    conn.setRequestProperty("Metadata-Flavor", "Google");
            if (conn.getResponseCode() != 200)
                throw new ConfigurationException("GoogleCloudSnitch was unable to execute the API call. Not a gce node?");

            // Read the information.
            int cl = conn.getContentLength();
            byte[] b = new byte[cl];
            d = new DataInputStream((FilterInputStream) conn.getContent());
            d.readFully(b);
            return new String(b, StandardCharsets.UTF_8);
        }
        finally
        {
            FileUtils.close(d);
            conn.disconnect();
        }
    }

    public String getRack(InetAddress endpoint)
    {
        if (endpoint.equals(FBUtilities.getBroadcastAddress()))
            return gceZone;
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
            return gceRegion;
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
