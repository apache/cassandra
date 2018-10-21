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
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
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
 * A snitch that assumes an EC2 region is a DC and an EC2 availability_zone
 *  is a rack. This information is available in the config for the node.
 */
public class Ec2Snitch extends AbstractNetworkTopologySnitch
{
    protected static final Logger logger = LoggerFactory.getLogger(Ec2Snitch.class);

    private static final String SNITCH_PROP_NAMING_SCHEME = "ec2_naming_scheme";
    static final String EC2_NAMING_LEGACY = "legacy";
    private static final String EC2_NAMING_STANDARD = "standard";

    private static final String ZONE_NAME_QUERY_URL = "http://169.254.169.254/latest/meta-data/placement/availability-zone";
    private static final String DEFAULT_DC = "UNKNOWN-DC";
    private static final String DEFAULT_RACK = "UNKNOWN-RACK";

    final String ec2region;
    private final String ec2zone;
    private final boolean usingLegacyNaming;

    private Map<InetAddressAndPort, Map<String, String>> savedEndpoints;

    public Ec2Snitch() throws IOException, ConfigurationException
    {
        this(new SnitchProperties());
    }

    public Ec2Snitch(SnitchProperties props) throws IOException, ConfigurationException
    {
        String az = awsApiCall(ZONE_NAME_QUERY_URL);

        // if using the full naming scheme, region name is created by removing letters from the
        // end of the availability zone and zone is the full zone name
        usingLegacyNaming = isUsingLegacyNaming(props);
        String region;
        if (usingLegacyNaming)
        {
            // Split "us-east-1a" or "asia-1a" into "us-east"/"1a" and "asia"/"1a".
            String[] splits = az.split("-");
            ec2zone = splits[splits.length - 1];

            // hack for CASSANDRA-4026
            region = az.substring(0, az.length() - 1);
            if (region.endsWith("1"))
                region = az.substring(0, az.length() - 3);
        }
        else
        {
            // grab the region name, which is embedded in the availability zone name.
            // thus an AZ of "us-east-1a" yields the region name "us-east-1"
            region = az.replaceFirst("[a-z]+$","");
            ec2zone = az;
        }

        String datacenterSuffix = props.get("dc_suffix", "");
        ec2region = region.concat(datacenterSuffix);
        logger.info("EC2Snitch using region: {}, zone: {}.", ec2region, ec2zone);
    }

    private static boolean isUsingLegacyNaming(SnitchProperties props)
    {
        return props.get(SNITCH_PROP_NAMING_SCHEME, EC2_NAMING_STANDARD).equalsIgnoreCase(EC2_NAMING_LEGACY);
    }

    String awsApiCall(String url) throws IOException, ConfigurationException
    {
        // Populate the region and zone by introspection, fail if 404 on metadata
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        DataInputStream d = null;
        try
        {
            conn.setRequestMethod("GET");
            if (conn.getResponseCode() != 200)
                throw new ConfigurationException("Ec2Snitch was unable to execute the API call. Not an ec2 node?");

            // Read the information. I wish I could say (String) conn.getContent() here...
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

    public String getRack(InetAddressAndPort endpoint)
    {
        if (endpoint.equals(FBUtilities.getBroadcastAddressAndPort()))
            return ec2zone;
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

    public String getDatacenter(InetAddressAndPort endpoint)
    {
        if (endpoint.equals(FBUtilities.getBroadcastAddressAndPort()))
            return ec2region;
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

    @Override
    public boolean validate(Set<String> datacenters, Set<String> racks)
    {
        return validate(datacenters, racks, usingLegacyNaming);
    }

    @VisibleForTesting
    static boolean validate(Set<String> datacenters, Set<String> racks, boolean usingLegacyNaming)
    {
        boolean valid = true;

        for (String dc : datacenters)
        {
            // predicated on the late-2017 AWS naming 'convention' that all region names end with a digit.
            // Unfortunately, life isn't that simple. Since we allow custom datacenter suffixes (CASSANDRA-5155),
            // an operator could conceiveably make the suffix "a", and thus create a region name that looks just like
            // one of the region's availability zones. (for example, "us-east-1a").
            // Further, we can't make any assumptions of what that suffix might be by looking at this node's
            // datacenterSuffix as conceivably their could be many different suffixes in play for a given region.
            //
            // Thus, the best we can do is make sure the region name follows
            // the basic region naming pattern: "us-east-1<custom-suffix>"
            boolean dcUsesLegacyFormat = !dc.matches("[a-z]+-[a-z].+-[\\d].*");
            if (dcUsesLegacyFormat != usingLegacyNaming)
                valid = false;
        }

        for (String rack : racks)
        {
            // predicated on late-2017 AWS naming 'convention' that AZs do not have a digit as the first char -
            // we had that in our legacy AZ (rack) names. Thus we test to see if the rack is in the legacy format.
            //
            // NOTE: the allowed custom suffix only applies to datacenter (region) names, not availability zones.
            boolean rackUsesLegacyFormat = rack.matches("[\\d][a-z]");
            if (rackUsesLegacyFormat != usingLegacyNaming)
                valid = false;
        }

        if (!valid)
        {
            logger.error("This ec2-enabled snitch appears to be using the {} naming scheme for regions, " +
                         "but existing nodes in cluster are using the opposite: region(s) = {}, availability zone(s) = {}. " +
                         "Please check the {} property in the {} configuration file for more details.",
                         usingLegacyNaming ? "legacy" : "standard", datacenters, racks,
                         SNITCH_PROP_NAMING_SCHEME, SnitchProperties.RACKDC_PROPERTY_FILENAME);
        }

        return valid;
    }
}
