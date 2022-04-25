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

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A snitch that assumes an EC2 region is a DC and an EC2 availability_zone
 * is a rack. This information is available in the config for the node.

 * Since CASSANDRA-16555, it is possible to choose version of AWS IMDS.
 *
 * By default, since CASSANDRA-16555, IMDSv2 is used.
 *
 * The version of IMDS is driven by property {@link Ec2MetadataServiceConnector#EC2_METADATA_TYPE_PROPERTY} and
 * can be of value either 'v1' or 'v2'.
 *
 * It is possible to specify custom URL of IMDS by {@link Ec2MetadataServiceConnector#EC2_METADATA_URL_PROPERTY}.
 * A user is not meant to change this under normal circumstances, it is suitable for testing only.
 *
 * IMDSv2 is secured by a token which needs to be fetched from IDMSv2 first, and it has to be passed in a header
 * for the actual queries to IDMSv2. Ec2Snitch is doing this automatically. The only configuration parameter exposed
 * to a user is {@link Ec2MetadataServiceConnector.V2Connector#AWS_EC2_METADATA_TOKEN_TTL_SECONDS_HEADER_PROPERTY}
 * which is by default set to {@link Ec2MetadataServiceConnector.V2Connector#MAX_TOKEN_TIME_IN_SECONDS}. TTL has
 * to be an integer from the range [30, 21600].
 */
public class Ec2Snitch extends AbstractNetworkTopologySnitch
{
    protected static final Logger logger = LoggerFactory.getLogger(Ec2Snitch.class);
    @VisibleForTesting
    public static final String ZONE_NAME_QUERY = "/latest/meta-data/placement/availability-zone";
    private static final String DEFAULT_DC = "UNKNOWN-DC";
    private static final String DEFAULT_RACK = "UNKNOWN-RACK";
    private Map<InetAddress, Map<String, String>> savedEndpoints;
    protected String ec2zone;
    protected String ec2region;

    protected final Ec2MetadataServiceConnector connector;

    public Ec2Snitch() throws IOException, ConfigurationException
    {
        this(new SnitchProperties());
    }

    public Ec2Snitch(SnitchProperties props) throws IOException, ConfigurationException
    {
        this(props, Ec2MetadataServiceConnector.create(props));
    }

    @VisibleForTesting
    public Ec2Snitch(SnitchProperties props, Ec2MetadataServiceConnector connector) throws IOException
    {
        this.connector = connector;
        String az = connector.apiCall(ZONE_NAME_QUERY);
        // Split "us-east-1a" or "asia-1a" into "us-east"/"1a" and "asia"/"1a".
        String[] splits = az.split("-");
        ec2zone = splits[splits.length - 1];

        // hack for CASSANDRA-4026
        ec2region = az.substring(0, az.length() - 1);
        if (ec2region.endsWith("1"))
            ec2region = az.substring(0, az.length() - 3);

        String datacenterSuffix = (new SnitchProperties()).get("dc_suffix", "");
        ec2region = ec2region.concat(datacenterSuffix);
        logger.info("EC2Snitch using region: {}, zone: {}.", ec2region, ec2zone);
    }

    public String getRack(InetAddress endpoint)
    {
        if (endpoint.equals(FBUtilities.getBroadcastAddress()))
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

    public String getDatacenter(InetAddress endpoint)
    {
        if (endpoint.equals(FBUtilities.getBroadcastAddress()))
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
}
