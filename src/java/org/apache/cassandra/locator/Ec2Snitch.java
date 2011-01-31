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

import java.io.DataInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A snitch that assumes an EC2 region is a DC and an EC2 availability_zone
 *  is a rack. This information is available in the config for the node.
 */
public class Ec2Snitch extends AbstractNetworkTopologySnitch
{
    protected static Logger logger = LoggerFactory.getLogger(Ec2Snitch.class);
    protected String ec2zone;
    protected String ec2region;

    public Ec2Snitch() throws IOException, ConfigurationException
    {
        // Populate the region and zone by introspection, fail if 404 on metadata
        HttpURLConnection conn = (HttpURLConnection) new URL("http://169.254.169.254/latest/meta-data/placement/availability-zone").openConnection();
        conn.setRequestMethod("GET");
        if (conn.getResponseCode() != 200)
        {
            throw new ConfigurationException("Ec2Snitch was unable to find region/zone data. Not an ec2 node?");
        }

        // Read the information. I wish I could say (String) conn.getContent() here...
        int cl = conn.getContentLength();
        byte[] b = new byte[cl];
        DataInputStream d = new DataInputStream((FilterInputStream)conn.getContent());
        d.readFully(b);

        // Split "us-east-1a" or "asia-1a" into "us-east"/"1a" and "asia"/"1a".
        String azone = new String(b ,"UTF-8");
        String[] splits = azone.split("-");
        ec2zone = splits[splits.length - 1];
        ec2region = splits.length < 3 ? splits[0] : splits[0]+"-"+splits[1];
        logger.info("EC2Snitch using region: " + ec2region + ", zone: " + ec2zone + ".");
    }

    public String getRack(InetAddress endpoint)
    {
        if (endpoint == FBUtilities.getLocalAddress())
            return ec2zone;
        else
            return Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.RACK).value;
    }

    public String getDatacenter(InetAddress endpoint)
    {
        if (endpoint == FBUtilities.getLocalAddress())
            return ec2region;
        else
            return Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.DC).value;
    }

    @Override
    public void gossiperStarting()
    {
        // Share EC2 info via gossip.  We have to wait until Gossiper is initialized though.
        logger.info("Ec2Snitch adding ApplicationState ec2region=" + ec2region + " ec2zone=" + ec2zone);
        Gossiper.instance.addLocalApplicationState(ApplicationState.DC, StorageService.instance.valueFactory.datacenter(ec2region));
        Gossiper.instance.addLocalApplicationState(ApplicationState.RACK, StorageService.instance.valueFactory.rack(ec2zone));
    }
}
