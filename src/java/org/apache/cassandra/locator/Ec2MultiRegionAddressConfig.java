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
import java.net.UnknownHostException;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;

public class Ec2MultiRegionAddressConfig implements NodeAddressConfig
{
    private static final Logger logger = LoggerFactory.getLogger(Ec2MultiRegionAddressConfig.class);

    @VisibleForTesting
    public static final String PUBLIC_IP_QUERY = "/latest/meta-data/public-ipv4";
    @VisibleForTesting
    public static final String PRIVATE_IP_QUERY = "/latest/meta-data/local-ipv4";
    private final String localPublicAddress;
    private final String localPrivateAddress;

    /**
     * Used via reflection by DatabaseDescriptor::createAddressConfig
     */
    public Ec2MultiRegionAddressConfig() throws IOException
    {
        this(Ec2MetadataServiceConnector.create(new SnitchProperties()));
    }

    @VisibleForTesting
    public Ec2MultiRegionAddressConfig(AbstractCloudMetadataServiceConnector connector) throws IOException
    {
        this.localPublicAddress = connector.apiCall(PUBLIC_IP_QUERY);
        logger.info("EC2 multi region address config using publicIP as identifier: {}", localPublicAddress);
        this.localPrivateAddress = connector.apiCall(PRIVATE_IP_QUERY);
    }

    @Override
    public void configureAddresses()
    {
        // use the Public IP to broadcast Address to other nodes.
        try
        {
            InetAddress broadcastAddress = InetAddress.getByName(localPublicAddress);
            DatabaseDescriptor.setBroadcastAddress(broadcastAddress);
            if (DatabaseDescriptor.getBroadcastRpcAddress() == null)
            {
                logger.info("broadcast_rpc_address unset, broadcasting public IP as rpc_address: {}", localPublicAddress);
                DatabaseDescriptor.setBroadcastRpcAddress(broadcastAddress);
            }
        }
        catch (UnknownHostException e)
        {
            throw new ConfigurationException("Unable to obtain public address for node from cloud metadata service", e);
        }

        try
        {
            InetAddress privateAddress = InetAddress.getByName(localPrivateAddress);
            FBUtilities.setLocalAddress(privateAddress);
        }
        catch (UnknownHostException e)
        {
            throw new ConfigurationException("Unable to obtain private address for node from cloud metadata service", e);
        }
    }

    @Override
    public boolean preferLocalConnections()
    {
        // Always prefer re-connecting on private addresses if in the same datacenter (i.e. region)
        return true;
    }
}
