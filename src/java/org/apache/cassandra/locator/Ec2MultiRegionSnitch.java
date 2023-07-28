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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.StorageService;

/**
 * 1) Snitch will automatically set the public IP by querying the AWS API
 *
 * 2) Snitch will set the private IP as a Gossip application state.
 *
 * 3) Uses a helper class that implements IESCS and will reset the public IP connection if it is within the
 * same region to communicate via private IP.
 *
 * Operational: All the nodes in this cluster needs to be able to (modify the
 * Security group settings in AWS) communicate via Public IP's.
 */
public class Ec2MultiRegionSnitch extends Ec2Snitch
{
    @VisibleForTesting
    static final String PUBLIC_IP_QUERY = "/latest/meta-data/public-ipv4";
    @VisibleForTesting
    static final String PRIVATE_IP_QUERY = "/latest/meta-data/local-ipv4";
    private final String localPrivateAddress;

    public Ec2MultiRegionSnitch() throws IOException, ConfigurationException
    {
        this(new SnitchProperties());
    }

    public Ec2MultiRegionSnitch(SnitchProperties props) throws IOException, ConfigurationException
    {
        this(Ec2MetadataServiceConnector.create(props));
    }

    Ec2MultiRegionSnitch(AbstractCloudMetadataServiceConnector connector) throws IOException
    {
        super(connector);
        InetAddress localPublicAddress = InetAddress.getByName(connector.apiCall(PUBLIC_IP_QUERY));
        logger.info("EC2Snitch using publicIP as identifier: {}", localPublicAddress);
        localPrivateAddress = connector.apiCall(PRIVATE_IP_QUERY);
        // use the Public IP to broadcast Address to other nodes.
        DatabaseDescriptor.setBroadcastAddress(localPublicAddress);
        if (DatabaseDescriptor.getBroadcastRpcAddress() == null)
        {
            logger.info("broadcast_rpc_address unset, broadcasting public IP as rpc_address: {}", localPublicAddress);
            DatabaseDescriptor.setBroadcastRpcAddress(localPublicAddress);
        }
    }

    @Override
    public void gossiperStarting()
    {
        super.gossiperStarting();
        InetAddressAndPort address;
        try
        {
            address = InetAddressAndPort.getByName(localPrivateAddress);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
        Gossiper.instance.addLocalApplicationState(ApplicationState.INTERNAL_ADDRESS_AND_PORT, StorageService.instance.valueFactory.internalAddressAndPort(address));
        Gossiper.instance.addLocalApplicationState(ApplicationState.INTERNAL_IP, StorageService.instance.valueFactory.internalIP(address.getAddress()));
        Gossiper.instance.register(new ReconnectableSnitchHelper(this, getLocalDatacenter(), true));
    }
}
