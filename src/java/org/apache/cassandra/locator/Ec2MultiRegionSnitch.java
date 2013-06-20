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

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;

/**
 * 1) Snitch will automatically set the public IP by querying the AWS API
 *
 * 2) Snitch will set the private IP as a Gossip application state.
 *
 * 3) Snitch implements IESCS and will reset the connection if it is within the
 * same region to communicate via private IP.
 *
 * Implements Ec2Snitch to inherit its functionality and extend it for
 * Multi-Region.
 *
 * Operational: All the nodes in this cluster needs to be able to (modify the
 * Security group settings in AWS) communicate via Public IP's.
 */
public class Ec2MultiRegionSnitch extends Ec2Snitch implements IEndpointStateChangeSubscriber
{
    private static final String PUBLIC_IP_QUERY_URL = "http://169.254.169.254/latest/meta-data/public-ipv4";
    private static final String PRIVATE_IP_QUERY_URL = "http://169.254.169.254/latest/meta-data/local-ipv4";
    private final InetAddress localPublicAddress;
    private final String localPrivateAddress;

    public Ec2MultiRegionSnitch() throws IOException, ConfigurationException
    {
        super();
        localPublicAddress = InetAddress.getByName(awsApiCall(PUBLIC_IP_QUERY_URL));
        logger.info("EC2Snitch using publicIP as identifier: " + localPublicAddress);
        localPrivateAddress = awsApiCall(PRIVATE_IP_QUERY_URL);
        // use the Public IP to broadcast Address to other nodes.
        DatabaseDescriptor.setBroadcastAddress(localPublicAddress);
    }

    public void onJoin(InetAddress endpoint, EndpointState epState)
    {
        if (epState.getApplicationState(ApplicationState.INTERNAL_IP) != null)
            reconnect(endpoint, epState.getApplicationState(ApplicationState.INTERNAL_IP));
    }

    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value)
    {
        if (state == ApplicationState.INTERNAL_IP)
            reconnect(endpoint, value);
    }

    public void onAlive(InetAddress endpoint, EndpointState state)
    {
        if (state.getApplicationState(ApplicationState.INTERNAL_IP) != null)
            reconnect(endpoint, state.getApplicationState(ApplicationState.INTERNAL_IP));
    }

    public void onDead(InetAddress endpoint, EndpointState state)
    {
        // do nothing
    }

    public void onRestart(InetAddress endpoint, EndpointState state)
    {
        // do nothing
    }

    public void onRemove(InetAddress endpoint)
    {
        // do nothing.
    }

    private void reconnect(InetAddress publicAddress, VersionedValue localAddressValue)
    {
        try
        {
            reconnect(publicAddress, InetAddress.getByName(localAddressValue.value));
        }
        catch (UnknownHostException e)
        {
            logger.error("Error in getting the IP address resolved: ", e);
        }
    }

    private void reconnect(InetAddress publicAddress, InetAddress localAddress)
    {
        if (getDatacenter(publicAddress).equals(getDatacenter(localPublicAddress))
            && MessagingService.instance().getVersion(publicAddress) == MessagingService.current_version
            && !MessagingService.instance().getConnectionPool(publicAddress).endPoint().equals(localAddress))
        {
            MessagingService.instance().getConnectionPool(publicAddress).reset(localAddress);
            logger.debug(String.format("Intiated reconnect to an Internal IP %s for the %s", localAddress, publicAddress));
        }
    }

    @Override
    public void gossiperStarting()
    {
        super.gossiperStarting();
        Gossiper.instance.addLocalApplicationState(ApplicationState.INTERNAL_IP, StorageService.instance.valueFactory.internalIP(localPrivateAddress));
        Gossiper.instance.register(this);
    }
}
