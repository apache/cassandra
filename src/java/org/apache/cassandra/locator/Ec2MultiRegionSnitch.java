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

import org.apache.cassandra.config.ConfigurationException;
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
    private final InetAddress public_ip;
    private final String private_ip;

    public Ec2MultiRegionSnitch() throws IOException, ConfigurationException
    {
        super();
        public_ip = InetAddress.getByName(awsApiCall(PUBLIC_IP_QUERY_URL));
        logger.info("EC2Snitch using publicIP as identifier: " + public_ip);
        private_ip = awsApiCall(PRIVATE_IP_QUERY_URL);
        // use the Public IP to broadcast Address to other nodes.
        DatabaseDescriptor.setBroadcastAddress(public_ip);
    }
    
    @Override
    public void onJoin(InetAddress endpoint, EndpointState epState)
    {
        if (epState.getApplicationState(ApplicationState.INTERNAL_IP) != null)
            reConnect(endpoint, epState.getApplicationState(ApplicationState.INTERNAL_IP));
    }

    @Override
    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value)
    {
        if (state == ApplicationState.INTERNAL_IP)
            reConnect(endpoint, value);
    }

    @Override
    public void onAlive(InetAddress endpoint, EndpointState state)
    {
        if (state.getApplicationState(ApplicationState.INTERNAL_IP) != null)
            reConnect(endpoint, state.getApplicationState(ApplicationState.INTERNAL_IP));
    }

    @Override
    public void onDead(InetAddress endpoint, EndpointState state)
    {
        // do nothing
    }

    @Override
    public void onRestart(InetAddress endpoint, EndpointState state)
    {
        // do nothing
    }

    @Override
    public void onRemove(InetAddress endpoint)
    {
        // do nothing.
    }

    private void reConnect(InetAddress endpoint, VersionedValue versionedValue)
    {
        if (!getDatacenter(endpoint).equals(getDatacenter(public_ip)))
            return; // do nothing return back...
        
        try
        {
            InetAddress remoteIP = InetAddress.getByName(versionedValue.value);
            MessagingService.instance().getConnectionPool(endpoint).reset(remoteIP);
            logger.debug(String.format("Intiated reconnect to an Internal IP %s for the %s", remoteIP, endpoint));
        } catch (UnknownHostException e)
        {
            logger.error("Error in getting the IP address resolved: ", e);
        }
    }
    
    public void gossiperStarting()
    {
        super.gossiperStarting();
        Gossiper.instance.addLocalApplicationState(ApplicationState.INTERNAL_IP, StorageService.instance.valueFactory.internalIP(private_ip));
        Gossiper.instance.register(this);
    }
}
