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

import java.net.UnknownHostException;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.gms.*;
import org.apache.cassandra.net.ConnectionCategory;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.OutboundConnectionSettings;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.auth.IInternodeAuthenticator.InternodeConnectionDirection.OUTBOUND_PRECONNECT;

/**
 * Sidekick helper for snitches that want to reconnect from one IP addr for a node to another.
 * Typically, this is for situations like EC2 where a node will have a public address and a private address,
 * where we connect on the public, discover the private, and reconnect on the private.
 */
public class ReconnectableSnitchHelper implements IEndpointStateChangeSubscriber
{
    private static final Logger logger = LoggerFactory.getLogger(ReconnectableSnitchHelper.class);
    private final IEndpointSnitch snitch;
    private final String localDc;
    private final boolean preferLocal;

    public ReconnectableSnitchHelper(IEndpointSnitch snitch, String localDc, boolean preferLocal)
    {
        this.snitch = snitch;
        this.localDc = localDc;
        this.preferLocal = preferLocal;
    }

    private void reconnect(InetAddressAndPort publicAddress, VersionedValue localAddressValue)
    {
        try
        {
            reconnect(publicAddress, InetAddressAndPort.getByName(localAddressValue.value), snitch, localDc);
        }
        catch (UnknownHostException e)
        {
            logger.error("Error in getting the IP address resolved: ", e);
        }
    }

    @VisibleForTesting
    static void reconnect(InetAddressAndPort publicAddress, InetAddressAndPort localAddress, IEndpointSnitch snitch, String localDc)
    {
        final OutboundConnectionSettings settings = new OutboundConnectionSettings(publicAddress, localAddress).withDefaults(ConnectionCategory.MESSAGING);
        if (!settings.authenticator().authenticate(settings.to.getAddress(), settings.to.getPort(), null, OUTBOUND_PRECONNECT))
        {
            logger.debug("InternodeAuthenticator said don't reconnect to {} on {}", publicAddress, localAddress);
            return;
        }

        if (snitch.getDatacenter(publicAddress).equals(localDc))
        {
            MessagingService.instance().maybeReconnectWithNewIp(publicAddress, localAddress);
            logger.debug("Initiated reconnect to an Internal IP {} for the {}", localAddress, publicAddress);
        }
    }

    public void beforeChange(InetAddressAndPort endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue)
    {
        // no-op
    }

    public void onJoin(InetAddressAndPort endpoint, EndpointState epState)
    {
        if (preferLocal && !Gossiper.instance.isDeadState(epState))
        {
            VersionedValue address = epState.getApplicationState(ApplicationState.INTERNAL_ADDRESS_AND_PORT);
            if (address == null)
            {
                address = epState.getApplicationState(ApplicationState.INTERNAL_ADDRESS_AND_PORT);
            }
            if (address != null)
            {
                reconnect(endpoint, address);
            }
        }
    }

    //Skeptical this will always do the right thing all the time port wise. It will converge on the right thing
    //eventually once INTERNAL_ADDRESS_AND_PORT is populated
    public void onChange(InetAddressAndPort endpoint, ApplicationState state, VersionedValue value)
    {
        if (preferLocal && !Gossiper.instance.isDeadState(Gossiper.instance.getEndpointStateForEndpoint(endpoint)))
        {
            if (state == ApplicationState.INTERNAL_ADDRESS_AND_PORT)
            {
                reconnect(endpoint, value);
            }
            else if (state == ApplicationState.INTERNAL_IP &&
                     null == Gossiper.instance.getEndpointStateForEndpoint(endpoint).getApplicationState(ApplicationState.INTERNAL_ADDRESS_AND_PORT))
            {
                //Only use INTERNAL_IP if INTERNAL_ADDRESS_AND_PORT is unavailable
                reconnect(endpoint, value);
            }
        }
    }

    public void onAlive(InetAddressAndPort endpoint, EndpointState state)
    {
        VersionedValue internalIP = state.getApplicationState(ApplicationState.INTERNAL_IP);
        VersionedValue internalIPAndPorts = state.getApplicationState(ApplicationState.INTERNAL_ADDRESS_AND_PORT);
        if (preferLocal && internalIP != null)
            reconnect(endpoint, internalIPAndPorts != null ? internalIPAndPorts : internalIP);
    }

    public void onDead(InetAddressAndPort endpoint, EndpointState state)
    {
        // do nothing.
    }

    public void onRemove(InetAddressAndPort endpoint)
    {
        // do nothing.
    }

    public void onRestart(InetAddressAndPort endpoint, EndpointState state)
    {
        // do nothing.
    }
}
