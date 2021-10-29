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

package org.apache.cassandra.gms;


import org.apache.cassandra.diag.DiagnosticEventService;
import org.apache.cassandra.gms.GossiperEvent.GossiperEventType;
import org.apache.cassandra.locator.InetAddressAndPort;

/**
 * Utility methods for DiagnosticEvent activities.
 */
final class GossiperDiagnostics
{
    private static final DiagnosticEventService service = DiagnosticEventService.instance();

    private GossiperDiagnostics()
    {
    }

    static void markedAsShutdown(Gossiper gossiper, InetAddressAndPort endpoint)
    {
        if (isEnabled(GossiperEventType.MARKED_AS_SHUTDOWN))
            service.publish(new GossiperEvent(GossiperEventType.MARKED_AS_SHUTDOWN, gossiper, endpoint, null, null));
    }

    static void convicted(Gossiper gossiper, InetAddressAndPort endpoint, double phi)
    {
        if (isEnabled(GossiperEventType.CONVICTED))
            service.publish(new GossiperEvent(GossiperEventType.CONVICTED, gossiper, endpoint, null, null));
    }

    static void replacementQuarantine(Gossiper gossiper, InetAddressAndPort endpoint)
    {
        if (isEnabled(GossiperEventType.REPLACEMENT_QUARANTINE))
            service.publish(new GossiperEvent(GossiperEventType.REPLACEMENT_QUARANTINE, gossiper, endpoint, null, null));
    }

    static void replacedEndpoint(Gossiper gossiper, InetAddressAndPort endpoint)
    {
        if (isEnabled(GossiperEventType.REPLACED_ENDPOINT))
            service.publish(new GossiperEvent(GossiperEventType.REPLACED_ENDPOINT, gossiper, endpoint, null, null));
    }

    static void evictedFromMembership(Gossiper gossiper, InetAddressAndPort endpoint)
    {
        if (isEnabled(GossiperEventType.EVICTED_FROM_MEMBERSHIP))
            service.publish(new GossiperEvent(GossiperEventType.EVICTED_FROM_MEMBERSHIP, gossiper, endpoint, null, null));
    }

    static void removedEndpoint(Gossiper gossiper, InetAddressAndPort endpoint)
    {
        if (isEnabled(GossiperEventType.REMOVED_ENDPOINT))
            service.publish(new GossiperEvent(GossiperEventType.REMOVED_ENDPOINT, gossiper, endpoint, null, null));
    }

    static void quarantinedEndpoint(Gossiper gossiper, InetAddressAndPort endpoint, long quarantineExpiration)
    {
        if (isEnabled(GossiperEventType.QUARANTINED_ENDPOINT))
            service.publish(new GossiperEvent(GossiperEventType.QUARANTINED_ENDPOINT, gossiper, endpoint, quarantineExpiration, null));
    }

    static void markedAlive(Gossiper gossiper, InetAddressAndPort addr, EndpointState localState)
    {
        if (isEnabled(GossiperEventType.MARKED_ALIVE))
            service.publish(new GossiperEvent(GossiperEventType.MARKED_ALIVE, gossiper, addr, null, localState));
    }

    static void realMarkedAlive(Gossiper gossiper, InetAddressAndPort addr, EndpointState localState)
    {
        if (isEnabled(GossiperEventType.REAL_MARKED_ALIVE))
            service.publish(new GossiperEvent(GossiperEventType.REAL_MARKED_ALIVE, gossiper, addr, null, localState));
    }

    static void markedDead(Gossiper gossiper, InetAddressAndPort addr, EndpointState localState)
    {
        if (isEnabled(GossiperEventType.MARKED_DEAD))
            service.publish(new GossiperEvent(GossiperEventType.MARKED_DEAD, gossiper, addr, null, localState));
    }

    static void majorStateChangeHandled(Gossiper gossiper, InetAddressAndPort addr, EndpointState state)
    {
        if (isEnabled(GossiperEventType.MAJOR_STATE_CHANGE_HANDLED))
            service.publish(new GossiperEvent(GossiperEventType.MAJOR_STATE_CHANGE_HANDLED, gossiper, addr, null, state));
    }

    static void sendGossipDigestSyn(Gossiper gossiper, InetAddressAndPort to)
    {
        if (isEnabled(GossiperEventType.SEND_GOSSIP_DIGEST_SYN))
            service.publish(new GossiperEvent(GossiperEventType.SEND_GOSSIP_DIGEST_SYN, gossiper, to, null, null));
    }

    private static boolean isEnabled(GossiperEventType type)
    {
        return service.isEnabled(GossiperEvent.class, type);
    }
}
