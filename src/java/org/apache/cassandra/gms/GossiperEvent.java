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

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

import org.apache.cassandra.diag.DiagnosticEvent;
import org.apache.cassandra.locator.InetAddressAndPort;

/**
 * DiagnosticEvent implementation for {@link Gossiper} activities.
 */
public final class GossiperEvent extends DiagnosticEvent
{
    private final InetAddressAndPort endpoint;
    @Nullable
    private final Long quarantineExpiration;
    @Nullable
    private final EndpointState localState;

    private final Map<InetAddressAndPort, EndpointState> endpointStateMap;
    private final boolean inShadowRound;
    private final Map<InetAddressAndPort, Long> justRemovedEndpoints;
    private final long lastProcessedMessageAt;
    private final Set<InetAddressAndPort> liveEndpoints;
    private final List<String> seeds;
    private final Set<InetAddressAndPort> seedsInShadowRound;
    private final Map<InetAddressAndPort, Long> unreachableEndpoints;


    public enum GossiperEventType
    {
        MARKED_AS_SHUTDOWN,
        CONVICTED,
        REPLACEMENT_QUARANTINE,
        REPLACED_ENDPOINT,
        EVICTED_FROM_MEMBERSHIP,
        REMOVED_ENDPOINT,
        QUARANTINED_ENDPOINT,
        MARKED_ALIVE,
        REAL_MARKED_ALIVE,
        MARKED_DEAD,
        MAJOR_STATE_CHANGE_HANDLED,
        SEND_GOSSIP_DIGEST_SYN
    }

    public GossiperEventType type;


    GossiperEvent(GossiperEventType type, Gossiper gossiper, InetAddressAndPort endpoint,
                  @Nullable Long quarantineExpiration, @Nullable EndpointState localState)
    {
        this.type = type;
        this.endpoint = endpoint;
        this.quarantineExpiration = quarantineExpiration;
        this.localState = localState;

        this.endpointStateMap = gossiper.getEndpointStateMap();
        this.inShadowRound = gossiper.isInShadowRound();
        this.justRemovedEndpoints = gossiper.getJustRemovedEndpoints();
        this.lastProcessedMessageAt = gossiper.getLastProcessedMessageAt();
        this.liveEndpoints = gossiper.getLiveMembers();
        this.seeds = gossiper.getSeeds();
        this.seedsInShadowRound = gossiper.getSeedsInShadowRound();
        this.unreachableEndpoints = gossiper.getUnreachableEndpoints();
    }

    public Enum<GossiperEventType> getType()
    {
        return type;
    }

    public HashMap<String, Serializable> toMap()
    {
        // be extra defensive against nulls and bugs
        HashMap<String, Serializable> ret = new HashMap<>();
        if (endpoint != null) ret.put("endpoint", endpoint.getHostAddressAndPort());
        ret.put("quarantineExpiration", quarantineExpiration);
        ret.put("localState", String.valueOf(localState));
        ret.put("endpointStateMap", String.valueOf(endpointStateMap));
        ret.put("inShadowRound", inShadowRound);
        ret.put("justRemovedEndpoints", String.valueOf(justRemovedEndpoints));
        ret.put("lastProcessedMessageAt", lastProcessedMessageAt);
        ret.put("liveEndpoints", String.valueOf(liveEndpoints));
        ret.put("seeds", String.valueOf(seeds));
        ret.put("seedsInShadowRound", String.valueOf(seedsInShadowRound));
        ret.put("unreachableEndpoints", String.valueOf(unreachableEndpoints));
        return ret;
    }
}
