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

package org.apache.cassandra.tcm;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.listeners.ChangeListener;
import org.apache.cassandra.tcm.membership.EndpointLookup;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.Pair;

public class CMSLookup
{
    private static final Logger logger = LoggerFactory.getLogger(CMSLookup.class);

    public enum State { PRE_INIT, ACTIVE, RETIRED };

    public final static CMSLookup NO_OP = new CMSLookup(State.PRE_INIT, Epoch.EMPTY, new HashMap<>());
    public static InitialBuilder builder(ClusterMetadata metadata)
    {
        return new InitialBuilder(metadata);
    }

    private final Map<NodeId, Pair<InetAddressAndPort, InetAddressAndPort>> overrides;
    private final Epoch lastModified;
    private final State state;

    private CMSLookup(State state, Epoch epoch, Map<NodeId, Pair<InetAddressAndPort, InetAddressAndPort>> overrides)
    {
        this.state = state;
        this.lastModified = epoch;
        this.overrides = Maps.newHashMapWithExpectedSize(overrides.size());
        for (Map.Entry<NodeId, Pair<InetAddressAndPort, InetAddressAndPort>> e : overrides.entrySet())
            this.overrides.put(e.getKey(), e.getValue());
    }

    public boolean isUninitialized()
    {
        return state == State.PRE_INIT;
    }

    public boolean isActive()
    {
        return state == State.ACTIVE;
    }

    public EndpointLookup asNodeLookup(EndpointLookup lookup)
    {
        return new EndpointLookup()
        {
            @Override
            public InetAddressAndPort endpoint(NodeId id)
            {
                if (overrides.containsKey(id))
                    return overrides.get(id).right;
                return lookup.endpoint(id);
            }
        };
    }

    public CMSLookup rebuild(ClusterMetadata prev, ClusterMetadata next, boolean fromSnapshot)
    {
        logger.debug("Rebuilding CMS lookup {} with metadata from epoch {}", this, next.epoch.getEpoch());

        // All address changes have been enacted, nothing to do
        if (state == State.RETIRED)
            return this;

        if (!next.epoch.isEqualOrBefore(Epoch.FIRST)
            && !fromSnapshot
            && next.directory.lastModified().equals(prev.directory.lastModified()))
            return this;

        // Filters from the override list those which are no longer necessary as a transformation has now
        // replaced the old address with the new one for that node id
        Predicate<Map.Entry<NodeId, Pair<InetAddressAndPort, InetAddressAndPort>>> overrideNowEnacted = entry -> {
            NodeId nodeId = entry.getKey();
            if (!Objects.equals(prev.directory.getNodeAddresses(nodeId), next.directory.getNodeAddresses(nodeId)))
            {
                Pair<InetAddressAndPort, InetAddressAndPort> override = overrides.get(nodeId);
                if (override != null)
                {
                    // If there was an override for this nodeId && the address being overriden matches the prev
                    // directory entry, filter out the override from the map which will be used to build the new version
                    // (i.e. return false from Predicate::test). This indicates the override is no longer required.
                    return !override.left.equals(prev.directory.endpoint(nodeId));
                }
            }
            return true;
        };

        logger.debug("Current endpoint overrides: {}", overrides);
        Map<NodeId, Pair<InetAddressAndPort, InetAddressAndPort>> nextOverrides
            = overrides.entrySet()
                       .stream()
                       .filter(overrideNowEnacted)
                       .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        logger.debug("Proposed endpoint overrides: {}", nextOverrides);
        State state = nextOverrides.isEmpty() ? State.RETIRED : State.ACTIVE;
        return new CMSLookup(state, next.epoch, nextOverrides);
    }

    @Override
    public String toString()
    {
        return "CMSLookup{" +
               "state=" + state +
               ", epoch=" + lastModified +
               ", overrides=" + overrides +
               '}';
    }

    public static class InitialBuilder
    {
        private final Epoch epoch;
        private final Map<NodeId, Pair<InetAddressAndPort, InetAddressAndPort>> overrides;

        private InitialBuilder(ClusterMetadata metadata)
        {
            this.epoch = metadata.epoch;
            this.overrides = new HashMap<>();
        }

        public InitialBuilder withOverride(NodeId id, InetAddressAndPort originalAddress, InetAddressAndPort newAddress)
        {
            overrides.put(id, Pair.create(originalAddress, newAddress));
            return this;
        }

        public CMSLookup build()
        {
            return new CMSLookup(State.ACTIVE, epoch, overrides);
        }
    }

    public static class LogListener implements ChangeListener
    {
        @Override
        public void notifyPreCommit(ClusterMetadata prev, ClusterMetadata next, boolean fromSnapshot)
        {
            logger.debug("Reevaluating CMSLookup from {} at epoch {}", prev.epoch,  next.epoch);
            next.refreshCMSLookup(prev, fromSnapshot);
        }
    }
}
