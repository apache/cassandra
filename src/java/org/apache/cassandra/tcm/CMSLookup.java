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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.listeners.ChangeListener;
import org.apache.cassandra.tcm.membership.EndpointLookup;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.utils.Pair;

public class CMSLookup
{
    private static final Logger logger = LoggerFactory.getLogger(CMSLookup.class);

    public enum State { PRE_INIT, ACTIVE, RETIRED };

    public final static CMSLookup NO_OP = new CMSLookup(State.PRE_INIT, Epoch.EMPTY, ImmutableMap.of());
    public static InitialBuilder builder(ClusterMetadata metadata)
    {
        return new InitialBuilder(metadata);
    }

    @VisibleForTesting
    public final ImmutableMap<NodeId, Pair<InetAddressAndPort, InetAddressAndPort>> overrides;
    private final Epoch lastModified;
    private final State state;

    private CMSLookup(State state, Epoch epoch, ImmutableMap<NodeId, Pair<InetAddressAndPort, InetAddressAndPort>> overrides)
    {
        this.state = state;
        this.lastModified = epoch;
        this.overrides = overrides;
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
                Pair<InetAddressAndPort, InetAddressAndPort> override = overrides.get(id);
                if (override != null)
                    return override.right;
                return lookup.endpoint(id);
            }
        };
    }

    public CMSLookup rebuild(ClusterMetadata prev, ClusterMetadata next, boolean fromSnapshot)
    {
        logger.info("Rebuilding CMS lookup {} with metadata from epoch {}", this, next.epoch.getEpoch());

        // All address changes have been enacted, nothing to do
        if (state == State.RETIRED)
            return this;

        // If there are no directory changes, there can be nothing to do
        if (!next.epoch.isEqualOrBefore(Epoch.FIRST)
            && !fromSnapshot
            && next.directory.lastModified().equals(prev.directory.lastModified()))
            return this;

        // Filter from the override list those which are no longer necessary as a transformation has now replaced the
        // old address with a new one for that node id, or because the node is no longer part of the cluster, having
        // been replaced, decommissioned or assassinated.
        logger.info("Current endpoint overrides: {}", overrides);
        ImmutableMap.Builder<NodeId, Pair<InetAddressAndPort, InetAddressAndPort>> builder = ImmutableMap.builder();
        for (Map.Entry<NodeId, Pair<InetAddressAndPort, InetAddressAndPort>> override : overrides.entrySet())
        {
            NodeId nodeId = override.getKey();
            // If prev.directory doesn't contain the id, then the node has been removed already. This implies that we
            // didn't witness this directly, but have caught up via a snapshot. In this case, filter from the required
            // set of overrides. Likewise, if the next directory doesn't contain the node id, it has already been
            // removed, so maintaining an address override for it is pointless.
            if (!prev.directory.peerIds().contains(nodeId) || !next.directory.peerIds().contains(nodeId))
                continue;

            // If the node has already left, no need to maintain its override.
            if (next.directory.peerState(nodeId) == NodeState.LEFT)
                continue;

            Pair<InetAddressAndPort, InetAddressAndPort> mapping = override.getValue();
            InetAddressAndPort prevEndpoint = prev.directory.endpoint(nodeId);
            InetAddressAndPort nextEndpoint = next.directory.endpoint(nodeId);

            // The expected change has been enacted so the override is no longer required
            if (nextEndpoint.equals(mapping.right))
                continue;

            // If the previous endpoint doesn't match the address being overridden, the override has been superceded.
            // This may imply that the node has changed address multiple times since the overrides were initially built.
            // We should/will learn of the new address via the usual replication mechanism, but even if we don't a
            // restart of this node will rebuild any necessary, new overrides and repeat the process. In any case, this
            // means that that this specific override is no longer required and all that can be usefully done is to log.
            if (!prevEndpoint.equals(mapping.left))
            {
                logger.info("Pending CMS address changes for node {} from {} to {} appears to have been superceded. " +
                            "Detected new address {} in cluster metadata at epoch {}",
                            nodeId, mapping.left, mapping.right, nextEndpoint, next.epoch.getEpoch());
                continue;
            }

            // As far as we can tell because we have yet to learn of any other endpoint for this node, the override as
            // specified is still valid.
            builder.put(override.getKey(), override.getValue());
        }

        ImmutableMap<NodeId, Pair<InetAddressAndPort, InetAddressAndPort>> nextOverrides = builder.build();
        if (nextOverrides.equals(overrides))
        {
            logger.info("No changes to endpoint overrides detected");
            return this;
        }

        logger.info("Proposed endpoint overrides: {}", nextOverrides);
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

        public boolean hasOverrides()
        {
            return !overrides.isEmpty();
        }

        public CMSLookup build()
        {
            if (overrides.isEmpty())
                throw new IllegalStateException("No overrides detected");
            return new CMSLookup(State.ACTIVE, epoch, ImmutableMap.copyOf(overrides));
        }
    }

    public static class LogListener implements ChangeListener
    {
        @Override
        public void notifyPreCommit(ClusterMetadata prev, ClusterMetadata next, boolean fromSnapshot)
        {
            logger.info("Reevaluating CMSLookup from {} at epoch {}", prev.epoch,  next.epoch);
            next.refreshCMSLookup(prev, fromSnapshot);
            if (next.cmsLookup.state == State.RETIRED)
            {
                logger.info("CMSLookup state is RETIRED, removing log listener");
                ClusterMetadataService.instance().log().removeListener(this);
            }
        }
    }
}
