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

package org.apache.cassandra.tcm.transformations;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataKey;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.PlacementDeltas;
import org.apache.cassandra.tcm.ownership.PlacementProvider;
import org.apache.cassandra.tcm.sequences.LeaveStreams;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.sequences.ReconfigureCMS;
import org.apache.cassandra.tcm.sequences.UnbootstrapAndLeave;

public class Assassinate extends PrepareLeave
{
    public static final Serializer<Assassinate> serializer = new Serializer<Assassinate>()
    {
        @Override
        public Assassinate construct(NodeId leaving, boolean force, PlacementProvider placementProvider, LeaveStreams.Kind streamKind)
        {
            return new Assassinate(leaving, placementProvider);
        }
    };

    public Assassinate(NodeId leaving, PlacementProvider placementProvider)
    {
        super(leaving, true, placementProvider, LeaveStreams.Kind.ASSASSINATE);
    }

    /**
     * Force-remove endpoint from token map.
     *
     * @param endpoint endpoint to remove
     */
    public static void assassinateEndpoint(InetAddressAndPort endpoint)
    {
        ClusterMetadata metadata = ClusterMetadata.current();
        // Gossip implementation of assassinate was a no-op. Preserving this behaviour.
        if (!metadata.directory.isRegistered(endpoint))
            return;

        ReconfigureCMS.maybeReconfigureCMS(metadata, endpoint);

        NodeId nodeId = metadata.directory.peerId(endpoint);
        ClusterMetadataService.instance().commit(new Assassinate(nodeId,
                                                                 ClusterMetadataService.instance().placementProvider()));
    }

    @Override
    public Kind kind()
    {
        return Kind.ASSASSINATE;
    }

    public String toString()
    {
        return "Assassinate{" +
               "leaving=" + leaving +
               '}';
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        Result result = super.execute(prev);
        if (result.isRejected())
            return result;

        Success success = result.success();
        ClusterMetadata metadata = success.metadata;
        Epoch forceEpoch = metadata.epoch;
        metadata = success.metadata.forceEpoch(prev.epoch);

        UnbootstrapAndLeave plan = (UnbootstrapAndLeave) metadata.inProgressSequences.get(nodeId());

        ImmutableSet.Builder<MetadataKey> modifiedKeys = ImmutableSet.builder();

        success = plan.startLeave.execute(metadata).success();
        metadata = success.metadata.forceEpoch(prev.epoch);
        modifiedKeys.addAll(success.affectedMetadata);

        success = plan.midLeave.execute(metadata).success();
        metadata = success.metadata.forceEpoch(prev.epoch);
        modifiedKeys.addAll(success.affectedMetadata);

        success = plan.finishLeave.execute(metadata).success();
        metadata = success.metadata;
        modifiedKeys.addAll(success.affectedMetadata);

        metadata = metadata.forceEpoch(forceEpoch);

        return new Success(metadata, LockedRanges.AffectedRanges.EMPTY, modifiedKeys.build());
    }

    public static LeaveStreams LEAVE_STREAMS = new LeaveStreams()
    {
        @Override
        public void execute(NodeId leaving, PlacementDeltas startLeave, PlacementDeltas midLeave, PlacementDeltas finishLeave)
        {

        }

        @Override
        public Kind kind()
        {
            return Kind.ASSASSINATE;
        }

        public String status()
        {
            return "streaming finished";
        }
    };
}