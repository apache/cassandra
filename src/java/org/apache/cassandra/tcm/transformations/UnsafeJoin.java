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

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MetadataKey;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.ownership.PlacementProvider;
import org.apache.cassandra.tcm.sequences.BootstrapAndJoin;
import org.apache.cassandra.tcm.sequences.LockedRanges;

// Unsafe Join is implemented as a two-step process to keep the replay logic consistent with other
// transformations. In-progress sequence is going to get picked up, and executed.
public class UnsafeJoin extends PrepareJoin
{
    public static final Serializer<UnsafeJoin> serializer = new Serializer<UnsafeJoin>()
    {
        public UnsafeJoin construct(NodeId nodeId, Set<Token> tokens, PlacementProvider placementProvider, boolean joinTokenRing, boolean streamData)
        {
            assert joinTokenRing;
            assert !streamData;
            return new UnsafeJoin(nodeId, tokens, placementProvider);
        }
    };

    public UnsafeJoin(NodeId nodeId, Set<Token> tokens, PlacementProvider placementProvider)
    {
        super(nodeId, tokens, placementProvider, true, false);
    }

    public String toString()
    {
        return "UnsafeJoin{" +
               "id=" + nodeId +
               '}';
    }

    @Override
    public Kind kind()
    {
        return Kind.UNSAFE_JOIN;
    }

    @Override
    public Result execute(ClusterMetadata prev)
    {
        Result result = super.execute(prev);
        if (result.isRejected())
            return result;

        ClusterMetadata metadata = result.success().metadata;
        Epoch forceEpoch = metadata.epoch;

        BootstrapAndJoin plan = (BootstrapAndJoin) metadata.inProgressSequences.get(nodeId());

        Success success;
        ImmutableSet.Builder<MetadataKey> modifiedKeys = ImmutableSet.builder();

        success = plan.startJoin.execute(metadata).success();
        metadata = success.metadata;
        modifiedKeys.addAll(success.affectedMetadata);

        success = plan.midJoin.execute(metadata).success();
        metadata = success.metadata;
        modifiedKeys.addAll(success.affectedMetadata);

        success = plan.finishJoin.execute(metadata).success();
        metadata = success.metadata;
        modifiedKeys.addAll(success.affectedMetadata);

        metadata = metadata.forceEpoch(forceEpoch);

        return new Success(metadata, LockedRanges.AffectedRanges.EMPTY, modifiedKeys.build());
    }

    public static void unsafeJoin(NodeId nodeId, Set<Token> tokens)
    {
        UnsafeJoin join = new UnsafeJoin(nodeId, tokens, ClusterMetadataService.instance().placementProvider());
        ClusterMetadataService.instance().commit(join);
    }
}
