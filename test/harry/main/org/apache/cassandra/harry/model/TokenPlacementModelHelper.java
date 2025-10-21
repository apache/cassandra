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

package org.apache.cassandra.harry.model;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;

import static org.apache.cassandra.harry.model.TokenPlacementModel.peerStateToNodes;

public class TokenPlacementModelHelper
{
    /**
     * Updates to the peers table is async, which can see partial state... to avoid this issue retry until the writes have become stable.
     */
    private static List<TokenPlacementModel.Node> retryWhenNotComplete(ICoordinator coordinator, String query)
    {
        TokenPlacementModel.IncompletePeersStateException last = null;
        // 20 retries with 500ms ~10s...
        for (int i = 0; i < 20; i++)
        {
            try
            {
                return peerStateToNodes(coordinator.execute(query, ConsistencyLevel.ONE));
            }
            catch (TokenPlacementModel.IncompletePeersStateException e)
            {
                last = e;
                Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
            }
        }
        throw last;
    }

    public static TokenPlacementModel.ReplicatedRanges getRing(ICoordinator coordinator, TokenPlacementModel.ReplicationFactor rf)
    {
        List<TokenPlacementModel.Node> other = retryWhenNotComplete(coordinator, "select peer, tokens, data_center, rack from system.peers");
        List<TokenPlacementModel.Node> self = retryWhenNotComplete(coordinator, "select broadcast_address, tokens, data_center, rack from system.local");
        List<TokenPlacementModel.Node> all = new ArrayList<>();
        all.addAll(self);
        all.addAll(other);
        all.sort(TokenPlacementModel.Node::compareTo);
        return rf.replicate(all);
    }
}