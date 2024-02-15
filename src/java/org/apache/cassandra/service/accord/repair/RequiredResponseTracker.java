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

package org.apache.cassandra.service.accord.repair;

import java.util.HashSet;
import java.util.Set;

import accord.coordinate.tracking.AbstractSimpleTracker;
import accord.coordinate.tracking.RequestStatus;
import accord.coordinate.tracking.ShardTracker;
import accord.local.Node;
import accord.topology.Shard;
import accord.topology.Topologies;

import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.Fail;
import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.NoChange;
import static accord.coordinate.tracking.AbstractTracker.ShardOutcomes.Success;

public class RequiredResponseTracker extends AbstractSimpleTracker<RequiredResponseTracker.RequiredResponseShardTracker>
{
    public static class RequiredResponseShardTracker extends ShardTracker
    {
        private final Set<Node.Id> outstandingResponses;

        public RequiredResponseShardTracker(Set<Node.Id> requiredResponses, Shard shard)
        {
            super(shard);
            this.outstandingResponses = new HashSet<>();
            for (Node.Id id : shard.nodes)
            {
                if (requiredResponses.contains(id))
                    outstandingResponses.add(id);
            }
        }

        public ShardOutcomes onSuccess(Node.Id node)
        {
            return outstandingResponses.remove(node) && outstandingResponses.isEmpty() ? Success : NoChange;
        }

        public ShardOutcomes onFailure(Object ignore)
        {
            return !outstandingResponses.isEmpty() ? Fail : NoChange;
        }
    }

    public RequiredResponseTracker(Set<Node.Id> requiredResponses, Topologies topologies)
    {
        super(topologies, RequiredResponseShardTracker[]::new, shard -> new RequiredResponseShardTracker(requiredResponses, shard));
    }

    @Override
    public RequestStatus recordSuccess(Node.Id node)
    {
        return recordResponse(this, node, RequiredResponseShardTracker::onSuccess, node);
    }

    @Override
    public RequestStatus recordFailure(Node.Id node)
    {
        return recordResponse(this, node, RequiredResponseShardTracker::onFailure, null);
    }
}
