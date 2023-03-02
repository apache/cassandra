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

package org.apache.cassandra.tcm.listeners;

import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;

import static org.apache.cassandra.tcm.membership.NodeState.LEFT;

public class PaxosRepairListener implements ChangeListener
{
    @Override
    public void notifyPostCommit(ClusterMetadata prev, ClusterMetadata next)
    {
        if (!next.directory.lastModified().equals(prev.directory.lastModified()))
        {
            for (NodeId node : next.directory.peerIds())
            {
                NodeState oldState = prev.directory.peerState(node);
                NodeState newState = next.directory.peerState(node);
                if (oldState != newState && newState == LEFT)
                {
                    StorageService.instance.repairPaxosForTopologyChange("decommission");
                    break;
                }
            }
        }
    }
}
