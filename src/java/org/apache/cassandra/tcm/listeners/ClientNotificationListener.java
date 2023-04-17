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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeState;
import org.apache.cassandra.utils.Pair;

import static org.apache.cassandra.tcm.listeners.ClientNotificationListener.ChangeType.LEAVE;
import static org.apache.cassandra.tcm.membership.NodeState.BOOTSTRAPPING;
import static org.apache.cassandra.tcm.membership.NodeState.MOVING;
import static org.apache.cassandra.tcm.membership.NodeState.REGISTERED;

public class ClientNotificationListener implements ChangeListener
{
    private static final Logger logger = LoggerFactory.getLogger(ClientNotificationListener.class);

    /**
     * notify clients when node JOIN/MOVE/LEAVE
     *
     * note that we don't register any listeners in StorageService until starting the native protocol
     * so we won't send any notifications during startup replay (todo: should we start native before doing the background catchup?)
     */
    public void notifyPostCommit(ClusterMetadata prev, ClusterMetadata next)
    {
        List<Pair<NodeId, ChangeType>> diff = diff(prev.directory, next.directory);
        logger.debug("Maybe notify listeners about {}", diff);

        for (Pair<NodeId, ChangeType> change : diff)
        {
            InetAddressAndPort endpoint = next.directory.endpoint(change.left);
            switch (change.right)
            {
                case JOIN:
                    StorageService.instance.notifyJoined(endpoint);
                    break;
                case MOVE:
                    StorageService.instance.notifyMoved(endpoint);
                    break;
                case LEAVE:
                    StorageService.instance.notifyLeft(endpoint);
                    break;
            }
        }
    }

    enum ChangeType
    {
        MOVE,
        JOIN,
        LEAVE
    }

    private static List<Pair<NodeId, ChangeType>> diff(Directory prev, Directory next)
    {
        if (!prev.lastModified().equals(next.lastModified()))
        {
            List<Pair<NodeId, ChangeType>> changes = new ArrayList<>();
            for (NodeId node : next.peerIds())
            {
                NodeState prevState = prev.peerState(node);
                NodeState nextState = next.peerState(node);
                ChangeType ct = fromNodeStateTransition(prevState, nextState);
                if (ct != null)
                    changes.add(Pair.create(node, ct));
            }
            return changes;
        }
        return Collections.emptyList();
    }

    static ChangeType fromNodeStateTransition(NodeState prev, NodeState next)
    {
        if (next == null)
            return LEAVE;
        if (prev == next)
            return null;
        switch (next)
        {
            case MOVING:
            case LEAVING:
                // if we see this NodeState but the previous state was null, it means we must have missed the JOINED state
                if (prev == null || prev == BOOTSTRAPPING || prev == REGISTERED)
                    return ChangeType.JOIN;
                return null;
            case JOINED:
                if (prev == MOVING)
                    return ChangeType.MOVE;
                return ChangeType.JOIN;
            case LEFT:
                return LEAVE;
            case BOOTSTRAPPING:
            case REGISTERED:
                return null;
            default:
                throw new IllegalStateException("Unknown NodeState: " + next);
        }
    }
}
