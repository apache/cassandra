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

package org.apache.cassandra.service.accord.topology;

import javax.annotation.Nullable;

import accord.api.TopologySorter;
import accord.local.Node;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;

/**
 * Maps network addresses to accord ids and vice versa
 */
public interface AccordEndpointMap
{
    enum NodeStatus
    {
        REMOVED(TopologySorter.NodeStatus.UNAVAILABLE),
        UNKNOWN(TopologySorter.NodeStatus.UNAVAILABLE),
        UNAVAILABLE(TopologySorter.NodeStatus.UNAVAILABLE),
        UNREADABLE(TopologySorter.NodeStatus.UNREADABLE),
        HEALTHY(TopologySorter.NodeStatus.HEALTHY);

        public final TopologySorter.NodeStatus accordStatus;

        NodeStatus(TopologySorter.NodeStatus accordStatus)
        {
            this.accordStatus = accordStatus;
        }

        public boolean isRemoved()
        {
            return this == REMOVED;
        }
    }

    default @Nullable Node.Id mappedIdOrNull(InetAddressAndPort endpoint) { return mappedIdOrNull(endpoint, null); }
    @Nullable Node.Id mappedIdOrNull(InetAddressAndPort endpoint, @Nullable Object logIdentityIfUnmapped);
    default @Nullable InetAddressAndPort mappedEndpointOrNull(Node.Id id) { return mappedEndpointOrNull(id, null); }
    @Nullable InetAddressAndPort mappedEndpointOrNull(Node.Id id, @Nullable Object logIdentityIfUnmapped);

    NodeStatus nodeStatus(Node.Id id);
    default void updateMapping(ClusterMetadata metadata) {}
}
