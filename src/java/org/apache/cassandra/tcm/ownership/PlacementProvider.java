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

package org.apache.cassandra.tcm.ownership;

import java.util.List;
import java.util.Set;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;

public interface PlacementProvider
{
    DataPlacements calculatePlacements(List<Range<Token>> ranges, ClusterMetadata metadata, Keyspaces keyspaces);
    // TODO naming
    PlacementTransitionPlan planForJoin(ClusterMetadata metadata,
                                        NodeId joining,
                                        Set<Token> tokens,
                                        Keyspaces keyspaces);

    PlacementTransitionPlan planForMove(ClusterMetadata metadata,
                                        NodeId nodeId,
                                        Set<Token> tokens,
                                        Keyspaces keyspaces);

    // TODO: maybe leave, for consistency?
    PlacementTransitionPlan planForDecommission(ClusterMetadata metadata,
                                                NodeId nodeId,
                                                Keyspaces keyspaces);

    PlacementTransitionPlan planForReplacement(ClusterMetadata metadata,
                                               NodeId replaced,
                                               NodeId replacement,
                                               Keyspaces keyspaces);
}
