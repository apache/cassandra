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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.ReplicationParams;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;

/**
 * The defining feature of this placement stategy is that all layouts (i.e. replication params) use the same
 * set of ranges. So when splitting the current ranges, we only need to calculate the splits once and apply to
 * all existing placements.
 *
 * Also, when using this strategy, the read and write placements should (eventually) be identical. While range
 * movements/bootstraps/decommissions are in-flight, this will not be the case as the read and write replica
 * sets will diverge while nodes are acquiring/relinquishing ranges. Although there may always be such operations
 * ongoing, this is technically a temporary state.
 *
 * Because of this, when calculating the steps to transition between the current state and a proposed new state,
 * we work from the associated TokenMaps, the assumption being that eventually both the read and write placements
 * will converge and will, at that point, reflect those TokenMaps.
 * This means that the starting point of each transition is the intended end state of the preceding transitions.
 *
 * To do this calculation, we create a canonical DataPlacement from the current TokenMap and split it according
 * to the proposed tokens. As we iterate through and split the existing ranges, we construct a new DataPlacement for
 * each currently defined. There is no movement of data between the initial and new placements, only splitting of
 * existing ranges, so we can simply copy the replica groups from the old ranges to the new.
 * e.g.:
 * We start with a portion of the ring containing tokens 100, 200 assigned to nodes A & B respectively. The node
 * with the next highest token after B is C. RF is 2. We want to add a new token, 150 and assign it to a new node, D.
 * Starting placement, with tokens 100, 200
 *  (0, 100]    : {A,B}
 *  (100, 200]  : {B,C}
 * Next placement, with tokens 100, 150, 200 but no ownership changes
 *  (0, 100]    : {A,B}
 *  (100, 150]  : {B,C}
 *  (150, 200]  : {B,C}
 * No actual movement has occurred, only the ranges have been adjusted.
 * We then calculate the desired end state, with new nodes taking ownership of their assigned tokens.
 * If Node D takes ownership of Token 150, the desired end state would be:
 *  (0, 100]    : {A,D}
 *  (100, 150]  : {D,C}
 *  (150, 200]  : {B,C}
 * As mentioned, distinct placements are maintained for reads and writes, enabling ranges to be temporarily
 * overreplicated during a movement operation (analogous to the previous concept of pending ranges).
 */
public class UniformRangePlacement implements PlacementProvider
{
    private static final Logger logger = LoggerFactory.getLogger(UniformRangePlacement.class);

    @Override
    public PlacementTransitionPlan planForJoin(ClusterMetadata metadata,
                                               NodeId joining,
                                               Set<Token> tokens,
                                               Keyspaces keyspaces)
    {
        // There are no other nodes in the cluster, so the joining node will be taking ownership of the entire range.
        if (metadata.tokenMap.isEmpty())
        {
            DataPlacements placements = calculatePlacements(metadata.transformer()
                                                                    .proposeToken(joining, tokens)
                                                                    .addToRackAndDC(joining)
                                                                    .build()
                                                            .metadata,
                                                            keyspaces);
            PlacementDeltas.Builder toStart = PlacementDeltas.builder(placements.size());
            placements.withDistributed((params, placement) -> {
                toStart.put(params, DataPlacement.empty().difference(placements.get(params)));
            });
            return new PlacementTransitionPlan(toStart.build(),
                                               PlacementDeltas.empty(),
                                               PlacementDeltas.empty(),
                                               PlacementDeltas.empty());
        }

        DataPlacements base = calculatePlacements(metadata, keyspaces);
        DataPlacements start = splitRanges(metadata.tokenMap, metadata.tokenMap.assignTokens(joining, tokens), base);

        DataPlacements finalPlacements = calculatePlacements(metadata.transformer()
                                                                     .proposeToken(joining, tokens)
                                                                     .addToRackAndDC(joining)
                                                                     .build()
                                                             .metadata,
                                                             keyspaces);

        DataPlacements maximalPlacements = start.combineReplicaGroups(finalPlacements);

        PlacementDeltas.Builder toStart = PlacementDeltas.builder(base.size());
        PlacementDeltas.Builder toMaximal = PlacementDeltas.builder(base.size());
        PlacementDeltas.Builder toFinal = PlacementDeltas.builder(base.size());

        start.withDistributed((params, startPlacement) -> {
            toStart.put(params, base.get(params).difference(startPlacement));
            toMaximal.put(params, startPlacement.difference(maximalPlacements.get(params)));
            toFinal.put(params, maximalPlacements.get(params).difference(finalPlacements.get(params)));
        });

        return new PlacementTransitionPlan(toStart.build(), toMaximal.build(), toFinal.build(), PlacementDeltas.empty());
    }

    @Override
    public PlacementTransitionPlan planForMove(ClusterMetadata metadata, NodeId nodeId, Set<Token> tokens, Keyspaces keyspaces)
    {
        DataPlacements base = calculatePlacements(metadata, keyspaces);

        TokenMap withMoved = metadata.transformer()
                                     .proposeToken(nodeId, tokens)
                                     .build().metadata.tokenMap;
        // Introduce new tokens, but do not change the ownership just yet
        DataPlacements start = splitRanges(metadata.tokenMap, withMoved, base);

        DataPlacements withoutLeaving = calculatePlacements(metadata.transformer()
                                                                    .unproposeTokens(nodeId)
                                                                    .proposeToken(nodeId, tokens)
                                                                    .build().metadata,
                                                            keyspaces);

        // Old tokens owned by the move target are now owned by the next-in-ring;
        // Move target now owns its newly assigned tokens
        DataPlacements end = splitRanges(metadata.tokenMap, withMoved, withoutLeaving);

        DataPlacements maximal = start.combineReplicaGroups(end);


        PlacementDeltas.Builder split = PlacementDeltas.builder();
        PlacementDeltas.Builder toMaximal = PlacementDeltas.builder();
        PlacementDeltas.Builder toFinal = PlacementDeltas.builder();
        PlacementDeltas.Builder merge = PlacementDeltas.builder();

        base.withDistributed((params, placement) -> {
            split.put(params, placement.difference(start.get(params)));
        });

        maximal.withDistributed((params, placement) -> {
            toMaximal.put(params, start.get(params).difference(placement));
            toFinal.put(params, placement.difference(end.get(params)));
        });

        end.withDistributed((params, placement) -> {
            merge.put(params, placement.difference(withoutLeaving.get(params)));
        });

        return new PlacementTransitionPlan(split.build(), toMaximal.build(), toFinal.build(), merge.build());
    }

    @Override
    public PlacementTransitionPlan planForDecommission(ClusterMetadata metadata,
                                                       NodeId nodeId,
                                                       Keyspaces keyspaces)
    {
        // Determine the set of placements to start from. This is the canonical set of placements based on the current
        // TokenMap and collection of Keyspaces/ReplicationParams.
        List<Range<Token>> currentRanges = calculateRanges(metadata.tokenMap);
        DataPlacements start = calculatePlacements(currentRanges, metadata, keyspaces);

        ClusterMetadata proposed = metadata.transformer()
                                           .unproposeTokens(nodeId)
                                           .build()
                                   .metadata;
        DataPlacements withoutLeaving = calculatePlacements(proposed, keyspaces);

        DataPlacements finalPlacement = splitRanges(proposed.tokenMap, metadata.tokenMap,
                                                    withoutLeaving);

        DataPlacements maximal = start.combineReplicaGroups(finalPlacement);

        PlacementDeltas.Builder toMaximal = PlacementDeltas.builder(start.size());
        PlacementDeltas.Builder toFinal = PlacementDeltas.builder(start.size());
        PlacementDeltas.Builder merge = PlacementDeltas.builder(start.size());

        maximal.withDistributed((params, maxPlacement) -> {
            // Add new nodes as replicas
            PlacementDeltas.PlacementDelta maxDelta = start.get(params).difference(maxPlacement);
            toMaximal.put(params, maxDelta);

            PlacementDeltas.PlacementDelta leftDelta = maxPlacement.difference(finalPlacement.get(params));
            toFinal.put(params, leftDelta);

            PlacementDeltas.PlacementDelta finalDelta = finalPlacement.get(params).difference(withoutLeaving.get(params));
            merge.put(params, finalDelta);
        });

        return new PlacementTransitionPlan(PlacementDeltas.empty(), toMaximal.build(), toFinal.build(), merge.build());
    }

    @Override
    public PlacementTransitionPlan planForReplacement(ClusterMetadata metadata,
                                                      NodeId replaced,
                                                      NodeId replacement,
                                                      Keyspaces keyspaces)
    {
        DataPlacements startPlacements = calculatePlacements(metadata, keyspaces);
        DataPlacements finalPlacements = calculatePlacements(metadata.transformer()
                                                                     .unproposeTokens(replaced)
                                                                     .proposeToken(replacement, metadata.tokenMap.tokens(replaced))
                                                                     .build()
                                                             .metadata,
                                                             keyspaces);
        DataPlacements maximalPlacements = startPlacements.combineReplicaGroups(finalPlacements);

        PlacementDeltas.Builder toMaximal = PlacementDeltas.builder(startPlacements.size());
        PlacementDeltas.Builder toFinal = PlacementDeltas.builder(startPlacements.size());

        maximalPlacements.withDistributed((params, max) -> {
            toMaximal.put(params, startPlacements.get(params).difference(max));
            toFinal.put(params, max.difference(finalPlacements.get(params)));
        });

        return new PlacementTransitionPlan(PlacementDeltas.empty(), toMaximal.build(), toFinal.build(), PlacementDeltas.empty());
    }

    public DataPlacements splitRanges(TokenMap current,
                                      TokenMap proposed,
                                      DataPlacements currentPlacements)
    {
        ImmutableList<Token> currentTokens = current.tokens();
        ImmutableList<Token> proposedTokens = proposed.tokens();
        if (currentTokens.isEmpty() || currentTokens.equals(proposedTokens))
        {
            return currentPlacements;
        }
        else
        {
            if (!proposedTokens.containsAll(currentTokens))
                throw new IllegalArgumentException("Proposed tokens must be superset of existing tokens");
            // we need to split some existing ranges, so apply the new set of tokens to the current canonical
            // placements to get a set of placements with the proposed ranges but the current replicas
            return splitRangesForAllPlacements(proposedTokens, currentPlacements);
        }
    }

    @VisibleForTesting
    DataPlacements splitRangesForAllPlacements(List<Token> proposedTokens, DataPlacements current)
    {
        DataPlacements.Builder builder = DataPlacements.builder(current.size());
        current.asMap().forEach((params, placement) -> {
            // Don't split ranges for local-only placements
            if (params.isLocal() || params.isMeta())
                builder.with(params, placement);
            else
                builder.with(params, placement.splitRangesForPlacement(proposedTokens));
        });
        return builder.build();
    }

    public DataPlacements calculatePlacements(ClusterMetadata metadata, Keyspaces keyspaces)
    {
        if (metadata.tokenMap.tokens().isEmpty())
            return DataPlacements.empty();

        return calculatePlacements(keyspaces, metadata);
    }

    private DataPlacements calculatePlacements(Keyspaces keyspaces, ClusterMetadata metadata)
    {
        List<Range<Token>> ranges = calculateRanges(metadata.tokenMap);
        return calculatePlacements(ranges, metadata, keyspaces);
    }

    public List<Range<Token>> calculateRanges(TokenMap tokenMap)
    {
        List<Token> tokens = tokenMap.tokens();
        List<Range<Token>> ranges = new ArrayList<>(tokens.size() + 1);
        maybeAdd(ranges, new Range<>(tokenMap.partitioner().getMinimumToken(), tokens.get(0)));
        for (int i = 1; i < tokens.size(); i++)
            maybeAdd(ranges, new Range<>(tokens.get(i - 1), tokens.get(i)));
        maybeAdd(ranges, new Range<>(tokens.get(tokens.size() - 1), tokenMap.partitioner().getMinimumToken()));
        if (ranges.isEmpty())
            ranges.add(new Range<>(tokenMap.partitioner().getMinimumToken(), tokenMap.partitioner().getMinimumToken()));
        return ranges;
    }

    public DataPlacements calculatePlacements(List<Range<Token>> ranges,
                                              ClusterMetadata metadata,
                                              Keyspaces keyspaces)
    {
        Map<ReplicationParams, DataPlacement> placements = new HashMap<>();
        for (KeyspaceMetadata ksMetadata : keyspaces)
        {
            logger.trace("Calculating data placements for {}", ksMetadata.name);
            AbstractReplicationStrategy replication = ksMetadata.replicationStrategy;
            ReplicationParams params = ksMetadata.params.replication;
            placements.computeIfAbsent(params, p -> replication.calculateDataPlacement(ranges, metadata));
        }

        return DataPlacements.builder(placements).build();
    }

    private void maybeAdd(List<Range<Token>> ranges, Range<Token> r)
    {
        if (r.left.compareTo(r.right) != 0)
            ranges.add(r);
    }
}