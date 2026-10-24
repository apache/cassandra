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
package org.apache.cassandra.locator;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.BiMultiValMap;
import org.apache.cassandra.utils.Pair;

/**
 * Computes transit pairs (pending endpoint -> replaced natural endpoint) for replica slot grouping
 * using TokenMetadata-based topology simulation. This is specific to Cassandra 4.1's TokenMetadata model.
 *
 * On Cassandra trunk (TCM), this class is replaced by a TcmTransitPairCalculator that derives
 * the same information from DataPlacement and MovementMap.
 *
 * Algorithm: Process each pending replica INDEPENDENTLY against the original beforeMetadata.
 * For each topology change (bootstrap, leave, move):
 *   1. Clone beforeMetadata
 *   2. Apply ONLY this change
 *   3. Compare before vs after for each token boundary
 *   4. Find new pending replicas and who they're replacing
 *   5. Check for overlap (constraint violation)
 *   6. Discard clone, use original beforeMetadata for next change
 */
public class TokenMetadataTransitPairCalculator
{
    private static final Logger logger = LoggerFactory.getLogger(TokenMetadataTransitPairCalculator.class);

    /**
     * Result of transit pair computation.
     */
    public static class TransitPairResult
    {
        /** Per-token mapping: token -> (pendingEndpoint, replacedNaturalEndpoint) */
        public final Map<Token, Pair<InetAddressAndPort, InetAddressAndPort>> tokenToPendingSlot;
        /** All token boundaries that need slot groups built */
        public final Set<Token> allTokens;
        /** Whether computation succeeded (false = constraint violation, should fall back) */
        public final boolean valid;

        private TransitPairResult(Map<Token, Pair<InetAddressAndPort, InetAddressAndPort>> tokenToPendingSlot,
                                  Set<Token> allTokens,
                                  boolean valid)
        {
            this.tokenToPendingSlot = tokenToPendingSlot;
            this.allTokens = allTokens;
            this.valid = valid;
        }

        public static TransitPairResult success(
            Map<Token, Pair<InetAddressAndPort, InetAddressAndPort>> tokenToPendingSlot,
            Set<Token> allTokens)
        {
            return new TransitPairResult(tokenToPendingSlot, allTokens, true);
        }

        public static TransitPairResult constraintViolation()
        {
            return new TransitPairResult(null, null, false);
        }
    }

    private final AbstractReplicationStrategy strategy;
    private final TokenMetadata beforeMetadata;
    private final BiMultiValMap<Token, InetAddressAndPort> bootstrapTokens;
    private final Set<InetAddressAndPort> leavingEndpoints;
    private final Set<Pair<Token, InetAddressAndPort>> movingEndpoints;

    public TokenMetadataTransitPairCalculator(AbstractReplicationStrategy strategy,
                                              TokenMetadata beforeMetadata,
                                              BiMultiValMap<Token, InetAddressAndPort> bootstrapTokens,
                                              Set<InetAddressAndPort> leavingEndpoints,
                                              Set<Pair<Token, InetAddressAndPort>> movingEndpoints)
    {
        this.strategy = strategy;
        this.beforeMetadata = beforeMetadata;
        this.bootstrapTokens = bootstrapTokens;
        this.leavingEndpoints = leavingEndpoints;
        this.movingEndpoints = movingEndpoints;
    }

    /**
     * Compute transit pairs for a keyspace.
     *
     * @param keyspaceName The keyspace name (used for logging)
     * @return TransitPairResult with valid=true on success, valid=false on constraint violation
     */
    public TransitPairResult computeTransitPairs(String keyspaceName)
    {
        Map<Token, Pair<InetAddressAndPort, InetAddressAndPort>> tokenToPendingSlot = new HashMap<>();
        Set<Token> allTokens = new TreeSet<>(beforeMetadata.sortedTokens());

        // Process each BOOTSTRAP independently
        Multimap<InetAddressAndPort, Token> bootstrapAddresses = bootstrapTokens.inverse();
        for (InetAddressAndPort endpoint : bootstrapAddresses.keySet())
        {
            Collection<Token> tokens = bootstrapAddresses.get(endpoint);

            TokenMetadata afterMetadata = beforeMetadata.cloneOnlyTokenMap();
            afterMetadata.updateNormalTokens(tokens, endpoint);
            allTokens.addAll(tokens);

            if (!findAffectedTokensAndRecord(beforeMetadata, afterMetadata,
                    allTokens, tokenToPendingSlot, keyspaceName))
            {
                return TransitPairResult.constraintViolation();
            }
        }

        // Process each LEAVE independently
        for (InetAddressAndPort endpoint : leavingEndpoints)
        {
            TokenMetadata afterMetadata = beforeMetadata.cloneOnlyTokenMap();
            afterMetadata.removeEndpoint(endpoint);

            if (!findAffectedTokensAndRecord(beforeMetadata, afterMetadata,
                    allTokens, tokenToPendingSlot, keyspaceName))
            {
                return TransitPairResult.constraintViolation();
            }
        }

        // Process each MOVE independently
        for (Pair<Token, InetAddressAndPort> moving : movingEndpoints)
        {
            Token newToken = moving.left;
            InetAddressAndPort endpoint = moving.right;

            TokenMetadata afterMetadata = beforeMetadata.cloneOnlyTokenMap();
            afterMetadata.updateNormalToken(newToken, endpoint);
            allTokens.add(newToken);

            if (!findAffectedTokensAndRecord(beforeMetadata, afterMetadata,
                    allTokens, tokenToPendingSlot, keyspaceName))
            {
                return TransitPairResult.constraintViolation();
            }
        }

        return TransitPairResult.success(tokenToPendingSlot, allTokens);
    }

    /**
     * Find tokens affected by a topology change.
     *
     * Compares beforeMetadata vs afterMetadata for each token boundary.
     * For any token where a new replica appears (in after but not in before),
     * that's a pending replica taking over a slot.
     *
     * @return false if constraint violated (token already has a pending), true otherwise
     */
    private boolean findAffectedTokensAndRecord(
        TokenMetadata beforeMeta,
        TokenMetadata afterMeta,
        Set<Token> allTokens,
        Map<Token, Pair<InetAddressAndPort, InetAddressAndPort>> tokenToPendingSlot,
        String keyspaceName)
    {
        for (Token token : allTokens)
        {
            EndpointsForRange beforeReplicas = strategy.calculateNaturalReplicas(token, beforeMeta);
            EndpointsForRange afterReplicas = strategy.calculateNaturalReplicas(token, afterMeta);

            for (InetAddressAndPort pendingEp : afterReplicas.endpoints())
            {
                if (!beforeReplicas.endpoints().contains(pendingEp))
                {
                    // Find who this pending is replacing (in before but not in after)
                    InetAddressAndPort replacedEp = null;
                    for (InetAddressAndPort ep : beforeReplicas.endpoints())
                    {
                        if (!afterReplicas.endpoints().contains(ep))
                        {
                            replacedEp = ep;
                            break;
                        }
                    }

                    // Check for constraint violation: token already affected by another pending?
                    Pair<InetAddressAndPort, InetAddressAndPort> existing = tokenToPendingSlot.get(token);
                    if (existing != null)
                    {
                        logger.warn("Keyspace {} token {} already has pending {}" +
                                   " but {} also affects it - constraint violated",
                                   keyspaceName, token, existing.left, pendingEp);
                        return false;
                    }

                    tokenToPendingSlot.put(token, Pair.create(pendingEp, replacedEp));
                }
            }
        }
        return true;
    }
}
