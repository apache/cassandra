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
package org.apache.cassandra.dht.tokenallocator;

import java.util.*;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;

/**
 * A Replication Aware allocator for tokens, that attempts to ensure an even distribution of ownership across
 * the known cluster for the provided replication strategy.
 *
 * A unit is shorthand for a "unit of ownership" which translates roughly to a node, or a disk on the node,
 * a CPU on the node, or some other relevant unit of ownership. These units should be the lowest rung over which
 * ownership needs to be evenly distributed. At the moment only nodes as a whole are treated as units, but that
 * will change with the introduction of token ranges per disk.
 */
class ReplicationAwareTokenAllocator<Unit> extends TokenAllocatorBase<Unit>
{
    final Multimap<Unit, Token> unitToTokens;
    final int replicas;

    ReplicationAwareTokenAllocator(NavigableMap<Token, Unit> sortedTokens, ReplicationStrategy<Unit> strategy, IPartitioner partitioner)
    {
        super(sortedTokens, strategy, partitioner);
        unitToTokens = HashMultimap.create();
        for (Map.Entry<Token, Unit> en : sortedTokens.entrySet())
            unitToTokens.put(en.getValue(), en.getKey());
        this.replicas = strategy.replicas();
    }

    public int getReplicas()
    {
        return replicas;
    }

    public Collection<Token> addUnit(Unit newUnit, int numTokens)
    {
        assert !unitToTokens.containsKey(newUnit);

        if (unitCount() < replicas)
            // Allocation does not matter; everything replicates everywhere.
            return generateRandomTokens(newUnit, numTokens);
        if (numTokens > sortedTokens.size())
            // Some of the heuristics below can't deal with this case. Use random for now, later allocations can fix any problems this may cause.
            return generateRandomTokens(newUnit, numTokens);

        // ============= construct our initial token ring state =============

        double optTokenOwnership = optimalTokenOwnership(numTokens);
        Map<Object, GroupInfo> groups = Maps.newHashMap();
        Map<Unit, UnitInfo<Unit>> unitInfos = createUnitInfos(groups);
        if (groups.size() < replicas)
        {
            // We need at least replicas groups to do allocation correctly. If there aren't enough, 
            // use random allocation.
            // This part of the code should only be reached via the RATATest. StrategyAdapter should disallow
            // token allocation in this case as the algorithm is not able to cover the behavior of NetworkTopologyStrategy.
            return generateRandomTokens(newUnit, numTokens);
        }

        // initialise our new unit's state (with an idealised ownership)
        // strategy must already know about this unit
        UnitInfo<Unit> newUnitInfo = new UnitInfo<>(newUnit, numTokens * optTokenOwnership, groups, strategy);

        // build the current token ring state
        TokenInfo<Unit> tokens = createTokenInfos(unitInfos, newUnitInfo.group);
        newUnitInfo.tokenCount = numTokens;

        // ============= construct and rank our candidate token allocations =============

        // walk the token ring, constructing the set of candidates in ring order
        // as the midpoints between all existing tokens
        CandidateInfo<Unit> candidates = createCandidates(tokens, newUnitInfo, optTokenOwnership);

        // Evaluate the expected improvements from all candidates and form a priority queue.
        PriorityQueue<Weighted<CandidateInfo<Unit>>> improvements = new PriorityQueue<>(sortedTokens.size());
        CandidateInfo<Unit> candidate = candidates;
        do
        {
            double impr = evaluateImprovement(candidate, optTokenOwnership, 1.0 / numTokens);
            improvements.add(new Weighted<>(impr, candidate));
            candidate = candidate.next;
        } while (candidate != candidates);

        // ============= iteratively take the best candidate, and re-rank =============

        CandidateInfo<Unit> bestToken = improvements.remove().value;
        for (int vn = 1; ; ++vn)
        {
            candidates = bestToken.removeFrom(candidates);
            confirmCandidate(bestToken);

            if (vn == numTokens)
                break;

            while (true)
            {
                // Get the next candidate in the queue. Its improvement may have changed (esp. if multiple tokens
                // were good suggestions because they could improve the same problem)-- evaluate it again to check
                // if it is still a good candidate.
                bestToken = improvements.remove().value;
                double impr = evaluateImprovement(bestToken, optTokenOwnership, (vn + 1.0) / numTokens);
                Weighted<CandidateInfo<Unit>> next = improvements.peek();

                // If it is better than the next in the queue, it is good enough. This is a heuristic that doesn't
                // get the best results, but works well enough and on average cuts search time by a factor of O(vnodes).
                if (next == null || impr >= next.weight)
                    break;
                improvements.add(new Weighted<>(impr, bestToken));
            }
        }

        return ImmutableList.copyOf(unitToTokens.get(newUnit));
    }

    private Collection<Token> generateRandomTokens(Unit newUnit, int numTokens)
    {
        Set<Token> tokens = new HashSet<>(numTokens);
        while (tokens.size() < numTokens)
        {
            Token token = partitioner.getRandomToken();
            if (!sortedTokens.containsKey(token))
            {
                tokens.add(token);
                sortedTokens.put(token, newUnit);
                unitToTokens.put(newUnit, token);
            }
        }
        return tokens;
    }

    /**
     * Construct the token ring as a CircularList of TokenInfo,
     * and populate the ownership of the UnitInfo's provided
     */
    private TokenInfo<Unit> createTokenInfos(Map<Unit, UnitInfo<Unit>> units, GroupInfo newUnitGroup)
    {
        // build the circular list
        TokenInfo<Unit> prev = null;
        TokenInfo<Unit> first = null;
        for (Map.Entry<Token, Unit> en : sortedTokens.entrySet())
        {
            Token t = en.getKey();
            UnitInfo<Unit> ni = units.get(en.getValue());
            TokenInfo<Unit> ti = new TokenInfo<>(t, ni);
            first = ti.insertAfter(first, prev);
            prev = ti;
        }

        TokenInfo<Unit> curr = first;
        do
        {
            populateTokenInfoAndAdjustUnit(curr, newUnitGroup);
            curr = curr.next;
        } while (curr != first);

        return first;
    }

    private CandidateInfo<Unit> createCandidates(TokenInfo<Unit> tokens, UnitInfo<Unit> newUnitInfo, double initialTokenOwnership)
    {
        TokenInfo<Unit> curr = tokens;
        CandidateInfo<Unit> first = null;
        CandidateInfo<Unit> prev = null;
        do
        {
            CandidateInfo<Unit> candidate = new CandidateInfo<Unit>(partitioner.midpoint(curr.prev.token, curr.token), curr, newUnitInfo);
            first = candidate.insertAfter(first, prev);

            candidate.replicatedOwnership = initialTokenOwnership;
            populateCandidate(candidate);

            prev = candidate;
            curr = curr.next;
        } while (curr != tokens);
        prev.next = first;
        return first;
    }

    private void populateCandidate(CandidateInfo<Unit> candidate)
    {
        // Only finding replication start would do.
        populateTokenInfo(candidate, candidate.owningUnit.group);
    }

    /**
     * Incorporates the selected candidate into the ring, adjusting ownership information and calculated token
     * information.
     */
    private void confirmCandidate(CandidateInfo<Unit> candidate)
    {
        // This process is less efficient than it could be (loops through each vnode's replication span instead
        // of recalculating replicationStart, replicationThreshold from existing data + new token data in an O(1)
        // case analysis similar to evaluateImprovement). This is fine as the method does not dominate processing
        // time.

        // Put the accepted candidate in the token list.
        UnitInfo<Unit> newUnit = candidate.owningUnit;
        Token newToken = candidate.token;
        sortedTokens.put(newToken, newUnit.unit);
        unitToTokens.put(newUnit.unit, newToken);

        TokenInfo<Unit> prev = candidate.prevInRing();
        TokenInfo<Unit> newTokenInfo = new TokenInfo<>(newToken, newUnit);
        newTokenInfo.replicatedOwnership = candidate.replicatedOwnership;
        newTokenInfo.insertAfter(prev, prev);   // List is not empty so this won't need to change head of list.

        // Update data for candidate.
        populateTokenInfoAndAdjustUnit(newTokenInfo, newUnit.group);

        ReplicationVisitor replicationVisitor = new ReplicationVisitor();
        assert newTokenInfo.next == candidate.split;
        for (TokenInfo<Unit> curr = newTokenInfo.next; !replicationVisitor.visitedAll(); curr = curr.next)
        {
            // update the candidate between curr and next
            candidate = candidate.next;
            populateCandidate(candidate);

            if (!replicationVisitor.add(curr.owningUnit.group))
                continue;    // If we've already seen this group, the token cannot be affected.

            populateTokenInfoAndAdjustUnit(curr, newUnit.group);
        }

        replicationVisitor.clean();
    }

    /**
     * Calculates the {@code replicationStart} of a token, as well as {@code replicationThreshold} which is chosen in a way
     * that permits {@code findUpdatedReplicationStart} to quickly identify changes in ownership.
     */
    private Token populateTokenInfo(BaseTokenInfo<Unit, ?> token, GroupInfo newUnitGroup)
    {
        GroupInfo tokenGroup = token.owningUnit.group;
        PopulateVisitor visitor = new PopulateVisitor();

        // Replication start = the end of a token from the RF'th different group seen before the token.
        Token replicationStart;
        // The end of a token from the RF-1'th different group seen before the token.
        Token replicationThreshold = token.token;
        GroupInfo currGroup;
        for (TokenInfo<Unit> curr = token.prevInRing(); ; curr = curr.prev)
        {
            replicationStart = curr.token;
            currGroup = curr.owningUnit.group;
            if (!visitor.add(currGroup))
                continue; // Group is already seen.
            if (visitor.visitedAll())
                break;

            replicationThreshold = replicationStart;
            // Another instance of the same group precedes us in the replication range of the ring,
            // so this is where our replication range begins
            if (currGroup == tokenGroup)
                break;
        }
        if (newUnitGroup == tokenGroup)
            // new token is always a boundary (as long as it's closer than replicationStart)
            replicationThreshold = token.token;
        else if (newUnitGroup != currGroup && visitor.seen(newUnitGroup))
            // already has new group in replication span before last seen. cannot be affected
            replicationThreshold = replicationStart;
        visitor.clean();

        token.replicationThreshold = replicationThreshold;
        token.replicationStart = replicationStart;
        return replicationStart;
    }

    private void populateTokenInfoAndAdjustUnit(TokenInfo<Unit> populate, GroupInfo newUnitGroup)
    {
        Token replicationStart = populateTokenInfo(populate, newUnitGroup);
        double newOwnership = replicationStart.size(populate.token);
        double oldOwnership = populate.replicatedOwnership;
        populate.replicatedOwnership = newOwnership;
        populate.owningUnit.ownership += newOwnership - oldOwnership;
    }

    /**
     * Evaluates the improvement in variance for both units and individual tokens when candidate is inserted into the
     * ring.
     */
    private double evaluateImprovement(CandidateInfo<Unit> candidate, double optTokenOwnership, double newUnitMult)
    {
        double tokenChange = 0;

        UnitInfo<Unit> candidateUnit = candidate.owningUnit;
        Token candidateEnd = candidate.token;

        // Form a chain of units affected by the insertion to be able to qualify change of unit ownership.
        // A unit may be affected more than once.
        UnitAdjustmentTracker<Unit> unitTracker = new UnitAdjustmentTracker<>(candidateUnit);

        // Reflect change in ownership of the splitting token (candidate).
        tokenChange += applyOwnershipAdjustment(candidate, candidateUnit, candidate.replicationStart, candidateEnd, optTokenOwnership, unitTracker);

        // Loop through all vnodes that replicate candidate or split and update their ownership.
        ReplicationVisitor replicationVisitor = new ReplicationVisitor();
        for (TokenInfo<Unit> curr = candidate.split; !replicationVisitor.visitedAll(); curr = curr.next)
        {
            UnitInfo<Unit> currUnit = curr.owningUnit;

            if (!replicationVisitor.add(currUnit.group))
                continue;    // If this group is already seen, the token cannot be affected.

            Token replicationEnd = curr.token;
            Token replicationStart = findUpdatedReplicationStart(curr, candidate);
            tokenChange += applyOwnershipAdjustment(curr, currUnit, replicationStart, replicationEnd, optTokenOwnership, unitTracker);
        }
        replicationVisitor.clean();

        double nodeChange = unitTracker.calculateUnitChange(newUnitMult, optTokenOwnership);
        return -(tokenChange + nodeChange);
    }

    /**
     * Returns the start of the replication span for the token {@code curr} when {@code candidate} is inserted into the
     * ring.
     */
    private Token findUpdatedReplicationStart(TokenInfo<Unit> curr, CandidateInfo<Unit> candidate)
    {
        return furtherStartToken(curr.replicationThreshold, candidate.token, curr.token);
    }

    /**
     * Applies the ownership adjustment for the given element, updating tracked unit ownership and returning the change
     * of variance.
     */
    private double applyOwnershipAdjustment(BaseTokenInfo<Unit, ?> curr, UnitInfo<Unit> currUnit,
            Token replicationStart, Token replicationEnd,
            double optTokenOwnership, UnitAdjustmentTracker<Unit> unitTracker)
    {
        double oldOwnership = curr.replicatedOwnership;
        double newOwnership = replicationStart.size(replicationEnd);
        double tokenCount = currUnit.tokenCount;
        assert tokenCount > 0;
        unitTracker.add(currUnit, newOwnership - oldOwnership);
        return (sq(newOwnership - optTokenOwnership) - sq(oldOwnership - optTokenOwnership)) / sq(tokenCount);
    }

    /**
     * Tracker for unit ownership changes. The changes are tracked by a chain of UnitInfos where the adjustedOwnership
     * field is being updated as we see changes in token ownership.
     *
     * The chain ends with an element that points to itself; this element must be specified as argument to the
     * constructor as well as be the first unit with which 'add' is called; when calculating the variance change
     * a separate multiplier is applied to it (used to permit more freedom in choosing the first tokens of a unit).
     */
    private static class UnitAdjustmentTracker<Unit>
    {
        UnitInfo<Unit> unitsChain;

        UnitAdjustmentTracker(UnitInfo<Unit> newUnit)
        {
            unitsChain = newUnit;
        }

        void add(UnitInfo<Unit> currUnit, double diff)
        {
            if (currUnit.prevUsed == null)
            {
                assert unitsChain.prevUsed != null || currUnit == unitsChain;

                currUnit.adjustedOwnership = currUnit.ownership + diff;
                currUnit.prevUsed = unitsChain;
                unitsChain = currUnit;
            }
            else
            {
                currUnit.adjustedOwnership += diff;
            }
        }

        double calculateUnitChange(double newUnitMult, double optTokenOwnership)
        {
            double unitChange = 0;
            UnitInfo<Unit> unitsChain = this.unitsChain;
            // Now loop through the units chain and add the unit-level changes. Also clear the groups' seen marks.
            while (true)
            {
                double newOwnership = unitsChain.adjustedOwnership;
                double oldOwnership = unitsChain.ownership;
                double tokenCount = unitsChain.tokenCount;
                double diff = (sq(newOwnership / tokenCount - optTokenOwnership) - sq(oldOwnership / tokenCount - optTokenOwnership));
                UnitInfo<Unit> prev = unitsChain.prevUsed;
                unitsChain.prevUsed = null;
                if (unitsChain != prev)
                    unitChange += diff;
                else
                {
                    unitChange += diff * newUnitMult;
                    break;
                }
                unitsChain = prev;
            }
            this.unitsChain = unitsChain;
            return unitChange;
        }
    }


    /**
     * Helper class for marking/unmarking visited a chain of groups
     */
    private abstract class GroupVisitor
    {
        GroupInfo groupChain = GroupInfo.TERMINATOR;
        int seen = 0;

        abstract GroupInfo prevSeen(GroupInfo group);
        abstract void setPrevSeen(GroupInfo group, GroupInfo prevSeen);

        // true iff this is the first time we've visited this group
        boolean add(GroupInfo group)
        {
            if (prevSeen(group) != null)
                return false;
            ++seen;
            setPrevSeen(group, groupChain);
            groupChain = group;
            return true;
        }

        boolean visitedAll()
        {
            return seen >= replicas;
        }

        boolean seen(GroupInfo group)
        {
            return prevSeen(group) != null;
        }

        // Clean group seen markers.
        void clean()
        {
            GroupInfo groupChain = this.groupChain;
            while (groupChain != GroupInfo.TERMINATOR)
            {
                GroupInfo prev = prevSeen(groupChain);
                setPrevSeen(groupChain, null);
                groupChain = prev;
            }
            this.groupChain = GroupInfo.TERMINATOR;
        }
    }

    private class ReplicationVisitor extends GroupVisitor
    {
        GroupInfo prevSeen(GroupInfo group)
        {
            return group.prevSeen;
        }

        void setPrevSeen(GroupInfo group, GroupInfo prevSeen)
        {
            group.prevSeen = prevSeen;
        }
    }

    private class PopulateVisitor extends GroupVisitor
    {
        GroupInfo prevSeen(GroupInfo group)
        {
            return group.prevPopulate;
        }

        void setPrevSeen(GroupInfo group, GroupInfo prevSeen)
        {
            group.prevPopulate = prevSeen;
        }
    }

    private double optimalTokenOwnership(int tokensToAdd)
    {
        return 1.0 * replicas / (sortedTokens.size() + tokensToAdd);
    }

    /**
     * Selects from {@code t1}, {@code t2} the token that forms a bigger range with {@code towards} as the upper bound,
     * taking into account wrapping.
     * Unlike Token.size(), equality is taken to mean "same as" rather than covering the whole range.
     */
    private static Token furtherStartToken(Token t1, Token t2, Token towards)
    {
        if (t1.equals(towards))
            return t2;
        if (t2.equals(towards))
            return t1;

        return t1.size(towards) > t2.size(towards) ? t1 : t2;
    }

    private static double sq(double d)
    {
        return d * d;
    }


    /**
     * For testing, remove the given unit preserving correct state of the allocator.
     */
    void removeUnit(Unit n)
    {
        Collection<Token> tokens = unitToTokens.removeAll(n);
        sortedTokens.keySet().removeAll(tokens);
    }

    public int unitCount()
    {
        return unitToTokens.asMap().size();
    }

    public String toString()
    {
        return getClass().getSimpleName();
    }

    /**
     * TokenInfo about candidate new tokens/vnodes.
     */
    private static class CandidateInfo<Unit> extends BaseTokenInfo<Unit, CandidateInfo<Unit>>
    {
        // directly preceding token in the current token ring
        final TokenInfo<Unit> split;

        public CandidateInfo(Token token, TokenInfo<Unit> split, UnitInfo<Unit> owningUnit)
        {
            super(token, owningUnit);
            this.split = split;
        }

        TokenInfo<Unit> prevInRing()
        {
            return split.prev;
        }
    }

    static void dumpTokens(String lead, BaseTokenInfo<?, ?> tokens)
    {
        BaseTokenInfo<?, ?> token = tokens;
        do
        {
            System.out.format("%s%s: rs %s rt %s size %.2e%n", lead, token, token.replicationStart, token.replicationThreshold, token.replicatedOwnership);
            token = token.next;
        } while (token != null && token != tokens);
    }
}

