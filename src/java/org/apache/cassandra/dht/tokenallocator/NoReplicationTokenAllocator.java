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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.PriorityQueue;
import java.util.Queue;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;

public class NoReplicationTokenAllocator<Unit> extends TokenAllocatorBase<Unit>
{
    PriorityQueue<Weighted<UnitInfo>> sortedUnits = Queues.newPriorityQueue();
    Map<Unit, PriorityQueue<Weighted<TokenInfo>>> tokensInUnits = Maps.newHashMap();

    private static final double MAX_TAKEOVER_RATIO = 0.90;
    private static final double MIN_TAKEOVER_RATIO = 1.0 - MAX_TAKEOVER_RATIO;

    public NoReplicationTokenAllocator(NavigableMap<Token, Unit> sortedTokens,
                                       ReplicationStrategy<Unit> strategy,
                                       IPartitioner partitioner)
    {
        super(sortedTokens, strategy, partitioner);
    }

    /**
     * Construct the token ring as a CircularList of TokenInfo,
     * and populate the ownership of the UnitInfo's provided
     */
    private TokenInfo<Unit> createTokenInfos(Map<Unit, UnitInfo<Unit>> units)
    {
        if (units.isEmpty())
            return null;

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
        tokensInUnits.clear();
        sortedUnits.clear();
        do
        {
            populateTokenInfoAndAdjustUnit(curr);
            curr = curr.next;
        } while (curr != first);

        for (UnitInfo<Unit> unitInfo : units.values())
        {
            sortedUnits.add(new Weighted<UnitInfo>(unitInfo.ownership, unitInfo));
        }

        TokenAllocatorDiagnostics.tokenInfosCreated(this, sortedUnits, sortedTokens, first);
        return first;
    }

    /**
     * Used in tests.
     */
    protected void createTokenInfos()
    {
        createTokenInfos(createUnitInfos(Maps.newHashMap()));
    }

    private void populateTokenInfoAndAdjustUnit(TokenInfo<Unit> token)
    {
        token.replicationStart = token.prevInRing().token;
        token.replicationThreshold = token.token;
        token.replicatedOwnership = token.replicationStart.size(token.token);
        token.owningUnit.ownership += token.replicatedOwnership;

        PriorityQueue<Weighted<TokenInfo>> unitTokens = tokensInUnits.get(token.owningUnit.unit);
        if (unitTokens == null)
        {
            unitTokens = Queues.newPriorityQueue();
            tokensInUnits.put(token.owningUnit.unit, unitTokens);
        }
        unitTokens.add(new Weighted<TokenInfo>(token.replicatedOwnership, token));
    }

    public Collection<Token> addUnit(Unit newUnit, int numTokens)
    {
        assert !tokensInUnits.containsKey(newUnit);

        Map<Object, GroupInfo> groups = Maps.newHashMap();
        UnitInfo<Unit> newUnitInfo = new UnitInfo<>(newUnit, 0, groups, strategy);
        Map<Unit, UnitInfo<Unit>> unitInfos = createUnitInfos(groups);

        if (unitInfos.isEmpty())
            return generateSplits(newUnit, numTokens);

        if (numTokens > sortedTokens.size())
            return generateSplits(newUnit, numTokens);

        TokenInfo<Unit> head = createTokenInfos(unitInfos);

        // Select the nodes we will work with, extract them from sortedUnits and calculate targetAverage
        double targetAverage = 0.0;
        double sum = 0.0;
        List<Weighted<UnitInfo>> unitsToChange = new ArrayList<>();

        for (int i = 0; i < numTokens; i++)
        {
            Weighted<UnitInfo> unit = sortedUnits.peek();

            if (unit == null)
                break;

            sum += unit.weight;
            double average = sum / (unitsToChange.size() + 2); // unit and newUnit must be counted
            if (unit.weight <= average)
                // No point to include later nodes, target can only decrease from here.
                break;

            sortedUnits.remove();
            unitsToChange.add(unit);
            targetAverage = average;
        }

        List<Token> newTokens = Lists.newArrayListWithCapacity(numTokens);
        // Generate different size nodes, at most at 2/(numTokens*2+1) difference,
        // but tighten the spread as the number of nodes grows (since it increases the time until we need to use nodes
        // we have just split).
        double sizeCorrection = Math.min(1.0, (numTokens + 1.0) / (unitInfos.size() + 1.0));
        double spread = targetAverage * sizeCorrection * 2.0 / (2 * numTokens + 1);

        // The biggest target is assigned to the biggest existing node. This should result in better balance in
        // the amount of data that needs to be streamed from the different sources to the new node.
        double target = targetAverage + spread / 2;

        // This step intentionally divides by the count (rather than count - 1) because we also need to count the new
        // node. This leaves the last position in the spread (i.e. the smallest size, least data to stream) for it.
        double step = spread / unitsToChange.size();
        int nr = 0;
        // calculate the tokens
        for (Weighted<UnitInfo> unit : unitsToChange)
        {
            // TODO: Any better ways to assign how many tokens to change in each node?
            int tokensToChange = numTokens / unitsToChange.size() + (nr < numTokens % unitsToChange.size() ? 1 : 0);

            Queue<Weighted<TokenInfo>> unitTokens = tokensInUnits.get(unit.value.unit);
            List<Weighted<TokenInfo>> tokens = Lists.newArrayListWithCapacity(tokensToChange);

            double workWeight = 0;
            // Extract biggest vnodes and calculate how much weight we can work with.
            for (int i = 0; i < tokensToChange; i++)
            {
                Weighted<TokenInfo> wt = unitTokens.remove();
                tokens.add(wt);
                workWeight += wt.weight;
                unit.value.ownership -= wt.weight;
            }

            double toTakeOver = unit.weight - target;
            // Split toTakeOver proportionally between the vnodes.
            for (Weighted<TokenInfo> wt : tokens)
            {
                double slice;
                Token token;

                if (toTakeOver < workWeight)
                {
                    // Spread decrease.
                    slice = toTakeOver / workWeight;

                    if (slice < MIN_TAKEOVER_RATIO)
                        slice = MIN_TAKEOVER_RATIO;
                    if (slice > MAX_TAKEOVER_RATIO)
                        slice = MAX_TAKEOVER_RATIO;
                }
                else
                {
                    slice = MAX_TAKEOVER_RATIO;
                }
                token = partitioner.split(wt.value.prevInRing().token, wt.value.token, slice);

                //Token selected, now change all data
                sortedTokens.put(token, newUnit);

                TokenInfo<Unit> ti = new TokenInfo<>(token, newUnitInfo);

                ti.insertAfter(head, wt.value.prevInRing());

                populateTokenInfoAndAdjustUnit(ti);
                populateTokenInfoAndAdjustUnit(wt.value);
                newTokens.add(token);
            }

            // adjust the weight for current unit
            sortedUnits.add(new Weighted<>(unit.value.ownership, unit.value));
            target -= step;
            ++nr;
        }
        sortedUnits.add(new Weighted<>(newUnitInfo.ownership, newUnitInfo));

        TokenAllocatorDiagnostics.unitedAdded(this, numTokens, sortedUnits, sortedTokens, newTokens, newUnit);
        return newTokens;
    }

    @Override
    Collection<Token> generateSplits(Unit newUnit, int numTokens)
    {
        Collection<Token> tokens = super.generateSplits(newUnit, numTokens);
        TokenAllocatorDiagnostics.splitsGenerated(this, numTokens, sortedUnits, sortedTokens, newUnit, tokens);
        return tokens;
    }

    /**
     * For testing, remove the given unit preserving correct state of the allocator.
     */
    void removeUnit(Unit n)
    {
        Iterator<Weighted<UnitInfo>> it = sortedUnits.iterator();
        while (it.hasNext())
        {
            if (it.next().value.unit.equals(n))
            {
                it.remove();
                break;
            }
        }

        PriorityQueue<Weighted<TokenInfo>> tokenInfos = tokensInUnits.remove(n);
        Collection<Token> tokens = Lists.newArrayListWithCapacity(tokenInfos.size());
        for (Weighted<TokenInfo> tokenInfo : tokenInfos)
        {
            tokens.add(tokenInfo.value.token);
        }
        sortedTokens.keySet().removeAll(tokens);
        TokenAllocatorDiagnostics.unitRemoved(this, n, sortedUnits, sortedTokens);
    }

    public int getReplicas()
    {
        return 1;
    }

    public String toString()
    {
        return getClass().getSimpleName();
    }
}
