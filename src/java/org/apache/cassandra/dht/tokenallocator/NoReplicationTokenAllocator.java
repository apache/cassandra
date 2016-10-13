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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

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

    private Collection<Token> generateRandomTokens(UnitInfo<Unit> newUnit, int numTokens, Map<Unit, UnitInfo<Unit>> unitInfos)
    {
        Set<Token> tokens = new HashSet<>(numTokens);
        while (tokens.size() < numTokens)
        {
            Token token = partitioner.getRandomToken();
            if (!sortedTokens.containsKey(token))
            {
                tokens.add(token);
                sortedTokens.put(token, newUnit.unit);
            }
        }
        unitInfos.put(newUnit.unit, newUnit);
        createTokenInfos(unitInfos);
        return tokens;
    }

    public Collection<Token> addUnit(Unit newUnit, int numTokens)
    {
        assert !tokensInUnits.containsKey(newUnit);

        Map<Object, GroupInfo> groups = Maps.newHashMap();
        UnitInfo<Unit> newUnitInfo = new UnitInfo<>(newUnit, 0, groups, strategy);
        Map<Unit, UnitInfo<Unit>> unitInfos = createUnitInfos(groups);

        if (unitInfos.isEmpty())
            return generateRandomTokens(newUnitInfo, numTokens, unitInfos);

        if (numTokens > sortedTokens.size())
            return generateRandomTokens(newUnitInfo, numTokens, unitInfos);

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

            double toTakeOver = unit.weight - targetAverage;
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
            ++nr;
        }
        sortedUnits.add(new Weighted<>(newUnitInfo.ownership, newUnitInfo));

        return newTokens;
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
    }

    public int getReplicas()
    {
        return 1;
    }
}
