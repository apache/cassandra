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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;

public abstract class TokenAllocatorBase<Unit> implements TokenAllocator<Unit>
{
    static final double MIN_INITIAL_SPLITS_RATIO = 1.0 - 1.0 / Math.sqrt(5.0);
    static final double MAX_INITIAL_SPLITS_RATIO = MIN_INITIAL_SPLITS_RATIO + 0.075;

    final NavigableMap<Token, Unit> sortedTokens;
    final ReplicationStrategy<Unit> strategy;
    final IPartitioner partitioner;

    protected TokenAllocatorBase(NavigableMap<Token, Unit> sortedTokens,
                             ReplicationStrategy<Unit> strategy,
                             IPartitioner partitioner)
    {
        this.sortedTokens = sortedTokens;
        this.strategy = strategy;
        this.partitioner = partitioner;
    }

    public abstract int getReplicas();

    protected Map<Unit, UnitInfo<Unit>> createUnitInfos(Map<Object, GroupInfo> groups)
    {
        Map<Unit, UnitInfo<Unit>> map = Maps.newHashMap();
        for (Unit n : sortedTokens.values())
        {
            UnitInfo<Unit> ni = map.get(n);
            if (ni == null)
                map.put(n, ni = new UnitInfo<>(n, 0, groups, strategy));
            ni.tokenCount++;
        }
        return map;
    }

    private Map.Entry<Token, Unit> mapEntryFor(Token t)
    {
        Map.Entry<Token, Unit> en = sortedTokens.floorEntry(t);
        if (en == null)
            en = sortedTokens.lastEntry();
        return en;
    }

    Unit unitFor(Token t)
    {
        return mapEntryFor(t).getValue();
    }

    // get or initialise the shared GroupInfo associated with the unit
    private static <Unit> GroupInfo getGroup(Unit unit, Map<Object, GroupInfo> groupMap, ReplicationStrategy<Unit> strategy)
    {
        Object groupClass = strategy.getGroup(unit);
        GroupInfo group = groupMap.get(groupClass);
        if (group == null)
            groupMap.put(groupClass, group = new GroupInfo(groupClass));
        return group;
    }

    Collection<Token> generateSplits(Unit newUnit, int numTokens)
    {
        return generateSplits(newUnit, numTokens, MIN_INITIAL_SPLITS_RATIO, MAX_INITIAL_SPLITS_RATIO);
    }
    /**
     * Selects tokens by repeatedly splitting the largest range in the ring at the given ratio.
     *
     * This is used to choose tokens for the first nodes in the ring where the algorithm cannot be applied (e.g. when
     * number of nodes < RF). It generates a reasonably chaotic initial token split, after which the algorithm behaves
     * well for an unbounded number of nodes.
     */
    Collection<Token> generateSplits(Unit newUnit, int numTokens, double minRatio, double maxRatio)
    {
        Random random = new Random(sortedTokens.size());

        double potentialRatioGrowth = maxRatio - minRatio;

        List<Token> tokens = Lists.newArrayListWithExpectedSize(numTokens);

        if (sortedTokens.isEmpty())
        {
            // Select a random start token. This has no effect on distribution, only on where the local ring is "centered".
            // Using a random start decreases the chances of clash with the tokens of other datacenters in the ring.
            Token t = partitioner.getRandomToken();
            tokens.add(t);
            sortedTokens.put(t, newUnit);
        }

        while (tokens.size() < numTokens)
        {
            // split max span using given ratio
            Token prev = sortedTokens.lastKey();
            double maxsz = 0;
            Token t1 = null;
            Token t2 = null;
            for (Token curr : sortedTokens.keySet())
            {
                double sz = prev.size(curr);
                if (sz > maxsz)
                {
                    maxsz = sz;
                    t1 = prev; t2 = curr;
                }
                prev = curr;
            }
            assert t1 != null;
            Token t = partitioner.split(t1, t2, minRatio + potentialRatioGrowth * random.nextDouble());
            tokens.add(t);
            sortedTokens.put(t, newUnit);
        }
        return tokens;
    }

    /**
     * Unique group object that one or more UnitInfo objects link to.
     */
    static class GroupInfo
    {
        /**
         * Group identifier given by ReplicationStrategy.getGroup(Unit).
         */
        final Object group;

        /**
         * Seen marker. When non-null, the group is already seen in replication walks.
         * Also points to previous seen group to enable walking the seen groups and clearing the seen markers.
         */
        GroupInfo prevSeen = null;
        /**
         * Same marker/chain used by populateTokenInfo.
         */
        GroupInfo prevPopulate = null;

        /**
         * Value used as terminator for seen chains.
         */
        static GroupInfo TERMINATOR = new GroupInfo(null);

        public GroupInfo(Object group)
        {
            this.group = group;
        }

        public String toString()
        {
            return group.toString() + (prevSeen != null ? "*" : "");
        }
    }

    /**
     * Unit information created and used by ReplicationAwareTokenDistributor. Contained vnodes all point to the same
     * instance.
     */
    static class UnitInfo<Unit>
    {
        final Unit unit;
        final GroupInfo group;
        double ownership;
        int tokenCount;

        /**
         * During evaluateImprovement this is used to form a chain of units affected by the candidate insertion.
         */
        UnitInfo<Unit> prevUsed;
        /**
         * During evaluateImprovement this holds the ownership after the candidate insertion.
         */
        double adjustedOwnership;

        private UnitInfo(Unit unit, GroupInfo group)
        {
            this.unit = unit;
            this.group = group;
            this.tokenCount = 0;
        }

        public UnitInfo(Unit unit, double ownership, Map<Object, GroupInfo> groupMap, ReplicationStrategy<Unit> strategy)
        {
            this(unit, getGroup(unit, groupMap, strategy));
            this.ownership = ownership;
        }

        public String toString()
        {
            return String.format("%s%s(%.2e)%s",
                                 unit, unit == group.group ? (group.prevSeen != null ? "*" : "") : ":" + group.toString(),
                                 ownership, prevUsed != null ? (prevUsed == this ? "#" : "->" + prevUsed.toString()) : "");
        }
    }

    private static class CircularList<T extends CircularList<T>>
    {
        T prev;
        T next;

        /**
         * Inserts this after unit in the circular list which starts at head. Returns the new head of the list, which
         * only changes if head was null.
         */
        @SuppressWarnings("unchecked")
        T insertAfter(T head, T unit)
        {
            if (head == null)
            {
                return prev = next = (T) this;
            }
            assert unit != null;
            assert unit.next != null;
            prev = unit;
            next = unit.next;
            prev.next = (T) this;
            next.prev = (T) this;
            return head;
        }

        /**
         * Removes this from the list that starts at head. Returns the new head of the list, which only changes if the
         * head was removed.
         */
        T removeFrom(T head)
        {
            next.prev = prev;
            prev.next = next;
            return this == head ? (this == next ? null : next) : head;
        }
    }

    static class BaseTokenInfo<Unit, T extends BaseTokenInfo<Unit, T>> extends CircularList<T>
    {
        final Token token;
        final UnitInfo<Unit> owningUnit;

        /**
         * Start of the replication span for the vnode, i.e. the first token of the RF'th group seen before the token.
         * The replicated ownership of the unit is the range between {@code replicationStart} and {@code token}.
         */
        Token replicationStart;
        /**
         * The closest position that the new candidate can take to become the new replication start. If candidate is
         * closer, the start moves to this position. Used to determine replicationStart after insertion of new token.
         *
         * Usually the RF minus one boundary, i.e. the first token of the RF-1'th group seen before the token.
         */
        Token replicationThreshold;
        /**
         * Current replicated ownership. This number is reflected in the owning unit's ownership.
         */
        double replicatedOwnership = 0;

        public BaseTokenInfo(Token token, UnitInfo<Unit> owningUnit)
        {
            this.token = token;
            this.owningUnit = owningUnit;
        }

        public String toString()
        {
            return String.format("%s(%s)", token, owningUnit);
        }

        /**
         * Previous unit in the token ring. For existing tokens this is prev,
         * for candidates it's "split".
         */
        TokenInfo<Unit> prevInRing()
        {
            return null;
        }
    }

    /**
     * TokenInfo about existing tokens/vnodes.
     */
    static class TokenInfo<Unit> extends BaseTokenInfo<Unit, TokenInfo<Unit>>
    {
        public TokenInfo(Token token, UnitInfo<Unit> owningUnit)
        {
            super(token, owningUnit);
        }

        TokenInfo<Unit> prevInRing()
        {
            return prev;
        }
    }

    static class Weighted<T> implements Comparable<Weighted<T>>
    {
        final double weight;
        final T value;

        public Weighted(double weight, T value)
        {
            this.weight = weight;
            this.value = value;
        }

        @Override
        public int compareTo(Weighted<T> o)
        {
            int cmp = Double.compare(o.weight, this.weight);
            return cmp;
        }

        @Override
        public String toString()
        {
            return String.format("%s<%s>", value, weight);
        }
    }
}
