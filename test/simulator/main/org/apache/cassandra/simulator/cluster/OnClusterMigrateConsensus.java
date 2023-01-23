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

package org.apache.cassandra.simulator.cluster;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.utils.Pair;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.simulator.Action.Modifiers.NONE;

class OnClusterMigrateConsensus extends Action
{
    private final KeyspaceActions actions;

    OnClusterMigrateConsensus(KeyspaceActions actions)
    {
        super("Performing consensus migration", NONE, NONE);
        this.actions = actions;
    }

    public ActionList performSimple()
    {
        List<Action> result = new ArrayList<>();
        List<Pair<Integer, Entry<String, String>>> ranges = new ArrayList<>();
        TokenMetadata tmd = actions.tokenMetadata;
        IPartitioner partitioner = tmd.partitioner;
        List<InetAddressAndPort> endpoints = new ArrayList<>(tmd.getEndpointToTokenMapForReading().keySet());
        Collections.sort(endpoints);
        for (int i = 0; i < endpoints.size(); i++)
        {
            InetAddressAndPort ep = endpoints.get(i);
            List<Range<Token>> tokenRanges = new ArrayList<>();
            for (Token token : tmd.sortedTokens())
            {
                EndpointsForRange replicas = actions.strategy().calculateNaturalReplicas(token, tmd);
                if (replicas.size() > 0 && replicas.get(0).endpoint().equals(ep))
                {
                    checkState(replicas.get(0).isFull());
                    tokenRanges.add(new Range<>(tmd.getPredecessor(token), token));
                }
            }
            checkState(tokenRanges.size() == 1, "Expect only 1, not handling vnodes tokenRanges " + tokenRanges);
            Range<Token> tokenRange = tokenRanges.get(0);
            Range<Token> firstRange = new Range<>(tokenRange.left, partitioner.split(tokenRange.left, tokenRange.right, 0.33));
            Range<Token> secondRange = new Range<>(firstRange.right, partitioner.split(tokenRange.left, tokenRange.right, 0.66));
            Range<Token> thirdRange = new Range<>(secondRange.right, tokenRange.right);
            ranges.add(Pair.create(i + 1, new SimpleEntry<>(firstRange.left.toString(), firstRange.right.toString())));
            ranges.add(Pair.create(i + 1, new SimpleEntry<>(secondRange.left.toString(), secondRange.right.toString())));
            ranges.add(Pair.create(i + 1, new SimpleEntry<>(thirdRange.left.toString(), thirdRange.right.toString())));
        }

        Collections.shuffle(ranges);

        System.out.println("Ranges to migrate " + ranges);

        ranges.stream().forEach(p -> result.add(new OnClusterMigrateConsensusOneRange(actions, p.left(), p.right())));
        return ActionList.of(result);
    }
}
