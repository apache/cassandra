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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.consensus.TransactionalMode;
import org.apache.cassandra.service.consensus.migration.TransactionalMigrationFromMode;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.simulator.RandomSource;
import org.apache.cassandra.simulator.systems.NonInterceptible;
import org.apache.cassandra.tcm.compatibility.TokenRingUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.simulator.Action.Modifiers.NONE;
import static org.apache.cassandra.simulator.systems.NonInterceptible.Permit.REQUIRED;

class OnClusterMigrateConsensus extends Action
{
    private final KeyspaceActions actions;
    private final TransactionalMode fromMode;
    private final TransactionalMode targetMode;

    OnClusterMigrateConsensus(KeyspaceActions actions, TransactionalMode fromMode, TransactionalMode targetMode)
    {
        super("Performing consensus migration from " + fromMode + " to " + targetMode, NONE, NONE);
        this.actions = actions;
        this.fromMode = fromMode;
        this.targetMode = targetMode;
    }

    @Override
    public ActionList performSimple()
    {
        RandomSource rs = actions.random;
        IInvokableInstance instance1 = actions.cluster.get(1);
        String keyspace = actions.keyspace;
        String table = actions.table;
        for (IInvokableInstance instance : actions.cluster)
        {
            if (instance.isShutdown())
                continue;
            TransactionalMigrationFromMode transactionalMigrationFrom = TransactionalMigrationFromMode.valueOf(
                NonInterceptible.apply(REQUIRED, () -> instance1.unsafeCallOnThisThread(() -> Schema.instance.getTableMetadata(keyspace, table).params.transactionalMigrationFrom.toString())));
            checkState(transactionalMigrationFrom == TransactionalMigrationFromMode.none);
        }

        List<Action> result = new ArrayList<>();
        result.add(ClusterReliableQueryAction.schemaChange("ALTER TABLE " + keyspace + "." + table + " WITH TransactionalModel.full", actions, actions.random.uniform(1, actions.cluster.size() + 1), "ALTER TABLE " + keyspace + "." + table + " WITH " + targetMode.asCqlParam()));

        Map<Integer, Token> idToToken = new HashMap<>();
        List<Token> tokens = new ArrayList<>();
        IPartitioner partitioner = null;
        for (int i = 1; i <= actions.cluster.size(); i++)
        {
            IInvokableInstance instance = actions.cluster.get(i);
            String tokenString = instance.config().getString("initial_token");
            partitioner = FBUtilities.newPartitioner(instance.config().getString("partitioner"));
            Token token = partitioner.getTokenFactory().fromString(tokenString);
            tokens.add(token);
            idToToken.put(i, token);
        }

        boolean partialMigration = false;//rs.decide(0.25f);
        List<Integer> subRangesForNode = new ArrayList<>();
        int totalSubranges = 0;
        for (int ii = 0; ii < actions.all.size; ii++)
        {
            int subRangesThisNode = rs.uniform(1, 4);
            totalSubranges += subRangesThisNode;
            subRangesForNode.add(subRangesThisNode);
        }
        int stopSubrange = partialMigration ? rs.uniform(0, totalSubranges - 1) : Integer.MAX_VALUE;

        List<Pair<Integer, SimpleEntry<String, String>>> ranges = new ArrayList<>();
        for (int ii = 0; ii < actions.all.size; ii++)
        {
            int nodeIdx = ii + 1;
            Token token = idToToken.get(nodeIdx);
            Range<Token> tokenRange = new Range(TokenRingUtils.getPredecessor(tokens, token), token);

            int numSubRanges = subRangesForNode.get(0);
            List<Range<Token>> subRanges = new ArrayList<>();
            switch (numSubRanges)
            {
                default:
                    throw new IllegalStateException();
                case 1:
                    subRanges.add(tokenRange);
                    break;
                case 2:
                    Range<Token> firstRange = new Range<>(tokenRange.left, partitioner.split(tokenRange.left, tokenRange.right, 0.5));
                    subRanges.add(firstRange);
                    subRanges.add(new Range<>(firstRange.right, tokenRange.right));
                    break;
                case 3:
                    firstRange = new Range<>(tokenRange.left, partitioner.split(tokenRange.left, tokenRange.right, 0.33));
                    Range<Token> secondRange = new Range<>(firstRange.right, partitioner.split(tokenRange.left, tokenRange.right, 0.66));
                    Range<Token> thirdRange = new Range<>(secondRange.right, tokenRange.right);
                    subRanges.add(firstRange);
                    subRanges.add(secondRange);
                    subRanges.add(thirdRange);
                    break;
            }
            subRanges.stream().map(range -> Pair.create(nodeIdx, new SimpleEntry<>(range.left.toString(), range.right.toString()))).forEach(ranges::add);
            if (subRanges.size() >= stopSubrange)
            {
                ranges = ranges.subList(0, stopSubrange);
                break;
            }
        }

        if (rs.decide(0.5f))
            Collections.shuffle(ranges, new Random(actions.random.uniform(Long.MIN_VALUE, Long.MAX_VALUE)));

        ranges.stream().forEach(p -> result.add(new OnClusterMigrateConsensusOneRange(actions, p.left(), p.right(), targetMode)));
        if (!partialMigration)
            result.add(new OnClusterAssertMigrationComplete(actions));
        return ActionList.of(result).setStrictlySequential();
    }
}
