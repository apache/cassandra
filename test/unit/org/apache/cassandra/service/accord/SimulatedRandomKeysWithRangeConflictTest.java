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

package org.apache.cassandra.service.accord;

import accord.api.RoutingKey;
import accord.primitives.FullRangeRoute;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.RoutingKeys;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.Property;
import accord.utils.RandomSource;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.utils.FailingConsumer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static accord.utils.Property.commands;
import static accord.utils.Property.stateful;
import static org.apache.cassandra.dht.Murmur3Partitioner.LongToken.keyForToken;
import static org.apache.cassandra.service.accord.AccordTestUtils.createTxn;

public class SimulatedRandomKeysWithRangeConflictTest extends SimulatedAccordCommandStoreTestBase
{
    private static Property.SimpleCommand<State> insertKey(RandomSource rs, State state)
    {
        long token = rs.nextLong(Long.MIN_VALUE  + 1, Long.MAX_VALUE);
        RoutingKey key = new TokenKey(state.tbl.id, new LongToken(token));
        Txn keyTxn = createTxn(wrapInTxn("INSERT INTO " + state.tbl + "(pk, value) VALUES (?, ?)"),
                Arrays.asList(keyForToken(token), 42));
        Keys keys = (Keys) keyTxn.keys();
        FullRoute<RoutingKey> keyRoute = keys.toRoute(keys.get(0).toUnseekable());

        return new Property.SimpleCommand<>("Write Txn: " + keys, FailingConsumer.orFail(s -> {
            s.instance.maybeCacheEvict(keyRoute, s.wholeRange);
            var k = assertDepsMessage(s.instance, rs.pick(DepsMessage.values()), keyTxn, keyRoute, Map.of(key, s.keyConflicts.computeIfAbsent(key, ignore -> new ArrayList<>())), Collections.emptyMap());
            s.keyConflicts.get(key).add(k);
        }));
    }

    private static Property.SimpleCommand<State> insertRange(RandomSource rs, State state)
    {
        return new Property.SimpleCommand<>("Range Txn: " + state.wholeRange, FailingConsumer.orFail(s -> {
            s.instance.maybeCacheEvict(RoutingKeys.EMPTY, s.wholeRange);
            s.rangeConflicts.add(assertDepsMessage(s.instance, rs.pick(DepsMessage.values()), s.rangeTxn, s.rangeRoute, s.keyConflicts, rangeConflicts(s.rangeConflicts, s.wholeRange)));
        }));
    }


    @Test
    public void keysAllOverConflictingWithRange()
    {
        stateful().withSteps(State.steps).check(commands(() -> State::new)
                .add(SimulatedRandomKeysWithRangeConflictTest::insertKey)
                .add(SimulatedRandomKeysWithRangeConflictTest::insertRange)
                .build());
    }

    public static class State
    {
        static final int steps = 300;
        final SimulatedAccordCommandStore instance;
        final Map<RoutingKey, List<TxnId>> keyConflicts = new HashMap<>();
        final List<TxnId> rangeConflicts = new ArrayList<>(steps);

        final TableMetadata tbl = reverseTokenTbl;
        final Ranges wholeRange = Ranges.of(fullRange(tbl.id));
        final FullRangeRoute rangeRoute = wholeRange.toRoute(wholeRange.get(0).end());
        final Txn rangeTxn = createTxn(Txn.Kind.ExclusiveSyncPoint, wholeRange);

        public State(RandomSource rs)
        {
            AccordKeyspace.unsafeClear();
            this.instance = new SimulatedAccordCommandStore(rs);
        }

        @Override
        public String toString()
        {
            return "Storage Ranges: " + instance.topology.ranges();
        }
    }
}
