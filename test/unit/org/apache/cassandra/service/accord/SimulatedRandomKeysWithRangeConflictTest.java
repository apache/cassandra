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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import accord.api.Key;
import accord.primitives.FullRangeRoute;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.async.AsyncResult;
import org.apache.cassandra.service.accord.api.PartitionKey;

import static accord.utils.Property.qt;
import static org.apache.cassandra.dht.Murmur3Partitioner.LongToken.keyForToken;
import static org.apache.cassandra.service.accord.AccordTestUtils.createTxn;

public class SimulatedRandomKeysWithRangeConflictTest extends SimulatedAccordCommandStoreTestBase
{
    @Test
    public void keysAllOverConflictingWithRange()
    {
        var tbl = reverseTokenTbl;
        Ranges wholeRange = Ranges.of(fullRange(tbl.id));
        FullRangeRoute rangeRoute = wholeRange.toRoute(wholeRange.get(0).end());
        Txn rangeTxn = createTxn(Txn.Kind.ExclusiveSyncPoint, wholeRange);
        int numSamples = 300;

        qt().withExamples(10).check(rs -> {
            AccordKeyspace.unsafeClear();
            try (var instance = new SimulatedAccordCommandStore(rs))
            {
                Map<Key, List<TxnId>> keyConflicts = new HashMap<>();
                List<TxnId> rangeConflicts = new ArrayList<>(numSamples);
                boolean concurrent = rs.nextBoolean();
                List<AsyncResult<?>> asyncs = !concurrent ? null : new ArrayList<>(numSamples * 2);
                for (int i = 0; i < numSamples; i++)
                {
                    long token = rs.nextLong(Long.MIN_VALUE  + 1, Long.MAX_VALUE);
                    Key key = new PartitionKey(tbl.id, tbl.partitioner.decorateKey(keyForToken(token)));
                    Txn keyTxn = createTxn(wrapInTxn("INSERT INTO " + tbl + "(pk, value) VALUES (?, ?)"),
                                           Arrays.asList(keyForToken(token), 42));
                    Keys keys = (Keys) keyTxn.keys();
                    FullRoute<?> keyRoute = keys.toRoute(keys.get(0).toUnseekable());

                    instance.maybeCacheEvict((Keys) keyTxn.keys(), wholeRange);

                    if (concurrent)
                    {
                        var k = assertDepsMessageAsync(instance, rs.pick(DepsMessage.values()), keyTxn, keyRoute, Map.of(key, keyConflicts.computeIfAbsent(key, ignore -> new ArrayList<>())), Collections.emptyMap());
                        keyConflicts.get(key).add(k.left);
                        asyncs.add(k.right);

                        var r = assertDepsMessageAsync(instance, rs.pick(DepsMessage.values()), rangeTxn, rangeRoute, keyConflicts, rangeConflicts(rangeConflicts, wholeRange));
                        rangeConflicts.add(r.left);
                        asyncs.add(r.right);
                    }
                    else
                    {
                        var k = assertDepsMessage(instance, rs.pick(DepsMessage.values()), keyTxn, keyRoute, Map.of(key, keyConflicts.computeIfAbsent(key, ignore -> new ArrayList<>())), Collections.emptyMap());
                        keyConflicts.get(key).add(k);
                        rangeConflicts.add(assertDepsMessage(instance, rs.pick(DepsMessage.values()), rangeTxn, rangeRoute, keyConflicts, rangeConflicts(rangeConflicts, wholeRange)));
                    }
                }
                if (concurrent)
                {
                    instance.processAll();
                    safeBlock(asyncs);
                }
            }
        });
    }
}
