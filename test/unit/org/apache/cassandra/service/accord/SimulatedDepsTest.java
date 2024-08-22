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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import accord.api.Key;
import accord.primitives.FullKeyRoute;
import accord.primitives.FullRangeRoute;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.api.PartitionKey;

import static accord.utils.Property.qt;
import static org.apache.cassandra.service.accord.AccordTestUtils.createTxn;

public class SimulatedDepsTest extends SimulatedAccordCommandStoreTestBase
{
    @Test
    public void keyConflicts()
    {
        TableMetadata tbl = intTbl;
        int numSamples = 100;

        qt().withExamples(10).check(rs -> {
            AccordKeyspace.unsafeClear();
            int key = rs.nextInt();
            PartitionKey pk = new PartitionKey(tbl.id, tbl.partitioner.decorateKey(Int32Type.instance.decompose(key)));
            Keys keys = Keys.of(pk);
            FullKeyRoute route = keys.toRoute(pk.toUnseekable());
            Txn txn = createTxn(wrapInTxn("INSERT INTO " + tbl + "(pk, value) VALUES (?, ?)"), Arrays.asList(key, 42));
            try (var instance = new SimulatedAccordCommandStore(rs))
            {
                List<TxnId> conflicts = new ArrayList<>(numSamples);
                for (int i = 0; i < numSamples; i++)
                {
                    instance.maybeCacheEvict(keys, Ranges.EMPTY);
                    conflicts.add(assertDepsMessage(instance, rs.pick(DepsMessage.values()), txn, route, keyConflicts(conflicts, keys)));
                }
            }
        });
    }

    @Test
    public void rangePartialKeyMatch()
    {
        var tbl = reverseTokenTbl;
        int numSamples = 250;
        int numConflictKeyTxns = 10;

        qt().withExamples(10).check(rs -> {
            AccordKeyspace.unsafeClear();
            try (var instance = new SimulatedAccordCommandStore(rs))
            {
                long token = rs.nextLong(Long.MIN_VALUE  + 1, Long.MAX_VALUE);
                Ranges partialRange = Ranges.of(tokenRange(tbl.id, token - 1, token));
                Ranges partialRangeSliced = instance.slice(partialRange);
                long outOfRangeToken = token - 10;
                if (outOfRangeToken == Long.MIN_VALUE) // if this wraps around that is fine, just can't be min
                    outOfRangeToken++;
                Key key = new PartitionKey(tbl.id, tbl.partitioner.decorateKey(LongToken.keyForToken(token)));
                Key outOfRangeKey = new PartitionKey(tbl.id, tbl.partitioner.decorateKey(LongToken.keyForToken(outOfRangeToken)));
                Txn keyTxn = createTxn(wrapInTxn("INSERT INTO " + tbl + "(pk, value) VALUES (?, ?)",
                                                 "INSERT INTO " + tbl + "(pk, value) VALUES (?, ?)"),
                                       Arrays.asList(LongToken.keyForToken(token), 42,
                                                     LongToken.keyForToken(outOfRangeToken), 42));
                Keys keys = (Keys) keyTxn.keys();
                FullRoute<?> keyRoute = keys.toRoute(keys.get(0).toUnseekable());

                Txn conflictingKeyTxn = createTxn(wrapInTxn("INSERT INTO " + tbl + "(pk, value) VALUES (?, ?)"),
                                                  Arrays.asList(LongToken.keyForToken(outOfRangeToken), 42));
                Keys conflictingKeys = (Keys) conflictingKeyTxn.keys();
                FullRoute<?> conflictingRoute = conflictingKeys.toRoute(conflictingKeys.get(0).toUnseekable());

                FullRangeRoute rangeRoute = partialRange.toRoute(key.toUnseekable());
                Txn rangeTxn = createTxn(Txn.Kind.ExclusiveSyncPoint, partialRange);

                List<TxnId> keyConflicts = new ArrayList<>(numSamples);
                List<TxnId> outOfRangeKeyConflicts = new ArrayList<>(numSamples);
                List<TxnId> rangeConflicts = new ArrayList<>(numSamples);
                for (int i = 0; i < numSamples; i++)
                {
                    instance.maybeCacheEvict((Keys) keyTxn.keys(), partialRange);
                    for (int j = 0; j < numConflictKeyTxns; j++)
                        outOfRangeKeyConflicts.add(assertDepsMessage(instance, rs.pick(DepsMessage.values()), conflictingKeyTxn, conflictingRoute, Map.of(outOfRangeKey, outOfRangeKeyConflicts)));

                    TxnId id = assertDepsMessage(instance, rs.pick(DepsMessage.values()), keyTxn, keyRoute, Map.of(key, keyConflicts, outOfRangeKey, outOfRangeKeyConflicts));
                    keyConflicts.add(id);
                    outOfRangeKeyConflicts.add(id);

                    rangeConflicts.add(assertDepsMessage(instance, rs.pick(DepsMessage.values()), rangeTxn, rangeRoute, Map.of(key, keyConflicts), rangeConflicts(rangeConflicts, partialRangeSliced)));
                }
            }
        });
    }

    @Test
    public void simpleRangeConflicts()
    {
        var tbl = reverseTokenTbl;
        Ranges wholeRange = Ranges.of(fullRange(tbl.id));
        int numSamples = 100;

        qt().withExamples(10).check(rs -> {
            AccordKeyspace.unsafeClear();
            try (var instance = new SimulatedAccordCommandStore(rs))
            {
                long token = rs.nextLong(Long.MIN_VALUE  + 1, Long.MAX_VALUE);
                ByteBuffer key = LongToken.keyForToken(token);
                PartitionKey pk = new PartitionKey(tbl.id, tbl.partitioner.decorateKey(key));
                Keys keys = Keys.of(pk);
                FullKeyRoute keyRoute = keys.toRoute(pk.toUnseekable());
                Txn keyTxn = createTxn(wrapInTxn("INSERT INTO " + tbl + "(pk, value) VALUES (?, ?)"), Arrays.asList(key, 42));

                Ranges partialRange = Ranges.of(tokenRange(tbl.id, token - 1, token));
                boolean useWholeRange = rs.nextBoolean();
                Ranges ranges = useWholeRange ? wholeRange : partialRange;
                FullRangeRoute rangeRoute = ranges.toRoute(pk.toUnseekable());
                Txn rangeTxn = createTxn(Txn.Kind.ExclusiveSyncPoint, ranges);

                List<TxnId> keyConflicts = new ArrayList<>(numSamples);
                List<TxnId> rangeConflicts = new ArrayList<>(numSamples);
                for (int i = 0; i < numSamples; i++)
                {
                    instance.maybeCacheEvict(keys, ranges);
                    keyConflicts.add(assertDepsMessage(instance, rs.pick(DepsMessage.values()), keyTxn, keyRoute, keyConflicts(keyConflicts, keys)));
                    rangeConflicts.add(assertDepsMessage(instance, rs.pick(DepsMessage.values()), rangeTxn, rangeRoute, keyConflicts(keyConflicts, keys), rangeConflicts(rangeConflicts, instance.slice(ranges))));
                }
            }
        });
    }

    @Test
    public void expandingRangeConflicts()
    {
        var tbl = reverseTokenTbl;
        int numSamples = 100;

        qt().withExamples(10).check(rs -> {
            AccordKeyspace.unsafeClear();
            try (var instance = new SimulatedAccordCommandStore(rs))
            {
                long token = rs.nextLong(Long.MIN_VALUE + numSamples + 1, Long.MAX_VALUE - numSamples);
                ByteBuffer key = LongToken.keyForToken(token);
                PartitionKey pk = new PartitionKey(tbl.id, tbl.partitioner.decorateKey(key));
                Keys keys = Keys.of(pk);
                FullKeyRoute keyRoute = keys.toRoute(pk.toUnseekable());
                Txn keyTxn = createTxn(wrapInTxn("INSERT INTO " + tbl + "(pk, value) VALUES (?, ?)"), Arrays.asList(key, 42));

                List<TxnId> keyConflicts = new ArrayList<>(numSamples);
                Map<Range, List<TxnId>> rangeConflicts = new HashMap<>();
                for (int i = 0; i < numSamples; i++)
                {
                    Ranges partialRange = Ranges.of(tokenRange(tbl.id, token - i - 1, token + i));
                    FullRangeRoute rangeRoute = partialRange.toRoute(pk.toUnseekable());
                    Txn rangeTxn = createTxn(Txn.Kind.ExclusiveSyncPoint, partialRange);
                    try
                    {
                        instance.maybeCacheEvict(keys, partialRange);
                        keyConflicts.add(assertDepsMessage(instance, rs.pick(DepsMessage.values()), keyTxn, keyRoute, keyConflicts(keyConflicts, keys)));
                        rangeConflicts.put(partialRange.get(0), Collections.singletonList(assertDepsMessage(instance, rs.pick(DepsMessage.values()), rangeTxn, rangeRoute, keyConflicts(keyConflicts, keys), rangeConflicts)));
                    }
                    catch (Throwable t)
                    {
                        AssertionError error = new AssertionError("Unexpected error: i=" + i + ", token=" + token + ", range=" + partialRange.get(0));
                        t.addSuppressed(error);
                        throw t;
                    }
                }
            }
        });
    }

    @Test
    public void overlappingRangeConflicts()
    {
        var tbl = reverseTokenTbl;
        int numSamples = 100;

        qt().withExamples(10).check(rs -> {
            AccordKeyspace.unsafeClear();
            try (var instance = new SimulatedAccordCommandStore(rs))
            {
                long token = rs.nextLong(Long.MIN_VALUE + numSamples + 1, Long.MAX_VALUE - numSamples);
                ByteBuffer key = LongToken.keyForToken(token);
                PartitionKey pk = new PartitionKey(tbl.id, tbl.partitioner.decorateKey(key));
                Keys keys = Keys.of(pk);
                FullKeyRoute keyRoute = keys.toRoute(pk.toUnseekable());
                Txn keyTxn = createTxn(wrapInTxn("INSERT INTO " + tbl + "(pk, value) VALUES (?, ?)"), Arrays.asList(key, 42));

                Range left = tokenRange(tbl.id, token - 10, token + 5);
                Range right = tokenRange(tbl.id, token - 5, token + 10);

                List<TxnId> keyConflicts = new ArrayList<>(numSamples);
                Map<Range, List<TxnId>> rangeConflicts = new HashMap<>();
                rangeConflicts.put(left, new ArrayList<>());
                rangeConflicts.put(right, new ArrayList<>());
                for (int i = 0; i < numSamples; i++)
                {
                    Ranges partialRange = Ranges.of(rs.nextBoolean() ? left : right);
                    try
                    {
                        instance.maybeCacheEvict(keys, partialRange);
                        keyConflicts.add(assertDepsMessage(instance, rs.pick(DepsMessage.values()), keyTxn, keyRoute, keyConflicts(keyConflicts, keys)));

                        FullRangeRoute rangeRoute = partialRange.toRoute(pk.toUnseekable());
                        Txn rangeTxn = createTxn(Txn.Kind.ExclusiveSyncPoint, partialRange);
                        rangeConflicts.get(partialRange.get(0)).add(assertDepsMessage(instance, rs.pick(DepsMessage.values()), rangeTxn, rangeRoute, keyConflicts(keyConflicts, keys), rangeConflicts));
                    }
                    catch (Throwable t)
                    {
                        AssertionError error = new AssertionError("Unexpected error: i=" + i + ", token=" + token + ", range=" + partialRange.get(0));
                        t.addSuppressed(error);
                        throw t;
                    }
                }
            }
        });
    }
}
