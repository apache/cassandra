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

package org.apache.cassandra.service.accord.txn;

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import accord.api.Key;
import accord.api.Read;
import accord.api.RoutingKey;
import accord.primitives.Keys;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.RoutingKeys;
import accord.primitives.Seekables;
import accord.primitives.Unseekables;
import accord.utils.RandomSource;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.TableMetadatas;
import org.apache.cassandra.utils.ByteBufferUtil;

import static accord.primitives.Routables.Slice.Minimal;
import static accord.utils.Property.qt;
import static org.apache.cassandra.service.accord.txn.TxnData.TxnDataNameKind.USER;
import static org.apache.cassandra.service.accord.txn.TxnData.txnDataName;
import static org.junit.Assert.assertEquals;

public class TxnReadTest
{
    private static final TableId TABLE  = TableId.fromString("00000000-0000-0000-0000-000000000001");
    private static final int TOKENS = 15;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void selectTest()
    {
        qt().check(rs -> {
            TxnRead original = randomRead(rs);
            Ranges ranges = randomRanges(rs);
            RoutingKeys keys = randomRoutingKeys(rs, original);
            Seekables<?, ?>[] possiblePieces = { original.keys().slice(ranges, Minimal),
                                                 original.keys().intersecting((Unseekables<?>) ranges, Minimal),
                                                 original.keys().intersecting(keys, Minimal) };

            Read[] actual = { original.slice(ranges), original.intersecting(ranges), original.intersecting(keys) };
            for (int i = 0; i < actual.length; i++)
            {
                Seekables<?, ?> pieces = possiblePieces[i];
                List<TxnNamedRead> expected = new ArrayList<>();
                for (TxnNamedRead read : original)
                {
                    if (pieces.domain() == Routable.Domain.Key)
                    {
                        if (((Keys) pieces).indexOf((Key) read.key()) >= 0)
                            expected.add(read);
                    }
                    else
                    {
                        for (Range piece : ((Ranges) pieces).mergeTouching())
                            if (((Range) read.key()).contains(piece))
                                expected.add(read.slice(piece));
                    }
                }

                // Ensure that select is equivalent to an unoptimized version
                assertEquals(TxnRead.createTxnRead(TableMetadatas.none(), expected, null, original.keys().domain()), actual[i]);
            }
        });
    }

    @Test
    public void mergeTest()
    {
        qt().check(rs -> {
            TxnRead original = randomRead(rs);
            TxnRead left = (TxnRead) original.slice(randomRanges(rs)), right = (TxnRead) original.slice(randomRanges(rs));

            Seekables<?, ?> expected = original.keys().domain() == Routable.Domain.Key
                                       ? ((Keys) left.keys()).with((Keys) right.keys())
                                       : ((Ranges) left.keys()).with((Ranges) right.keys()).mergeTouching();

            // Ensure that no keys are lost after merging
            assertEquals(expected, left.merge(right).keys());
            assertEquals(expected, right.merge(left).keys());
        });
    }

    private static TxnRead randomRead(RandomSource rs)
    {
        Routable.Domain domain = rs.nextBoolean() ? Routable.Domain.Range : Routable.Domain.Key;
        List<TxnNamedRead> reads = new ArrayList<>();
        if (domain == Routable.Domain.Range)
        {
            for (Range range : randomRanges(rs).mergeTouching())
                reads.add(new TxnNamedRead(txnDataName(USER, reads.size()), range, null));
        }
        else
        {
            int count = rs.nextInt(0, 5);
            for (int i = 0; i < count; i++)
            {
                long token = rs.nextLong(-TOKENS, TOKENS);
                PartitionKey key = new PartitionKey(TABLE, new BufferDecoratedKey(new LongToken(token), ByteBufferUtil.bytes(token)));
                reads.add(new TxnNamedRead(txnDataName(USER, reads.size()), key, null));
            }
        }
        return TxnRead.createTxnRead(TableMetadatas.none(), reads, null, domain);
    }

    private static Ranges randomRanges(RandomSource rs)
    {
        TokenRange[] ranges = new TokenRange[rs.nextInt(0, 5)];
        for (int i = 0; i < ranges.length; i++)
        {
            long start = rs.nextLong(-TOKENS, TOKENS - 1);
            ranges[i] = TokenRange.create(TABLE, new LongToken(start), new LongToken(rs.nextLong(start + 1, TOKENS)));
        }
        return Ranges.of(ranges);
    }

    private static RoutingKeys randomRoutingKeys(RandomSource rs, TxnRead read)
    {
        List<RoutingKey> keys = new ArrayList<>();
        for (TxnNamedRead r : read)
            if (rs.nextBoolean())
                keys.add(r.key().someIntersectingRoutingKey(null));
        return RoutingKeys.of(keys);
    }
}
