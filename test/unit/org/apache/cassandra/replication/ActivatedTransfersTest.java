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
package org.apache.cassandra.replication;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import accord.utils.Gen;
import accord.utils.Gens;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.AccordGenerators;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraGenerators;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.qt;

public class ActivatedTransfersTest
{
    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    private static Token tk(long token)
    {
        return new Murmur3Partitioner.LongToken(token);
    }

    private static PartitionPosition pos(long token)
    {
        Token t = tk(token);
        return new BufferDecoratedKey(t, ByteBufferUtil.bytes(token));
    }

    private static Bounds<PartitionPosition> bounds(long left, long right)
    {
        return (Bounds) bounds(left, true, right, true);
    }

    private static Range<PartitionPosition> range(long left, long right)
    {
        return (Range) bounds(left, false, right, true);
    }

    private static AbstractBounds<PartitionPosition> bounds(long left, boolean leftInclusive, long right, boolean rightInclusive)
    {
        return AbstractBounds.bounds(pos(left), leftInclusive, pos(right), rightInclusive);
    }

    private static ShortMutationId id(int logId, int offset)
    {
        return new ShortMutationId(logId, offset);
    }

    private static Gen<Token> tokenGen()
    {
        return AccordGenerators.fromQT(CassandraGenerators.murmurToken());
    }

    private static Gen<CoordinatorLogId> logIdGen()
    {
        return rs -> new CoordinatorLogId(rs.nextInt(), rs.nextInt());
    }

    private static Gen<ShortMutationId> idGen()
    {
        return rs -> {
            int offset = (short) rs.nextInt(Short.MIN_VALUE, Short.MAX_VALUE);
            return new ShortMutationId(logIdGen().next(rs).asLong(), offset);
        };
    }

    private static Gen<ActivatedTransfers> activatedTransfersGen()
    {
        return rs -> {
            List<ActivatedTransfers.ActivatedTransfer> entries = Gens.lists(activatedTransferGen()).ofSizeBetween(0, 10).next(rs);
            ActivatedTransfers transfers = new ActivatedTransfers();
            entries.forEach(entry -> transfers.add(entry.id, entry.bounds));
            return transfers;
        };
    }

    private static Gen<ActivatedTransfers.ActivatedTransfer> activatedTransferGen()
    {
        return rs -> {
            ShortMutationId id = idGen().next(rs);
            while (true)
            {
                Token left = tokenGen().next(rs);
                Token right = tokenGen().next(rs);

                if (!AbstractBounds.strictlyWrapsAround(left, right))
                    return new ActivatedTransfers.ActivatedTransfer(id, new Bounds<>(left, right));
            }
        };
    }

    @Test
    public void testSerdeRoundtrip()
    {
        qt()
        .forAll(activatedTransfersGen())
        .check(transfers -> {
            int version = MessagingService.current_version;
            ActivatedTransfers deserialized;
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                ActivatedTransfers.serializer.serialize(transfers, out, version);

                try (DataInputBuffer in = new DataInputBuffer(out.buffer(), true))
                {
                    deserialized = ActivatedTransfers.serializer.deserialize(in, version);
                }
            }

            Assertions.assertThat(deserialized).isEqualTo(transfers);
        });
    }

    @Test
    public void testIntersectsSingle()
    {
        ActivatedTransfers transfers = new ActivatedTransfers();
        ShortMutationId id1 = id(1, 0);
        transfers.add(id1, new Bounds<>(tk(400), tk(500))); // [400, 500]

        final long min = Murmur3Partitioner.instance.getMinimumToken().token;

        // Token
        assertNoIntersection(transfers, tk(0));
        assertIntersects(transfers, id1, tk(400));
        assertIntersects(transfers, id1, tk(450));
        assertIntersects(transfers, id1, tk(500));
        assertNoIntersection(transfers, tk(550));

        // Bounds []
        assertNoIntersection(transfers, bounds(100, 300));    // [100, 300]
        assertIntersects(transfers, id1, bounds(100, 400L));  // [100, 400] - overlap at boundary
        assertIntersects(transfers, id1, bounds(100, 450L));  // [100, 450]
        assertIntersects(transfers, id1, bounds(400, 500L));  // [400, 500]
        assertIntersects(transfers, id1, bounds(500, 600));   // [500, 600] - overlap at boundary
        assertNoIntersection(transfers, bounds(600, 700));    // [600, 700]
        assertIntersects(transfers, id1, bounds(0, 1000));    // [0, 1000]
        assertIntersects(transfers, id1, bounds(400, 400));    // [400, 400]
        assertIntersects(transfers, id1, new Bounds<>(tk(500).minKeyBound(), tk(min).maxKeyBound()));    // [400, 400]

        // Range (]
        assertNoIntersection(transfers, range(100, 300));    // (100, 300]
        assertIntersects(transfers, id1, range(100, 400L));  // (100, 400] - overlap at boundary
        assertIntersects(transfers, id1, range(100, 450L));  // (100, 450]
        assertIntersects(transfers, id1, range(400, 500L));  // (400, 500]
        assertNoIntersection(transfers, range(500, 600));    // (500, 600]
        assertNoIntersection(transfers, range(600, 700));    // (600, 700]
        assertIntersects(transfers, id1, range(0, 1000));    // (0, 1000]
        assertIntersects(transfers, id1, range(0, min)); // (0, MIN]
        assertIntersects(transfers, id1, range(450, min)); // (450, MIN]
        assertIntersects(transfers, id1, range(0, 0)); // (0, 0]
        assertNoIntersection(transfers, range(600, 300)); // (600, 300]

        // [)
        assertNoIntersection(transfers, bounds(100, true, 300, false));  // [100, 300)
        assertNoIntersection(transfers, bounds(100, true, 400, false));  // [100, 400)
        assertIntersects(transfers, id1, bounds(100, true, 450, false)); // [100, 450)
        assertIntersects(transfers, id1, bounds(400, true, 500, false)); // [400, 500)
        assertIntersects(transfers, id1, bounds(500, true, 600, false)); // [500, 600)
        assertNoIntersection(transfers, bounds(600, true, 700, false));  // [600, 700)
        assertIntersects(transfers, id1, bounds(0, true, 1000, false));  // [0, 1000)

        // ()
        assertNoIntersection(transfers, bounds(100, false, 300, false));  // (100, 300)
        assertNoIntersection(transfers, bounds(100, false, 400, false));  // (100, 400)
        assertIntersects(transfers, id1, bounds(100, false, 450, false)); // (100, 450)
        assertIntersects(transfers, id1, bounds(400, false, 500, false)); // (400, 500)
        assertNoIntersection(transfers, bounds(500, false, 600, false));  // (500, 600)
        assertNoIntersection(transfers, bounds(600, false, 700, false));  // (600, 700)
        assertIntersects(transfers, id1, bounds(0, false, 1000, false));  // (0, 1000)
    }

    @Test
    public void testIntersectsMultiple()
    {
        ActivatedTransfers transfers = new ActivatedTransfers();
        ShortMutationId id1 = id(100, 1);
        ShortMutationId id2 = id(100, 2);
        ShortMutationId id3 = id(100, 3);

        transfers.add(id1, new Bounds<>(tk(100), tk(200)));
        transfers.add(id2, new Bounds<>(tk(300), tk(400)));
        transfers.add(id3, new Bounds<>(tk(500), tk(600)));

        Set<ShortMutationId> ids1 = new HashSet<>();
        transfers.forEachIntersecting(new Bounds<>(pos(50), pos(150)), ids1::add);
        Assertions.assertThat(ids1).containsExactly(id1);

        Set<ShortMutationId> ids2 = new HashSet<>();
        transfers.forEachIntersecting(new Bounds<>(pos(50), pos(350)), ids2::add);
        Assertions.assertThat(ids2).containsExactly(id1, id2);

        Set<ShortMutationId> ids3 = new HashSet<>();
        transfers.forEachIntersecting(new Bounds<>(pos(0), pos(700)), ids3::add);
        Assertions.assertThat(ids3).containsExactly(id1, id2, id3);
    }

    @Test
    public void testRemove()
    {
        ActivatedTransfers transfers = new ActivatedTransfers();
        ShortMutationId id1 = id(100, 1);
        ShortMutationId id2 = id(100, 2);
        ShortMutationId id3 = id(100, 3);

        transfers.add(id1, new Bounds<>(tk(100), tk(200)));
        transfers.add(id2, new Bounds<>(tk(300), tk(400)));
        transfers.add(id3, new Bounds<>(tk(500), tk(600)));

        transfers.removeOffset(1);

        assertNoIntersection(transfers, bounds(50, 100));
    }

    private void assertIntersects(ActivatedTransfers transfers, ShortMutationId expectedId, Token token)
    {
        Set<ShortMutationId> ids = new HashSet<>();
        transfers.forEachIntersecting(token, ids::add);
        Assertions.assertThat(ids).contains(expectedId);
    }

    private void assertIntersects(ActivatedTransfers transfers, ShortMutationId expectedId, AbstractBounds<PartitionPosition> range)
    {
        Set<ShortMutationId> ids = new HashSet<>();
        transfers.forEachIntersecting(range, ids::add);
        Assertions.assertThat(ids).contains(expectedId);
    }

    private void assertNoIntersection(ActivatedTransfers transfers, Token token)
    {
        Set<ShortMutationId> ids = new HashSet<>();
        transfers.forEachIntersecting(token, ids::add);
        Assertions.assertThat(ids).isEmpty();
    }

    private void assertNoIntersection(ActivatedTransfers transfers, AbstractBounds<PartitionPosition> range)
    {
        Set<ShortMutationId> ids = new HashSet<>();
        transfers.forEachIntersecting(range, ids::add);
        Assertions.assertThat(ids).isEmpty();
    }
}
