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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.function.Consumer;

import org.junit.BeforeClass;
import org.junit.Test;

import accord.utils.RandomSource;
import accord.utils.SimpleBitSets;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.AccordTimestamps;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.TableMetadatas;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.utils.ByteBufferUtil;

import static accord.utils.Property.qt;
import static org.apache.cassandra.db.SerializationHeader.StableHeaderSerializer.STABLE;
import static org.apache.cassandra.db.rows.DeserializationHelper.Flag.FROM_REMOTE;
import static org.apache.cassandra.db.rows.EncodingStats.DELETION_TIME_EPOCH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Authored by LLM
 *
 * Base updates of transactions that do not preserve their timestamps are normalised at construction
 * (see {@link TxnWrite.Fragment#normaliseBaseUpdateTimes}), and bound to the transaction's execution time on apply,
 * either by offsetting the serialization header as they are deserialized ({@link TxnWrite.TimeOffsetHeaderDeserializer}),
 * or by deserializing them as-is and rebasing them ({@link PartitionUpdate.Builder#updateTimesForAccord}).
 * <p>
 * Which path is taken depends on the transaction's flags and on the cluster's maximum supported deletion time
 * (see {@link TxnWrite#canDecodeNowInSecWithOffset}), so replicas may take different paths and must produce
 * identical data either way. These tests check that both paths are equivalent, and that both bind every time
 * to the execution time: live data at the execution timestamp, deletions at the timestamp before, local deletion
 * times at the execution time and local expiration times at the execution time plus the TTL.
 */
public class TxnWriteTimeOffsetTest
{
    private static final long TIMESTAMP = 1794920000123456L; // 2026-11-17
    private static final long NOW_IN_SEC = TIMESTAMP / 1_000_000L;

    private static TableMetadata metadata;
    private static TableMetadatas tables;
    private static ColumnMetadata s, ss, v, w, m;

    @BeforeClass
    public static void setup()
    {
        // client initialisation disables the storage compatibility mode, so the maximum deletion time is
        // deterministic (and late enough to apply the offset) irrespective of the test configuration
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        metadata = TableMetadata.builder("ks", "tbl")
                                .partitioner(Murmur3Partitioner.instance)
                                .addPartitionKeyColumn("k", Int32Type.instance)
                                .addClusteringColumn("c", Int32Type.instance)
                                .addStaticColumn("s", Int32Type.instance)
                                .addStaticColumn("ss", SetType.getInstance(Int32Type.instance, true))
                                .addRegularColumn("v", Int32Type.instance)
                                .addRegularColumn("w", Int32Type.instance)
                                .addRegularColumn("m", MapType.getInstance(Int32Type.instance, Int32Type.instance, true))
                                .build();
        tables = TableMetadatas.of(metadata);
        s = column("s");
        ss = column("ss");
        v = column("v");
        w = column("w");
        m = column("m");
        assertEquals(Cell.MAX_DELETION_TIME, Cell.getVersionedMaxDeletionTime());
        assertTrue(TxnWrite.canDecodeNowInSecWithOffset(NOW_IN_SEC));
    }

    private static ColumnMetadata column(String name)
    {
        return metadata.getColumn(ByteBufferUtil.bytes(name));
    }

    private static ByteBuffer i(int value)
    {
        return Int32Type.instance.decompose(value);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Specific cases, built with arbitrary times and normalised with the production normaliser
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void testLiveOnly() throws IOException
    {
        check(normalisedRow(r -> {
            r.addPrimaryKeyLivenessInfo(LivenessInfo.create(5));
            r.addCell(BufferCell.live(v, 5, i(1)));
        }));
    }

    @Test
    public void testTombstoneOnly() throws IOException
    {
        check(normalisedRow(r -> r.addCell(BufferCell.tombstone(v, 5, 9))));
    }

    @Test
    public void testPartitionDeletionOnly() throws IOException
    {
        PartitionUpdate.Builder builder = builder();
        builder.addPartitionDeletion(DeletionTime.build(5, 9));
        check(TxnWrite.Fragment.normaliseBaseUpdateTimes(builder.build()));
    }

    @Test
    public void testTTLOnly() throws IOException
    {
        // the smallest local deletion time of the normalised update is its smallest TTL, not zero
        PartitionUpdate offset = check(normalisedRow(r -> {
            r.addPrimaryKeyLivenessInfo(LivenessInfo.expiring(5, 100, 9));
            r.addCell(BufferCell.expiring(v, 5, 100, 9, i(1)));
            r.addCell(BufferCell.expiring(w, 5, 200, 9, i(1)));
        }));
        Row row = offset.iterator().next();
        assertEquals(NOW_IN_SEC + 100, row.primaryKeyLivenessInfo().localExpirationTime());
        assertEquals(NOW_IN_SEC + 100, row.getCell(v).localDeletionTime());
        assertEquals(NOW_IN_SEC + 200, row.getCell(w).localDeletionTime());
    }

    @Test
    public void testTTLAndTombstone() throws IOException
    {
        check(normalisedRow(r -> {
            r.addCell(BufferCell.expiring(v, 5, 100, 9, i(1)));
            r.addCell(BufferCell.tombstone(w, 5, 9));
        }));
    }

    @Test
    public void testExpiredLiveness() throws IOException
    {
        check(normalisedRow(r -> r.addPrimaryKeyLivenessInfo(LivenessInfo.withExpirationTime(5, LivenessInfo.EXPIRED_LIVENESS_TTL, 9))));
    }

    /**
     * A normalised local expiration time of exactly {@link org.apache.cassandra.db.rows.EncodingStats#DELETION_TIME_EPOCH}
     * yields serialization header stats indistinguishable from those of an update with no local deletion times.
     * This TTL exceeds {@link Attributes#MAX_TTL} so should not occur in practice, but the offset should apply regardless.
     */
    @Test
    public void testTTLEqualToDeletionTimeEpoch() throws IOException
    {
        PartitionUpdate normalised = normalisedRow(r -> r.addCell(BufferCell.expiring(v, 5, DELETION_TIME_EPOCH, 9, i(1))));
        assertEquals(DELETION_TIME_EPOCH, normalised.stats().minLocalDeletionTime);
        PartitionUpdate offset = check(normalised);
        assertEquals(NOW_IN_SEC + DELETION_TIME_EPOCH, offset.iterator().next().getCell(v).localDeletionTime());
    }

    @Test
    public void testTTLEqualToDeletionTimeEpochWithLargerTTL() throws IOException
    {
        check(normalisedRow(r -> {
            r.addCell(BufferCell.expiring(v, 5, DELETION_TIME_EPOCH, 9, i(1)));
            r.addCell(BufferCell.expiring(w, 5, DELETION_TIME_EPOCH + 7, 9, i(1)));
        }));
    }

    @Test
    public void testCanDecodeNowInSecWithOffset()
    {
        long latest = Cell.getVersionedMaxDeletionTime() - Attributes.MAX_TTL;
        assertTrue(TxnWrite.canDecodeNowInSecWithOffset(latest));
        assertFalse(TxnWrite.canDecodeNowInSecWithOffset(latest + 1));
    }

    private static PartitionUpdate.Builder builder()
    {
        return new PartitionUpdate.Builder(metadata, metadata.partitioner.decorateKey(i(0)), metadata.regularAndStaticColumns(), 1);
    }

    private static PartitionUpdate normalisedRow(Consumer<Row.Builder> contents)
    {
        Row.Builder row = BTreeRow.unsortedBuilder();
        row.newRow(Clustering.make(i(0)));
        contents.accept(row);
        PartitionUpdate.Builder builder = builder();
        builder.add(row.build());
        return TxnWrite.Fragment.normaliseBaseUpdateTimes(builder.build());
    }

    /**
     * Bind a single normalised update to {@link #TIMESTAMP} via both paths, and check they agree
     * @return the update bound via the offset path
     */
    private static PartitionUpdate check(PartitionUpdate normalised) throws IOException
    {
        PartitionUpdate rebased = rebase(roundTrip(normalised, STABLE));
        PartitionUpdate offset = roundTrip(normalised, new TxnWrite.TimeOffsetHeaderDeserializer(TIMESTAMP, NOW_IN_SEC));
        assertEquivalent(rebased, offset);
        assertBoundTo(rebased, TIMESTAMP, NOW_IN_SEC);
        return offset;
    }

    private static PartitionUpdate roundTrip(PartitionUpdate update, SerializationHeader.AbstractStableHeaderSerializer headerSerializer) throws IOException
    {
        int version = Version.LATEST.messageVersion();
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            PartitionUpdate.serializer.serializeWithoutKey(update, tables, out, version);
            try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
            {
                return PartitionUpdate.serializer.deserialize(PartitionKey.of(update), tables, in, version, FROM_REMOTE, headerSerializer, null);
            }
        }
    }

    private static PartitionUpdate rebase(PartitionUpdate update)
    {
        return rebase(update, TIMESTAMP, NOW_IN_SEC);
    }

    private static PartitionUpdate rebase(PartitionUpdate update, long timestamp, long nowInSec)
    {
        return new PartitionUpdate.Builder(update, 0).updateTimesForAccord(timestamp, nowInSec).build();
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Randomised comparison, via the TxnUpdate fragments that TxnWrite.applyDirect binds
    // ---------------------------------------------------------------------------------------------------------------

    @Test
    public void testOffsetAndRebaseAreEquivalent()
    {
        qt().withExamples(1000).check(rs -> {
            // any time at which the offset may be applied, and an arbitrary sub-second component
            long nowInSec = rs.nextLong(DELETION_TIME_EPOCH, Cell.getVersionedMaxDeletionTime() - Attributes.MAX_TTL + 1);
            long timestamp = nowInSec * 1_000_000L + rs.nextLong(1_000_000L);
            assertTrue(TxnWrite.canDecodeNowInSecWithOffset(nowInSec));

            // several fragments, as produced by multiple statements in a transaction, over a small number of partitions
            List<TxnWrite.Fragment> fragments = new ArrayList<>();
            TreeSet<PartitionKey> keys = new TreeSet<>();
            int fragmentCount = rs.nextInt(1, 6);
            for (int index = 0 ; index < fragmentCount ; ++index)
            {
                PartitionUpdate normalised = TxnWrite.Fragment.normaliseBaseUpdateTimes(randomUpdate(rs, rs.nextInt(3)));
                PartitionKey key = PartitionKey.of(normalised);
                keys.add(key);
                fragments.add(new TxnWrite.Fragment(key, index, normalised, TxnReferenceOperations.empty(), TxnWrite.NO_TIMESTAMP));
            }

            TxnUpdate txnUpdate = new TxnUpdate(tables, fragments, TxnCondition.none(), null, AccordTimestamps.NORMALISED);
            assertTrue(txnUpdate.hasNormalisedTimes());
            for (PartitionKey key : keys)
            {
                List<PartitionUpdate> offset = new ArrayList<>();
                txnUpdate.completeWritesForKey(offset, SimpleBitSets.allSet(1), key, tables, new TxnWrite.TimeOffsetHeaderDeserializer(timestamp, nowInSec));

                List<PartitionUpdate> rebased = new ArrayList<>();
                txnUpdate.completeWritesForKey(rebased, SimpleBitSets.allSet(1), key, tables, STABLE);
                rebased.replaceAll(update -> rebase(update, timestamp, nowInSec));

                assertEquals(rebased.size(), offset.size());
                for (int j = 0 ; j < rebased.size() ; ++j)
                {
                    assertEquivalent(rebased.get(j), offset.get(j));
                    assertBoundTo(offset.get(j), timestamp, nowInSec);
                }

                PartitionUpdate mergedRebased = rebased.size() == 1 ? rebased.get(0) : PartitionUpdate.merge(rebased);
                PartitionUpdate mergedOffset = offset.size() == 1 ? offset.get(0) : PartitionUpdate.merge(offset);
                assertEquivalent(mergedRebased, mergedOffset);
            }
        });
    }

    /**
     * An update with arbitrary times (the normaliser should discard them all) containing any combination of
     * partition and range deletions, static and regular rows, row deletions, live, expiring and expired liveness,
     * live, expiring and tombstone simple cells, and complex deletions with live, expiring and tombstone elements
     */
    private static PartitionUpdate randomUpdate(RandomSource rs, int partition)
    {
        PartitionUpdate.Builder builder = new PartitionUpdate.Builder(metadata, metadata.partitioner.decorateKey(i(partition)), metadata.regularAndStaticColumns(), 4);
        if (rs.decide(0.1f))
            builder.addPartitionDeletion(randomDeletionTime(rs));
        if (rs.decide(0.2f))
        {
            int start = rs.nextInt(10);
            builder.add(new RangeTombstone(Slice.make(Clustering.make(i(start)), Clustering.make(i(start + rs.nextInt(3)))), randomDeletionTime(rs)));
        }
        if (rs.decide(0.3f))
            builder.add(randomRow(rs, Clustering.STATIC_CLUSTERING));

        int rowCount = rs.nextInt(4);
        TreeSet<Integer> clusterings = new TreeSet<>();
        while (clusterings.size() < rowCount)
            clusterings.add(rs.nextInt(10));
        for (int clustering : clusterings)
            builder.add(randomRow(rs, Clustering.make(i(clustering))));

        return builder.build();
    }

    private static Row randomRow(RandomSource rs, Clustering<?> clustering)
    {
        boolean isStatic = clustering == Clustering.STATIC_CLUSTERING;
        Row.Builder row = BTreeRow.unsortedBuilder();
        row.newRow(clustering);
        if (!isStatic)
        {
            switch (rs.nextInt(4))
            {
                case 0: break;
                case 1: row.addPrimaryKeyLivenessInfo(LivenessInfo.create(randomTimestamp(rs))); break;
                case 2: row.addPrimaryKeyLivenessInfo(LivenessInfo.expiring(randomTimestamp(rs), randomTTL(rs), randomNowInSec(rs))); break;
                case 3: row.addPrimaryKeyLivenessInfo(LivenessInfo.withExpirationTime(randomTimestamp(rs), LivenessInfo.EXPIRED_LIVENESS_TTL, randomNowInSec(rs))); break;
            }
            if (rs.decide(0.15f))
                row.addRowDeletion(Row.Deletion.regular(randomDeletionTime(rs)));
        }

        for (ColumnMetadata column : isStatic ? new ColumnMetadata[] { s } : new ColumnMetadata[] { v, w })
        {
            if (rs.nextBoolean())
                row.addCell(randomCell(rs, column, i(rs.nextInt()), null));
        }

        ColumnMetadata complex = isStatic ? ss : m;
        if (rs.decide(0.3f))
            row.addComplexDeletion(complex, randomDeletionTime(rs));
        int elementCount = rs.nextInt(4);
        TreeSet<Integer> elements = new TreeSet<>();
        while (elements.size() < elementCount)
            elements.add(rs.nextInt(10));
        for (int element : elements)
            row.addCell(randomCell(rs, complex, isStatic ? ByteBufferUtil.EMPTY_BYTE_BUFFER : i(rs.nextInt()), CellPath.create(i(element))));

        return row.build();
    }

    private static Cell<?> randomCell(RandomSource rs, ColumnMetadata column, ByteBuffer value, CellPath path)
    {
        switch (rs.nextInt(3))
        {
            case 0: return BufferCell.live(column, randomTimestamp(rs), value, path);
            case 1: return BufferCell.expiring(column, randomTimestamp(rs), randomTTL(rs), randomNowInSec(rs), value, path);
            default: return BufferCell.tombstone(column, randomTimestamp(rs), randomNowInSec(rs), path);
        }
    }

    private static DeletionTime randomDeletionTime(RandomSource rs)
    {
        return DeletionTime.build(randomTimestamp(rs), randomNowInSec(rs));
    }

    private static long randomTimestamp(RandomSource rs)
    {
        return rs.nextLong(0, Long.MAX_VALUE);
    }

    private static long randomNowInSec(RandomSource rs)
    {
        return rs.nextLong(0, Cell.getVersionedMaxDeletionTime() - Attributes.MAX_TTL);
    }

    private static int randomTTL(RandomSource rs)
    {
        // favour small TTLs, so that the smallest is often below any tombstone's local deletion time
        return rs.decide(0.5f) ? rs.nextInt(1, 1000) : rs.nextInt(1, Attributes.MAX_TTL + 1);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Assertions
    // ---------------------------------------------------------------------------------------------------------------

    private static void assertEquivalent(PartitionUpdate expected, PartitionUpdate actual)
    {
        String message = "\nexpected: " + expected + "\n  actual: " + actual;
        assertEquals(message, expected.partitionKey(), actual.partitionKey());
        assertEquals(message, expected.partitionLevelDeletion(), actual.partitionLevelDeletion());
        assertEquals(message, expected.staticRow(), actual.staticRow());
        try (UnfilteredRowIterator e = expected.unfilteredIterator(); UnfilteredRowIterator a = actual.unfilteredIterator())
        {
            while (e.hasNext())
            {
                assertTrue(message, a.hasNext());
                Unfiltered next = e.next();
                assertEquals(message, next, a.next());
            }
            assertFalse(message, a.hasNext());
        }
    }

    /**
     * Assert all live data is written at {@code timestamp}, all deletions at {@code timestamp - 1}, all deletion
     * times are {@code nowInSec}, and all expiration times are {@code nowInSec + ttl}
     */
    private static void assertBoundTo(PartitionUpdate update, long timestamp, long nowInSec)
    {
        String message = "\nupdate: " + update;
        DeletionTime deletionTime = DeletionTime.build(timestamp - 1, nowInSec);
        if (!update.partitionLevelDeletion().isLive())
            assertEquals(message, deletionTime, update.partitionLevelDeletion());
        for (Iterator<RangeTombstone> iter = update.deletionInfo().rangeIterator(false) ; iter.hasNext() ; )
            assertEquals(message, deletionTime, iter.next().deletionTime());

        List<Row> rows = new ArrayList<>();
        if (!update.staticRow().isEmpty())
            rows.add(update.staticRow());
        update.iterator().forEachRemaining(rows::add);
        for (Row row : rows)
        {
            LivenessInfo liveness = row.primaryKeyLivenessInfo();
            if (!liveness.isEmpty())
            {
                assertEquals(message, timestamp, liveness.timestamp());
                if (liveness.isExpiring())
                {
                    long expected = liveness.ttl() == LivenessInfo.EXPIRED_LIVENESS_TTL ? nowInSec : nowInSec + liveness.ttl();
                    assertEquals(message, expected, liveness.localExpirationTime());
                }
            }
            if (!row.deletion().isLive())
                assertEquals(message, deletionTime, row.deletion().time());

            for (ColumnData cd : row)
            {
                if (cd.column().isComplex() && !((ComplexColumnData) cd).complexDeletion().isLive())
                    assertEquals(message, deletionTime, ((ComplexColumnData) cd).complexDeletion());
            }
            for (Cell<?> cell : row.cells())
            {
                if (cell.isTombstone())
                {
                    assertEquals(message, timestamp - 1, cell.timestamp());
                    assertEquals(message, nowInSec, cell.localDeletionTime());
                }
                else
                {
                    assertEquals(message, timestamp, cell.timestamp());
                    assertEquals(message, cell.isExpiring() ? nowInSec + cell.ttl() : Cell.NO_DELETION_TIME, cell.localDeletionTime());
                }
            }
        }
    }
}
