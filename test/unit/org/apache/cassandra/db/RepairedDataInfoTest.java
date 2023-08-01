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

package org.apache.cassandra.db;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.IntStream;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.Util.clustering;
import static org.apache.cassandra.Util.dk;
import static org.apache.cassandra.utils.ByteBufferUtil.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class RepairedDataInfoTest
{
    private static ColumnFamilyStore cfs;
    private static TableMetadata metadata;
    private static ColumnMetadata valueMetadata;
    private static ColumnMetadata staticMetadata;

    private final long nowInSec = FBUtilities.nowInSeconds();

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
        MockSchema.cleanup();
        String ks = "repaired_data_info_test";
        cfs = MockSchema.newCFS(ks, metadata -> metadata.addStaticColumn("s", UTF8Type.instance));
        metadata = cfs.metadata();
        valueMetadata = metadata.regularColumns().getSimple(0);
        staticMetadata = metadata.staticColumns().getSimple(0);
    }

    @Test
    public void withTrackingAppliesRepairedDataCounter()
    {
        DataLimits.Counter counter = DataLimits.cqlLimits(15).newCounter(nowInSec, false, false, false).onlyCount();
        RepairedDataInfo info = new RepairedDataInfo(counter);
        info.prepare(cfs, nowInSec, Integer.MAX_VALUE);
        UnfilteredRowIterator[] partitions = new UnfilteredRowIterator[3];
        for (int i=0; i<3; i++)
            partitions[i] = partition(bytes(i), rows(0, 5, nowInSec));

        UnfilteredPartitionIterator iter = partitions(partitions);
        iter = info.withRepairedDataInfo(iter);
        consume(iter);

        assertEquals(15, counter.counted());
        assertEquals(5, counter.countedInCurrentPartition());
    }

    @Test
    public void digestOfSinglePartitionWithSingleRowAndEmptyStaticRow()
    {
        Digest manualDigest = Digest.forRepairedDataTracking();
        Row[] rows = rows(0, 1, nowInSec);
        UnfilteredRowIterator partition = partition(bytes(0), rows);
        addToDigest(manualDigest,
                    partition.partitionKey().getKey(),
                    partition.partitionLevelDeletion(),
                    Rows.EMPTY_STATIC_ROW,
                    rows);
        byte[] fromRepairedInfo = consume(partition);
        assertArrayEquals(manualDigest.digest(), fromRepairedInfo);
    }

    @Test
    public void digestOfSinglePartitionWithMultipleRowsAndEmptyStaticRow()
    {
        Digest manualDigest = Digest.forRepairedDataTracking();
        Row[] rows = rows(0, 5, nowInSec);
        UnfilteredRowIterator partition = partition(bytes(0), rows);
        addToDigest(manualDigest,
                    partition.partitionKey().getKey(),
                    partition.partitionLevelDeletion(),
                    Rows.EMPTY_STATIC_ROW,
                    rows);
        byte[] fromRepairedInfo = consume(partition);
        assertArrayEquals(manualDigest.digest(), fromRepairedInfo);
    }

    @Test
    public void digestOfSinglePartitionWithMultipleRowsAndTombstones()
    {
        Digest manualDigest = Digest.forRepairedDataTracking();
        Unfiltered[] unfiltereds = new Unfiltered[]
                                   {
                                       open(0), close(0),
                                       row(1, 1, nowInSec),
                                       open(2), close(4),
                                       row(5, 7, nowInSec)
                                   };
        UnfilteredRowIterator partition = partition(bytes(0), unfiltereds);
        addToDigest(manualDigest,
                    partition.partitionKey().getKey(),
                    partition.partitionLevelDeletion(),
                    Rows.EMPTY_STATIC_ROW,
                    unfiltereds);
        byte[] fromRepairedInfo = consume(partition);
        assertArrayEquals(manualDigest.digest(), fromRepairedInfo);
    }

    @Test
    public void digestOfMultiplePartitionsWithMultipleRowsAndNonEmptyStaticRows()
    {
        Digest manualDigest = Digest.forRepairedDataTracking();
        Row staticRow = staticRow(nowInSec);
        Row[] rows = rows(0, 5, nowInSec);
        UnfilteredRowIterator[] partitionsArray = new UnfilteredRowIterator[5];
        for (int i=0; i<5; i++)
        {
            UnfilteredRowIterator partition = partitionWithStaticRow(bytes(i), staticRow, rows);
            partitionsArray[i] = partition;
            addToDigest(manualDigest,
                        partition.partitionKey().getKey(),
                        partition.partitionLevelDeletion(),
                        staticRow,
                        rows);
        }

        UnfilteredPartitionIterator partitions = partitions(partitionsArray);
        byte[] fromRepairedInfo = consume(partitions);
        assertArrayEquals(manualDigest.digest(), fromRepairedInfo);
    }

    @Test
    public void digestOfFullyPurgedPartition()
    {
        long deletionTime = nowInSec - cfs.metadata().params.gcGraceSeconds - 1;
        DeletionTime deletion = DeletionTime.build((deletionTime * 1000), deletionTime);
        Row staticRow = staticRow(nowInSec, deletion);
        Row row = row(1, nowInSec, deletion);
        UnfilteredRowIterator partition = partitionWithStaticRow(bytes(0), staticRow, row);

        // The partition is fully purged, so nothing should be added to the digest
        byte[] fromRepairedInfo = consume(partition);
        assertEquals(0, fromRepairedInfo.length);
    }

    @Test
    public void digestOfEmptyPartition()
    {
        // Static row is read greedily during transformation and if the underlying
        // SSTableIterator doesn't contain the partition, an empty but non-null
        // static row is read and digested.
        UnfilteredRowIterator partition = partition(bytes(0));
        // The partition is completely empty, so nothing should be added to the digest
        byte[] fromRepairedInfo = consume(partition);
        assertEquals(0, fromRepairedInfo.length);
    }

    private RepairedDataInfo info()
    {
        return new RepairedDataInfo(DataLimits.NONE.newCounter(nowInSec, false, false, false));
    }

    private Digest addToDigest(Digest aggregate,
                               ByteBuffer partitionKey,
                               DeletionTime deletion,
                               Row staticRow,
                               Unfiltered...unfiltereds)
    {
        Digest perPartitionDigest = Digest.forRepairedDataTracking();
        if (staticRow != null && !staticRow.isEmpty())
            staticRow.digest(perPartitionDigest);
        perPartitionDigest.update(partitionKey);
        deletion.digest(perPartitionDigest);
        for (Unfiltered unfiltered : unfiltereds)
            unfiltered.digest(perPartitionDigest);
        byte[] rowDigestBytes = perPartitionDigest.digest();
        aggregate.update(rowDigestBytes, 0, rowDigestBytes.length);
        return aggregate;
    }

    private byte[] consume(UnfilteredPartitionIterator partitions)
    {
        RepairedDataInfo info = info();
        info.prepare(cfs, nowInSec, Long.MAX_VALUE);
        partitions.forEachRemaining(partition ->
        {
            try (UnfilteredRowIterator iter = info.withRepairedDataInfo(partition))
            {
                iter.forEachRemaining(u -> {});
            }
        });
        return getArray(info.getDigest());
    }

    private byte[] consume(UnfilteredRowIterator partition)
    {
        RepairedDataInfo info = info();
        info.prepare(cfs, nowInSec, Long.MAX_VALUE);
        try (UnfilteredRowIterator iter = info.withRepairedDataInfo(partition))
        {
            iter.forEachRemaining(u -> {});
        }
        return getArray(info.getDigest());
    }

    public static Cell<?> cell(ColumnMetadata def, Object value)
    {
        ByteBuffer bb = value instanceof ByteBuffer ? (ByteBuffer)value : ((AbstractType)def.type).decompose(value);
        return new BufferCell(def, 1L, BufferCell.NO_TTL, BufferCell.NO_DELETION_TIME, bb, null);
    }

    private Row staticRow(long nowInSec)
    {
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(Clustering.STATIC_CLUSTERING);
        builder.addCell(cell(staticMetadata, "static value"));
        return builder.build();
    }

    private Row staticRow(long nowInSec, DeletionTime deletion)
    {
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(Clustering.STATIC_CLUSTERING);
        builder.addRowDeletion(new Row.Deletion(deletion, false));
        return builder.build();
    }

    private Row row(int clustering, int value, long nowInSec)
    {
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(clustering(metadata.comparator, Integer.toString(clustering)));
        builder.addCell(cell(valueMetadata, Integer.toString(value)));
        return builder.build();
    }

    private Row row(int clustering, long nowInSec, DeletionTime deletion)
    {
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(clustering(metadata.comparator, Integer.toString(clustering)));
        builder.addRowDeletion(new Row.Deletion(deletion, false));
        return builder.build();
    }

    private Row[] rows(int clusteringStart, int clusteringEnd, long nowInSec)
    {
        return IntStream.range(clusteringStart, clusteringEnd)
                        .mapToObj(v -> row(v, v, nowInSec))
                        .toArray(Row[]::new);
    }

    private RangeTombstoneBoundMarker open(int start)
    {
        return new RangeTombstoneBoundMarker(
            ClusteringBound.create(ClusteringBound.boundKind(true, true),
                                   Clustering.make(Int32Type.instance.decompose(start))),
            DeletionTime.build(FBUtilities.timestampMicros(), FBUtilities.nowInSeconds()));
    }

    private RangeTombstoneBoundMarker close(int close)
    {
        return new RangeTombstoneBoundMarker(
            ClusteringBound.create(ClusteringBound.boundKind(false, true),
                                   Clustering.make(Int32Type.instance.decompose(close))),
            DeletionTime.build(FBUtilities.timestampMicros(), FBUtilities.nowInSeconds()));
    }

    private UnfilteredRowIterator partition(ByteBuffer pk, Unfiltered... unfiltereds)
    {
        return partitionWithStaticRow(pk, Rows.EMPTY_STATIC_ROW, unfiltereds);
    }

    private UnfilteredRowIterator partitionWithStaticRow(ByteBuffer pk, Row staticRow, Unfiltered... unfiltereds)
    {
        Iterator<Unfiltered> unfilteredIterator = Arrays.asList(unfiltereds).iterator();
        return new AbstractUnfilteredRowIterator(metadata, dk(pk), DeletionTime.LIVE, metadata.regularAndStaticColumns(), staticRow, false, EncodingStats.NO_STATS) {
            protected Unfiltered computeNext()
            {
                return unfilteredIterator.hasNext() ? unfilteredIterator.next() : endOfData();
            }
        };
    }

    private static UnfilteredPartitionIterator partitions(UnfilteredRowIterator...partitions)
    {
        Iterator<UnfilteredRowIterator> partitionsIter = Arrays.asList(partitions).iterator();
        return new AbstractUnfilteredPartitionIterator()
        {
            public TableMetadata metadata()
            {
                return metadata;
            }

            public boolean hasNext()
            {
                return partitionsIter.hasNext();
            }

            public UnfilteredRowIterator next()
            {
                return partitionsIter.next();
            }
        };
    }
}
