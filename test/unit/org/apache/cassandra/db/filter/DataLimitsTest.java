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

package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.aggregation.AggregationSpecification;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.db.filter.DataLimits.NO_LIMIT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataLimitsTest
{
    private final static Logger logger = LoggerFactory.getLogger(DataLimitsTest.class);

    final long nowInMs = System.currentTimeMillis();
    final long past1InMs = nowInMs - 10000;
    final long past2InMs = past1InMs - 10000;

    final int nowInSec = (int) (nowInMs / 1000L);
    final int past1InSec = (int) (past1InMs / 1000L);
    final int past2InSec = (int) (past2InMs / 1000L);

    final ColumnIdentifier vCol = ColumnIdentifier.getInterned("v", false);

    TableMetadata metadataSimple = TableMetadata.builder("ks", "tab")
                                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                                .addRegularColumn(vCol, Int32Type.instance)
                                                .build();
    ClusteringComparator comparatorSimple = metadataSimple.comparator;
    ColumnMetadata vSimple = metadataSimple.getColumn(vCol);

    final Row.Builder rowBuilder = BTreeRow.unsortedBuilder();


    ByteBuffer lastReturnedKey = ByteBufferUtil.bytes("lastReturnedKey");

    DataLimits cqlLimits = DataLimits.cqlLimits(19, 17);
    DataLimits cqlLimitsForPagingInRows = cqlLimits.forPaging(PageSize.inRows(13));
    DataLimits cqlLimitsForPagingInBytes = cqlLimits.forPaging(PageSize.inBytes(13));
    DataLimits cqlLimitsForPagingInRowsWithLastRow = cqlLimits.forPaging(PageSize.inRows(13), lastReturnedKey, 5);
    DataLimits cqlLimitsForPagingInBytesWithLastRow = cqlLimits.forPaging(PageSize.inBytes(13), lastReturnedKey, 5);
    DataLimits groupByLimits = DataLimits.groupByLimits(19, 17, NO_LIMIT, AggregationSpecification.AGGREGATE_EVERYTHING);
    DataLimits groupByLimitsForPagingInRows = groupByLimits.forPaging(PageSize.inRows(13));
    DataLimits groupByLimitsForPagingInBytes = groupByLimits.forPaging(PageSize.inBytes(13));
    DataLimits groupByLimitsForPagingInRowsWithLastRow = groupByLimits.forPaging(PageSize.inRows(13), lastReturnedKey, 5);
    DataLimits groupByLimitsForPagingInBytesWithLastRow = groupByLimits.forPaging(PageSize.inBytes(13), lastReturnedKey, 5);

    @BeforeClass
    public static void initClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void serializationTest()
    {
        for (MessagingService.Version version : MessagingService.Version.values())
        {
            checkSerialization(version, cqlLimits, "cql limits");
            checkSerialization(version, cqlLimitsForPagingInRows, "cql limits for paging in rows");
            checkSerialization(version, cqlLimitsForPagingInBytes, "cql limits for paging in bytes");
            checkSerialization(version, cqlLimitsForPagingInRowsWithLastRow, "cql limits for paging in rows with last row");
            checkSerialization(version, cqlLimitsForPagingInBytesWithLastRow, "cql limits for paging in bytes with last row");
            checkSerialization(version, groupByLimits, "group by limits");
            checkSerialization(version, groupByLimitsForPagingInRows, "group by limits for paging in rows");
            checkSerialization(version, groupByLimitsForPagingInBytes, "group by limits for paging in bytes");
            checkSerialization(version, groupByLimitsForPagingInRowsWithLastRow, "group by limits for paging in rows with last row");
            checkSerialization(version, groupByLimitsForPagingInBytesWithLastRow, "group by limits for paging in bytes with last row");
        }
    }

    @Test
    public void toStringTest()
    {
        String lastRetKeyStr = String.format("lastReturnedKey=%s", ByteBufferUtil.bytesToHex(lastReturnedKey));
        String lastRetKeyRemainingStr = "lastReturnedKeyRemaining=5";

        assertThat(cqlLimits.toString()).contains("ROWS LIMIT 19").contains("PER PARTITION LIMIT 17").doesNotContain("BYTES LIMIT");
        assertThat(cqlLimitsForPagingInRows.toString()).contains("ROWS LIMIT 13").contains("PER PARTITION LIMIT 17").doesNotContain("BYTES LIMIT");
        assertThat(cqlLimitsForPagingInBytes.toString()).contains("BYTES LIMIT 13").contains("ROWS LIMIT 19").contains("PER PARTITION LIMIT 17");
        assertThat(cqlLimitsForPagingInRowsWithLastRow.toString()).contains("ROWS LIMIT 13").contains("PER PARTITION LIMIT 17").doesNotContain("BYTES LIMIT").contains(lastRetKeyStr).contains(lastRetKeyRemainingStr);
        assertThat(cqlLimitsForPagingInBytesWithLastRow.toString()).contains("BYTES LIMIT 13").contains("ROWS LIMIT 19").contains("PER PARTITION LIMIT 17").contains(lastRetKeyStr).contains(lastRetKeyRemainingStr);

        assertThat(groupByLimits.toString()).contains("GROUP LIMIT 19").contains("GROUP PER PARTITION LIMIT 17").doesNotContain("ROWS LIMIT").doesNotContain("BYTES LIMIT");
        assertThat(groupByLimitsForPagingInRows.toString()).contains("GROUP LIMIT 19").contains("GROUP PER PARTITION LIMIT 17").contains("ROWS LIMIT 13").doesNotContain("BYTES LIMIT");
        assertThat(groupByLimitsForPagingInBytes.toString()).contains("GROUP LIMIT 19").contains("GROUP PER PARTITION LIMIT 17").doesNotContain("ROWS LIMIT").contains("BYTES LIMIT 13");
        assertThat(groupByLimitsForPagingInRowsWithLastRow.toString()).contains("GROUP LIMIT 19").contains("GROUP PER PARTITION LIMIT 17").contains("ROWS LIMIT 13").doesNotContain("BYTES LIMIT").contains(lastRetKeyStr).contains(lastRetKeyRemainingStr);
        assertThat(groupByLimitsForPagingInBytesWithLastRow.toString()).contains("GROUP LIMIT 19").contains("GROUP PER PARTITION LIMIT 17").doesNotContain("ROWS LIMIT").contains("BYTES LIMIT 13").contains(lastRetKeyStr).contains(lastRetKeyRemainingStr);
    }

    private void checkSerialization(MessagingService.Version version, DataLimits limits, String name)
    {
        String msg = String.format("serialization of %s for version %s", name, version);
        int size = (int) DataLimits.serializer.serializedSize(limits, version.value, null);
        try (DataOutputBuffer out = new DataOutputBuffer(2 * size))
        {
            DataLimits.serializer.serialize(limits, out, version.value, null);
            out.flush();
            assertThat(out.getLength()).describedAs(msg).isEqualTo(size);
            try (DataInputBuffer in = new DataInputBuffer(out.getData()))
            {
                DataLimits deserializedLimits = DataLimits.serializer.deserialize(in, version.value, metadataSimple);
                assertThat(deserializedLimits.count()).describedAs(msg).isEqualTo(limits.count());

                assertThat(deserializedLimits.count()).describedAs(msg).isEqualTo(limits.count());
                assertThat(deserializedLimits.perPartitionCount()).describedAs(msg).isEqualTo(limits.perPartitionCount());
                assertThat(deserializedLimits.isDistinct()).describedAs(msg).isEqualTo(limits.isDistinct());
                assertThat(deserializedLimits.isUnlimited()).describedAs(msg).isEqualTo(limits.isUnlimited());
                assertThat(deserializedLimits.kind()).describedAs(msg).isEqualTo(limits.kind());
                assertThat(deserializedLimits.isGroupByLimit()).describedAs(msg).isEqualTo(limits.isGroupByLimit());
            }
            catch (IOException e)
            {
                logger.error("Failed to deserialize: " + msg, e);
                fail(msg);
            }
        }
        catch (IOException e)
        {
            logger.error("Failed to serialize: " + msg, e);
            fail(msg);
        }
    }

    @Test
    public void verifyApplyingCqlLimits()
    {
        int rowSize = makeSimpleRow().dataSize();

        verifyApplyingCqlLimits(DataLimits.cqlLimits(3), 7, 3);
        verifyApplyingCqlLimits(DataLimits.cqlLimits(7), 3, 3);
        verifyApplyingCqlLimits(DataLimits.cqlLimits(3, 5), 7, 3);
        verifyApplyingCqlLimits(DataLimits.cqlLimits(3, 7), 5, 3);
        verifyApplyingCqlLimits(DataLimits.cqlLimits(5, 3), 7, 3);
        verifyApplyingCqlLimits(DataLimits.cqlLimits(5, 7), 3, 3);
        verifyApplyingCqlLimits(DataLimits.cqlLimits(7, 5), 3, 3);
        verifyApplyingCqlLimits(DataLimits.cqlLimits(7, 3), 5, 3);

        verifyApplyingCqlLimits(DataLimits.cqlLimits(10, 5).forPaging(PageSize.inRows(3)), 15, 3);
        verifyApplyingCqlLimits(DataLimits.cqlLimits(10, 5).forPaging(PageSize.inRows(7)), 15, 5);
        verifyApplyingCqlLimits(DataLimits.cqlLimits(10, 5).forPaging(PageSize.inBytes(3 * rowSize)), 15, 3);
        verifyApplyingCqlLimits(DataLimits.cqlLimits(10, 5).forPaging(PageSize.inBytes(7 * rowSize)), 15, 5);

        verifyApplyingCqlLimits(DataLimits.cqlLimits(10, 5).forPaging(PageSize.inRows(3)), 15, 3);
        verifyApplyingCqlLimits(DataLimits.cqlLimits(10, 5).forPaging(PageSize.inRows(7)), 15, 5);
        verifyApplyingCqlLimits(DataLimits.cqlLimits(10, 5).forPaging(PageSize.inBytes(3 * rowSize)), 15, 3);
        verifyApplyingCqlLimits(DataLimits.cqlLimits(10, 5).forPaging(PageSize.inBytes(7 * rowSize)), 15, 5);
    }

    @Test
    public void forPagingTest()
    {
        assertDataLimits(DataLimits.cqlLimits(10), 10, 10, NO_LIMIT);
        assertDataLimits(DataLimits.cqlLimits(10, 5), 10, 10, 5);
        assertDataLimits(DataLimits.cqlLimits(10, 5).forPaging(PageSize.inRows(3)), 3, 3, 5);
        assertDataLimits(DataLimits.cqlLimits(10, 5).forPaging(PageSize.inRows(13)), 10, 10, 5);
        assertDataLimits(DataLimits.cqlLimits(10, 5).forPaging(PageSize.inBytes(128)), 10, 10, 5);
    }

    private void assertDataLimits(DataLimits limits, int count, int rows, int perPartitionCount)
    {
        assertThat(limits.count()).isEqualTo(count);
        assertThat(limits.count()).isEqualTo(rows);
        assertThat(limits.perPartitionCount()).isEqualTo(perPartitionCount);
    }

    private Row makeSimpleRow()
    {
        rowBuilder.newRow(Clustering.EMPTY);
        rowBuilder.addCell(BufferCell.live(metadataSimple.getColumn(vCol), past1InMs, Int32Type.instance.decompose(1)));
        return rowBuilder.build();
    }

    private void verifyApplyingCqlLimits(DataLimits limits, int cnt, int expectedRows)
    {
        TableMetadata metadata = metadataSimple;
        RowIterator rowIt = createRowIterator(pk(1), metadata, metadata.regularAndStaticColumns(), Rows.EMPTY_STATIC_ROW, cnt, i -> makeSimpleRow());
        assertThat(verifyRowIterator(rowIt, cnt, limits)).isEqualTo(expectedRows);
    }

    private int verifyRowIterator(RowIterator rowIterator, int rowsCnt, DataLimits limits)
    {
        DataLimits.Counter counter = limits.newCounter(nowInSec, true, true, false);
        RowIterator limRowIt = counter.applyTo(rowIterator);

        assertCounter(counter, 0, 0, 0);

        AtomicInteger i = new AtomicInteger(0);
        AtomicInteger s = new AtomicInteger(0);
        AtomicInteger lastRowSize = new AtomicInteger(0);
        limRowIt.forEachRemaining(row -> {
            assertThat(row).isNotNull();
            int n = i.incrementAndGet();
            int rowSize = row.dataSize();
            lastRowSize.set(rowSize);
            int dataSize = s.addAndGet(rowSize);
            assertCounter(counter, n, n, n);
        });

        int rowsLimit = Math.min(limits.count(), limits.perPartitionCount());

        assertThat(i.get()).isLessThanOrEqualTo(Math.min(rowsCnt, rowsLimit));
        assertThat(counter.isDone()).isEqualTo(i.get() == limits.count());
        assertThat(counter.isDoneForPartition()).isEqualTo(counter.isDone() || i.get() == limits.perPartitionCount());
        return i.get();
    }

    private static void assertCounter(DataLimits.Counter counter, int counted, int rowsCountedInPartition, int rowsCounted)
    {
        assertThat(counter.counted()).isEqualTo(counted);
        assertThat(counter.rowsCounted()).isEqualTo(rowsCounted);
        assertThat(counter.rowsCountedInCurrentPartition()).isEqualTo(rowsCountedInPartition);
    }

    private RowIterator createRowIterator(DecoratedKey partitionKey, TableMetadata metadata, RegularAndStaticColumns columns, Row staticRow, int count, IntFunction<Row> rowGen)
    {
        AtomicInteger idx = new AtomicInteger(0);
        RowIterator rowIterator = mock(RowIterator.class);
        when(rowIterator.hasNext()).thenAnswer(ignored -> idx.get() < count);
        when(rowIterator.isEmpty()).thenReturn(count > 0);
        when(rowIterator.next()).thenAnswer(ignored -> {
            int i = idx.getAndIncrement();
            if (i >= count)
                throw new NoSuchElementException();
            return rowGen.apply(i);
        });
        when(rowIterator.isReverseOrder()).thenReturn(false);
        when(rowIterator.staticRow()).thenReturn(staticRow);
        when(rowIterator.partitionKey()).thenReturn(partitionKey);
        when(rowIterator.metadata()).thenReturn(metadata);
        when(rowIterator.columns()).thenReturn(columns);
        return rowIterator;
    }

    private PartitionIterator createPartitionIterator(int partitionCount, IntFunction<RowIterator> rowIteratorGen)
    {
        AtomicInteger partitionIdx = new AtomicInteger(0);

        PartitionIterator partitionIterator = mock(PartitionIterator.class);
        when(partitionIterator.hasNext()).thenAnswer(ignored -> partitionIdx.get() < partitionCount);
        when(partitionIterator.next()).thenAnswer(ignored -> {
            int idx = partitionIdx.getAndIncrement();
            if (idx >= partitionCount)
                throw new NoSuchElementException();
            return rowIteratorGen.apply(idx);
        });

        return partitionIterator;
    }

    private DecoratedKey pk(int i)
    {
        ByteBuffer key = Int32Type.instance.decompose(i);
        return new BufferDecoratedKey(DatabaseDescriptor.getPartitioner().getToken(key), key);
    }
}