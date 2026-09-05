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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.rows.WrappingUnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.test.log.ClusterMetadataTestHelper;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.metrics.ReadResponseMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ReadResponseTest
{
    private final Random random = new Random();
    private TableMetadata metadata;
    private TableMetadata metadataWithClustering;
    private TableMetadata metadataWithStatic;
    private TableMetadata metadataWithCollection;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.daemonInitialization();
        ClusterMetadataTestHelper.setInstanceForTest();
    }
    @Before
    public void setup()
    {
        metadata = TableMetadata.builder("ks", "t1")
                                .offline()
                                .addPartitionKeyColumn("p", Int32Type.instance)
                                .addRegularColumn("v", Int32Type.instance)
                                .partitioner(Murmur3Partitioner.instance)
                                .build();

        metadataWithClustering = TableMetadata.builder("ks", "t2")
                                              .offline()
                                              .addPartitionKeyColumn("p", Int32Type.instance)
                                              .addClusteringColumn("c", Int32Type.instance)
                                              .addRegularColumn("v", Int32Type.instance)
                                              .partitioner(Murmur3Partitioner.instance)
                                              .build();

        metadataWithCollection = TableMetadata.builder("ks", "t4")
                                              .offline()
                                              .addPartitionKeyColumn("p", Int32Type.instance)
                                              .addStaticColumn("s", Int32Type.instance)
                                              .addClusteringColumn("c", Int32Type.instance)
                                              .addRegularColumn("v", Int32Type.instance)
                                              .addRegularColumn("coll", SetType.getInstance(Int32Type.instance, true))
                                              .partitioner(Murmur3Partitioner.instance)
                                              .build();

        metadataWithStatic = TableMetadata.builder("ks", "t3")
                                          .offline()
                                          .addPartitionKeyColumn("p", Int32Type.instance)
                                          .addStaticColumn("s", Int32Type.instance)
                                          .addClusteringColumn("c", Int32Type.instance)
                                          .addRegularColumn("v", Int32Type.instance)
                                          .partitioner(Murmur3Partitioner.instance)
                                          .build();
    }

    @Test
    public void fromCommandWithConclusiveRepairedDigest()
    {
        ByteBuffer digest = digest();
        ReadCommand command = command(key(), metadata);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(digest, true);
        ReadResponse response = command.createResponse(EmptyIterators.unfilteredPartition(metadata), rdi);
        assertTrue(response.isRepairedDigestConclusive());
        assertEquals(digest, response.repairedDataDigest());
        verifySerDe(response);
    }

    @Test
    public void fromCommandWithInconclusiveRepairedDigest()
    {
        ByteBuffer digest = digest();
        ReadCommand command = command(key(), metadata);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(digest, false);
        ReadResponse response = command.createResponse(EmptyIterators.unfilteredPartition(metadata), rdi);
        assertFalse(response.isRepairedDigestConclusive());
        assertEquals(digest, response.repairedDataDigest());
        verifySerDe(response);
    }

    @Test
    public void fromCommandWithConclusiveEmptyRepairedDigest()
    {
        ReadCommand command = command(key(), metadata);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);
        ReadResponse response = command.createResponse(EmptyIterators.unfilteredPartition(metadata), rdi);
        assertTrue(response.isRepairedDigestConclusive());
        assertEquals(ByteBufferUtil.EMPTY_BYTE_BUFFER, response.repairedDataDigest());
        verifySerDe(response);
    }

    @Test
    public void fromCommandWithInconclusiveEmptyRepairedDigest()
    {
        ReadCommand command = command(key(), metadata);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, false);
        ReadResponse response = command.createResponse(EmptyIterators.unfilteredPartition(metadata), rdi);
        assertFalse(response.isRepairedDigestConclusive());
        assertEquals(ByteBufferUtil.EMPTY_BYTE_BUFFER, response.repairedDataDigest());
        verifySerDe(response);
    }

    /*
     * Digest responses should never include repaired data tracking as we only request
     * it in read repair or for range queries
     */
    @Test (expected = UnsupportedOperationException.class)
    public void digestResponseErrorsIfRepairedDataDigestRequested()
    {
        ReadCommand command = digestCommand(key(), metadata);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);
        ReadResponse response = command.createResponse(EmptyIterators.unfilteredPartition(metadata), rdi);
        assertTrue(response.isDigestResponse());
        assertFalse(response.mayIncludeRepairedDigest());
        response.repairedDataDigest();
    }

    @Test (expected = UnsupportedOperationException.class)
    public void digestResponseErrorsIfIsConclusiveRequested()
    {
        ReadCommand command = digestCommand(key(), metadata);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);
        ReadResponse response = command.createResponse(EmptyIterators.unfilteredPartition(metadata), rdi);
        assertTrue(response.isDigestResponse());
        assertFalse(response.mayIncludeRepairedDigest());
        response.isRepairedDigestConclusive();
    }

    @Test (expected = UnsupportedOperationException.class)
    public void digestResponseErrorsIfIteratorRequested()
    {
        ReadCommand command = digestCommand(key(), metadata);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);
        ReadResponse response = command.createResponse(EmptyIterators.unfilteredPartition(metadata), rdi);
        assertTrue(response.isDigestResponse());
        assertFalse(response.mayIncludeRepairedDigest());
        response.makeIterator(command);
    }

    @Test
    public void makeDigestDoesntConsiderRepairedDataInfo()
    {
        // It shouldn't be possible to get false positive DigestMismatchExceptions based
        // on differing repaired data tracking info because it isn't requested on initial
        // requests, only following a digest mismatch. Having a test doesn't hurt though
        int key = key();
        ByteBuffer digest1 = digest();
        ReadCommand command1 = command(key, metadata);
        StubRepairedDataInfo rdi1 = new StubRepairedDataInfo(digest1, true);
        ReadResponse response1 = command1.createResponse(EmptyIterators.unfilteredPartition(metadata), rdi1);

        ByteBuffer digest2 = digest();
        ReadCommand command2 = command(key, metadata);
        StubRepairedDataInfo rdi2 = new StubRepairedDataInfo(digest2, false);
        ReadResponse response2 = command1.createResponse(EmptyIterators.unfilteredPartition(metadata), rdi2);

        assertEquals(response1.digest(command1), response2.digest(command2));
    }

    @Test
    public void inMemoryResponseEmptyIteratorMatchesLocalDataResponse()
    {
        ReadCommand command = command(key(), metadata);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);

        ReadResponse localResponse = command.createResponse(EmptyIterators.unfilteredPartition(metadata), rdi);
        ReadResponse inMemoryResponse = command.createLocalObjectResponse(EmptyIterators.unfilteredPartition(metadata), rdi, false);

        assertIteratorsEqual(command, localResponse, inMemoryResponse);
    }

    @Test
    public void inMemoryResponseWithRowsMatchesLocalDataResponse()
    {
        int key = key();
        ReadCommand command = command(key, metadata);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);

        DecoratedKey dk = metadata.partitioner.decorateKey(ByteBufferUtil.bytes(key));
        Row row = buildRow(metadata, dk);
        PartitionUpdate update = PartitionUpdate.singleRowUpdate(metadata, dk, row);

        ReadResponse localResponse = command.createResponse(singlePartitionIterator(update), rdi);
        ReadResponse inMemoryResponse = command.createLocalObjectResponse(singlePartitionIterator(update), rdi, false);

        assertIteratorsEqual(command, localResponse, inMemoryResponse);
    }

    @Test
    public void inMemoryResponseDigestsLikeSerializedResponse()
    {
        // A partition contains a row deletion only.
        // Read from a table that declares a static column but holds no static data.
        int key = key();
        ReadCommand command = command(key, metadataWithStatic);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);

        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadataWithStatic, ByteBufferUtil.bytes(key)).timestamp(1);
        builder.row(0).delete();
        PartitionUpdate update = builder.build();

        ReadResponse localResponse = command.createResponse(readIterator(update, command), rdi);
        ReadResponse inMemoryResponse = command.createLocalObjectResponse(readIterator(update, command), rdi, true);

        assertEquals(ByteBufferUtil.bytesToHex(localResponse.digest(command)),
                     ByteBufferUtil.bytesToHex(inMemoryResponse.digest(command)));
    }

    @Test
    public void inMemoryResponseWithOverflowDigestsLikeSerializedResponse()
    {
        // Same as above for the overflow path, where the response is serialized from the in-memory prefix followed
        // by the rows that did not fit: the static row of the prefix must survive into that buffer.
        int key = key();
        ReadCommand command = command(key, metadataWithStatic);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);

        PartitionUpdate update = buildMultiRowUpdate(metadataWithStatic, key, 5);

        ReadResponse localResponse = command.createResponse(readIterator(update, command), rdi);
        ReadResponse overflowedResponse = ReadResponse.createInMemoryDataResponse(readIterator(update, command), command, rdi, 2, 0);

        assertEquals("response should have overflowed", 0, overflowedResponse.inMemoryUnfilteredCount());
        assertEquals(ByteBufferUtil.bytesToHex(localResponse.digest(command)),
                     ByteBufferUtil.bytesToHex(overflowedResponse.digest(command)));
    }

    @Test
    public void inMemoryResponseKeepsStaticRowContent()
    {
        // A partition which does hold static data: the static row must be kept as it is
        int key = key();
        ReadCommand command = command(key, metadataWithStatic);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);

        PartitionUpdate update = buildMultiRowUpdate(metadataWithStatic, key, 3);
        Row staticRow = staticRow(metadataWithStatic, 7);

        ReadResponse localResponse = command.createResponse(readIterator(update, command, staticRow), rdi);
        ReadResponse inMemoryResponse = command.createLocalObjectResponse(readIterator(update, command, staticRow), rdi, true);

        assertIteratorsEqual(command, localResponse, inMemoryResponse);
        assertEquals(ByteBufferUtil.bytesToHex(localResponse.digest(command)),
                     ByteBufferUtil.bytesToHex(inMemoryResponse.digest(command)));
    }

    @Test
    public void inMemoryResponseWithoutStaticRowDigestsLikeSerializedResponse()
    {
        // The other direction: a read that produced no static row at all must not gain one
        int key = key();
        ReadCommand command = command(key, metadataWithStatic);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);

        PartitionUpdate update = buildMultiRowUpdate(metadataWithStatic, key, 3);

        ReadResponse localResponse = command.createResponse(singlePartitionIterator(update), rdi);
        ReadResponse inMemoryResponse = command.createLocalObjectResponse(singlePartitionIterator(update), rdi, true);

        assertEquals(ByteBufferUtil.bytesToHex(localResponse.digest(command)),
                     ByteBufferUtil.bytesToHex(inMemoryResponse.digest(command)));
    }

    @Test
    public void inMemoryResponseKeepsColumnsOfAReadThatSelectedNoRows()
    {
        // A read whose clustering filter selects nothing (paging past the last slice of the page) returns
        // noRowsIterator, whose columns() are those of the static row alone and not the ones the filter fetches.
        // The digest covers columns().regulars, so the in-memory response has to report the very same columns.
        int key = key();
        ReadCommand command = command(key, metadataWithStatic);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);

        Row staticRow = staticRow(metadataWithStatic, 7);
        ReadResponse localResponse = command.createResponse(noRowsIterator(command, key, staticRow), rdi);
        ReadResponse inMemoryResponse = command.createLocalObjectResponse(noRowsIterator(command, key, staticRow), rdi, true);

        assertEquals(columnsOf(command, localResponse), columnsOf(command, inMemoryResponse));
        assertEquals(ByteBufferUtil.bytesToHex(localResponse.digest(command)),
                     ByteBufferUtil.bytesToHex(inMemoryResponse.digest(command)));
    }

    @Test
    public void inMemoryResponseDropsCellsShadowedByARowDeletion()
    {
        // A row deletion and a cell of the very same timestamp survive side by side in a memtable, neither
        // superseding the other. Serializing the response drops the cell when the row is rebuilt on read back, so a
        // response kept in memory has to drop it as well.
        int key = key();
        ReadCommand command = command(key, metadataWithClustering);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);
        Row row = deletedRowWithShadowedCell(metadataWithClustering, 1);

        ReadResponse localResponse = command.createResponse(singlePartitionIterator(rowIterator(command, key, row)), rdi);
        ReadResponse inMemoryResponse = command.createLocalObjectResponse(singlePartitionIterator(rowIterator(command, key, row)), rdi, true);

        assertIteratorsEqual(command, localResponse, inMemoryResponse);
        assertEquals(ByteBufferUtil.bytesToHex(localResponse.digest(command)),
                     ByteBufferUtil.bytesToHex(inMemoryResponse.digest(command)));
    }

    @Test
    public void inMemoryResponseMatchesSerializedResponseForAnEmptyPartition()
    {
        // A partition that is returned but holds nothing is serialized as a flag on its own and read back without
        // columns, so the in-memory response must present it that way and not with the columns the read fetched.
        int key = key();
        ReadCommand command = command(key, metadataWithStatic);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);

        ReadResponse localResponse = command.createResponse(emptyPartitionIterator(command, key), rdi);
        ReadResponse inMemoryResponse = command.createLocalObjectResponse(emptyPartitionIterator(command, key), rdi, true);

        assertEquals(columnsOf(command, localResponse), columnsOf(command, inMemoryResponse));
        assertEquals(ByteBufferUtil.bytesToHex(localResponse.digest(command)),
                     ByteBufferUtil.bytesToHex(inMemoryResponse.digest(command)));
    }

    @Test
    public void inMemoryResponseMatchesSerializedResponseForRandomPartitions()
    {
        // Whatever the read produced, a response kept in memory has to be indistinguishable from the same response
        // put through the serializer, both in what it returns and in its digest, since that digest is compared
        // against the one every other replica computes from the serialized form.
        long seed = nanoTime();
        Random rnd = new Random(seed);

        for (int i = 0; i < 300; i++)
        {
            RandomPartition partition = randomPartition(rnd);
            ReadCommand command = command(partition.key, metadataWithCollection, partition.reversed);
            StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);
            String context = "seed " + seed + ", iteration " + i;

            String serialized = describe(command, command.createResponse(partition.iterator(command), rdi));
            // both limits disabled: the response is kept as an object graph
            String inMemory = describe(command, ReadResponse.createInMemoryDataResponse(partition.iterator(command), command, rdi, 0, 0));
            // a row limit of one: anything longer overflows and is serialized in full
            String overflowed = describe(command, ReadResponse.createInMemoryDataResponse(partition.iterator(command), command, rdi, 1, 0));

            assertEquals(context, serialized, inMemory);
            assertEquals(context, serialized, overflowed);
        }
    }

    private RandomPartition randomPartition(Random rnd)
    {
        return new RandomPartition(rnd);
    }

    private class RandomPartition
    {
        private static final long ROW_TIMESTAMP = 10;

        final int key;
        final boolean reversed;

        private final PartitionUpdate update;
        private final Row staticRow;
        // report the columns of the content rather than the ones the filter fetches
        private final boolean contentColumns;
        // > 0: give every row a deletion of that timestamp, added after its cells so they survive in the row
        private final long shadowingTimestamp;

        RandomPartition(Random rnd)
        {
            key = key();
            reversed = rnd.nextBoolean();
            contentColumns = rnd.nextBoolean();
            shadowingTimestamp = rnd.nextInt(3) == 0 ? ROW_TIMESTAMP + rnd.nextInt(2) * 10 : 0;

            switch (rnd.nextInt(3))
            {
                case 0:
                    // no static row at all
                    staticRow = Rows.EMPTY_STATIC_ROW;
                    break;
                case 1:
                    // a partition without static data still carries an empty static row, see readIterator
                    staticRow = BTreeRow.emptyRow(Clustering.STATIC_CLUSTERING);
                    break;
                default:
                    staticRow = staticRow(metadataWithCollection, rnd.nextInt(100));
            }

            PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadataWithCollection, ByteBufferUtil.bytes(key));
            if (rnd.nextInt(6) == 0)
                builder.timestamp(1).delete();  // older than the rows below, so it does not shadow them
            builder.timestamp(ROW_TIMESTAMP);

            int rows = rnd.nextInt(5);
            for (int c = 0; c < rows; c++)
            {
                Row.SimpleBuilder row = builder.row(c);
                if (rnd.nextInt(5) == 0)
                    row.delete();
                else
                {
                    row.add("v", rnd.nextInt(100));
                    if (rnd.nextBoolean())
                        row.add("coll", Set.of(rnd.nextInt(10), 10 + rnd.nextInt(10)));
                }
            }

            for (int t = rnd.nextInt(3); t > 0; t--)
            {
                int start = rnd.nextInt(8);
                Slice slice = Slice.make(Clustering.make(ByteBufferUtil.bytes(start)),
                                         Clustering.make(ByteBufferUtil.bytes(start + 1 + rnd.nextInt(3))));
                builder.addRangeTombstone(new RangeTombstone(slice, DeletionTime.build(ROW_TIMESTAMP + 1, FBUtilities.nowInSeconds())));
            }

            update = builder.build();
        }

        UnfilteredPartitionIterator iterator(ReadCommand command)
        {
            UnfilteredRowIterator rowIter = update.unfilteredIterator(command.columnFilter(), Slices.ALL, reversed);
            RegularAndStaticColumns columns = contentColumns
                                              ? new RegularAndStaticColumns(Columns.from(staticRow), update.columns().regulars)
                                              : command.columnFilter().fetchedColumns();

            return singlePartitionIterator(new WrappingUnfilteredRowIterator()
            {
                public UnfilteredRowIterator wrapped() { return rowIter; }

                @Override
                public RegularAndStaticColumns columns() { return columns; }

                @Override
                public Row staticRow() { return staticRow; }

                @Override
                public Unfiltered next()
                {
                    Unfiltered next = rowIter.next();
                    return next.isRow() ? withShadowedCells((Row) next) : next;
                }
            });
        }

        private Row withShadowedCells(Row row)
        {
            if (shadowingTimestamp == 0 || row.isEmpty())
                return row;

            Row.Builder builder = BTreeRow.unsortedBuilder();
            builder.newRow(row.clustering());
            builder.addPrimaryKeyLivenessInfo(row.primaryKeyLivenessInfo());
            for (ColumnData cd : row)
            {
                if (cd.column().isSimple())
                {
                    builder.addCell((Cell<?>) cd);
                }
                else
                {
                    ComplexColumnData complexData = (ComplexColumnData) cd;
                    if (!complexData.complexDeletion().isLive())
                        builder.addComplexDeletion(complexData.column(), complexData.complexDeletion());
                    for (Cell<?> cell : complexData)
                        builder.addCell(cell);
                }
            }
            // added last, so the cells above are kept even where it covers them
            builder.addRowDeletion(Row.Deletion.regular(DeletionTime.build(shadowingTimestamp, FBUtilities.nowInSeconds())));
            return builder.build();
        }
    }

    private String describe(ReadCommand command, ReadResponse response)
    {
        StringBuilder description = new StringBuilder();
        try (UnfilteredPartitionIterator iter = response.makeIterator(command))
        {
            while (iter.hasNext())
            {
                try (UnfilteredRowIterator partition = iter.next())
                {
                    description.append("columns=").append(partition.columns())
                               .append(" partitionDeletion=").append(partition.partitionLevelDeletion())
                               .append(" reversed=").append(partition.isReverseOrder())
                               .append(" static=").append(partition.staticRow().toString(partition.metadata(), true));
                    while (partition.hasNext())
                        description.append("\n  ").append(partition.next().toString(partition.metadata(), true));
                }
            }
        }
        return description.append("\ndigest=").append(ByteBufferUtil.bytesToHex(response.digest(command))).toString();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void inMemoryResponseCannotBeSerialized()
    {
        ReadCommand command = command(key(), metadata);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);
        ReadResponse response = command.createLocalObjectResponse(EmptyIterators.unfilteredPartition(metadata), rdi, false);

        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            ReadResponse.serializer.serialize(response, out, MessagingService.current_version);
        }
        catch (IOException e)
        {
            fail("Unexpected IOException: " + e.getMessage());
        }
    }

    @Test
    public void inMemoryResponseUsesHigherLimitsForOneConsistency()
    {
        int key = key();
        ReadCommand command = command(key, metadataWithClustering);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);
        // More rows than the default row limit (128) but fewer than the ONE/LOCAL_ONE limit (512).
        int rowCount = 150;
        PartitionUpdate update = buildMultiRowUpdate(metadataWithClustering, key, rowCount);

        ReadResponse otherResponse = command.createLocalObjectResponse(singlePartitionIterator(update), rdi, false);
        ReadResponse oneResponse = command.createLocalObjectResponse(singlePartitionIterator(update), rdi, true);

        // The default-tier limits overflow this partition; ONE/LOCAL_ONE keeps more of it in memory.
        assertTrue("default tier should overflow", otherResponse.inMemoryUnfilteredCount() < rowCount);
        assertTrue("ONE/LOCAL_ONE tier should keep more rows in memory",
                   oneResponse.inMemoryUnfilteredCount() > otherResponse.inMemoryUnfilteredCount());
    }

    @Test
    public void inMemoryResponseCountsRowLimitHitOnOverflow()
    {
        // Row limit 1, size limit disabled: the rows beyond the first overflow, hitting the row-count limit.
        limitHitAssertion()
            .rows(5).maxRows(1)
            .expectRowLimitHit()
            .verify();
    }

    @Test
    public void inMemoryResponseCountsSizeLimitHitOnOverflow()
    {
        // Row limit disabled, tiny size limit: the rows beyond the first overflow, hitting the size limit.
        limitHitAssertion()
            .rows(5).maxSize(1)
            .expectSizeLimitHit()
            .verify();
    }

    @Test
    public void inMemoryResponseCountsRowLimitFirstWhenBothLimitsCrossed()
    {
        // Both limits would be crossed; the row-count limit is checked first, so only it is counted.
        limitHitAssertion()
            .rows(5).maxRows(1).maxSize(1)
            .expectRowLimitHit()
            .verify();
    }

    @Test
    public void inMemoryResponseDoesNotCountLimitHitWhenAllInMemory()
    {
        // Limits above the response size: nothing overflows.
        limitHitAssertion()
            .rows(3).maxRows(100).maxSize(Long.MAX_VALUE)
            .expectNoLimitHit()
            .verify();
    }

    @Test
    public void inMemoryResponseCountsEmptyResponseAsInMemory()
    {
        // An empty response is not serialized either, so it counts as an in-memory one.
        ReadCommand command = command(key(), metadataWithClustering);
        StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);

        long inMemoryBefore = ReadResponseMetrics.inMemoryResponses.getCount();
        ReadResponse response = ReadResponse.createInMemoryDataResponse(EmptyIterators.unfilteredPartition(metadataWithClustering), command, rdi, 10, 0);
        assertEquals(0, response.inMemoryUnfilteredCount());
        assertEquals(inMemoryBefore + 1, ReadResponseMetrics.inMemoryResponses.getCount());
    }

    private LimitHitAssertionBuilder limitHitAssertion()
    {
        return new LimitHitAssertionBuilder();
    }

    /**
     * Builds an in-memory response for a partition of {@link #rows} rows with the configured per-request limits
     * ({@code 0} disables a limit) and asserts which limit-hit counter(s) were incremented (each by one).
     */
    private class LimitHitAssertionBuilder
    {
        private int rows = 0;
        private int maxRows = 0;   // 0 = row-count limit disabled
        private long maxSize = 0;  // 0 = heap-size limit disabled
        private boolean expectRowHit = false;
        private boolean expectSizeHit = false;

        LimitHitAssertionBuilder rows(int rows) { this.rows = rows; return this; }
        LimitHitAssertionBuilder maxRows(int maxRows) { this.maxRows = maxRows; return this; }
        LimitHitAssertionBuilder maxSize(long maxSize) { this.maxSize = maxSize; return this; }
        LimitHitAssertionBuilder expectRowLimitHit() { this.expectRowHit = true; return this; }
        LimitHitAssertionBuilder expectSizeLimitHit() { this.expectSizeHit = true; return this; }
        LimitHitAssertionBuilder expectNoLimitHit() { this.expectRowHit = false; this.expectSizeHit = false; return this; }

        void verify()
        {
            int key = key();
            ReadCommand command = command(key, metadataWithClustering);
            StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);
            PartitionUpdate update = buildMultiRowUpdate(metadataWithClustering, key, rows);

            long rowHitsBefore = ReadResponseMetrics.inMemoryRowLimitHits.getCount();
            long sizeHitsBefore = ReadResponseMetrics.inMemorySizeLimitHits.getCount();
            long inMemoryBefore = ReadResponseMetrics.inMemoryResponses.getCount();
            ReadResponse.createInMemoryDataResponse(singlePartitionIterator(update), command, rdi, maxRows, maxSize);
            assertEquals("unexpected row limit hit count", rowHitsBefore + (expectRowHit ? 1 : 0), ReadResponseMetrics.inMemoryRowLimitHits.getCount());
            assertEquals("unexpected size limit hit count", sizeHitsBefore + (expectSizeHit ? 1 : 0), ReadResponseMetrics.inMemorySizeLimitHits.getCount());
            boolean keptInMemory = !expectRowHit && !expectSizeHit;
            assertEquals("unexpected in-memory response count", inMemoryBefore + (keptInMemory ? 1 : 0), ReadResponseMetrics.inMemoryResponses.getCount());
        }
    }

    @Test
    public void inMemoryResponseClosesRowIteratorOnceWhenAllInMemory()
    {
        // Nothing overflows, so the row iterator is only ever closed by the response builder itself.
        assertRowIteratorClosedOnce()
            .rows(3)
            .maxRows(10)
            .expectInMemory()
            .verify();
    }

    @Test
    public void inMemoryResponseClosesRowIteratorOnceOnRowLimitOverflow()
    {
        assertRowIteratorClosedOnce()
            .rows(5)
            .maxRows(2)
            .expectSerialized()
            .verify();
    }

    @Test
    public void inMemoryResponseClosesRowIteratorOnceOnSizeLimitOverflow()
    {
        assertRowIteratorClosedOnce()
            .rows(5)
            .maxSize(1)
            .expectSerialized()
            .verify();
    }

    private CloseAssertionBuilder assertRowIteratorClosedOnce()
    {
        return new CloseAssertionBuilder();
    }

    /**
     * Builds an in-memory response over a row iterator that counts how often it is closed, and asserts the count is
     * exactly one. On overflow the iterator is passed on to the serializer, which closes every partition it
     * serializes, so the response builder must not close it again. Closing a partition is what triggers the recording
     * of per-read metrics (see {@link ReadCommand#withMetricsRecording}) and similar end-of-partition work, and a
     * second close would count all of it twice. The count is taken on a wrapper handed straight to the response
     * builder rather than through the transform framework, whose iterators swallow repeated closes internally
     * ({@code BaseIterator.close}) and would therefore hide the problem.
     */
    private class CloseAssertionBuilder
    {
        private int rows = 0;
        private int maxRows = 0;   // 0 = row-count limit disabled
        private long maxSize = 0;  // 0 = heap-size limit disabled
        // true = the response is expected to stay in memory, false = a limit is crossed and it is serialized in full
        private Boolean expectInMemory = null;

        CloseAssertionBuilder rows(int rows) { this.rows = rows; return this; }
        CloseAssertionBuilder maxRows(int maxRows) { this.maxRows = maxRows; return this; }
        CloseAssertionBuilder maxSize(long maxSize) { this.maxSize = maxSize; return this; }
        CloseAssertionBuilder expectInMemory() { this.expectInMemory = true; return this; }
        CloseAssertionBuilder expectSerialized() { this.expectInMemory = false; return this; }

        void verify()
        {
            int partitionKey = key();
            ReadCommand command = command(partitionKey, metadataWithClustering);
            StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);
            PartitionUpdate update = buildMultiRowUpdate(metadataWithClustering, partitionKey, rows);

            CloseCountingRowIterator rowIter = new CloseCountingRowIterator(update.unfilteredIterator());
            ReadResponse response = ReadResponse.createInMemoryDataResponse(singlePartitionIterator(rowIter), command, rdi, maxRows, maxSize);

            // Guards against the assertion below passing on a path it was not meant to cover: a serialized response
            // keeps nothing in memory, one that stayed in memory holds every row.
            assertEquals("response took the wrong path", expectInMemory ? rows : 0, response.inMemoryUnfilteredCount());
            assertEquals("row iterator must be closed exactly once", 1, rowIter.closeCount);
        }
    }

    /**
     * Passes everything through to the wrapped iterator, counting the calls to close().
     */
    private static class CloseCountingRowIterator implements WrappingUnfilteredRowIterator
    {
        private final UnfilteredRowIterator wrapped;
        private int closeCount = 0;

        CloseCountingRowIterator(UnfilteredRowIterator wrapped)
        {
            this.wrapped = wrapped;
        }

        public UnfilteredRowIterator wrapped()
        {
            return wrapped;
        }

        @Override
        public void close()
        {
            closeCount++;
            wrapped.close();
        }
    }

    @Test
    public void inMemoryResponseWithOverflowMatchesLocalDataResponse()
    {
        // Row limit crossed: the whole response is serialized.
        inMemoryAssertion()
            .maxRows(2).rows(5)
            .expectSerialized()
            .verify();
    }

    @Test
    public void inMemoryResponseAllRowsInMemoryWhenUnderLimit()
    {
        inMemoryAssertion()
            .maxRows(10).rows(3)
            .expectInMemory()
            .verify();
    }

    @Test
    public void inMemoryResponseByteLimitWithOverflow()
    {
        // Row limit disabled; the byte limit is crossed, so the whole response is serialized.
        inMemoryAssertion()
            .maxBytesAsSizeOfFirstNRows(2).rows(5)
            .expectSerialized()
            .verify();
    }

    @Test
    public void inMemoryResponseByteLimitAllRowsInMemoryWhenUnderLimit()
    {
        // Byte limit sized above the whole response: it stays in memory.
        inMemoryAssertion()
            .maxBytesAsSizeOfFirstNRows(10).rows(3)
            .expectInMemory()
            .verify();
    }

    @Test
    public void inMemoryResponseByteLimitReachedBeforeRowLimit()
    {
        // Row limit (10) is not reached; the byte limit (sized for 2 rows) is crossed first, so the response is serialized.
        inMemoryAssertion()
            .maxRows(10).maxBytesAsSizeOfFirstNRows(2).rows(5)
            .expectSerialized()
            .verify();
    }

    @Test
    public void inMemoryResponseRowLimitReachedBeforeByteLimit()
    {
        // Byte limit (sized for all 5 rows) is not reached; the row limit (2) is crossed first, so the response is serialized.
        inMemoryAssertion()
            .maxRows(2).maxBytesAsSizeOfFirstNRows(5).rows(5)
            .expectSerialized()
            .verify();
    }

    @Test
    public void inMemoryResponseKeepsEverythingWhenBothLimitsDisabled()
    {
        // Both limits disabled: the whole partition is kept in memory.
        inMemoryAssertion()
            .maxRows(0).rows(4)
            .expectInMemory()
            .verify();
    }

    @Test
    public void inMemoryResponseCapturesRepairedDigestAfterIteratorIsConsumed()
    {
        // RepairedDataInfo updates its digest lazily as rows are consumed via withRepairedDataInfo
        // transformations; this test uses a stub that simulates the same timing.
        int key = key();
        ReadCommand command = command(key, metadataWithClustering);
        PartitionUpdate update = buildMultiRowUpdate(metadataWithClustering, key, 3);

        ByteBuffer expectedDigest = digest();
        // Returns EMPTY before any iteration; returns expectedDigest once the iterator has been consumed.
        LazyRepairedDataInfo rdi = new LazyRepairedDataInfo(expectedDigest);

        // large heap budget (row limit disabled) so all rows are kept in memory; this test only cares about digest capture timing
        ReadResponse response = ReadResponse.createInMemoryDataResponse(lazyRdiWrappedIterator(update, rdi), command, rdi, 0, Long.MAX_VALUE);

        assertTrue("digest should be non-empty after iterator was consumed", rdi.wasConsumedBeforeDigestCaptured());
        assertEquals(expectedDigest, response.repairedDataDigest());
        assertTrue(response.isRepairedDigestConclusive());
    }

    @Test
    public void inMemoryResponseWithRangeTombstoneAllInMemory()
    {
        // A single range tombstone (open+close markers) well under the limit: kept in memory.
        inMemoryAssertion()
            .maxRows(10).rows(0)
            .withTombstone(rangeTombstone(0, 5))
            .expectInMemory()
            .verify();
    }

    @Test
    public void inMemoryResponseWithRangeTombstoneOverflow()
    {
        // Rows plus a trailing range tombstone cross the row limit: the whole response is serialized.
        inMemoryAssertion()
            .maxRows(2).rows(5)
            .withTombstone(rangeTombstone(6, 9))
            .expectSerialized()
            .verify();
    }

    @Test
    public void inMemoryResponseWithTombstonesOnlyOverflow()
    {
        // Two range tombstones (open+close markers each) cross the row limit: the whole response is serialized.
        inMemoryAssertion()
            .maxRows(2).rows(0)
            .withTombstone(rangeTombstone(0, 3))
            .withTombstone(rangeTombstone(5, 8))
            .expectSerialized()
            .verify();
    }

    @Test
    public void inMemoryResponseWithRangeTombstoneAmongRowsOverflow()
    {
        // A range tombstone at [1, 2) interleaved with rows 0-4, crossing the row limit: serialized in full.
        inMemoryAssertion()
            .maxRows(2).rows(5)
            .withTombstone(rangeTombstone(1, 2))
            .expectSerialized()
            .verify();
    }

    @Test
    public void inMemoryResponseWithRangeTombstoneAmongRowsOverflowReversed()
    {
        // Same as above with reversed read order; the range tombstone is at [2, 3).
        inMemoryAssertion()
            .maxRows(2).rows(5).reversed()
            .withTombstone(rangeTombstone(2, 3))
            .expectSerialized()
            .verify();
    }

    @Test
    public void inMemoryResponseWithOverlappingRangeTombstonesAtDifferentTimestamps()
    {
        // Three overlapping RTs: [0,3) ts=100, [1,4) ts=200, [2,5) ts=50 form a single continuous open range
        // (bound, boundary, boundary, bound markers). Emission cannot stop while inside the open marker, so even
        // with a row limit of 2 the whole response is kept in memory.
        inMemoryAssertion()
            .maxRows(2).rows(5)
            .withTombstone(rangeTombstone(0, 3).timestamp(100))
            .withTombstone(rangeTombstone(1, 4).timestamp(200))
            .withTombstone(rangeTombstone(2, 5).timestamp(50))
            .expectInMemory()
            .verify();
    }

    @Test
    public void inMemoryResponseWithOverlappingRangeTombstonesAtDifferentTimestampsReversed()
    {
        // Same tombstones as above with reversed read order; still kept in memory (open marker cannot be split).
        inMemoryAssertion()
            .maxRows(2).rows(5).reversed()
            .withTombstone(rangeTombstone(0, 3).timestamp(100))
            .withTombstone(rangeTombstone(1, 4).timestamp(200))
            .withTombstone(rangeTombstone(2, 5).timestamp(50))
            .expectInMemory()
            .verify();
    }

    private InMemoryAssertionBuilder inMemoryAssertion()
    {
        return new InMemoryAssertionBuilder();
    }

    private class InMemoryAssertionBuilder
    {
        private int maxRows = 0;                 // per-request row limit; 0 = disabled
        private Integer maxBytes = null; // if set, the byte limit is sized to hold this many leading Unfiltered objects; null = disabled
        private int rows = 0;
        private boolean reversed = false;
        private final List<RangeTombstoneSpec> tombstones = new ArrayList<>();
        // true = the whole response is expected to be kept as an object graph, false = serialized in full
        private Boolean expectInMemory = null;

        InMemoryAssertionBuilder maxRows(int maxRows) { this.maxRows = maxRows; return this; }
        // Enables the byte limit, sizing it to exactly hold the first n Unfiltered objects the read produces
        InMemoryAssertionBuilder maxBytesAsSizeOfFirstNRows(int n) {this.maxBytes = n; return this; }
        InMemoryAssertionBuilder rows(int rows) { this.rows = rows; return this; }
        InMemoryAssertionBuilder reversed() { this.reversed = true; return this; }
        InMemoryAssertionBuilder withTombstone(RangeTombstoneSpec rt) { tombstones.add(rt); return this; }
        // The response stays within the limits and is kept in memory as an object graph.
        InMemoryAssertionBuilder expectInMemory() { this.expectInMemory = true; return this; }
        // A limit is crossed, so the whole response is serialized into a buffer.
        InMemoryAssertionBuilder expectSerialized() { this.expectInMemory = false; return this; }

        void verify()
        {
            int partitionKey = key();
            ReadCommand command = command(partitionKey, metadataWithClustering, reversed);
            StubRepairedDataInfo rdi = new StubRepairedDataInfo(ByteBufferUtil.EMPTY_BYTE_BUFFER, true);

            PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadataWithClustering, ByteBufferUtil.bytes(partitionKey)).timestamp(0);
            for (int i = 0; i < rows; i++)
                builder.row(i).add("v", i);
            for (RangeTombstoneSpec rt : tombstones)
            {
                Slice slice = Slice.make(Clustering.make(ByteBufferUtil.bytes(rt.start)),
                                         Clustering.make(ByteBufferUtil.bytes(rt.end)));
                builder.addRangeTombstone(new RangeTombstone(slice,
                                                             DeletionTime.build(rt.markedForDeleteAt, FBUtilities.nowInSeconds())
                                          )
                );
            }
            PartitionUpdate update = builder.build();

            long maxHeapSize = maxBytes == null ? 0 : heapSizeOfFirstNRows(update, reversed, maxBytes);

            ReadResponse localResponse = command.createResponse(singlePartitionIterator(update, reversed), rdi);
            ReadResponse inMemoryResponse = ReadResponse.createInMemoryDataResponse(singlePartitionIterator(update, reversed), command, rdi, maxRows, maxHeapSize);

            // The reconstructed response must match the plain serialized response regardless of representation.
            List<String> expected = collectUnfiltered(command, localResponse);
            assertEquals(expected, collectUnfiltered(command, inMemoryResponse));

            // When kept in memory the whole response is an object graph (inMemoryUnfilteredCount == all unfiltereds);
            // when serialized nothing is kept in memory (inMemoryUnfilteredCount == 0).
            if (expectInMemory != null)
                assertEquals("unexpected inMemoryUnfilteredCount",
                             expectInMemory ? expected.size() : 0,
                             inMemoryResponse.inMemoryUnfilteredCount());
        }
    }

    /**
     * Returns a heap size of the first {@code n} Unfiltered objects the read of
     * {@code update} produces. Feeding this to the in-memory response keeps those {@code n} objects in memory (the
     * (n+1)th crosses the >= threshold and overflows), which lets the assertions above express the byte-based split
     * as a count of leading Unfiltered objects even though the limit itself is a heap size.
     */
    private long heapSizeOfFirstNRows(PartitionUpdate update, boolean reversed, int n)
    {
        if (n <= 0)
            return 0;

        long size = 0;
        int count = 0;
        try (UnfilteredRowIterator rowIter = update.unfilteredIterator(ColumnFilter.SelectionColumnFilter.all(update.columns()), Slices.ALL, reversed))
        {
            while (rowIter.hasNext() && count < n)
            {
                size += unsharedHeapSize(rowIter.next());
                count++;
            }
        }
        return size;
    }

    private static long unsharedHeapSize(Unfiltered unfiltered)
    {
        return unfiltered.isRow()
               ? ((Row) unfiltered).unsharedHeapSize()
               : ((RangeTombstoneMarker) unfiltered).unsharedHeapSize();
    }

    private static RangeTombstoneSpec rangeTombstone(int start, int end)
    {
        return new RangeTombstoneSpec(start, end);
    }

    private static class RangeTombstoneSpec
    {
        final int start;
        final int end;
        long markedForDeleteAt;

        RangeTombstoneSpec(int start, int end)
        {
            this.start = start;
            this.end = end;
        }

        RangeTombstoneSpec timestamp(long ts)
        {
            this.markedForDeleteAt = ts;
            return this;
        }
    }

    private void assertIteratorsEqual(ReadCommand command, ReadResponse expected, ReadResponse actual)
    {
        List<String> expectedUnfiltered = collectUnfiltered(command, expected);
        List<String> actualUnfiltered = collectUnfiltered(command, actual);
        assertEquals(expectedUnfiltered, actualUnfiltered);
    }

    private List<String> collectUnfiltered(ReadCommand command, ReadResponse response)
    {
        List<String> result = new ArrayList<>();
        try (UnfilteredPartitionIterator iter = response.makeIterator(command))
        {
            while (iter.hasNext())
            {
                try (UnfilteredRowIterator partition = iter.next())
                {
                    // an empty static row is skipped, its presence is a matter of the digest, not of the content
                    if (!partition.staticRow().isEmpty())
                        result.add(partition.staticRow().toString(partition.metadata(), true));
                    while (partition.hasNext())
                        result.add(partition.next().toString(partition.metadata(), true));
                }
            }
        }
        return result;
    }

    private Row buildRow(TableMetadata metadata, DecoratedKey key)
    {
        ColumnMetadata col = metadata.getColumn(ByteBufferUtil.bytes("v"));
        Clustering<?> clustering = Clustering.EMPTY;
        return BTreeRow.singleCellRow(clustering, BufferCell.live(col, FBUtilities.timestampMicros(), ByteBufferUtil.bytes(42)));
    }

    private PartitionUpdate buildMultiRowUpdate(TableMetadata metadata, int partitionKey, int rowCount)
    {
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, ByteBufferUtil.bytes(partitionKey)).timestamp(0);
        for (int i = 0; i < rowCount; i++)
            builder.row(i).add("v", i);
        return builder.build();
    }

    /**
     * Iterates the update the way a read of a table with static columns does: the iterator reports the columns
     * fetched by the command's filter (see {@code AbstractSSTableIterator.columns()}) rather than only the ones the
     * update happens to contain, and a partition without static data still carries an empty static row that is not
     * the Rows.EMPTY_STATIC_ROW singleton (that is what the sstable read path produces).
     */
    private UnfilteredPartitionIterator readIterator(PartitionUpdate update, ReadCommand command)
    {
        return readIterator(update, command, BTreeRow.emptyRow(Clustering.STATIC_CLUSTERING));
    }

    private UnfilteredPartitionIterator readIterator(PartitionUpdate update, ReadCommand command, Row staticRow)
    {
        UnfilteredRowIterator rowIter = update.unfilteredIterator(command.columnFilter(), Slices.ALL, command.isReversed());
        return singlePartitionIterator(new WrappingUnfilteredRowIterator()
        {
            public UnfilteredRowIterator wrapped() { return rowIter; }

            @Override
            public Row staticRow() { return staticRow; }
        });
    }

    /**
     * The iterator a read returns when its clustering filter selects no clustering at all, as produced by
     * {@code AbstractBTreePartition} for {@link Slices#NONE}. Its columns are those of the static row it carries.
     */
    private UnfilteredPartitionIterator noRowsIterator(ReadCommand command, int key, Row staticRow)
    {
        DecoratedKey dk = command.metadata().partitioner.decorateKey(ByteBufferUtil.bytes(key));
        return singlePartitionIterator(UnfilteredRowIterators.noRowsIterator(command.metadata(), dk, staticRow,
                                                                             DeletionTime.LIVE, command.isReversed()));
    }

    /**
     * A partition that is returned by the read but holds nothing at all, still reporting the columns the filter
     * fetches, as an {@code AbstractBTreePartition} slice iterator does for a slice that matches no row.
     */
    private UnfilteredPartitionIterator emptyPartitionIterator(ReadCommand command, int key)
    {
        DecoratedKey dk = command.metadata().partitioner.decorateKey(ByteBufferUtil.bytes(key));
        return singlePartitionIterator(new AbstractUnfilteredRowIterator(command.metadata(), dk, DeletionTime.LIVE,
                                                                         command.columnFilter().fetchedColumns(),
                                                                         Rows.EMPTY_STATIC_ROW, command.isReversed(),
                                                                         EncodingStats.NO_STATS)
        {
            protected Unfiltered computeNext()
            {
                return endOfData();
            }
        });
    }

    private String columnsOf(ReadCommand command, ReadResponse response)
    {
        try (UnfilteredPartitionIterator iter = response.makeIterator(command))
        {
            StringBuilder columns = new StringBuilder();
            while (iter.hasNext())
            {
                try (UnfilteredRowIterator partition = iter.next())
                {
                    columns.append(partition.columns());
                }
            }
            return columns.toString();
        }
    }

    private Row deletedRowWithShadowedCell(TableMetadata metadata, int clusteringValue)
    {
        long timestamp = 25;
        long nowInSec = FBUtilities.nowInSeconds();
        ColumnMetadata col = metadata.getColumn(ByteBufferUtil.bytes("v"));

        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(Clustering.make(ByteBufferUtil.bytes(clusteringValue)));
        builder.addCell(BufferCell.tombstone(col, timestamp, nowInSec));
        builder.addRowDeletion(Row.Deletion.regular(DeletionTime.build(timestamp, nowInSec)));
        return builder.build();
    }

    private UnfilteredRowIterator rowIterator(ReadCommand command, int key, Row row)
    {
        DecoratedKey dk = command.metadata().partitioner.decorateKey(ByteBufferUtil.bytes(key));
        return UnfilteredRowIterators.singleton(row, command.metadata(), dk, DeletionTime.LIVE,
                                                command.columnFilter().fetchedColumns(), Rows.EMPTY_STATIC_ROW,
                                                command.isReversed(), EncodingStats.NO_STATS);
    }

    private Row staticRow(TableMetadata metadata, int value)
    {
        ColumnMetadata col = metadata.getColumn(ByteBufferUtil.bytes("s"));
        return BTreeRow.singleCellRow(Clustering.STATIC_CLUSTERING,
                                      BufferCell.live(col, FBUtilities.timestampMicros(), ByteBufferUtil.bytes(value)));
    }

    private UnfilteredPartitionIterator singlePartitionIterator(PartitionUpdate update)
    {
        return singlePartitionIterator(update, false);
    }
    private UnfilteredPartitionIterator singlePartitionIterator(PartitionUpdate update, boolean reversed)
    {
        return singlePartitionIterator(update.unfilteredIterator(ColumnFilter.SelectionColumnFilter.all(update.columns()), Slices.ALL, reversed));
    }

    private UnfilteredPartitionIterator singlePartitionIterator(UnfilteredRowIterator rowIter)
    {
        return new AbstractUnfilteredPartitionIterator()
        {
            private boolean returned = false;

            public TableMetadata metadata() { return rowIter.metadata(); }

            public boolean hasNext() { return !returned; }

            public UnfilteredRowIterator next()
            {
                returned = true;
                return rowIter;
            }
        };
    }

    private void verifySerDe(ReadResponse response) {
        // check that roundtripping through ReadResponse.serializer behaves as expected
        for (MessagingService.Version version : MessagingService.Version.supportedVersions())
            roundTripSerialization(response, version.value);

    }

    private void roundTripSerialization(ReadResponse response, int version)
    {
        try
        {
            DataOutputBuffer out = new DataOutputBuffer();
            ReadResponse.serializer.serialize(response, out, version);

            DataInputBuffer in = new DataInputBuffer(out.buffer(), false);
            ReadResponse deser = ReadResponse.serializer.deserialize(in, version);
            assertTrue(version >= MessagingService.VERSION_40);
            assertTrue(deser.mayIncludeRepairedDigest());
            assertEquals(response.repairedDataDigest(), deser.repairedDataDigest());
            assertEquals(response.isRepairedDigestConclusive(), deser.isRepairedDigestConclusive());
        }
        catch (IOException e)
        {
            fail("Caught unexpected IOException during SerDe: " + e.getMessage());
        }
    }


    private int key()
    {
        return random.nextInt();
    }

    private ByteBuffer digest()
    {
        byte[] bytes = new byte[4];
        random.nextBytes(bytes);
        return ByteBuffer.wrap(bytes);
    }

    private ReadCommand digestCommand(int key, TableMetadata metadata)
    {
        return new StubReadCommand(key, metadata, true, false);
    }

    private ReadCommand command(int key, TableMetadata metadata)
    {
        return command(key, metadata, false);
    }

    private ReadCommand command(int key, TableMetadata metadata, boolean reversed)
    {
        return new StubReadCommand(key, metadata, false, reversed);

    }

    private static class StubRepairedDataInfo extends RepairedDataInfo
    {
        private final ByteBuffer repairedDigest;
        private final boolean conclusive;

        public StubRepairedDataInfo(ByteBuffer repairedDigest, boolean conclusive)
        {
            super(null);
            this.repairedDigest = repairedDigest;
            this.conclusive = conclusive;
        }
        
        @Override
        public ByteBuffer getDigest()
        {
            return repairedDigest;
        }
        
        @Override
        public boolean isConclusive()
        {
            return conclusive;
        }
    }

    /**
     * Simulates a RepairedDataInfo that updates its digest lazily as rows are consumed.
     * Returns EMPTY_BYTE_BUFFER until the partition iterator wrapping it is fully consumed,
     * at which point getDigest() returns the provided expected digest.
     * This lets us verify that InMemoryDataResponse captures the digest *after* consumption.
     */
    private static class LazyRepairedDataInfo extends RepairedDataInfo
    {
        private final ByteBuffer finalDigest;
        private boolean iteratorConsumed = false;
        private boolean digestCapturedAfterConsumption = false;

        LazyRepairedDataInfo(ByteBuffer finalDigest)
        {
            super(null);
            this.finalDigest = finalDigest;
        }

        void markConsumed()
        {
            iteratorConsumed = true;
        }

        boolean wasConsumedBeforeDigestCaptured()
        {
            return digestCapturedAfterConsumption;
        }

        @Override
        public ByteBuffer getDigest()
        {
            if (iteratorConsumed)
                digestCapturedAfterConsumption = true;
            return iteratorConsumed ? finalDigest : ByteBufferUtil.EMPTY_BYTE_BUFFER;
        }

        @Override
        public boolean isConclusive()
        {
            return true;
        }
    }

    private UnfilteredPartitionIterator lazyRdiWrappedIterator(PartitionUpdate update, LazyRepairedDataInfo rdi)
    {
        // Wraps the row iterator so that close() marks rdi as consumed, simulating RepairedDataInfo
        // updating its digest lazily after the partition has been fully read.
        UnfilteredRowIterator baseRowIter = update.unfilteredIterator();
        return singlePartitionIterator(new WrappingUnfilteredRowIterator()
        {
            public UnfilteredRowIterator wrapped() { return baseRowIter; }

            @Override
            public void close()
            {
                rdi.markConsumed();
                baseRowIter.close();
            }
        });
    }

    private static class StubReadCommand extends SinglePartitionReadCommand
    {
        StubReadCommand(int key, TableMetadata metadata, boolean isDigest, boolean reversed)
        {
            super(metadata.epoch,
                  isDigest,
                  0,
                  false,
                  PotentialTxnConflicts.DISALLOW,
                  metadata,
                  FBUtilities.nowInSeconds(),
                  ColumnFilter.all(metadata),
                  RowFilter.none(),
                  DataLimits.NONE,
                  metadata.partitioner.decorateKey(ByteBufferUtil.bytes(key)),
                  new ClusteringIndexSliceFilter(Slices.ALL, reversed),
                  null,
                  false,
                  null);

        }

        @Override
        public boolean selectsFullPartition()
        {
            return true;
        }

        public UnfilteredPartitionIterator executeLocally(ReadExecutionController controller)
        {
            return EmptyIterators.unfilteredPartition(this.metadata());
        }
    }
}
