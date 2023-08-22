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
package org.apache.cassandra.io.sstable.format.big;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.google.common.primitives.Ints;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.partitions.ImmutableBTreePartition;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.IndexInfo;
import org.apache.cassandra.io.sstable.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.LongSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.btree.BTree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RowIndexEntryTest extends CQLTester
{
    private static final List<AbstractType<?>> clusterTypes = Collections.singletonList(LongType.instance);
    private static final ClusteringComparator comp = new ClusteringComparator(clusterTypes);

    private static final byte[] dummy_100k = new byte[100000];

    private static Clustering<?> cn(long l)
    {
        return Util.clustering(comp, l);
    }

    @BeforeClass
    public static void beforeClass()
    {
        Assume.assumeTrue(BigFormat.isSelected());
    }

    @Test
    public void testC11206AgainstPreviousArray() throws Exception
    {
        DatabaseDescriptor.setColumnIndexCacheSize(99999);
        testC11206AgainstPrevious();
    }

    @Test
    public void testC11206AgainstPreviousShallow() throws Exception
    {
        DatabaseDescriptor.setColumnIndexCacheSize(0);
        testC11206AgainstPrevious();
    }

    private static void testC11206AgainstPrevious() throws Exception
    {
        // partition without IndexInfo
        try (DoubleSerializer doubleSerializer = new DoubleSerializer())
        {
            doubleSerializer.build(null, partitionKey(42L),
                                   Collections.singletonList(cn(42)),
                                   0L);
            assertEquals(doubleSerializer.rieOldSerialized, doubleSerializer.rieNewSerialized);
        }

        // partition with multiple IndexInfo
        try (DoubleSerializer doubleSerializer = new DoubleSerializer())
        {
            doubleSerializer.build(null, partitionKey(42L),
                                   Arrays.asList(cn(42), cn(43), cn(44)),
                                   0L);

            RowIndexEntry newRie = doubleSerializer.rieSerializer.deserialize(new DataInputBuffer(doubleSerializer.rieNewSerialized, false), 0);
            Pre_C_11206_RowIndexEntry oldRie = doubleSerializer.oldSerializer.deserialize(new DataInputBuffer(doubleSerializer.rieOldSerialized, false));

            assertEquals(oldRie.position, newRie.getPosition());
            assertEquals(oldRie.deletionTime(), newRie.deletionTime());
            assertEquals(oldRie.columnsIndex().size(), newRie.blockCount());
        }

        // partition with multiple IndexInfo
        try (DoubleSerializer doubleSerializer = new DoubleSerializer())
        {
            doubleSerializer.build(null, partitionKey(42L),
                                   Arrays.asList(cn(42), cn(43), cn(44), cn(45), cn(46), cn(47), cn(48), cn(49), cn(50), cn(51)),
                                   0L);

            RowIndexEntry newRie = doubleSerializer.rieSerializer.deserialize(new DataInputBuffer(doubleSerializer.rieNewSerialized, false), 0);
            Pre_C_11206_RowIndexEntry oldRie = doubleSerializer.oldSerializer.deserialize(new DataInputBuffer(doubleSerializer.rieOldSerialized, false));

            assertEquals(oldRie.position, newRie.getPosition());
            assertEquals(oldRie.deletionTime(), newRie.deletionTime());
            assertEquals(oldRie.columnsIndex().size(), newRie.blockCount());
        }
    }

    private static DecoratedKey partitionKey(long l)
    {
        ByteBuffer key = LongSerializer.instance.serialize(l);
        Token token = Murmur3Partitioner.instance.getToken(key);
        return new BufferDecoratedKey(token, key);
    }

    private static class DoubleSerializer implements AutoCloseable
    {
        TableMetadata metadata =
            CreateTableStatement.parse("CREATE TABLE pipe.dev_null (pk bigint, ck bigint, val text, PRIMARY KEY(pk, ck))", "foo")
                                .build();

        Version version = BigFormat.getInstance().getLatestVersion();

        DeletionTime deletionInfo = DeletionTime.build(FBUtilities.timestampMicros(), FBUtilities.nowInSeconds());
        LivenessInfo primaryKeyLivenessInfo = LivenessInfo.EMPTY;
        Row.Deletion deletion = Row.Deletion.LIVE;

        SerializationHeader header = new SerializationHeader(true, metadata, metadata.regularAndStaticColumns(), EncodingStats.NO_STATS);

        // create C-11206 + old serializer instances
        RowIndexEntry.IndexSerializer rieSerializer = new RowIndexEntry.Serializer(version, header, null);
        Pre_C_11206_RowIndexEntry.Serializer oldSerializer = new Pre_C_11206_RowIndexEntry.Serializer(metadata, version, header);

        final DataOutputBuffer rieOutput = new DataOutputBuffer(1024);
        final DataOutputBuffer oldOutput = new DataOutputBuffer(1024);

        final SequentialWriter dataWriterNew;
        final SequentialWriter dataWriterOld;
        final BigFormatPartitionWriter partitionWriter;

        RowIndexEntry rieNew;
        ByteBuffer rieNewSerialized;
        Pre_C_11206_RowIndexEntry rieOld;
        ByteBuffer rieOldSerialized;

        DoubleSerializer() throws IOException
        {
            SequentialWriterOption option = SequentialWriterOption.newBuilder().bufferSize(1024).build();
            File f = FileUtils.createTempFile("RowIndexEntryTest-", "db");
            dataWriterNew = new SequentialWriter(f, option);
            partitionWriter = new BigFormatPartitionWriter(header, dataWriterNew, version, rieSerializer.indexInfoSerializer());

            f = FileUtils.createTempFile("RowIndexEntryTest-", "db");
            dataWriterOld = new SequentialWriter(f, option);
        }

        public void close() throws Exception
        {
            dataWriterNew.close();
            dataWriterOld.close();
        }

        void build(Row staticRow, DecoratedKey partitionKey,
                   Collection<Clustering<?>> clusterings, long startPosition) throws IOException
        {

            Iterator<Clustering<?>> clusteringIter = clusterings.iterator();
            partitionWriter.start(partitionKey, DeletionTime.LIVE);
            if (staticRow != null)
                partitionWriter.addStaticRow(staticRow);
            AbstractUnfilteredRowIterator rowIter = makeRowIter(staticRow, partitionKey, clusteringIter, dataWriterNew);
            while (rowIter.hasNext())
                partitionWriter.addUnfiltered(rowIter.next());
            partitionWriter.finish();

            rieNew = RowIndexEntry.create(startPosition, 0L,
                                          deletionInfo, partitionWriter.getHeaderLength(), partitionWriter.getColumnIndexCount(),
                                          partitionWriter.indexInfoSerializedSize(),
                                          partitionWriter.indexSamples(), partitionWriter.offsets(),
                                          rieSerializer.indexInfoSerializer(),
                                          BigFormat.getInstance().getLatestVersion());
            rieSerializer.serialize(rieNew, rieOutput, partitionWriter.buffer());
            rieNewSerialized = rieOutput.buffer().duplicate();

            Iterator<Clustering<?>> clusteringIter2 = clusterings.iterator();
            ColumnIndex columnIndex = RowIndexEntryTest.ColumnIndex.writeAndBuildIndex(makeRowIter(staticRow, partitionKey, clusteringIter2, dataWriterOld),
                                                                                       dataWriterOld, header, Collections.emptySet(), BigFormat.getInstance().getLatestVersion());
            rieOld = Pre_C_11206_RowIndexEntry.create(startPosition, deletionInfo, columnIndex, version);
            oldSerializer.serialize(rieOld, oldOutput);
            rieOldSerialized = oldOutput.buffer().duplicate();
        }

        private AbstractUnfilteredRowIterator makeRowIter(Row staticRow, DecoratedKey partitionKey,
                                                          Iterator<Clustering<?>> clusteringIter, SequentialWriter dataWriter)
        {
            return new AbstractUnfilteredRowIterator(metadata, partitionKey, deletionInfo, metadata.regularAndStaticColumns(),
                                                     staticRow, false, new EncodingStats(0, 0, 0))
            {
                protected Unfiltered computeNext()
                {
                    if (!clusteringIter.hasNext())
                        return endOfData();
                    try
                    {
                        // write some fake bytes to the data file to force writing the IndexInfo object
                        dataWriter.write(dummy_100k);
                    }
                    catch (IOException e)
                    {
                        throw new RuntimeException(e);
                    }
                    return buildRow(clusteringIter.next());
                }
            };
        }

        private Unfiltered buildRow(Clustering<?> clustering)
        {
            BTree.Builder<ColumnData> builder = BTree.builder(ColumnData.comparator);
            builder.add(BufferCell.live(metadata.regularAndStaticColumns().iterator().next(),
                                        1L,
                                        ByteBuffer.allocate(0)));
            return BTreeRow.create(clustering, primaryKeyLivenessInfo, deletion, builder.build());
        }
    }

    /**
     * Pre C-11206 code.
     */
    static final class ColumnIndex
    {
        final long partitionHeaderLength;
        final List<IndexInfo> columnsIndex;

        private static final ColumnIndex EMPTY = new ColumnIndex(-1, Collections.emptyList());

        private ColumnIndex(long partitionHeaderLength, List<IndexInfo> columnsIndex)
        {
            assert columnsIndex != null;

            this.partitionHeaderLength = partitionHeaderLength;
            this.columnsIndex = columnsIndex;
        }

        static ColumnIndex writeAndBuildIndex(UnfilteredRowIterator iterator,
                                              SequentialWriter output,
                                              SerializationHeader header,
                                              Collection<SSTableFlushObserver> observers,
                                              Version version) throws IOException
        {
            assert !iterator.isEmpty();

            Builder builder = new Builder(iterator, output, header, observers, version);
            return builder.build();
        }

        public static ColumnIndex nothing()
        {
            return EMPTY;
        }

        /**
         * Help to create an index for a column family based on size of columns,
         * and write said columns to disk.
         */
        private static class Builder
        {
            private final UnfilteredRowIterator iterator;
            private final SequentialWriter writer;
            private final SerializationHelper helper;
            private final SerializationHeader header;
            private final Version version;

            private final List<IndexInfo> columnsIndex = new ArrayList<>();
            private final long initialPosition;
            private long headerLength = -1;

            private long startPosition = -1;

            private int written;
            private long previousRowStart;

            private ClusteringPrefix<?> firstClustering;
            private ClusteringPrefix<?> lastClustering;

            private DeletionTime openMarker;

            private final Collection<SSTableFlushObserver> observers;

            Builder(UnfilteredRowIterator iterator,
                           SequentialWriter writer,
                           SerializationHeader header,
                           Collection<SSTableFlushObserver> observers,
                           Version version)
            {
                this.iterator = iterator;
                this.writer = writer;
                this.helper = new SerializationHelper(header);
                this.header = header;
                this.version = version;
                this.observers = observers == null ? Collections.emptyList() : observers;
                this.initialPosition = writer.position();
            }

            private void writePartitionHeader(UnfilteredRowIterator iterator) throws IOException
            {
                ByteBufferUtil.writeWithShortLength(iterator.partitionKey().getKey(), writer);
                DeletionTime.getSerializer(version).serialize(iterator.partitionLevelDeletion(), writer);
                if (header.hasStatic())
                    UnfilteredSerializer.serializer.serializeStaticRow(iterator.staticRow(), helper, writer, version.correspondingMessagingVersion());
            }

            public ColumnIndex build() throws IOException
            {
                writePartitionHeader(iterator);
                this.headerLength = writer.position() - initialPosition;

                while (iterator.hasNext())
                    add(iterator.next());

                return close();
            }

            private long currentPosition()
            {
                return writer.position() - initialPosition;
            }

            private void addIndexBlock()
            {
                IndexInfo cIndexInfo = new IndexInfo(firstClustering,
                                                     lastClustering,
                                                     startPosition,
                                                     currentPosition() - startPosition,
                                                     openMarker);
                columnsIndex.add(cIndexInfo);
                firstClustering = null;
            }

            private void add(Unfiltered unfiltered) throws IOException
            {
                long pos = currentPosition();

                if (firstClustering == null)
                {
                    // Beginning of an index block. Remember the start and position
                    firstClustering = unfiltered.clustering();
                    startPosition = pos;
                }

                UnfilteredSerializer.serializer.serialize(unfiltered, helper, writer, pos - previousRowStart, version.correspondingMessagingVersion());

                // notify observers about each new row
                if (!observers.isEmpty())
                    observers.forEach((o) -> o.nextUnfilteredCluster(unfiltered));

                lastClustering = unfiltered.clustering();
                previousRowStart = pos;
                ++written;

                if (unfiltered.kind() == Unfiltered.Kind.RANGE_TOMBSTONE_MARKER)
                {
                    RangeTombstoneMarker marker = (RangeTombstoneMarker)unfiltered;
                    openMarker = marker.isOpen(false) ? marker.openDeletionTime(false) : null;
                }

                // if we hit the column index size that we have to index after, go ahead and index it.
                if (currentPosition() - startPosition >= DatabaseDescriptor.getColumnIndexSize(BigFormatPartitionWriter.DEFAULT_GRANULARITY))
                    addIndexBlock();

            }

            private ColumnIndex close() throws IOException
            {
                UnfilteredSerializer.serializer.writeEndOfPartition(writer);

                // It's possible we add no rows, just a top level deletion
                if (written == 0)
                    return RowIndexEntryTest.ColumnIndex.EMPTY;

                // the last column may have fallen on an index boundary already.  if not, index it explicitly.
                if (firstClustering != null)
                    addIndexBlock();

                // we should always have at least one computed index block, but we only write it out if there is more than that.
                assert !columnsIndex.isEmpty() && headerLength >= 0;
                return new ColumnIndex(headerLength, columnsIndex);
            }
        }
    }

    @Test
    public void testSerializedSize() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a int, b text, c int, PRIMARY KEY(a, b))");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);
        Version version = BigFormat.getInstance().getLatestVersion();

        Pre_C_11206_RowIndexEntry simple = new Pre_C_11206_RowIndexEntry(123);

        DataOutputBuffer buffer = new DataOutputBuffer();
        SerializationHeader header = new SerializationHeader(true, cfs.metadata(), cfs.metadata().regularAndStaticColumns(), EncodingStats.NO_STATS);
        Pre_C_11206_RowIndexEntry.Serializer serializer = new Pre_C_11206_RowIndexEntry.Serializer(cfs.metadata(), version, header);

        serializer.serialize(simple, buffer);

        assertEquals(buffer.getLength(), serializer.serializedSize(simple));

        // write enough rows to ensure we get a few column index entries
        for (int i = 0; i <= DatabaseDescriptor.getColumnIndexSize(BigFormatPartitionWriter.DEFAULT_GRANULARITY) / 4; i++)
            execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?)", 0, String.valueOf(i), i);

        ImmutableBTreePartition partition = Util.getOnlyPartitionUnfiltered(Util.cmd(cfs).build());

        File tempFile = FileUtils.createTempFile("row_index_entry_test", null);
        tempFile.deleteOnExit();
        SequentialWriter writer = new SequentialWriter(tempFile);
        ColumnIndex columnIndex = RowIndexEntryTest.ColumnIndex.writeAndBuildIndex(partition.unfilteredIterator(), writer, header, Collections.emptySet(), version);
        Pre_C_11206_RowIndexEntry withIndex = Pre_C_11206_RowIndexEntry.create(0xdeadbeef, DeletionTime.LIVE, columnIndex, version);
        IndexInfo.Serializer indexSerializer = IndexInfo.serializer(version, header);

        // sanity check
        assertTrue(columnIndex.columnsIndex.size() >= 3);

        buffer = new DataOutputBuffer();
        serializer.serialize(withIndex, buffer);
        assertEquals(buffer.getLength(), serializer.serializedSize(withIndex));

        // serialization check

        ByteBuffer bb = buffer.buffer();
        DataInputBuffer input = new DataInputBuffer(bb, false);
        serializationCheck(withIndex, indexSerializer, bb, input, version);

        // test with an output stream that doesn't support a file-pointer
        buffer = new DataOutputBuffer()
        {
            public boolean hasPosition()
            {
                return false;
            }

            public long position()
            {
                throw new UnsupportedOperationException();
            }
        };
        serializer.serialize(withIndex, buffer);
        bb = buffer.buffer();
        input = new DataInputBuffer(bb, false);
        serializationCheck(withIndex, indexSerializer, bb, input, version);

        //

        bb = buffer.buffer();
        input = new DataInputBuffer(bb, false);
        Pre_C_11206_RowIndexEntry.Serializer.skip(input, BigFormat.getInstance().getLatestVersion());
        Assert.assertEquals(0, bb.remaining());
    }

    private static void serializationCheck(Pre_C_11206_RowIndexEntry withIndex, IndexInfo.Serializer indexSerializer, ByteBuffer bb, DataInputBuffer input, Version version) throws IOException
    {
        Assert.assertEquals(0xdeadbeef, input.readUnsignedVInt());
        Assert.assertEquals(withIndex.promotedSize(indexSerializer), input.readUnsignedVInt());

        Assert.assertEquals(withIndex.headerLength(), input.readUnsignedVInt());
        Assert.assertEquals(withIndex.deletionTime(), DeletionTime.getSerializer(version).deserialize(input));
        Assert.assertEquals(withIndex.columnsIndex().size(), input.readUnsignedVInt());

        int offset = bb.position();
        int[] offsets = new int[withIndex.columnsIndex().size()];
        for (int i = 0; i < withIndex.columnsIndex().size(); i++)
        {
            int pos = bb.position();
            offsets[i] = pos - offset;
            IndexInfo info = indexSerializer.deserialize(input);
            int end = bb.position();

            Assert.assertEquals(indexSerializer.serializedSize(info), end - pos);

            Assert.assertEquals(withIndex.columnsIndex().get(i).offset, info.offset);
            Assert.assertEquals(withIndex.columnsIndex().get(i).width, info.width);
            Assert.assertEquals(withIndex.columnsIndex().get(i).endOpenMarker, info.endOpenMarker);
            Assert.assertEquals(withIndex.columnsIndex().get(i).firstName, info.firstName);
            Assert.assertEquals(withIndex.columnsIndex().get(i).lastName, info.lastName);
        }

        for (int i = 0; i < withIndex.columnsIndex().size(); i++)
            Assert.assertEquals(offsets[i], input.readInt());

        Assert.assertEquals(0, bb.remaining());
    }

    static class Pre_C_11206_RowIndexEntry implements IMeasurableMemory
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new Pre_C_11206_RowIndexEntry(0));

        public final long position;

        Pre_C_11206_RowIndexEntry(long position)
        {
            this.position = position;
        }

        protected int promotedSize(IndexInfo.Serializer idxSerializer)
        {
            return 0;
        }

        public static Pre_C_11206_RowIndexEntry create(long position, DeletionTime deletionTime, ColumnIndex index, Version version)
        {
            assert index != null;
            assert deletionTime != null;

            // we only consider the columns summary when determining whether to create an IndexedEntry,
            // since if there are insufficient columns to be worth indexing we're going to seek to
            // the beginning of the row anyway, so we might as well read the tombstone there as well.
            if (index.columnsIndex.size() > 1)
                return new Pre_C_11206_RowIndexEntry.IndexedEntry(position, deletionTime, index.partitionHeaderLength, index.columnsIndex, version);
            else
                return new Pre_C_11206_RowIndexEntry(position);
        }

        /**
         * @return true if this index entry contains the row-level tombstone and column summary.  Otherwise,
         * caller should fetch these from the row header.
         */
        public boolean isIndexed()
        {
            return !columnsIndex().isEmpty();
        }

        public DeletionTime deletionTime()
        {
            throw new UnsupportedOperationException();
        }

        /**
         * The length of the row header (partition key, partition deletion and static row).
         * This value is only provided for indexed entries and this method will throw
         * {@code UnsupportedOperationException} if {@code !isIndexed()}.
         */
        public long headerLength()
        {
            throw new UnsupportedOperationException();
        }

        public List<IndexInfo> columnsIndex()
        {
            return Collections.emptyList();
        }

        public long unsharedHeapSize()
        {
            return EMPTY_SIZE;
        }

        public static class Serializer
        {
            private final IndexInfo.Serializer idxSerializer;
            private final Version version;

            Serializer(TableMetadata metadata, Version version, SerializationHeader header)
            {
                this.idxSerializer = IndexInfo.serializer(version, header);
                this.version = version;
            }

            public void serialize(Pre_C_11206_RowIndexEntry rie, DataOutputPlus out) throws IOException
            {
                out.writeUnsignedVInt(rie.position);
                out.writeUnsignedVInt32(rie.promotedSize(idxSerializer));

                if (rie.isIndexed())
                {
                    out.writeUnsignedVInt(rie.headerLength());
                    DeletionTime.getSerializer(version).serialize(rie.deletionTime(), out);
                    out.writeUnsignedVInt32(rie.columnsIndex().size());

                    // Calculate and write the offsets to the IndexInfo objects.

                    int[] offsets = new int[rie.columnsIndex().size()];

                    if (out.hasPosition())
                    {
                        // Out is usually a SequentialWriter, so using the file-pointer is fine to generate the offsets.
                        // A DataOutputBuffer also works.
                        long start = out.position();
                        int i = 0;
                        for (IndexInfo info : rie.columnsIndex())
                        {
                            offsets[i] = i == 0 ? 0 : (int)(out.position() - start);
                            i++;
                            idxSerializer.serialize(info, out);
                        }
                    }
                    else
                    {
                        // Not sure this branch will ever be needed, but if it is called, it has to calculate the
                        // serialized sizes instead of simply using the file-pointer.
                        int i = 0;
                        int offset = 0;
                        for (IndexInfo info : rie.columnsIndex())
                        {
                            offsets[i++] = offset;
                            idxSerializer.serialize(info, out);
                            offset += idxSerializer.serializedSize(info);
                        }
                    }

                    for (int off : offsets)
                        out.writeInt(off);
                }
            }

            public Pre_C_11206_RowIndexEntry deserialize(DataInputPlus in) throws IOException
            {
                long position = in.readUnsignedVInt();

                int size = in.readUnsignedVInt32();
                if (size > 0)
                {
                    long headerLength = in.readUnsignedVInt();
                    DeletionTime deletionTime = DeletionTime.getSerializer(version).deserialize(in);
                    int entries = in.readUnsignedVInt32();
                    List<IndexInfo> columnsIndex = new ArrayList<>(entries);
                    for (int i = 0; i < entries; i++)
                        columnsIndex.add(idxSerializer.deserialize(in));

                    in.skipBytesFully(entries * TypeSizes.sizeof(0));

                    return new Pre_C_11206_RowIndexEntry.IndexedEntry(position, deletionTime, headerLength, columnsIndex, version);
                }
                else
                {
                    return new Pre_C_11206_RowIndexEntry(position);
                }
            }

            // Reads only the data 'position' of the index entry and returns it. Note that this left 'in' in the middle
            // of reading an entry, so this is only useful if you know what you are doing and in most case 'deserialize'
            // should be used instead.
            static long readPosition(DataInputPlus in, Version version) throws IOException
            {
                return in.readUnsignedVInt();
            }

            public static void skip(DataInputPlus in, Version version) throws IOException
            {
                readPosition(in, version);
                skipPromotedIndex(in, version);
            }

            private static void skipPromotedIndex(DataInputPlus in, Version version) throws IOException
            {
                int size = in.readUnsignedVInt32();
                if (size <= 0)
                    return;

                in.skipBytesFully(size);
            }

            public int serializedSize(Pre_C_11206_RowIndexEntry rie)
            {
                int indexedSize = 0;
                if (rie.isIndexed())
                {
                    List<IndexInfo> index = rie.columnsIndex();

                    indexedSize += TypeSizes.sizeofUnsignedVInt(rie.headerLength());
                    indexedSize += DeletionTime.getSerializer(version).serializedSize(rie.deletionTime());
                    indexedSize += TypeSizes.sizeofUnsignedVInt(index.size());

                    for (IndexInfo info : index)
                        indexedSize += idxSerializer.serializedSize(info);

                    indexedSize += index.size() * TypeSizes.sizeof(0);
                }

                return TypeSizes.sizeofUnsignedVInt(rie.position) + TypeSizes.sizeofUnsignedVInt(indexedSize) + indexedSize;
            }
        }

        /**
         * An entry in the row index for a row whose columns are indexed.
         */
        private static final class IndexedEntry extends Pre_C_11206_RowIndexEntry
        {
            private final DeletionTime deletionTime;

            // The offset in the file when the index entry end
            private final long headerLength;
            private final List<IndexInfo> columnsIndex;
            private final Version version;
            private static final long BASE_SIZE =
            ObjectSizes.measure(new IndexedEntry(0, DeletionTime.LIVE, 0, Arrays.asList(null, null), null))
            + ObjectSizes.measure(new ArrayList<>(1)) + ObjectSizes.measure(BigFormat.getInstance().getLatestVersion());

            private IndexedEntry(long position, DeletionTime deletionTime, long headerLength, List<IndexInfo> columnsIndex, Version version)
            {
                super(position);
                assert deletionTime != null;
                assert columnsIndex != null && columnsIndex.size() > 1;
                this.deletionTime = deletionTime;
                this.headerLength = headerLength;
                this.columnsIndex = columnsIndex;
                this.version = version;
            }

            @Override
            public DeletionTime deletionTime()
            {
                return deletionTime;
            }

            @Override
            public long headerLength()
            {
                return headerLength;
            }

            @Override
            public List<IndexInfo> columnsIndex()
            {
                return columnsIndex;
            }

            @Override
            protected int promotedSize(IndexInfo.Serializer idxSerializer)
            {
                long size = TypeSizes.sizeofUnsignedVInt(headerLength)
                            + DeletionTime.getSerializer(version).serializedSize(deletionTime)
                            + TypeSizes.sizeofUnsignedVInt(columnsIndex.size()); // number of entries
                for (IndexInfo info : columnsIndex)
                    size += idxSerializer.serializedSize(info);

                size += columnsIndex.size() * TypeSizes.sizeof(0);

                return Ints.checkedCast(size);
            }

            @Override
            public long unsharedHeapSize()
            {
                long entrySize = 0;
                for (IndexInfo idx : columnsIndex)
                    entrySize += idx.unsharedHeapSize();

                return BASE_SIZE
                       + entrySize
                       + deletionTime.unsharedHeapSize()
                       + ObjectSizes.sizeOfReferenceArray(columnsIndex.size());
            }
        }
    }

    @Test
    public void testIndexFor() throws IOException
    {
        DeletionTime deletionInfo = DeletionTime.build(FBUtilities.timestampMicros(), FBUtilities.nowInSeconds());

        List<IndexInfo> indexes = new ArrayList<>();
        indexes.add(new IndexInfo(cn(0L), cn(5L), 0, 0, deletionInfo));
        indexes.add(new IndexInfo(cn(10L), cn(15L), 0, 0, deletionInfo));
        indexes.add(new IndexInfo(cn(20L), cn(25L), 0, 0, deletionInfo));

        RowIndexEntry rie = new RowIndexEntry(0L)
        {
            public IndexInfoRetriever openWithIndex(FileHandle indexFile)
            {
                return new IndexInfoRetriever()
                {
                    public IndexInfo columnsIndex(int index)
                    {
                        return indexes.get(index);
                    }

                    public void close()
                    {
                    }
                };
            }

            public int blockCount()
            {
                return indexes.size();
            }
        };
        
        IndexState indexState = new IndexState(
            null, comp, rie, false, null                                                                                              
        );
        
        assertEquals(0, indexState.indexFor(cn(-1L), -1));
        assertEquals(0, indexState.indexFor(cn(5L), -1));
        assertEquals(1, indexState.indexFor(cn(12L), -1));
        assertEquals(2, indexState.indexFor(cn(17L), -1));
        assertEquals(3, indexState.indexFor(cn(100L), -1));
        assertEquals(3, indexState.indexFor(cn(100L), 0));
        assertEquals(3, indexState.indexFor(cn(100L), 1));
        assertEquals(3, indexState.indexFor(cn(100L), 2));
        assertEquals(3, indexState.indexFor(cn(100L), 3));

        indexState = new IndexState(
            null, comp, rie, true, null
        );

        assertEquals(-1, indexState.indexFor(cn(-1L), -1));
        assertEquals(0, indexState.indexFor(cn(5L), 3));
        assertEquals(0, indexState.indexFor(cn(5L), 2));
        assertEquals(1, indexState.indexFor(cn(17L), 3));
        assertEquals(2, indexState.indexFor(cn(100L), 3));
        assertEquals(2, indexState.indexFor(cn(100L), 4));
        assertEquals(1, indexState.indexFor(cn(12L), 3));
        assertEquals(1, indexState.indexFor(cn(12L), 2));
        assertEquals(1, indexState.indexFor(cn(100L), 1));
        assertEquals(2, indexState.indexFor(cn(100L), 2));
    }
}
