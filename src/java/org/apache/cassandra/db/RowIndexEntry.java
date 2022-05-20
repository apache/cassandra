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
import java.util.List;

import com.codahale.metrics.Histogram;
import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.filter.RowIndexEntryReadSizeTooLargeException;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.sstable.IndexInfo;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.TrackedDataInputPlus;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.MetricNameFactory;
import org.apache.cassandra.net.ParamType;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.vint.VIntCoding;
import org.github.jamm.Unmetered;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

/**
 * Binary format of {@code RowIndexEntry} is defined as follows:
 * {@code
 * (long) position (64 bit long, vint encoded)
 *  (int) serialized size of data that follows (32 bit int, vint encoded)
 * -- following for indexed entries only (so serialized size > 0)
 *  (int) DeletionTime.localDeletionTime
 * (long) DeletionTime.markedForDeletionAt
 *  (int) number of IndexInfo objects (32 bit int, vint encoded)
 *    (*) serialized IndexInfo objects, see below
 *    (*) offsets of serialized IndexInfo objects, since version "ma" (3.0)
 *        Each IndexInfo object's offset is relative to the first IndexInfo object.
 * }
 * <p>
 * See {@link IndexInfo} for a description of the serialized format.
 * </p>
 *
 * <p>
 * For each partition, the layout of the index file looks like this:
 * </p>
 * <ol>
 *     <li>partition key - prefixed with {@code short} length</li>
 *     <li>serialized {@code RowIndexEntry} objects</li>
 * </ol>
 *
 * <p>
 *     Generally, we distinguish between index entries that have <i>index
 *     samples</i> (list of {@link IndexInfo} objects) and those who don't.
 *     For each <i>portion</i> of data for a single partition in the data file,
 *     an index sample is created. The size of that <i>portion</i> is defined
 *     by {@link org.apache.cassandra.config.Config#column_index_size}.
 * </p>
 * <p>
 *     Index entries with less than 2 index samples, will just store the
 *     position in the data file.
 * </p>
 * <p>
 *     Note: legacy sstables for index entries are those sstable formats that
 *     do <i>not</i> have an offsets table to index samples ({@link IndexInfo}
 *     objects). These are those sstables created on Cassandra versions
 *     earlier than 3.0.
 * </p>
 * <p>
 *     For index entries with index samples we store the index samples
 *     ({@link IndexInfo} objects). The bigger the partition, the more
 *     index samples are created. Since a huge amount of index samples
 *     will "pollute" the heap and cause huge GC pressure, Cassandra 3.6
 *     (CASSANDRA-11206) distinguishes between index entries with an
 *     "acceptable" amount of index samples per partition and those
 *     with an "enormous" amount of index samples. The barrier
 *     is controlled by the configuration parameter
 *     {@link org.apache.cassandra.config.Config#column_index_cache_size}.
 *     Index entries with a total serialized size of index samples up to
 *     {@code column_index_cache_size} will be held in an array.
 *     Index entries exceeding that value will always be accessed from
 *     disk.
 * </p>
 * <p>
 *     This results in these classes:
 * </p>
 * <ul>
 *     <li>{@link RowIndexEntry} just stores the offset in the data file.</li>
 *     <li>{@link IndexedEntry} is for index entries with index samples
 *     and used for both current and legacy sstables, which do not exceed
 *     {@link org.apache.cassandra.config.Config#column_index_cache_size}.</li>
 *     <li>{@link ShallowIndexedEntry} is for index entries with index samples
 *     that exceed {@link org.apache.cassandra.config.Config#column_index_cache_size}
 *     for sstables with an offset table to the index samples.</li>
 * </ul>
 * <p>
 *     Since access to index samples on disk (obviously) requires some file
 *     reader, that functionality is encapsulated in implementations of
 *     {@link IndexInfoRetriever}. There is an implementation to access
 *     index samples of legacy sstables (without the offsets table),
 *     an implementation of access sstables with an offsets table.
 * </p>
 * <p>
 *     Until now (Cassandra 3.x), we still support reading from <i>legacy</i> sstables -
 *     i.e. sstables created by Cassandra &lt; 3.0 (see {@link org.apache.cassandra.io.sstable.format.big.BigFormat}.
 * </p>
 *
 */
public class RowIndexEntry<T> implements IMeasurableMemory
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new RowIndexEntry(0));

    // constants for type of row-index-entry as serialized for saved-cache
    static final int CACHE_NOT_INDEXED = 0;
    static final int CACHE_INDEXED = 1;
    static final int CACHE_INDEXED_SHALLOW = 2;

    static final Histogram indexEntrySizeHistogram;
    static final Histogram indexInfoCountHistogram;
    static final Histogram indexInfoGetsHistogram;
    static final Histogram indexInfoReadsHistogram;
    static
    {
        MetricNameFactory factory = new DefaultNameFactory("Index", "RowIndexEntry");
        indexEntrySizeHistogram = Metrics.histogram(factory.createMetricName("IndexedEntrySize"), false);
        indexInfoCountHistogram = Metrics.histogram(factory.createMetricName("IndexInfoCount"), false);
        indexInfoGetsHistogram = Metrics.histogram(factory.createMetricName("IndexInfoGets"), false);
        indexInfoReadsHistogram = Metrics.histogram(factory.createMetricName("IndexInfoReads"), false);
    }

    public final long position;

    public RowIndexEntry(long position)
    {
        this.position = position;
    }

    /**
     * @return true if this index entry contains the row-level tombstone and column summary.  Otherwise,
     * caller should fetch these from the row header.
     */
    public boolean isIndexed()
    {
        return columnsIndexCount() > 1;
    }

    public boolean indexOnHeap()
    {
        return false;
    }

    public DeletionTime deletionTime()
    {
        throw new UnsupportedOperationException();
    }

    public int columnsIndexCount()
    {
        return 0;
    }

    public long unsharedHeapSize()
    {
        return EMPTY_SIZE;
    }

    /**
     * @param dataFilePosition  position of the partition in the {@link org.apache.cassandra.io.sstable.Component.Type#DATA} file
     * @param indexFilePosition position in the {@link org.apache.cassandra.io.sstable.Component.Type#PRIMARY_INDEX} of the {@link RowIndexEntry}
     * @param deletionTime      deletion time of {@link RowIndexEntry}
     * @param headerLength      deletion time of {@link RowIndexEntry}
     * @param columnIndexCount  number of {@link IndexInfo} entries in the {@link RowIndexEntry}
     * @param indexedPartSize   serialized size of all serialized {@link IndexInfo} objects and their offsets
     * @param indexSamples      list with IndexInfo offsets (if total serialized size is less than {@link org.apache.cassandra.config.Config#column_index_cache_size}
     * @param offsets           offsets of IndexInfo offsets
     * @param idxInfoSerializer the {@link IndexInfo} serializer
     */
    public static RowIndexEntry<IndexInfo> create(long dataFilePosition, long indexFilePosition,
                                                  DeletionTime deletionTime, long headerLength, int columnIndexCount,
                                                  int indexedPartSize,
                                                  List<IndexInfo> indexSamples, int[] offsets,
                                                  ISerializer<IndexInfo> idxInfoSerializer)
    {
        // If the "partition building code" in BigTableWriter.append() via ColumnIndex returns a list
        // of IndexInfo objects, which is the case if the serialized size is less than
        // Config.column_index_cache_size, AND we have more than one IndexInfo object, we
        // construct an IndexedEntry object. (note: indexSamples.size() and columnIndexCount have the same meaning)
        if (indexSamples != null && indexSamples.size() > 1)
            return new IndexedEntry(dataFilePosition, deletionTime, headerLength,
                                    indexSamples.toArray(new IndexInfo[indexSamples.size()]), offsets,
                                    indexedPartSize, idxInfoSerializer);
        // Here we have to decide whether we have serialized IndexInfo objects that exceeds
        // Config.column_index_cache_size (not exceeding case covered above).
        // Such a "big" indexed-entry is represented as a shallow one.
        if (columnIndexCount > 1)
            return new ShallowIndexedEntry(dataFilePosition, indexFilePosition,
                                           deletionTime, headerLength, columnIndexCount,
                                           indexedPartSize, idxInfoSerializer);
        // Last case is that there are no index samples.
        return new RowIndexEntry<>(dataFilePosition);
    }

    public IndexInfoRetriever openWithIndex(FileHandle indexFile)
    {
        return null;
    }

    public interface IndexSerializer<T>
    {
        void serialize(RowIndexEntry<T> rie, DataOutputPlus out, ByteBuffer indexInfo) throws IOException;

        RowIndexEntry<T> deserialize(DataInputPlus in, long indexFilePosition) throws IOException;
        default RowIndexEntry<T> deserialize(RandomAccessReader reader) throws IOException
        {
            return deserialize(reader, reader.getFilePointer());

        }

        default RowIndexEntry<T> deserialize(FileDataInput input) throws IOException
        {
            return deserialize(input, input.getFilePointer());

        }

        void serializeForCache(RowIndexEntry<T> rie, DataOutputPlus out) throws IOException;
        RowIndexEntry<T> deserializeForCache(DataInputPlus in) throws IOException;

        long deserializePositionAndSkip(DataInputPlus in) throws IOException;

        ISerializer<T> indexInfoSerializer();
    }

    public static final class Serializer implements IndexSerializer<IndexInfo>
    {
        private final IndexInfo.Serializer idxInfoSerializer;
        private final Version version;

        public Serializer(Version version, SerializationHeader header)
        {
            this.idxInfoSerializer = IndexInfo.serializer(version, header);
            this.version = version;
        }

        public IndexInfo.Serializer indexInfoSerializer()
        {
            return idxInfoSerializer;
        }

        public void serialize(RowIndexEntry<IndexInfo> rie, DataOutputPlus out, ByteBuffer indexInfo) throws IOException
        {
            rie.serialize(out, indexInfo);
        }

        public void serializeForCache(RowIndexEntry<IndexInfo> rie, DataOutputPlus out) throws IOException
        {
            rie.serializeForCache(out);
        }

        public RowIndexEntry<IndexInfo> deserializeForCache(DataInputPlus in) throws IOException
        {
            long position = in.readUnsignedVInt();

            switch (in.readByte())
            {
                case CACHE_NOT_INDEXED:
                    return new RowIndexEntry<>(position);
                case CACHE_INDEXED:
                    return new IndexedEntry(position, in, idxInfoSerializer);
                case CACHE_INDEXED_SHALLOW:
                    return new ShallowIndexedEntry(position, in, idxInfoSerializer);
                default:
                    throw new AssertionError();
            }
        }

        public static void skipForCache(DataInputPlus in) throws IOException
        {
            in.readUnsignedVInt();
            switch (in.readByte())
            {
                case CACHE_NOT_INDEXED:
                    break;
                case CACHE_INDEXED:
                    IndexedEntry.skipForCache(in);
                    break;
                case CACHE_INDEXED_SHALLOW:
                    ShallowIndexedEntry.skipForCache(in);
                    break;
                default:
                    assert false;
            }
        }

        public RowIndexEntry<IndexInfo> deserialize(DataInputPlus in, long indexFilePosition) throws IOException
        {
            long position = in.readUnsignedVInt();

            int size = (int)in.readUnsignedVInt();
            if (size == 0)
            {
                return new RowIndexEntry<>(position);
            }
            else
            {
                long headerLength = in.readUnsignedVInt();
                DeletionTime deletionTime = DeletionTime.serializer.deserialize(in);
                int columnsIndexCount = (int) in.readUnsignedVInt();

                checkSize(columnsIndexCount, size);

                int indexedPartSize = size - serializedSize(deletionTime, headerLength, columnsIndexCount);

                if (size <= DatabaseDescriptor.getColumnIndexCacheSize())
                {
                    return new IndexedEntry(position, in, deletionTime, headerLength, columnsIndexCount,
                                            idxInfoSerializer, indexedPartSize);
                }
                else
                {
                    in.skipBytes(indexedPartSize);

                    return new ShallowIndexedEntry(position,
                                                   indexFilePosition,
                                                   deletionTime, headerLength, columnsIndexCount,
                                                   indexedPartSize, idxInfoSerializer);
                }
            }
        }

        private void checkSize(int entries, int bytes)
        {
            ReadCommand command = ReadCommand.getCommand();
            if (command == null || SchemaConstants.isSystemKeyspace(command.metadata().keyspace) || !DatabaseDescriptor.getReadThresholdsEnabled())
                return;

            DataStorageSpec.LongBytesBound warnThreshold = DatabaseDescriptor.getRowIndexReadSizeWarnThreshold();
            DataStorageSpec.LongBytesBound failThreshold = DatabaseDescriptor.getRowIndexReadSizeFailThreshold();
            if (warnThreshold == null && failThreshold == null)
                return;

            long estimatedMemory = estimateMaterializedIndexSize(entries, bytes);
            ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(command.metadata().id);
            if (cfs != null)
                cfs.metric.rowIndexSize.update(estimatedMemory);

            if (failThreshold != null && estimatedMemory > failThreshold.toBytes())
            {
                String msg = String.format("Query %s attempted to access a large RowIndexEntry estimated to be %d bytes " +
                                           "in-memory (total entries: %d, total bytes: %d) but the max allowed is %s;" +
                                           " query aborted  (see row_index_read_size_fail_threshold)",
                                           command.toCQLString(), estimatedMemory, entries, bytes, failThreshold);
                MessageParams.remove(ParamType.ROW_INDEX_READ_SIZE_WARN);
                MessageParams.add(ParamType.ROW_INDEX_READ_SIZE_FAIL, estimatedMemory);

                throw new RowIndexEntryReadSizeTooLargeException(msg);
            }
            else if (warnThreshold != null && estimatedMemory > warnThreshold.toBytes())
            {
                // use addIfLarger rather than add as a previous partition may be larger than this one
                Long current = MessageParams.get(ParamType.ROW_INDEX_READ_SIZE_WARN);
                if (current == null || current.compareTo(estimatedMemory) < 0)
                    MessageParams.add(ParamType.ROW_INDEX_READ_SIZE_WARN, estimatedMemory);
            }
        }

        private static long estimateMaterializedIndexSize(int entries, int bytes)
        {
            long overhead = IndexInfo.EMPTY_SIZE
                            + ArrayClustering.EMPTY_SIZE
                            + DeletionTime.EMPTY_SIZE;

            return (overhead * entries) + bytes;
        }

        public long deserializePositionAndSkip(DataInputPlus in) throws IOException
        {
            long position = in.readUnsignedVInt();

            int size = (int) in.readUnsignedVInt();
            if (size > 0)
                in.skipBytesFully(size);

            return position;
        }

        /**
         * Reads only the data 'position' of the index entry and returns it. Note that this left 'in' in the middle
         * of reading an entry, so this is only useful if you know what you are doing and in most case 'deserialize'
         * should be used instead.
         */
        public static long readPosition(DataInputPlus in) throws IOException
        {
            return in.readUnsignedVInt();
        }

        public static void skip(DataInputPlus in, Version version) throws IOException
        {
            readPosition(in);
            skipPromotedIndex(in);
        }

        private static void skipPromotedIndex(DataInputPlus in) throws IOException
        {
            int size = (int)in.readUnsignedVInt();
            if (size <= 0)
                return;

            in.skipBytesFully(size);
        }

        public static void serializeOffsets(DataOutputBuffer out, int[] indexOffsets, int columnIndexCount) throws IOException
        {
            for (int i = 0; i < columnIndexCount; i++)
                out.writeInt(indexOffsets[i]);
        }
    }

    private static int serializedSize(DeletionTime deletionTime, long headerLength, int columnIndexCount)
    {
        return TypeSizes.sizeofUnsignedVInt(headerLength)
               + (int) DeletionTime.serializer.serializedSize(deletionTime)
               + TypeSizes.sizeofUnsignedVInt(columnIndexCount);
    }

    public void serialize(DataOutputPlus out, ByteBuffer indexInfo) throws IOException
    {
        out.writeUnsignedVInt(position);

        out.writeUnsignedVInt(0);
    }

    public void serializeForCache(DataOutputPlus out) throws IOException
    {
        out.writeUnsignedVInt(position);

        out.writeByte(CACHE_NOT_INDEXED);
    }

    /**
     * An entry in the row index for a row whose columns are indexed - used for both legacy and current formats.
     */
    private static final class IndexedEntry extends RowIndexEntry<IndexInfo>
    {
        private static final long BASE_SIZE;

        static
        {
            BASE_SIZE = ObjectSizes.measure(new IndexedEntry(0, DeletionTime.LIVE, 0, null, null, 0, null));
        }

        private final DeletionTime deletionTime;
        private final long headerLength;

        private final IndexInfo[] columnsIndex;
        private final int[] offsets;
        private final int indexedPartSize;
        @Unmetered
        private final ISerializer<IndexInfo> idxInfoSerializer;

        private IndexedEntry(long dataFilePosition, DeletionTime deletionTime, long headerLength,
                             IndexInfo[] columnsIndex, int[] offsets,
                             int indexedPartSize, ISerializer<IndexInfo> idxInfoSerializer)
        {
            super(dataFilePosition);

            this.headerLength = headerLength;
            this.deletionTime = deletionTime;

            this.columnsIndex = columnsIndex;
            this.offsets = offsets;
            this.indexedPartSize = indexedPartSize;
            this.idxInfoSerializer = idxInfoSerializer;
        }

        private IndexedEntry(long dataFilePosition, DataInputPlus in,
                             DeletionTime deletionTime, long headerLength, int columnIndexCount,
                             IndexInfo.Serializer idxInfoSerializer, int indexedPartSize) throws IOException
        {
            super(dataFilePosition);

            this.headerLength = headerLength;
            this.deletionTime = deletionTime;
            int columnsIndexCount = columnIndexCount;

            this.columnsIndex = new IndexInfo[columnsIndexCount];
            for (int i = 0; i < columnsIndexCount; i++)
                this.columnsIndex[i] = idxInfoSerializer.deserialize(in);

            this.offsets = new int[this.columnsIndex.length];
            for (int i = 0; i < offsets.length; i++)
                offsets[i] = in.readInt();

            this.indexedPartSize = indexedPartSize;

            this.idxInfoSerializer = idxInfoSerializer;
        }

        /**
         * Constructor called from {@link Serializer#deserializeForCache(org.apache.cassandra.io.util.DataInputPlus)}.
         */
        private IndexedEntry(long dataFilePosition, DataInputPlus in, ISerializer<IndexInfo> idxInfoSerializer) throws IOException
        {
            super(dataFilePosition);

            this.headerLength = in.readUnsignedVInt();
            this.deletionTime = DeletionTime.serializer.deserialize(in);
            int columnsIndexCount = (int) in.readUnsignedVInt();

            TrackedDataInputPlus trackedIn = new TrackedDataInputPlus(in);

            this.columnsIndex = new IndexInfo[columnsIndexCount];
            for (int i = 0; i < columnsIndexCount; i++)
                this.columnsIndex[i] = idxInfoSerializer.deserialize(trackedIn);

            this.offsets = null;

            this.indexedPartSize = (int) trackedIn.getBytesRead();

            this.idxInfoSerializer = idxInfoSerializer;
        }

        @Override
        public boolean indexOnHeap()
        {
            return true;
        }

        @Override
        public int columnsIndexCount()
        {
            return columnsIndex.length;
        }

        @Override
        public DeletionTime deletionTime()
        {
            return deletionTime;
        }

        @Override
        public IndexInfoRetriever openWithIndex(FileHandle indexFile)
        {
            indexEntrySizeHistogram.update(serializedSize(deletionTime, headerLength, columnsIndex.length) + indexedPartSize);
            indexInfoCountHistogram.update(columnsIndex.length);
            return new IndexInfoRetriever()
            {
                private int retrievals;

                @Override
                public IndexInfo columnsIndex(int index)
                {
                    retrievals++;
                    return columnsIndex[index];
                }

                public void close()
                {
                    indexInfoGetsHistogram.update(retrievals);
                }
            };
        }

        @Override
        public long unsharedHeapSize()
        {
            long entrySize = 0;
            for (IndexInfo idx : columnsIndex)
                entrySize += idx.unsharedHeapSize();
            return BASE_SIZE
                + entrySize
                + ObjectSizes.sizeOfReferenceArray(columnsIndex.length);
        }

        public void serialize(DataOutputPlus out, ByteBuffer indexInfo) throws IOException
        {
            assert indexedPartSize != Integer.MIN_VALUE;

            out.writeUnsignedVInt(position);

            out.writeUnsignedVInt(serializedSize(deletionTime, headerLength, columnsIndex.length) + indexedPartSize);

            out.writeUnsignedVInt(headerLength);
            DeletionTime.serializer.serialize(deletionTime, out);
            out.writeUnsignedVInt(columnsIndex.length);
            for (IndexInfo info : columnsIndex)
                idxInfoSerializer.serialize(info, out);
            for (int offset : offsets)
                out.writeInt(offset);
        }

        @Override
        public void serializeForCache(DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt(position);
            out.writeByte(CACHE_INDEXED);

            out.writeUnsignedVInt(headerLength);
            DeletionTime.serializer.serialize(deletionTime, out);
            out.writeUnsignedVInt(columnsIndexCount());

            for (IndexInfo indexInfo : columnsIndex)
                idxInfoSerializer.serialize(indexInfo, out);
        }

        static void skipForCache(DataInputPlus in) throws IOException
        {
            in.readUnsignedVInt();
            DeletionTime.serializer.skip(in);
            in.readUnsignedVInt();

            in.readUnsignedVInt();
        }
    }

    /**
     * An entry in the row index for a row whose columns are indexed and the {@link IndexInfo} objects
     * are not read into the key cache.
     */
    private static final class ShallowIndexedEntry extends RowIndexEntry<IndexInfo>
    {
        private static final long BASE_SIZE;

        static
        {
            BASE_SIZE = ObjectSizes.measure(new ShallowIndexedEntry(0, 0, DeletionTime.LIVE, 0, 10, 0, null));
        }

        private final long indexFilePosition;

        private final DeletionTime deletionTime;
        private final long headerLength;
        private final int columnsIndexCount;

        private final int indexedPartSize;
        private final int offsetsOffset;
        @Unmetered
        private final ISerializer<IndexInfo> idxInfoSerializer;
        private final int fieldsSerializedSize;

        /**
         * See {@link #create(long, long, DeletionTime, long, int, int, List, int[], ISerializer)} for a description
         * of the parameters.
         */
        private ShallowIndexedEntry(long dataFilePosition, long indexFilePosition,
                                    DeletionTime deletionTime, long headerLength, int columnIndexCount,
                                    int indexedPartSize, ISerializer<IndexInfo> idxInfoSerializer)
        {
            super(dataFilePosition);

            assert columnIndexCount > 1;

            this.indexFilePosition = indexFilePosition;
            this.headerLength = headerLength;
            this.deletionTime = deletionTime;
            this.columnsIndexCount = columnIndexCount;

            this.indexedPartSize = indexedPartSize;
            this.idxInfoSerializer = idxInfoSerializer;

            this.fieldsSerializedSize = serializedSize(deletionTime, headerLength, columnIndexCount);
            this.offsetsOffset = indexedPartSize + fieldsSerializedSize - columnsIndexCount * TypeSizes.INT_SIZE;
        }

        /**
         * Constructor for key-cache deserialization
         */
        private ShallowIndexedEntry(long dataFilePosition, DataInputPlus in, IndexInfo.Serializer idxInfoSerializer) throws IOException
        {
            super(dataFilePosition);

            this.indexFilePosition = in.readUnsignedVInt();

            this.headerLength = in.readUnsignedVInt();
            this.deletionTime = DeletionTime.serializer.deserialize(in);
            this.columnsIndexCount = (int) in.readUnsignedVInt();

            this.indexedPartSize = (int) in.readUnsignedVInt();

            this.idxInfoSerializer = idxInfoSerializer;

            this.fieldsSerializedSize = serializedSize(deletionTime, headerLength, columnsIndexCount);
            this.offsetsOffset = indexedPartSize + fieldsSerializedSize - columnsIndexCount * TypeSizes.INT_SIZE;
        }

        @Override
        public int columnsIndexCount()
        {
            return columnsIndexCount;
        }

        @Override
        public DeletionTime deletionTime()
        {
            return deletionTime;
        }

        @Override
        public IndexInfoRetriever openWithIndex(FileHandle indexFile)
        {
            indexEntrySizeHistogram.update(indexedPartSize + fieldsSerializedSize);
            indexInfoCountHistogram.update(columnsIndexCount);
            return new ShallowInfoRetriever(indexFilePosition +
                                            VIntCoding.computeUnsignedVIntSize(position) +
                                            VIntCoding.computeUnsignedVIntSize(indexedPartSize + fieldsSerializedSize) +
                                            fieldsSerializedSize,
                                            offsetsOffset - fieldsSerializedSize,
                                            indexFile.createReader(), idxInfoSerializer);
        }

        @Override
        public long unsharedHeapSize()
        {
            return BASE_SIZE;
        }

        @Override
        public void serialize(DataOutputPlus out, ByteBuffer indexInfo) throws IOException
        {
            out.writeUnsignedVInt(position);

            out.writeUnsignedVInt(fieldsSerializedSize + indexInfo.limit());

            out.writeUnsignedVInt(headerLength);
            DeletionTime.serializer.serialize(deletionTime, out);
            out.writeUnsignedVInt(columnsIndexCount);

            out.write(indexInfo);
        }

        @Override
        public void serializeForCache(DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt(position);
            out.writeByte(CACHE_INDEXED_SHALLOW);

            out.writeUnsignedVInt(indexFilePosition);

            out.writeUnsignedVInt(headerLength);
            DeletionTime.serializer.serialize(deletionTime, out);
            out.writeUnsignedVInt(columnsIndexCount);

            out.writeUnsignedVInt(indexedPartSize);
        }

        static void skipForCache(DataInputPlus in) throws IOException
        {
            in.readUnsignedVInt();

            in.readUnsignedVInt();
            DeletionTime.serializer.skip(in);
            in.readUnsignedVInt();

            in.readUnsignedVInt();
        }
    }

    private static final class ShallowInfoRetriever extends FileIndexInfoRetriever
    {
        private final int offsetsOffset;

        private ShallowInfoRetriever(long indexInfoFilePosition, int offsetsOffset,
                                     FileDataInput indexReader, ISerializer<IndexInfo> idxInfoSerializer)
        {
            super(indexInfoFilePosition, indexReader, idxInfoSerializer);
            this.offsetsOffset = offsetsOffset;
        }

        IndexInfo fetchIndex(int index) throws IOException
        {
            // seek to position in "offsets to IndexInfo" table
            indexReader.seek(indexInfoFilePosition + offsetsOffset + index * TypeSizes.INT_SIZE);

            // read offset of IndexInfo
            int indexInfoPos = indexReader.readInt();

            // seek to posision of IndexInfo
            indexReader.seek(indexInfoFilePosition + indexInfoPos);

            // finally, deserialize IndexInfo
            return idxInfoSerializer.deserialize(indexReader);
        }
    }

    /**
     * Base class to access {@link IndexInfo} objects.
     */
    public interface IndexInfoRetriever extends AutoCloseable
    {
        IndexInfo columnsIndex(int index) throws IOException;

        void close() throws IOException;
    }

    /**
     * Base class to access {@link IndexInfo} objects on disk that keeps already
     * read {@link IndexInfo} on heap.
     */
    private abstract static class FileIndexInfoRetriever implements IndexInfoRetriever
    {
        final long indexInfoFilePosition;
        final ISerializer<IndexInfo> idxInfoSerializer;
        final FileDataInput indexReader;
        int retrievals;

        /**
         *
         * @param indexInfoFilePosition offset of first serialized {@link IndexInfo} object
         * @param indexReader file data input to access the index file, closed by this instance
         * @param idxInfoSerializer the index serializer to deserialize {@link IndexInfo} objects
         */
        FileIndexInfoRetriever(long indexInfoFilePosition, FileDataInput indexReader, ISerializer<IndexInfo> idxInfoSerializer)
        {
            this.indexInfoFilePosition = indexInfoFilePosition;
            this.idxInfoSerializer = idxInfoSerializer;
            this.indexReader = indexReader;
        }

        public final IndexInfo columnsIndex(int index) throws IOException
        {
            retrievals++;
            return fetchIndex(index);
        }

        abstract IndexInfo fetchIndex(int index) throws IOException;

        public void close() throws IOException
        {
            indexReader.close();

            indexInfoGetsHistogram.update(retrievals);
            indexInfoReadsHistogram.update(retrievals);
        }
    }
}
