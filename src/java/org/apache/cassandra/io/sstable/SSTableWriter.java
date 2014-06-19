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
package org.apache.cassandra.io.sstable;

import java.io.Closeable;
import java.io.DataInput;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ArrayBackedSortedColumns;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnIndex;
import org.apache.cassandra.db.ColumnSerializer;
import org.apache.cassandra.db.CounterCell;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.OnDiskAtom;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RowIndexEntry;
import org.apache.cassandra.db.compaction.AbstractCompactedRow;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.DataOutputStreamAndChannel;
import org.apache.cassandra.io.util.FileMark;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SegmentedFile;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.StreamingHistogram;

public class SSTableWriter extends SSTable
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableWriter.class);

    // not very random, but the only value that can't be mistaken for a legal column-name length
    public static final int END_OF_ROW = 0x0000;

    private IndexWriter iwriter;
    private SegmentedFile.Builder dbuilder;
    private final SequentialWriter dataFile;
    private DecoratedKey lastWrittenKey;
    private FileMark dataMark;
    private final MetadataCollector sstableMetadataCollector;
    private final long repairedAt;

    public SSTableWriter(String filename, long keyCount, long repairedAt)
    {
        this(filename,
             keyCount,
             repairedAt,
             Schema.instance.getCFMetaData(Descriptor.fromFilename(filename)),
             StorageService.getPartitioner(),
             new MetadataCollector(Schema.instance.getCFMetaData(Descriptor.fromFilename(filename)).comparator));
    }

    private static Set<Component> components(CFMetaData metadata)
    {
        Set<Component> components = new HashSet<Component>(Arrays.asList(Component.DATA,
                                                                         Component.PRIMARY_INDEX,
                                                                         Component.STATS,
                                                                         Component.SUMMARY,
                                                                         Component.TOC,
                                                                         Component.DIGEST));

        if (metadata.getBloomFilterFpChance() < 1.0)
            components.add(Component.FILTER);

        if (metadata.compressionParameters().sstableCompressor != null)
        {
            components.add(Component.COMPRESSION_INFO);
        }
        else
        {
            // it would feel safer to actually add this component later in maybeWriteDigest(),
            // but the components are unmodifiable after construction
            components.add(Component.CRC);
        }
        return components;
    }

    public SSTableWriter(String filename,
                         long keyCount,
                         long repairedAt,
                         CFMetaData metadata,
                         IPartitioner<?> partitioner,
                         MetadataCollector sstableMetadataCollector)
    {
        super(Descriptor.fromFilename(filename),
              components(metadata),
              metadata,
              partitioner);
        this.repairedAt = repairedAt;
        iwriter = new IndexWriter(keyCount);

        if (compression)
        {
            dataFile = SequentialWriter.open(getFilename(),
                                             descriptor.filenameFor(Component.COMPRESSION_INFO),
                                             metadata.compressionParameters(),
                                             sstableMetadataCollector);
            dbuilder = SegmentedFile.getCompressedBuilder((CompressedSequentialWriter) dataFile);
        }
        else
        {
            dataFile = SequentialWriter.open(new File(getFilename()), new File(descriptor.filenameFor(Component.CRC)));
            dbuilder = SegmentedFile.getBuilder(DatabaseDescriptor.getDiskAccessMode());
        }

        this.sstableMetadataCollector = sstableMetadataCollector;
    }

    public void mark()
    {
        dataMark = dataFile.mark();
        iwriter.mark();
    }

    public void resetAndTruncate()
    {
        dataFile.resetAndTruncate(dataMark);
        iwriter.resetAndTruncate();
    }

    /**
     * Perform sanity checks on @param decoratedKey and @return the position in the data file before any data is written
     */
    private long beforeAppend(DecoratedKey decoratedKey)
    {
        assert decoratedKey != null : "Keys must not be null"; // empty keys ARE allowed b/c of indexed column values
        if (lastWrittenKey != null && lastWrittenKey.compareTo(decoratedKey) >= 0)
            throw new RuntimeException("Last written key " + lastWrittenKey + " >= current key " + decoratedKey + " writing into " + getFilename());
        return (lastWrittenKey == null) ? 0 : dataFile.getFilePointer();
    }

    private void afterAppend(DecoratedKey decoratedKey, long dataPosition, RowIndexEntry index)
    {
        sstableMetadataCollector.addKey(decoratedKey.getKey());
        lastWrittenKey = decoratedKey;
        last = lastWrittenKey;
        if (first == null)
            first = lastWrittenKey;

        if (logger.isTraceEnabled())
            logger.trace("wrote {} at {}", decoratedKey, dataPosition);
        iwriter.append(decoratedKey, index);
        dbuilder.addPotentialBoundary(dataPosition);
    }

    /**
     * @param row
     * @return null if the row was compacted away entirely; otherwise, the PK index entry for this row
     */
    public RowIndexEntry append(AbstractCompactedRow row)
    {
        long currentPosition = beforeAppend(row.key);
        RowIndexEntry entry;
        try
        {
            entry = row.write(currentPosition, dataFile.stream);
            if (entry == null)
                return null;
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, dataFile.getPath());
        }
        sstableMetadataCollector.update(dataFile.getFilePointer() - currentPosition, row.columnStats());
        afterAppend(row.key, currentPosition, entry);
        return entry;
    }

    public void append(DecoratedKey decoratedKey, ColumnFamily cf)
    {
        long startPosition = beforeAppend(decoratedKey);
        try
        {
            RowIndexEntry entry = rawAppend(cf, startPosition, decoratedKey, dataFile.stream);
            afterAppend(decoratedKey, startPosition, entry);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, dataFile.getPath());
        }
        sstableMetadataCollector.update(dataFile.getFilePointer() - startPosition, cf.getColumnStats());
    }

    public static RowIndexEntry rawAppend(ColumnFamily cf, long startPosition, DecoratedKey key, DataOutputPlus out) throws IOException
    {
        assert cf.hasColumns() || cf.isMarkedForDelete();

        ColumnIndex.Builder builder = new ColumnIndex.Builder(cf, key.getKey(), out);
        ColumnIndex index = builder.build(cf);

        out.writeShort(END_OF_ROW);
        return RowIndexEntry.create(startPosition, cf.deletionInfo().getTopLevelDeletion(), index);
    }

    /**
     * @throws IOException if a read from the DataInput fails
     * @throws FSWriteError if a write to the dataFile fails
     */
    public long appendFromStream(DecoratedKey key, CFMetaData metadata, DataInput in, Descriptor.Version version) throws IOException
    {
        long currentPosition = beforeAppend(key);

        // deserialize each column to obtain maxTimestamp and immediately serialize it.
        long minTimestamp = Long.MAX_VALUE;
        long maxTimestamp = Long.MIN_VALUE;
        int maxLocalDeletionTime = Integer.MIN_VALUE;
        List<ByteBuffer> minColumnNames = Collections.emptyList();
        List<ByteBuffer> maxColumnNames = Collections.emptyList();
        StreamingHistogram tombstones = new StreamingHistogram(TOMBSTONE_HISTOGRAM_BIN_SIZE);
        boolean hasLegacyCounterShards = false;

        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(metadata);
        cf.delete(DeletionTime.serializer.deserialize(in));

        ColumnIndex.Builder columnIndexer = new ColumnIndex.Builder(cf, key.getKey(), dataFile.stream);

        if (cf.deletionInfo().getTopLevelDeletion().localDeletionTime < Integer.MAX_VALUE)
            tombstones.update(cf.deletionInfo().getTopLevelDeletion().localDeletionTime);

        Iterator<RangeTombstone> rangeTombstoneIterator = cf.deletionInfo().rangeIterator();
        while (rangeTombstoneIterator.hasNext())
        {
            RangeTombstone rangeTombstone = rangeTombstoneIterator.next();
            tombstones.update(rangeTombstone.getLocalDeletionTime());

            minColumnNames = ColumnNameHelper.minComponents(minColumnNames, rangeTombstone.min, metadata.comparator);
            maxColumnNames = ColumnNameHelper.maxComponents(maxColumnNames, rangeTombstone.max, metadata.comparator);
        }

        Iterator<OnDiskAtom> iter = metadata.getOnDiskIterator(in, ColumnSerializer.Flag.PRESERVE_SIZE, Integer.MIN_VALUE, version);
        try
        {
            while (iter.hasNext())
            {
                // deserialize column with PRESERVE_SIZE because we've written the cellDataSize based on the
                // data size received, so we must reserialize the exact same data
                OnDiskAtom atom = iter.next();
                if (atom == null)
                    break;

                if (atom instanceof CounterCell)
                {
                    atom = ((CounterCell) atom).markLocalToBeCleared();
                    hasLegacyCounterShards = hasLegacyCounterShards || ((CounterCell) atom).hasLegacyShards();
                }

                int deletionTime = atom.getLocalDeletionTime();
                if (deletionTime < Integer.MAX_VALUE)
                    tombstones.update(deletionTime);
                minTimestamp = Math.min(minTimestamp, atom.timestamp());
                maxTimestamp = Math.max(maxTimestamp, atom.timestamp());
                minColumnNames = ColumnNameHelper.minComponents(minColumnNames, atom.name(), metadata.comparator);
                maxColumnNames = ColumnNameHelper.maxComponents(maxColumnNames, atom.name(), metadata.comparator);
                maxLocalDeletionTime = Math.max(maxLocalDeletionTime, atom.getLocalDeletionTime());

                columnIndexer.add(atom); // This write the atom on disk too
            }

            columnIndexer.maybeWriteEmptyRowHeader();
            dataFile.stream.writeShort(END_OF_ROW);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, dataFile.getPath());
        }

        sstableMetadataCollector.updateMinTimestamp(minTimestamp)
                                .updateMaxTimestamp(maxTimestamp)
                                .updateMaxLocalDeletionTime(maxLocalDeletionTime)
                                .addRowSize(dataFile.getFilePointer() - currentPosition)
                                .addColumnCount(columnIndexer.writtenAtomCount())
                                .mergeTombstoneHistogram(tombstones)
                                .updateMinColumnNames(minColumnNames)
                                .updateMaxColumnNames(maxColumnNames)
                                .updateHasLegacyCounterShards(hasLegacyCounterShards);
        afterAppend(key, currentPosition, RowIndexEntry.create(currentPosition, cf.deletionInfo().getTopLevelDeletion(), columnIndexer.build()));
        return currentPosition;
    }

    /**
     * After failure, attempt to close the index writer and data file before deleting all temp components for the sstable
     */
    public void abort()
    {
        abort(true);
    }
    public void abort(boolean closeBf)
    {
        assert descriptor.type.isTemporary;
        if (iwriter == null && dataFile == null)
            return;
        if (iwriter != null)
        {
            FileUtils.closeQuietly(iwriter.indexFile);
            if (closeBf)
            {
                iwriter.bf.close();
            }
        }
        if (dataFile!= null)
            FileUtils.closeQuietly(dataFile);

        Set<Component> components = SSTable.componentsFor(descriptor);
        try
        {
            if (!components.isEmpty())
                SSTable.delete(descriptor, components);
        }
        catch (FSWriteError e)
        {
            logger.error(String.format("Failed deleting temp components for %s", descriptor), e);
            throw e;
        }
    }

    // we use this method to ensure any managed data we may have retained references to during the write are no
    // longer referenced, so that we do not need to enclose the expensive call to closeAndOpenReader() in a transaction
    public void isolateReferences()
    {
        // currently we only maintain references to first/last/lastWrittenKey from the data provided; all other
        // data retention is done through copying
        first = getMinimalKey(first);
        last = lastWrittenKey = getMinimalKey(last);
    }

    public SSTableReader openEarly(long maxDataAge)
    {
        StatsMetadata sstableMetadata = (StatsMetadata) sstableMetadataCollector.finalizeMetadata(partitioner.getClass().getCanonicalName(),
                                                  metadata.getBloomFilterFpChance(),
                                                  repairedAt).get(MetadataType.STATS);

        // find the max (exclusive) readable key
        DecoratedKey exclusiveUpperBoundOfReadableIndex = iwriter.getMaxReadableKey(0);
        if (exclusiveUpperBoundOfReadableIndex == null)
            return null;

        // create temp links if they don't already exist
        Descriptor link = descriptor.asType(Descriptor.Type.TEMPLINK);
        if (!new File(link.filenameFor(Component.PRIMARY_INDEX)).exists())
        {
            FileUtils.createHardLink(new File(descriptor.filenameFor(Component.PRIMARY_INDEX)), new File(link.filenameFor(Component.PRIMARY_INDEX)));
            FileUtils.createHardLink(new File(descriptor.filenameFor(Component.DATA)), new File(link.filenameFor(Component.DATA)));
        }

        // open the reader early, giving it a FINAL descriptor type so that it is indistinguishable for other consumers
        SegmentedFile ifile = iwriter.builder.openEarly(link.filenameFor(Component.PRIMARY_INDEX));
        SegmentedFile dfile = dbuilder.openEarly(link.filenameFor(Component.DATA));
        SSTableReader sstable = SSTableReader.internalOpen(descriptor.asType(Descriptor.Type.FINAL),
                                                           components, metadata,
                                                           partitioner, ifile,
                                                           dfile, iwriter.summary.build(partitioner, exclusiveUpperBoundOfReadableIndex),
                                                           iwriter.bf, maxDataAge, sstableMetadata, true);

        // now it's open, find the ACTUAL last readable key (i.e. for which the data file has also been flushed)
        sstable.first = getMinimalKey(first);
        sstable.last = getMinimalKey(exclusiveUpperBoundOfReadableIndex);
        DecoratedKey inclusiveUpperBoundOfReadableData = iwriter.getMaxReadableKey(1);
        if (inclusiveUpperBoundOfReadableData == null)
            return null;
        int offset = 2;
        while (true)
        {
            RowIndexEntry indexEntry = sstable.getPosition(inclusiveUpperBoundOfReadableData, SSTableReader.Operator.GT);
            if (indexEntry != null && indexEntry.position <= dataFile.getLastFlushOffset())
                break;
            inclusiveUpperBoundOfReadableData = iwriter.getMaxReadableKey(offset++);
            if (inclusiveUpperBoundOfReadableData == null)
                return null;
        }
        sstable.last = getMinimalKey(inclusiveUpperBoundOfReadableData);
        return sstable;
    }

    public SSTableReader closeAndOpenReader()
    {
        return closeAndOpenReader(System.currentTimeMillis());
    }

    public SSTableReader closeAndOpenReader(long maxDataAge)
    {
        return closeAndOpenReader(maxDataAge, this.repairedAt);
    }

    public SSTableReader closeAndOpenReader(long maxDataAge, long repairedAt)
    {
        Pair<Descriptor, StatsMetadata> p = close(repairedAt);
        Descriptor newdesc = p.left;
        StatsMetadata sstableMetadata = p.right;

        // finalize in-memory state for the reader
        SegmentedFile ifile = iwriter.builder.complete(newdesc.filenameFor(Component.PRIMARY_INDEX));
        SegmentedFile dfile = dbuilder.complete(newdesc.filenameFor(Component.DATA));
        SSTableReader sstable = SSTableReader.internalOpen(newdesc,
                                                           components,
                                                           metadata,
                                                           partitioner,
                                                           ifile,
                                                           dfile,
                                                           iwriter.summary.build(partitioner),
                                                           iwriter.bf,
                                                           maxDataAge,
                                                           sstableMetadata,
                                                           false);
        sstable.first = getMinimalKey(first);
        sstable.last = getMinimalKey(last);
        // try to save the summaries to disk
        sstable.saveSummary(iwriter.builder, dbuilder);
        iwriter = null;
        dbuilder = null;
        return sstable;
    }

    // Close the writer and return the descriptor to the new sstable and it's metadata
    public Pair<Descriptor, StatsMetadata> close()
    {
        return close(this.repairedAt);
    }

    private Pair<Descriptor, StatsMetadata> close(long repairedAt)
    {

        // index and filter
        iwriter.close();
        // main data, close will truncate if necessary
        dataFile.close();
        dataFile.writeFullChecksum(descriptor);
        // write sstable statistics
        Map<MetadataType, MetadataComponent> metadataComponents = sstableMetadataCollector.finalizeMetadata(
                                                                                    partitioner.getClass().getCanonicalName(),
                                                                                    metadata.getBloomFilterFpChance(),
                                                                                    repairedAt);
        writeMetadata(descriptor, metadataComponents);

        // save the table of components
        SSTable.appendTOC(descriptor, components);

        // remove the 'tmp' marker from all components
        return Pair.create(rename(descriptor, components), (StatsMetadata) metadataComponents.get(MetadataType.STATS));

    }


    private static void writeMetadata(Descriptor desc, Map<MetadataType, MetadataComponent> components)
    {
        SequentialWriter out = SequentialWriter.open(new File(desc.filenameFor(Component.STATS)));
        try
        {
            desc.getMetadataSerializer().serialize(components, out.stream);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, out.getPath());
        }
        finally
        {
            out.close();
        }
    }

    static Descriptor rename(Descriptor tmpdesc, Set<Component> components)
    {
        Descriptor newdesc = tmpdesc.asType(Descriptor.Type.FINAL);
        rename(tmpdesc, newdesc, components);
        return newdesc;
    }

    public static void rename(Descriptor tmpdesc, Descriptor newdesc, Set<Component> components)
    {
        for (Component component : Sets.difference(components, Sets.newHashSet(Component.DATA, Component.SUMMARY)))
        {
            FileUtils.renameWithConfirm(tmpdesc.filenameFor(component), newdesc.filenameFor(component));
        }

        // do -Data last because -Data present should mean the sstable was completely renamed before crash
        FileUtils.renameWithConfirm(tmpdesc.filenameFor(Component.DATA), newdesc.filenameFor(Component.DATA));

        // rename it without confirmation because summary can be available for loadNewSSTables but not for closeAndOpenReader
        FileUtils.renameWithOutConfirm(tmpdesc.filenameFor(Component.SUMMARY), newdesc.filenameFor(Component.SUMMARY));
    }

    public long getFilePointer()
    {
        return dataFile.getFilePointer();
    }

    public long getOnDiskFilePointer()
    {
        return dataFile.getOnDiskFilePointer();
    }

    /**
     * Encapsulates writing the index and filter for an SSTable. The state of this object is not valid until it has been closed.
     */
    class IndexWriter implements Closeable
    {
        private final SequentialWriter indexFile;
        public final SegmentedFile.Builder builder;
        public final IndexSummaryBuilder summary;
        public final IFilter bf;
        private FileMark mark;

        IndexWriter(long keyCount)
        {
            indexFile = SequentialWriter.open(new File(descriptor.filenameFor(Component.PRIMARY_INDEX)));
            builder = SegmentedFile.getBuilder(DatabaseDescriptor.getIndexAccessMode());
            summary = new IndexSummaryBuilder(keyCount, metadata.getMinIndexInterval(), Downsampling.BASE_SAMPLING_LEVEL);
            bf = FilterFactory.getFilter(keyCount, metadata.getBloomFilterFpChance(), true);
        }

        // finds the last (-offset) decorated key that can be guaranteed to occur fully in the flushed portion of the index file
        DecoratedKey getMaxReadableKey(int offset)
        {
            long maxIndexLength = indexFile.getLastFlushOffset();
            return summary.getMaxReadableKey(maxIndexLength, offset);
        }

        public void append(DecoratedKey key, RowIndexEntry indexEntry)
        {
            bf.add(key.getKey());
            long indexPosition = indexFile.getFilePointer();
            try
            {
                ByteBufferUtil.writeWithShortLength(key.getKey(), indexFile.stream);
                metadata.comparator.rowIndexEntrySerializer().serialize(indexEntry, indexFile.stream);
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, indexFile.getPath());
            }

            if (logger.isTraceEnabled())
                logger.trace("wrote index entry: {} at {}", indexEntry, indexPosition);

            summary.maybeAddEntry(key, indexPosition);
            builder.addPotentialBoundary(indexPosition);
        }

        /**
         * Closes the index and bloomfilter, making the public state of this writer valid for consumption.
         */
        public void close()
        {
            if (components.contains(Component.FILTER))
            {
                String path = descriptor.filenameFor(Component.FILTER);
                try
                {
                    // bloom filter
                    FileOutputStream fos = new FileOutputStream(path);
                    DataOutputStreamAndChannel stream = new DataOutputStreamAndChannel(fos);
                    FilterFactory.serialize(bf, stream);
                    stream.flush();
                    fos.getFD().sync();
                    stream.close();
                }
                catch (IOException e)
                {
                    throw new FSWriteError(e, path);
                }
            }

            // index
            long position = indexFile.getFilePointer();
            indexFile.close(); // calls force
            FileUtils.truncate(indexFile.getPath(), position);
        }

        public void mark()
        {
            mark = indexFile.mark();
        }

        public void resetAndTruncate()
        {
            // we can't un-set the bloom filter addition, but extra keys in there are harmless.
            // we can't reset dbuilder either, but that is the last thing called in afterappend so
            // we assume that if that worked then we won't be trying to reset.
            indexFile.resetAndTruncate(mark);
        }

        @Override
        public String toString()
        {
            return "IndexWriter(" + descriptor + ")";
        }
    }
}
