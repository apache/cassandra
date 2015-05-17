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

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.db.*;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.AbstractCompactedRow;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.StreamingHistogram;
import org.apache.cassandra.utils.concurrent.Transactional;

import static org.apache.cassandra.utils.Throwables.merge;

public class BigTableWriter extends SSTableWriter
{
    private static final Logger logger = LoggerFactory.getLogger(BigTableWriter.class);

    // not very random, but the only value that can't be mistaken for a legal column-name length
    public static final int END_OF_ROW = 0x0000;

    private final IndexWriter iwriter;
    private SegmentedFile.Builder dbuilder;
    private final SequentialWriter dataFile;
    private DecoratedKey lastWrittenKey;
    private FileMark dataMark;

    BigTableWriter(Descriptor descriptor, Long keyCount, Long repairedAt, CFMetaData metadata, IPartitioner partitioner, MetadataCollector metadataCollector)
    {
        super(descriptor, keyCount, repairedAt, metadata, partitioner, metadataCollector);

        if (compression)
        {
            dataFile = SequentialWriter.open(getFilename(),
                                             descriptor.filenameFor(Component.COMPRESSION_INFO),
                                             metadata.compressionParameters(),
                                             metadataCollector);
            dbuilder = SegmentedFile.getCompressedBuilder((CompressedSequentialWriter) dataFile);
        }
        else
        {
            dataFile = SequentialWriter.open(new File(getFilename()), new File(descriptor.filenameFor(Component.CRC)));
            dbuilder = SegmentedFile.getBuilder(DatabaseDescriptor.getDiskAccessMode(), false);
        }
        iwriter = new IndexWriter(keyCount, dataFile);
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

    private void afterAppend(DecoratedKey decoratedKey, long dataEnd, RowIndexEntry index) throws IOException
    {
        metadataCollector.addKey(decoratedKey.getKey());
        lastWrittenKey = decoratedKey;
        last = lastWrittenKey;
        if (first == null)
            first = lastWrittenKey;

        if (logger.isTraceEnabled())
            logger.trace("wrote {} at {}", decoratedKey, dataEnd);
        iwriter.append(decoratedKey, index, dataEnd);
        dbuilder.addPotentialBoundary(dataEnd);
    }

    /**
     * @param row
     * @return null if the row was compacted away entirely; otherwise, the PK index entry for this row
     */
    public RowIndexEntry append(AbstractCompactedRow row)
    {
        long startPosition = beforeAppend(row.key);
        RowIndexEntry entry;
        try
        {
            entry = row.write(startPosition, dataFile);
            if (entry == null)
                return null;
            long endPosition = dataFile.getFilePointer();
            metadataCollector.update(endPosition - startPosition, row.columnStats());
            afterAppend(row.key, endPosition, entry);
            return entry;
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, dataFile.getPath());
        }
    }

    public void append(DecoratedKey decoratedKey, ColumnFamily cf)
    {
        if (decoratedKey.getKey().remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
        {
            logger.error("Key size {} exceeds maximum of {}, skipping row",
                         decoratedKey.getKey().remaining(),
                         FBUtilities.MAX_UNSIGNED_SHORT);
            return;
        }

        long startPosition = beforeAppend(decoratedKey);
        long endPosition;
        try
        {
            RowIndexEntry entry = rawAppend(cf, startPosition, decoratedKey, dataFile.stream);
            endPosition = dataFile.getFilePointer();
            afterAppend(decoratedKey, endPosition, entry);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, dataFile.getPath());
        }
        metadataCollector.update(endPosition - startPosition, cf.getColumnStats());
    }

    private static RowIndexEntry rawAppend(ColumnFamily cf, long startPosition, DecoratedKey key, DataOutputPlus out) throws IOException
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
    public long appendFromStream(DecoratedKey key, CFMetaData metadata, DataInput in, Version version) throws IOException
    {
        long currentPosition = beforeAppend(key);

        ColumnStats.MaxLongTracker maxTimestampTracker = new ColumnStats.MaxLongTracker(Long.MAX_VALUE);
        ColumnStats.MinLongTracker minTimestampTracker = new ColumnStats.MinLongTracker(Long.MIN_VALUE);
        ColumnStats.MaxIntTracker maxDeletionTimeTracker = new ColumnStats.MaxIntTracker(Integer.MAX_VALUE);
        List<ByteBuffer> minColumnNames = Collections.emptyList();
        List<ByteBuffer> maxColumnNames = Collections.emptyList();
        StreamingHistogram tombstones = new StreamingHistogram(TOMBSTONE_HISTOGRAM_BIN_SIZE);
        boolean hasLegacyCounterShards = false;

        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(metadata);
        cf.delete(DeletionTime.serializer.deserialize(in));

        ColumnIndex.Builder columnIndexer = new ColumnIndex.Builder(cf, key.getKey(), dataFile.stream);

        if (cf.deletionInfo().getTopLevelDeletion().localDeletionTime < Integer.MAX_VALUE)
        {
            tombstones.update(cf.deletionInfo().getTopLevelDeletion().localDeletionTime);
            maxDeletionTimeTracker.update(cf.deletionInfo().getTopLevelDeletion().localDeletionTime);
            minTimestampTracker.update(cf.deletionInfo().getTopLevelDeletion().markedForDeleteAt);
            maxTimestampTracker.update(cf.deletionInfo().getTopLevelDeletion().markedForDeleteAt);
        }

        Iterator<RangeTombstone> rangeTombstoneIterator = cf.deletionInfo().rangeIterator();
        while (rangeTombstoneIterator.hasNext())
        {
            RangeTombstone rangeTombstone = rangeTombstoneIterator.next();
            tombstones.update(rangeTombstone.getLocalDeletionTime());
            minTimestampTracker.update(rangeTombstone.timestamp());
            maxTimestampTracker.update(rangeTombstone.timestamp());
            maxDeletionTimeTracker.update(rangeTombstone.getLocalDeletionTime());
            minColumnNames = ColumnNameHelper.minComponents(minColumnNames, rangeTombstone.min, metadata.comparator);
            maxColumnNames = ColumnNameHelper.maxComponents(maxColumnNames, rangeTombstone.max, metadata.comparator);
        }

        Iterator<OnDiskAtom> iter = AbstractCell.onDiskIterator(in, ColumnSerializer.Flag.PRESERVE_SIZE, Integer.MIN_VALUE, version, metadata.comparator);
        try
        {
            while (iter.hasNext())
            {
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
                minTimestampTracker.update(atom.timestamp());
                maxTimestampTracker.update(atom.timestamp());
                minColumnNames = ColumnNameHelper.minComponents(minColumnNames, atom.name(), metadata.comparator);
                maxColumnNames = ColumnNameHelper.maxComponents(maxColumnNames, atom.name(), metadata.comparator);
                maxDeletionTimeTracker.update(atom.getLocalDeletionTime());

                columnIndexer.add(atom); // This write the atom on disk too
            }

            columnIndexer.maybeWriteEmptyRowHeader();
            dataFile.stream.writeShort(END_OF_ROW);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, dataFile.getPath());
        }

        metadataCollector.updateMinTimestamp(minTimestampTracker.get())
                         .updateMaxTimestamp(maxTimestampTracker.get())
                         .updateMaxLocalDeletionTime(maxDeletionTimeTracker.get())
                         .addRowSize(dataFile.getFilePointer() - currentPosition)
                         .addColumnCount(columnIndexer.writtenAtomCount())
                         .mergeTombstoneHistogram(tombstones)
                         .updateMinColumnNames(minColumnNames)
                         .updateMaxColumnNames(maxColumnNames)
                         .updateHasLegacyCounterShards(hasLegacyCounterShards);

        afterAppend(key, currentPosition, RowIndexEntry.create(currentPosition, cf.deletionInfo().getTopLevelDeletion(), columnIndexer.build()));
        return currentPosition;
    }

    private Descriptor makeTmpLinks()
    {
        // create temp links if they don't already exist
        Descriptor link = descriptor.asType(Descriptor.Type.TEMPLINK);
        if (!new File(link.filenameFor(Component.PRIMARY_INDEX)).exists())
        {
            FileUtils.createHardLink(new File(descriptor.filenameFor(Component.PRIMARY_INDEX)), new File(link.filenameFor(Component.PRIMARY_INDEX)));
            FileUtils.createHardLink(new File(descriptor.filenameFor(Component.DATA)), new File(link.filenameFor(Component.DATA)));
        }
        return link;
    }

    public SSTableReader openEarly()
    {
        // find the max (exclusive) readable key
        IndexSummaryBuilder.ReadableBoundary boundary = iwriter.getMaxReadable();
        if (boundary == null)
            return null;

        StatsMetadata stats = statsMetadata();
        assert boundary.indexLength > 0 && boundary.dataLength > 0;
        Descriptor link = makeTmpLinks();
        // open the reader early, giving it a FINAL descriptor type so that it is indistinguishable for other consumers
        SegmentedFile ifile = iwriter.builder.complete(link.filenameFor(Component.PRIMARY_INDEX), boundary.indexLength);
        SegmentedFile dfile = dbuilder.complete(link.filenameFor(Component.DATA), boundary.dataLength);
        SSTableReader sstable = SSTableReader.internalOpen(descriptor.asType(Descriptor.Type.FINAL),
                                                           components, metadata,
                                                           partitioner, ifile,
                                                           dfile, iwriter.summary.build(partitioner, boundary),
                                                           iwriter.bf.sharedCopy(), maxDataAge, stats, SSTableReader.OpenReason.EARLY);

        // now it's open, find the ACTUAL last readable key (i.e. for which the data file has also been flushed)
        sstable.first = getMinimalKey(first);
        sstable.last = getMinimalKey(boundary.lastKey);
        return sstable;
    }

    public SSTableReader openFinalEarly()
    {
        // we must ensure the data is completely flushed to disk
        dataFile.sync();
        iwriter.indexFile.sync();
        return openFinal(makeTmpLinks(), SSTableReader.OpenReason.EARLY);
    }

    private SSTableReader openFinal(Descriptor desc, SSTableReader.OpenReason openReason)
    {
        if (maxDataAge < 0)
            maxDataAge = System.currentTimeMillis();

        StatsMetadata stats = statsMetadata();
        // finalize in-memory state for the reader
        SegmentedFile ifile = iwriter.builder.complete(desc.filenameFor(Component.PRIMARY_INDEX));
        SegmentedFile dfile = dbuilder.complete(desc.filenameFor(Component.DATA));
        SSTableReader sstable = SSTableReader.internalOpen(desc.asType(Descriptor.Type.FINAL),
                                                           components,
                                                           this.metadata,
                                                           partitioner,
                                                           ifile,
                                                           dfile,
                                                           iwriter.summary.build(partitioner),
                                                           iwriter.bf.sharedCopy(),
                                                           maxDataAge,
                                                           stats,
                                                           openReason);
        sstable.first = getMinimalKey(first);
        sstable.last = getMinimalKey(last);
        return sstable;
    }

    protected SSTableWriter.TransactionalProxy txnProxy()
    {
        return new TransactionalProxy();
    }

    class TransactionalProxy extends SSTableWriter.TransactionalProxy
    {
        // finalise our state on disk, including renaming
        protected void doPrepare()
        {
            Map<MetadataType, MetadataComponent> metadataComponents = finalizeMetadata();

            iwriter.prepareToCommit();

            // write sstable statistics
            dataFile.setDescriptor(descriptor).prepareToCommit();
            writeMetadata(descriptor, metadataComponents);

            // save the table of components
            SSTable.appendTOC(descriptor, components);

            // rename to final
            rename(descriptor, components);

            if (openResult)
                finalReader = openFinal(descriptor.asType(Descriptor.Type.FINAL), SSTableReader.OpenReason.NORMAL);
        }

        protected Throwable doCommit(Throwable accumulate)
        {
            accumulate = dataFile.commit(accumulate);
            accumulate = iwriter.commit(accumulate);
            return accumulate;
        }

        protected Throwable doCleanup(Throwable accumulate)
        {
            accumulate = dbuilder.close(accumulate);
            return accumulate;
        }

        protected Throwable doAbort(Throwable accumulate)
        {
            accumulate = iwriter.abort(accumulate);
            accumulate = dataFile.abort(accumulate);

            accumulate = delete(descriptor, accumulate);
            if (!openResult)
                accumulate = delete(descriptor.asType(Descriptor.Type.FINAL), accumulate);
            return accumulate;
        }

        private Throwable delete(Descriptor desc, Throwable accumulate)
        {
            try
            {
                Set<Component> components = SSTable.discoverComponentsFor(desc);
                if (!components.isEmpty())
                    SSTable.delete(desc, components);
            }
            catch (Throwable t)
            {
                logger.error(String.format("Failed deleting temp components for %s", descriptor), t);
                accumulate = merge(accumulate, t);
            }
            return accumulate;
        }
    }

    private static void writeMetadata(Descriptor desc, Map<MetadataType, MetadataComponent> components)
    {
        File file = new File(desc.filenameFor(Component.STATS));
        try (SequentialWriter out = SequentialWriter.open(file);)
        {
            desc.getMetadataSerializer().serialize(components, out.stream);
            out.setDescriptor(desc).finish();
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, file.getPath());
        }
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
    class IndexWriter extends AbstractTransactional implements Transactional
    {
        private final SequentialWriter indexFile;
        public final SegmentedFile.Builder builder;
        public final IndexSummaryBuilder summary;
        public final IFilter bf;
        private FileMark mark;

        IndexWriter(long keyCount, final SequentialWriter dataFile)
        {
            indexFile = SequentialWriter.open(new File(descriptor.filenameFor(Component.PRIMARY_INDEX)));
            builder = SegmentedFile.getBuilder(DatabaseDescriptor.getIndexAccessMode(), false);
            summary = new IndexSummaryBuilder(keyCount, metadata.getMinIndexInterval(), Downsampling.BASE_SAMPLING_LEVEL);
            bf = FilterFactory.getFilter(keyCount, metadata.getBloomFilterFpChance(), true);
            // register listeners to be alerted when the data files are flushed
            indexFile.setPostFlushListener(new Runnable()
            {
                public void run()
                {
                    summary.markIndexSynced(indexFile.getLastFlushOffset());
                }
            });
            dataFile.setPostFlushListener(new Runnable()
            {
                public void run()
                {
                    summary.markDataSynced(dataFile.getLastFlushOffset());
                }
            });
        }

        // finds the last (-offset) decorated key that can be guaranteed to occur fully in the flushed portion of the index file
        IndexSummaryBuilder.ReadableBoundary getMaxReadable()
        {
            return summary.getLastReadableBoundary();
        }

        public void append(DecoratedKey key, RowIndexEntry indexEntry, long dataEnd) throws IOException
        {
            bf.add(key);
            long indexStart = indexFile.getFilePointer();
            try
            {
                ByteBufferUtil.writeWithShortLength(key.getKey(), indexFile.stream);
                rowIndexEntrySerializer.serialize(indexEntry, indexFile.stream);
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, indexFile.getPath());
            }
            long indexEnd = indexFile.getFilePointer();

            if (logger.isTraceEnabled())
                logger.trace("wrote index entry: {} at {}", indexEntry, indexStart);

            summary.maybeAddEntry(key, indexStart, indexEnd, dataEnd);
            builder.addPotentialBoundary(indexStart);
        }

        /**
         * Closes the index and bloomfilter, making the public state of this writer valid for consumption.
         */
        void flushBf()
        {
            if (components.contains(Component.FILTER))
            {
                String path = descriptor.filenameFor(Component.FILTER);
                try
                {
                    // bloom filter
                    FileOutputStream fos = new FileOutputStream(path);
                    DataOutputStreamPlus stream = new BufferedDataOutputStreamPlus(fos);
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

        protected void doPrepare()
        {
            flushBf();

            // truncate index file
            long position = iwriter.indexFile.getFilePointer();
            iwriter.indexFile.setDescriptor(descriptor).prepareToCommit();
            FileUtils.truncate(iwriter.indexFile.getPath(), position);

            // save summary
            summary.prepareToCommit();
            try (IndexSummary summary = iwriter.summary.build(partitioner))
            {
                SSTableReader.saveSummary(descriptor, first, last, iwriter.builder, dbuilder, summary);
            }
        }

        protected Throwable doCommit(Throwable accumulate)
        {
            return indexFile.commit(accumulate);
        }

        protected Throwable doAbort(Throwable accumulate)
        {
            return indexFile.abort(accumulate);
        }

        protected Throwable doCleanup(Throwable accumulate)
        {
            accumulate = summary.close(accumulate);
            accumulate = bf.close(accumulate);
            accumulate = builder.close(accumulate);
            return accumulate;
        }
    }
}
