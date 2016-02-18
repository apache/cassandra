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
import java.util.Collection;
import java.util.Map;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.rows.*;
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
import org.apache.cassandra.utils.concurrent.Transactional;

import org.apache.cassandra.utils.SyncUtil;

public class BigTableWriter extends SSTableWriter
{
    private static final Logger logger = LoggerFactory.getLogger(BigTableWriter.class);

    private final IndexWriter iwriter;
    private final SegmentedFile.Builder dbuilder;
    protected final SequentialWriter dataFile;
    private DecoratedKey lastWrittenKey;
    private DataPosition dataMark;

    public BigTableWriter(Descriptor descriptor, 
                          Long keyCount, 
                          Long repairedAt, 
                          CFMetaData metadata, 
                          MetadataCollector metadataCollector, 
                          SerializationHeader header,
                          Collection<SSTableFlushObserver> observers,
                          LifecycleTransaction txn)
    {
        super(descriptor, keyCount, repairedAt, metadata, metadataCollector, header, observers);
        txn.trackNew(this); // must track before any files are created

        if (compression)
        {
            dataFile = SequentialWriter.open(getFilename(),
                                             descriptor.filenameFor(Component.COMPRESSION_INFO),
                                             metadata.params.compression,
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
    protected long beforeAppend(DecoratedKey decoratedKey)
    {
        assert decoratedKey != null : "Keys must not be null"; // empty keys ARE allowed b/c of indexed column values
        if (lastWrittenKey != null && lastWrittenKey.compareTo(decoratedKey) >= 0)
            throw new RuntimeException("Last written key " + lastWrittenKey + " >= current key " + decoratedKey + " writing into " + getFilename());
        return (lastWrittenKey == null) ? 0 : dataFile.position();
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
    }

    /**
     * Appends partition data to this writer.
     *
     * @param iterator the partition to write
     * @return the created index entry if something was written, that is if {@code iterator}
     * wasn't empty, {@code null} otherwise.
     *
     * @throws FSWriteError if a write to the dataFile fails
     */
    public RowIndexEntry append(UnfilteredRowIterator iterator)
    {
        DecoratedKey key = iterator.partitionKey();

        if (key.getKey().remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
        {
            logger.error("Key size {} exceeds maximum of {}, skipping row", key.getKey().remaining(), FBUtilities.MAX_UNSIGNED_SHORT);
            return null;
        }

        if (iterator.isEmpty())
            return null;

        long startPosition = beforeAppend(key);
        observers.forEach((o) -> o.startPartition(key, iwriter.indexFile.position()));

        try (UnfilteredRowIterator collecting = Transformation.apply(iterator, new StatsCollector(metadataCollector)))
        {
            ColumnIndex index = ColumnIndex.writeAndBuildIndex(collecting, dataFile, header, observers, descriptor.version);

            RowIndexEntry entry = RowIndexEntry.create(startPosition, collecting.partitionLevelDeletion(), index);

            long endPosition = dataFile.position();
            long rowSize = endPosition - startPosition;
            maybeLogLargePartitionWarning(key, rowSize);
            metadataCollector.addPartitionSizeInBytes(rowSize);
            afterAppend(key, endPosition, entry);
            return entry;
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, dataFile.getPath());
        }
    }

    private void maybeLogLargePartitionWarning(DecoratedKey key, long rowSize)
    {
        if (rowSize > DatabaseDescriptor.getCompactionLargePartitionWarningThreshold())
        {
            String keyString = metadata.getKeyValidator().getString(key.getKey());
            logger.warn("Writing large partition {}/{}:{} ({})", metadata.ksName, metadata.cfName, keyString, FBUtilities.prettyPrintMemory(rowSize));
        }
    }

    private static class StatsCollector extends Transformation
    {
        private final MetadataCollector collector;
        private int cellCount;

        StatsCollector(MetadataCollector collector)
        {
            this.collector = collector;
        }

        @Override
        public Row applyToStatic(Row row)
        {
            if (!row.isEmpty())
                cellCount += Rows.collectStats(row, collector);
            return row;
        }

        @Override
        public Row applyToRow(Row row)
        {
            collector.updateClusteringValues(row.clustering());
            cellCount += Rows.collectStats(row, collector);
            return row;
        }

        @Override
        public RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
        {
            collector.updateClusteringValues(marker.clustering());
            if (marker.isBoundary())
            {
                RangeTombstoneBoundaryMarker bm = (RangeTombstoneBoundaryMarker)marker;
                collector.update(bm.endDeletionTime());
                collector.update(bm.startDeletionTime());
            }
            else
            {
                collector.update(((RangeTombstoneBoundMarker)marker).deletionTime());
            }
            return marker;
        }

        @Override
        public void onPartitionClose()
        {
            collector.addCellPerPartitionCount(cellCount);
        }

        @Override
        public DeletionTime applyToDeletion(DeletionTime deletionTime)
        {
            collector.update(deletionTime);
            return deletionTime;
        }
    }

    @SuppressWarnings("resource")
    public SSTableReader openEarly()
    {
        // find the max (exclusive) readable key
        IndexSummaryBuilder.ReadableBoundary boundary = iwriter.getMaxReadable();
        if (boundary == null)
            return null;

        StatsMetadata stats = statsMetadata();
        assert boundary.indexLength > 0 && boundary.dataLength > 0;
        // open the reader early
        IndexSummary indexSummary = iwriter.summary.build(metadata.partitioner, boundary);
        SegmentedFile ifile = iwriter.builder.buildIndex(descriptor, indexSummary, boundary);
        SegmentedFile dfile = dbuilder.buildData(descriptor, stats, boundary);
        SSTableReader sstable = SSTableReader.internalOpen(descriptor,
                                                           components, metadata,
                                                           ifile, dfile, indexSummary,
                                                           iwriter.bf.sharedCopy(), maxDataAge, stats, SSTableReader.OpenReason.EARLY, header);

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

        return openFinal(descriptor, SSTableReader.OpenReason.EARLY);
    }

    @SuppressWarnings("resource")
    private SSTableReader openFinal(Descriptor desc, SSTableReader.OpenReason openReason)
    {
        if (maxDataAge < 0)
            maxDataAge = System.currentTimeMillis();

        StatsMetadata stats = statsMetadata();
        // finalize in-memory state for the reader
        IndexSummary indexSummary = iwriter.summary.build(this.metadata.partitioner);
        SegmentedFile ifile = iwriter.builder.buildIndex(desc, indexSummary);
        SegmentedFile dfile = dbuilder.buildData(desc, stats);
        SSTableReader sstable = SSTableReader.internalOpen(desc,
                                                           components,
                                                           this.metadata,
                                                           ifile,
                                                           dfile,
                                                           indexSummary,
                                                           iwriter.bf.sharedCopy(),
                                                           maxDataAge,
                                                           stats,
                                                           openReason,
                                                           header);
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
            iwriter.prepareToCommit();

            // write sstable statistics
            dataFile.setDescriptor(descriptor).prepareToCommit();
            writeMetadata(descriptor, finalizeMetadata());

            // save the table of components
            SSTable.appendTOC(descriptor, components);

            if (openResult)
                finalReader = openFinal(descriptor, SSTableReader.OpenReason.NORMAL);
        }

        protected Throwable doCommit(Throwable accumulate)
        {
            accumulate = dataFile.commit(accumulate);
            accumulate = iwriter.commit(accumulate);
            return accumulate;
        }

        @Override
        protected Throwable doPostCleanup(Throwable accumulate)
        {
            accumulate = dbuilder.close(accumulate);
            return accumulate;
        }

        protected Throwable doAbort(Throwable accumulate)
        {
            accumulate = iwriter.abort(accumulate);
            accumulate = dataFile.abort(accumulate);
            return accumulate;
        }
    }

    private static void writeMetadata(Descriptor desc, Map<MetadataType, MetadataComponent> components)
    {
        File file = new File(desc.filenameFor(Component.STATS));
        try (SequentialWriter out = SequentialWriter.open(file))
        {
            desc.getMetadataSerializer().serialize(components, out, desc.version);
            out.setDescriptor(desc).finish();
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, file.getPath());
        }
    }

    public long getFilePointer()
    {
        return dataFile.position();
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
        private DataPosition mark;

        IndexWriter(long keyCount, final SequentialWriter dataFile)
        {
            indexFile = SequentialWriter.open(new File(descriptor.filenameFor(Component.PRIMARY_INDEX)));
            builder = SegmentedFile.getBuilder(DatabaseDescriptor.getIndexAccessMode(), false);
            summary = new IndexSummaryBuilder(keyCount, metadata.params.minIndexInterval, Downsampling.BASE_SAMPLING_LEVEL);
            bf = FilterFactory.getFilter(keyCount, metadata.params.bloomFilterFpChance, true, descriptor.version.hasOldBfHashOrder());
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
            long indexStart = indexFile.position();
            try
            {
                ByteBufferUtil.writeWithShortLength(key.getKey(), indexFile);
                rowIndexEntrySerializer.serialize(indexEntry, indexFile);
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, indexFile.getPath());
            }
            long indexEnd = indexFile.position();

            if (logger.isTraceEnabled())
                logger.trace("wrote index entry: {} at {}", indexEntry, indexStart);

            summary.maybeAddEntry(key, indexStart, indexEnd, dataEnd);
        }

        /**
         * Closes the index and bloomfilter, making the public state of this writer valid for consumption.
         */
        void flushBf()
        {
            if (components.contains(Component.FILTER))
            {
                String path = descriptor.filenameFor(Component.FILTER);
                try (FileOutputStream fos = new FileOutputStream(path);
                     DataOutputStreamPlus stream = new BufferedDataOutputStreamPlus(fos))
                {
                    // bloom filter
                    FilterFactory.serialize(bf, stream);
                    stream.flush();
                    SyncUtil.sync(fos);
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
            long position = iwriter.indexFile.position();
            iwriter.indexFile.setDescriptor(descriptor).prepareToCommit();
            FileUtils.truncate(iwriter.indexFile.getPath(), position);

            // save summary
            summary.prepareToCommit();
            try (IndexSummary summary = iwriter.summary.build(getPartitioner()))
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

        @Override
        protected Throwable doPostCleanup(Throwable accumulate)
        {
            accumulate = summary.close(accumulate);
            accumulate = bf.close(accumulate);
            accumulate = builder.close(accumulate);
            return accumulate;
        }
    }
}
