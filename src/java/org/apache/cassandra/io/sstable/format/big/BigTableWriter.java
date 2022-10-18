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
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.rows.PartitionSerializationException;
import org.apache.cassandra.db.rows.RangeTombstoneBoundMarker;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.Downsampling;
import org.apache.cassandra.io.sstable.IndexSummary;
import org.apache.cassandra.io.sstable.IndexSummaryBuilder;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReaderBuilder;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.ChecksummedSequentialWriter;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.concurrent.Transactional;

public class BigTableWriter extends SSTableWriter
{
    private static final Logger logger = LoggerFactory.getLogger(BigTableWriter.class);

    protected final BigTableRowIndexEntry.IndexSerializer<IndexInfo> rowIndexEntrySerializer;
    private final ColumnIndex columnIndexWriter;
    private final IndexWriter iwriter;
    private final FileHandle.Builder dbuilder;
    protected final SequentialWriter dataFile;
    private DecoratedKey lastWrittenKey;
    private DataPosition dataMark;
    private long lastEarlyOpenLength = 0;

    private final SequentialWriterOption writerOption = SequentialWriterOption.newBuilder()
                                                        .trickleFsync(DatabaseDescriptor.getTrickleFsync())
                                                        .trickleFsyncByteInterval(DatabaseDescriptor.getTrickleFsyncIntervalInKb() * 1024)
                                                        .build();

    public BigTableWriter(Descriptor descriptor,
                          long keyCount,
                          long repairedAt,
                          UUID pendingRepair,
                          boolean isTransient,
                          TableMetadataRef metadata,
                          MetadataCollector metadataCollector,
                          SerializationHeader header,
                          Collection<SSTableFlushObserver> observers,
                          LifecycleNewTracker lifecycleNewTracker,
                          Set<Component> indexComponents)
    {
        super(descriptor, bigTableComponents(metadata.getLocal(), indexComponents), keyCount, repairedAt, pendingRepair, isTransient, metadata, metadataCollector, header, observers);
        lifecycleNewTracker.trackNew(this); // must track before any files are created

        if (compression)
        {
            final CompressionParams compressionParams = compressionFor(lifecycleNewTracker.opType(), metadata);

            dataFile = new CompressedSequentialWriter(getDataFile(),
                                             descriptor.fileFor(Component.COMPRESSION_INFO),
                                             descriptor.fileFor(Component.DIGEST),
                                             writerOption,
                                             compressionParams,
                                             metadataCollector);
        }
        else
        {
            dataFile = new ChecksummedSequentialWriter(getDataFile(),
                                                       descriptor.fileFor(Component.CRC),
                                                       descriptor.fileFor(Component.DIGEST),
                    writerOption);
        }
        dbuilder = SSTableReaderBuilder.defaultDataHandleBuilder(descriptor).compressed(compression);
        iwriter = new IndexWriter(keyCount);

        this.rowIndexEntrySerializer = new BigTableRowIndexEntry.Serializer(descriptor.version, header);
        columnIndexWriter = new ColumnIndex(this.header, dataFile, descriptor.version, this.observers, rowIndexEntrySerializer.indexInfoSerializer());
    }

    /**
     * Given an OpType, determine the correct Compression Parameters
     * @param opType
     * @return {@link org.apache.cassandra.schema.CompressionParams}
     */
    // TODO STAR-247: this method should be pulled up to SSTable
    public static CompressionParams compressionFor(final OperationType opType, TableMetadataRef metadata)
    {
        CompressionParams compressionParams = metadata.getLocal().params.compression;
        final ICompressor compressor = compressionParams.getSstableCompressor();

        if (null != compressor && opType == OperationType.FLUSH)
        {
            // When we are flushing out of the memtable throughput of the compressor is critical as flushes,
            // especially of large tables, can queue up and potentially block writes.
            // This optimization allows us to fall back to a faster compressor if a particular
            // compression algorithm indicates we should. See CASSANDRA-15379 for more details.
            switch (DatabaseDescriptor.getFlushCompression())
            {
                // It is relatively easier to insert a Noop compressor than to disable compressed writing
                // entirely as the "compression" member field is provided outside the scope of this class.
                // It may make sense in the future to refactor the ownership of the compression flag so that
                // We can bypass the CompressedSequentialWriter in this case entirely.
                case none:
                    compressionParams = CompressionParams.NOOP;
                    break;
                case fast:
                    if (!compressor.recommendedUses().contains(ICompressor.Uses.FAST_COMPRESSION))
                    {
                        // The default compressor is generally fast (LZ4 with 16KiB block size)
                        compressionParams = CompressionParams.DEFAULT;
                        break;
                    }
                case table:
                default:
            }
        }
        return compressionParams;
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
            throw new RuntimeException("Last written key " + lastWrittenKey + " >= current key " + decoratedKey + " writing into " + getDataFile());
        return (lastWrittenKey == null) ? 0 : dataFile.position();
    }

    private void afterAppend(DecoratedKey decoratedKey, long dataEnd, BigTableRowIndexEntry index, ByteBuffer indexInfo) throws IOException
    {
        metadataCollector.addKey(decoratedKey.getKey());
        lastWrittenKey = decoratedKey;
        last = lastWrittenKey;
        if (first == null)
            first = lastWrittenKey;

        if (logger.isTraceEnabled())
            logger.trace("wrote {} at {}", decoratedKey, dataEnd);
        iwriter.append(decoratedKey, index, dataEnd, indexInfo);
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
    public BigTableRowIndexEntry append(UnfilteredRowIterator iterator)
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

        //Reuse the writer for each row
        columnIndexWriter.reset();

        try (UnfilteredRowIterator collecting = Transformation.apply(iterator, new StatsCollector(metadataCollector)))
        {
            columnIndexWriter.buildRowIndex(collecting);

            // afterAppend() writes the partition key before the first RowIndexEntry - so we have to add it's
            // serialized size to the index-writer position
            long indexFilePosition = ByteBufferUtil.serializedSizeWithShortLength(key.getKey()) + iwriter.indexFile.position();

            BigTableRowIndexEntry entry = BigTableRowIndexEntry.create(startPosition, indexFilePosition,
                                                                       collecting.partitionLevelDeletion(),
                                                                       columnIndexWriter.headerLength,
                                                                       columnIndexWriter.columnIndexCount,
                                                                       columnIndexWriter.indexInfoSerializedSize(),
                                                                       columnIndexWriter.indexSamples(),
                                                                       columnIndexWriter.offsets(),
                                                                       rowIndexEntrySerializer.indexInfoSerializer());

            long endPosition = dataFile.position();
            long rowSize = endPosition - startPosition;
            maybeLogLargePartitionWarning(key, rowSize);
            metadataCollector.addPartitionSizeInBytes(rowSize);
            afterAppend(key, endPosition, entry, columnIndexWriter.buffer());
            return entry;
        }
        catch (BufferOverflowException boe)
        {
            throw new PartitionSerializationException(iterator, boe);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, dataFile.getFile());
        }
    }

    // TODO: Move this class to the parent SSTableReader or to a distinct file as it seems to be generic enough
    public static class StatsCollector extends Transformation
    {
        private final MetadataCollector collector;

        public StatsCollector(MetadataCollector collector)
        {
            this.collector = collector;
        }

        @Override
        public Row applyToStatic(Row row)
        {
            if (!row.isEmpty())
                Rows.collectStats(row, collector);
            return row;
        }

        @Override
        public Row applyToRow(Row row)
        {
            collector.updateClusteringValues(row.clustering());
            Rows.collectStats(row, collector);
            return row;
        }

        @Override
        public RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
        {
            collector.updateClusteringValuesByBoundOrBoundary(marker.clustering());
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
            collector.addCellPerPartitionCount();
        }

        @Override
        public DeletionTime applyToDeletion(DeletionTime deletionTime)
        {
            collector.updatePartitionDeletion(deletionTime);
            return deletionTime;
        }
    }

    @SuppressWarnings("resource")
    public boolean openEarly(Consumer<SSTableReader> callWhenReady)
    {
        // find the max (exclusive) readable key
        IndexSummaryBuilder.ReadableBoundary boundary = iwriter.getMaxReadable();
        if (boundary == null)
            return false;

        StatsMetadata stats = statsMetadata();
        assert boundary.indexLength > 0 && boundary.dataLength > 0;
        // open the reader early
        IndexSummary indexSummary = iwriter.summary.build(metadata().partitioner, boundary);
        long indexFileLength = descriptor.fileFor(Component.PRIMARY_INDEX).length();
        int indexBufferSize = optimizationStrategy.bufferSize(indexFileLength / indexSummary.size());
        FileHandle ifile = iwriter.builder.bufferSize(indexBufferSize).complete(boundary.indexLength);
        if (compression)
            dbuilder.withCompressionMetadata(((CompressedSequentialWriter) dataFile).open(boundary.dataLength));
        int dataBufferSize = optimizationStrategy.bufferSize(stats.estimatedPartitionSize.percentile(DatabaseDescriptor.getDiskOptimizationEstimatePercentile()));
        FileHandle dfile = dbuilder.bufferSize(dataBufferSize).complete(boundary.dataLength);
        invalidateCacheAtBoundary(dfile);
        SSTableReader sstable = BigTableReader.internalOpen(descriptor,
                                                           components, metadata,
                                                           ifile, dfile,
                                                           indexSummary,
                                                           iwriter.bf.sharedCopy(),
                                                           maxDataAge,
                                                           stats,
                                                           SSTableReader.OpenReason.EARLY,
                                                           header);

        // now it's open, find the ACTUAL last readable key (i.e. for which the data file has also been flushed)
        sstable.first = getMinimalKey(first);
        sstable.last = getMinimalKey(boundary.lastKey);
        callWhenReady.accept(sstable);
        return true;
    }

    void invalidateCacheAtBoundary(FileHandle dfile)
    {
        if (ChunkCache.instance != null && lastEarlyOpenLength != 0 && dfile.dataLength() > lastEarlyOpenLength)
            ChunkCache.instance.invalidatePosition(dfile, lastEarlyOpenLength);
        lastEarlyOpenLength = dfile.dataLength();
    }

    public SSTableReader openFinalEarly()
    {
        // we must ensure the data is completely flushed to disk
        dataFile.sync();
        iwriter.indexFile.sync();

        return openFinal(SSTableReader.OpenReason.EARLY);
    }

    @SuppressWarnings("resource")
    private SSTableReader openFinal(SSTableReader.OpenReason openReason)
    {
        if (maxDataAge < 0)
            maxDataAge = System.currentTimeMillis();

        StatsMetadata stats = statsMetadata();
        // finalize in-memory state for the reader
        IndexSummary indexSummary = iwriter.summary.build(metadata().partitioner);
        long indexFileLength = descriptor.fileFor(Component.PRIMARY_INDEX).length();
        int dataBufferSize = optimizationStrategy.bufferSize(stats.estimatedPartitionSize.percentile(DatabaseDescriptor.getDiskOptimizationEstimatePercentile()));
        int indexBufferSize = optimizationStrategy.bufferSize(indexFileLength / indexSummary.size());
        FileHandle ifile = iwriter.builder.bufferSize(indexBufferSize).complete();
        if (compression)
            dbuilder.withCompressionMetadata(((CompressedSequentialWriter) dataFile).open(0));
        FileHandle dfile = dbuilder.bufferSize(dataBufferSize).complete();
        invalidateCacheAtBoundary(dfile);
        SSTableReader sstable = SSTableReader.internalOpen(descriptor,
                                                           components,
                                                           metadata,
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
            dataFile.prepareToCommit();
            writeMetadata(descriptor, finalizeMetadata());

            // save the table of components
            SSTable.appendTOC(descriptor, components);

            if (openResult)
                finalReader = openFinal(SSTableReader.OpenReason.NORMAL);
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

    private void writeMetadata(Descriptor desc, Map<MetadataType, MetadataComponent> components)
    {
        File file = desc.fileFor(Component.STATS);
        try (SequentialWriter out = new SequentialWriter(file, writerOption))
        {
            desc.getMetadataSerializer().serialize(components, out, desc.version);
            out.finish();
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, file.path());
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

    public long getEstimatedOnDiskBytesWritten()
    {
        return dataFile.getEstimatedOnDiskBytesWritten();
    }

    /**
     * Encapsulates writing the index and filter for an SSTable. The state of this object is not valid until it has been closed.
     */
    class IndexWriter extends AbstractTransactional implements Transactional
    {
        private final SequentialWriter indexFile;
        public final FileHandle.Builder builder;
        public final IndexSummaryBuilder summary;
        public final IFilter bf;
        private DataPosition mark;

        IndexWriter(long keyCount)
        {
            indexFile = new SequentialWriter(descriptor.fileFor(Component.PRIMARY_INDEX), writerOption);
            builder = SSTableReaderBuilder.defaultIndexHandleBuilder(descriptor, Component.PRIMARY_INDEX);
            summary = new IndexSummaryBuilder(keyCount, metadata().params.minIndexInterval, Downsampling.BASE_SAMPLING_LEVEL);
            bf = FilterFactory.getFilter(keyCount, metadata().params.bloomFilterFpChance);
            // register listeners to be alerted when the data files are flushed
            indexFile.setPostFlushListener(() -> summary.markIndexSynced(indexFile.getLastFlushOffset()));
            dataFile.setPostFlushListener(() -> summary.markDataSynced(dataFile.getLastFlushOffset()));
        }

        // finds the last (-offset) decorated key that can be guaranteed to occur fully in the flushed portion of the index file
        IndexSummaryBuilder.ReadableBoundary getMaxReadable()
        {
            return summary.getLastReadableBoundary();
        }

        public void append(DecoratedKey key, BigTableRowIndexEntry indexEntry, long dataEnd, ByteBuffer indexInfo) throws IOException
        {
            bf.add(key);
            long indexStart = indexFile.position();
            try
            {
                ByteBufferUtil.writeWithShortLength(key.getKey(), indexFile);
                rowIndexEntrySerializer.serialize(indexEntry, indexFile, indexInfo);
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, indexFile.getFile());
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
                File path = descriptor.fileFor(Component.FILTER);
                try (FileOutputStreamPlus stream = new FileOutputStreamPlus(path))
                {
                    // bloom filter
                    BloomFilter.serializer.serialize((BloomFilter) bf, stream);
                    stream.flush();
                    stream.sync();
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
            long position = indexFile.position();
            indexFile.prepareToCommit();
            FileUtils.truncate(indexFile.getFile(), position);

            // save summary
            summary.prepareToCommit();
            try (IndexSummary indexSummary = summary.build(getPartitioner()))
            {
                SSTableReader.saveSummary(descriptor, first, last, indexSummary);
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
