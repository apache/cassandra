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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.format.FilterComponent;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.TOCComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.*;
import org.apache.cassandra.utils.concurrent.SharedCloseableImpl;
import org.apache.cassandra.utils.concurrent.Transactional;

import static org.apache.cassandra.io.util.FileHandle.Builder.NO_LENGTH_OVERRIDE;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

public class BigTableWriter extends SSTableWriter
{
    private static final Logger logger = LoggerFactory.getLogger(BigTableWriter.class);

    private final BigFormatPartitionWriter partitionWriter;
    private final IndexWriter indexWriter;
    protected final SequentialWriter dataWriter;
    private DecoratedKey lastWrittenKey;
    private DataPosition dataMark;
    private long lastEarlyOpenLength = 0;
    private final RowIndexEntry.IndexSerializer rowIndexEntrySerializer;

    public BigTableWriter(Descriptor descriptor,
                          long keyCount,
                          long repairedAt,
                          TimeUUID pendingRepair,
                          boolean isTransient,
                          TableMetadataRef metadata,
                          MetadataCollector metadataCollector, 
                          SerializationHeader header,
                          Collection<SSTableFlushObserver> observers,
                          LifecycleNewTracker lifecycleNewTracker)
    {
        super(descriptor, keyCount, repairedAt, pendingRepair, isTransient, metadata, metadataCollector, header, observers, components(metadata.getLocal()));
        lifecycleNewTracker.trackNew(this); // must track before any files are created

        this.rowIndexEntrySerializer = new RowIndexEntry.Serializer(descriptor.version, header);

        if (compression)
        {
            final CompressionParams compressionParams = compressionFor(lifecycleNewTracker.opType());

            dataWriter = new CompressedSequentialWriter(new File(getFilename()),
                                                        descriptor.filenameFor(Component.COMPRESSION_INFO),
                                                        new File(descriptor.filenameFor(Component.DIGEST)),
                                                        ioOptions.writerOptions,
                                                        compressionParams,
                                                        metadataCollector);
        }
        else
        {
            dataWriter = new ChecksummedSequentialWriter(new File(getFilename()),
                                                         new File(descriptor.filenameFor(Component.CRC)),
                                                         new File(descriptor.filenameFor(Component.DIGEST)),
                                                         ioOptions.writerOptions);
        }
        indexWriter = new IndexWriter(keyCount);

        partitionWriter = new BigFormatPartitionWriter(this.header, dataWriter, descriptor.version, rowIndexEntrySerializer.indexInfoSerializer());
    }

    /**
     * Given an OpType, determine the correct Compression Parameters
     * @param opType
     * @return {@link org.apache.cassandra.schema.CompressionParams}
     */
    private CompressionParams compressionFor(final OperationType opType)
    {
        CompressionParams compressionParams = metadata.getLocal().params.compression;
        final ICompressor compressor = compressionParams.getSstableCompressor();

        if (null != compressor && opType == OperationType.FLUSH)
        {
            // When we are flushing out of the memtable throughput of the compressor is critical as flushes,
            // especially of large tables, can queue up and potentially block writes.
            // This optimization allows us to fall back to a faster compressor if a particular
            // compression algorithm indicates we should. See CASSANDRA-15379 for more details.
            switch (ioOptions.flushCompression)
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
        dataMark = dataWriter.mark();
        indexWriter.mark();
    }

    public void resetAndTruncate()
    {
        dataWriter.resetAndTruncate(dataMark);
        indexWriter.resetAndTruncate();
    }

    /**
     * Appends partition data to this writer.
     *
     * @param partition the partition to write
     * @return the created index entry if something was written, that is if {@code iterator}
     * wasn't empty, {@code null} otherwise.
     * @throws FSWriteError if write to the dataFile fails
     */
    @Override
    public final RowIndexEntry append(UnfilteredRowIterator partition)
    {
        if (partition.isEmpty())
            return null;

        try
        {
            if (!verifyPartition(partition.partitionKey()))
                return null;

            startPartition(partition.partitionKey(), partition.partitionLevelDeletion());

            RowIndexEntry indexEntry;
            if (header.hasStatic())
                addStaticRow(partition.partitionKey(), partition.staticRow());

            while (partition.hasNext())
                addUnfiltered(partition.partitionKey(), partition.next());

            indexEntry = endPartition(partition.partitionKey(), partition.partitionLevelDeletion());

            return indexEntry;
        }
        catch (BufferOverflowException boe)
        {
            throw new PartitionSerializationException(partition, boe);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getFilename());
        }
    }

    private boolean verifyPartition(DecoratedKey key)
    {
        assert key != null : "Keys must not be null"; // empty keys ARE allowed b/c of indexed column values

        if (key.getKey().remaining() > FBUtilities.MAX_UNSIGNED_SHORT)
        {
            logger.error("Key size {} exceeds maximum of {}, skipping row", key.getKey().remaining(), FBUtilities.MAX_UNSIGNED_SHORT);
            return false;
        }

        if (lastWrittenKey != null && lastWrittenKey.compareTo(key) >= 0)
            throw new RuntimeException(String.format("Last written key %s >= current key %s, writing into %s", lastWrittenKey, key, getFilename()));

        return true;
    }

    private void startPartition(DecoratedKey key, DeletionTime partitionLevelDeletion) throws IOException
    {
        partitionWriter.start(key, partitionLevelDeletion);
        metadataCollector.update(partitionLevelDeletion);

        onStartPartition(key);
    }

    private void addStaticRow(DecoratedKey key, Row row) throws IOException
    {
        guardCollectionSize(metadata(), key, row);

        partitionWriter.addStaticRow(row);
        if (!row.isEmpty())
            Rows.collectStats(row, metadataCollector);

        onStaticRow(row);
    }

    private void addUnfiltered(DecoratedKey key, Unfiltered unfiltered) throws IOException
    {
        if (unfiltered.isRow())
        {
            addRow(key, (Row) unfiltered);
        }
        else
        {
            assert unfiltered.isRangeTombstoneMarker();
            addRangeTomstoneMarker((RangeTombstoneMarker) unfiltered);
        }
    }

    private void addRow(DecoratedKey key, Row row) throws IOException
    {
        guardCollectionSize(metadata(), key, row);

        partitionWriter.addUnfiltered(row);
        metadataCollector.updateClusteringValues(row.clustering());
        Rows.collectStats(row, metadataCollector);

        onRow(row);
    }

    private void addRangeTomstoneMarker(RangeTombstoneMarker marker) throws IOException
    {
        partitionWriter.addUnfiltered(marker);

        metadataCollector.updateClusteringValues(marker.clustering());
        if (marker.isBoundary())
        {
            RangeTombstoneBoundaryMarker bm = (RangeTombstoneBoundaryMarker) marker;
            metadataCollector.update(bm.endDeletionTime());
            metadataCollector.update(bm.startDeletionTime());
        }
        else
        {
            metadataCollector.update(((RangeTombstoneBoundMarker) marker).deletionTime());
        }

        onRangeTombstoneMarker(marker);
    }

    private RowIndexEntry endPartition(DecoratedKey key, DeletionTime partitionLevelDeletion) throws IOException
    {
        long finishResult = partitionWriter.finish();

        long endPosition = dataWriter.position();
        long rowSize = endPosition - partitionWriter.getInitialPosition();
        maybeLogLargePartitionWarning(key, rowSize);
        maybeLogManyTombstonesWarning(key, metadataCollector.totalTombstones);
        metadataCollector.addPartitionSizeInBytes(rowSize);
        metadataCollector.addKey(key.getKey());
        metadataCollector.addCellPerPartitionCount();

        lastWrittenKey = key;
        last = lastWrittenKey;
        if (first == null)
            first = lastWrittenKey;

        if (logger.isTraceEnabled())
            logger.trace("wrote {} at {}", key, endPosition);

        return createRowIndexEntry(key, partitionLevelDeletion, finishResult);
    }

    protected void onStartPartition(DecoratedKey key)
    {
        notifyObservers(o -> o.startPartition(key, partitionWriter.getInitialPosition(), indexWriter.writer.position()));
    }

    protected void onStaticRow(Row row)
    {
        notifyObservers(o -> o.staticRow(row));
    }

    protected void onRow(Row row)
    {
        notifyObservers(o -> o.nextUnfilteredCluster(row));
    }

    protected void onRangeTombstoneMarker(RangeTombstoneMarker marker)
    {
        notifyObservers(o -> o.nextUnfilteredCluster(marker));
    }

    protected RowIndexEntry createRowIndexEntry(DecoratedKey key, DeletionTime partitionLevelDeletion, long finishResult) throws IOException
    {
        // afterAppend() writes the partition key before the first RowIndexEntry - so we have to add it's
        // serialized size to the index-writer position
        long indexFilePosition = ByteBufferUtil.serializedSizeWithShortLength(key.getKey()) + indexWriter.writer.position();

        RowIndexEntry entry = RowIndexEntry.create(partitionWriter.getInitialPosition(),
                                                   indexFilePosition,
                                                   partitionLevelDeletion,
                                                   partitionWriter.getHeaderLength(),
                                                   partitionWriter.getColumnIndexCount(),
                                                   partitionWriter.indexInfoSerializedSize(),
                                                   partitionWriter.indexSamples(),
                                                   partitionWriter.offsets(),
                                                   rowIndexEntrySerializer.indexInfoSerializer());

        indexWriter.append(key, entry, dataWriter.position(), partitionWriter.buffer());
        return entry;
    }

    protected final void notifyObservers(Consumer<SSTableFlushObserver> action)
    {
        if (observers != null && !observers.isEmpty())
            observers.forEach(action);
    }

    private void maybeLogLargePartitionWarning(DecoratedKey key, long rowSize)
    {
        if (rowSize > DatabaseDescriptor.getCompactionLargePartitionWarningThreshold())
        {
            String keyString = metadata().partitionKeyType.getString(key.getKey());
            logger.warn("Writing large partition {}/{}:{} ({}) to sstable {}", metadata.keyspace, metadata.name, keyString, FBUtilities.prettyPrintMemory(rowSize), getFilename());
        }
    }

    private void maybeLogManyTombstonesWarning(DecoratedKey key, int tombstoneCount)
    {
        if (tombstoneCount > DatabaseDescriptor.getCompactionTombstoneWarningThreshold())
        {
            String keyString = metadata().partitionKeyType.getString(key.getKey());
            logger.warn("Writing {} tombstones to {}/{}:{} in sstable {}", tombstoneCount, metadata.keyspace, metadata.name, keyString, getFilename());
        }
    }

    @SuppressWarnings("resource")
    public SSTableReader openEarly()
    {
        // find the max (exclusive) readable key
        IndexSummaryBuilder.ReadableBoundary boundary = indexWriter.getMaxReadable();
        if (boundary == null)
            return null;

        IndexSummary indexSummary = null;
        FileHandle ifile = null;
        FileHandle dfile = null;
        SSTableReader sstable = null;

        try
        {
            StatsMetadata stats = statsMetadata();
            assert boundary.indexLength > 0 && boundary.dataLength > 0;
            // open the reader early
            indexSummary = indexWriter.summary.build(metadata().partitioner, boundary);
            long indexFileLength = new File(descriptor.filenameFor(Component.PRIMARY_INDEX)).length();
            int indexBufferSize = ioOptions.diskOptimizationStrategy.bufferSize(indexFileLength / indexSummary.size());
            ifile = indexWriter.builder.bufferSize(indexBufferSize).withLengthOverride(boundary.indexLength).complete();
            FileHandle.Builder dbuilder = new FileHandle.Builder(descriptor.fileFor(Component.DATA));
            dbuilder.mmapped(ioOptions.defaultDiskAccessMode);
            dbuilder.withChunkCache(chunkCache);
            if (compression)
                dbuilder.withCompressionMetadata(((CompressedSequentialWriter) dataWriter).open(boundary.dataLength));

            EstimatedHistogram partitionSizeHistogram = stats.estimatedPartitionSize;

            if (partitionSizeHistogram.isOverflowed())
            {
                logger.warn("Estimated partition size histogram for '{}' is overflowed ({} values greater than {}). " +
                            "Clearing the overflow bucket to allow for degraded mean and percentile calculations...",
                            descriptor, partitionSizeHistogram.overflowCount(), partitionSizeHistogram.getLargestBucketOffset());

                partitionSizeHistogram.clearOverflow();
            }

            int dataBufferSize = ioOptions.diskOptimizationStrategy.bufferSize(partitionSizeHistogram.percentile(ioOptions.diskOptimizationEstimatePercentile));
            dfile = dbuilder.bufferSize(dataBufferSize).withLengthOverride(boundary.dataLength).complete();

            invalidateCacheAtBoundary(dfile);
            sstable = new BigTableReaderBuilder(descriptor).setComponents(components)
                                                           .setTableMetadataRef(metadata)
                                                           .setIndexFile(ifile)
                                                           .setDataFile(dfile)
                                                           .setIndexSummary(indexSummary)
                                                           .setFilter(indexWriter.bf.sharedCopy())
                                                           .setMaxDataAge(maxDataAge)
                                                           .setStatsMetadata(stats)
                                                           .setOpenReason(SSTableReader.OpenReason.EARLY)
                                                           .setSerializationHeader(header)
                                                           .setFirst(first)
                                                           .setLast(boundary.lastKey)
                                                           .build(true, true);

            return sstable;
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            // If we successfully created our sstable, we can rely on its InstanceTidier to clean things up for us
            if (sstable != null)
                sstable.selfRef().release();
            else
                Throwables.closeAndAddSuppressed(t, indexSummary, ifile, dfile);
            throw t;
        }
    }

    void invalidateCacheAtBoundary(FileHandle dfile)
    {
        if (chunkCache != null) {
            if (lastEarlyOpenLength != 0 && dfile.dataLength() > lastEarlyOpenLength)
                chunkCache.invalidatePosition(dfile, lastEarlyOpenLength);
        }
        lastEarlyOpenLength = dfile.dataLength();
    }

    public SSTableReader openFinalEarly()
    {
        // we must ensure the data is completely flushed to disk
        dataWriter.sync();
        indexWriter.writer.sync();

        return openFinal(SSTableReader.OpenReason.EARLY);
    }

    @SuppressWarnings("resource")
    private SSTableReader openFinal(SSTableReader.OpenReason openReason)
    {
        if (maxDataAge < 0)
            maxDataAge = currentTimeMillis();

        IndexSummary indexSummary = null;
        FileHandle ifile = null;
        FileHandle dfile = null;
        SSTableReader sstable = null;

        try
        {
            StatsMetadata stats = statsMetadata();
            // finalize in-memory state for the reader
            indexSummary = indexWriter.summary.build(metadata().partitioner);
            long indexFileLength = new File(descriptor.filenameFor(Component.PRIMARY_INDEX)).length();
            int dataBufferSize = ioOptions.diskOptimizationStrategy.bufferSize(stats.estimatedPartitionSize.percentile(ioOptions.diskOptimizationEstimatePercentile));
            int indexBufferSize = ioOptions.diskOptimizationStrategy.bufferSize(indexFileLength / indexSummary.size());
            ifile = indexWriter.builder.bufferSize(indexBufferSize).withLengthOverride(NO_LENGTH_OVERRIDE).complete();

            FileHandle.Builder dbuilder = new FileHandle.Builder(descriptor.fileFor(Component.DATA));
            dbuilder.mmapped(ioOptions.defaultDiskAccessMode);
            dbuilder.withChunkCache(chunkCache);
            if (compression)
                dbuilder.withCompressionMetadata(((CompressedSequentialWriter) dataWriter).open(0));
            dfile = dbuilder.bufferSize(dataBufferSize).withLengthOverride(NO_LENGTH_OVERRIDE).complete();
            invalidateCacheAtBoundary(dfile);
            sstable = new BigTableReaderBuilder(descriptor).setComponents(components)
                                                           .setTableMetadataRef(metadata)
                                                           .setIndexFile(ifile)
                                                           .setDataFile(dfile)
                                                           .setIndexSummary(indexSummary)
                                                           .setFilter(indexWriter.bf.sharedCopy())
                                                           .setMaxDataAge(maxDataAge)
                                                           .setStatsMetadata(stats)
                                                           .setOpenReason(openReason)
                                                           .setSerializationHeader(header)
                                                           .setFirst(first)
                                                           .setLast(last)
                                                           .build(true, true);
            return sstable;
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            // If we successfully created our sstable, we can rely on its InstanceTidier to clean things up for us
            if (sstable != null)
                sstable.selfRef().release();
            else
                Stream.of(indexSummary, ifile, dfile).filter(Objects::nonNull).forEach(SharedCloseableImpl::close);
            throw t;
        }
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
            indexWriter.prepareToCommit();

            // write sstable statistics
            dataWriter.prepareToCommit();
            writeMetadata(descriptor, finalizeMetadata());

            // save the table of components
            TOCComponent.appendTOC(descriptor, components);

            if (openResult)
                finalReader = openFinal(SSTableReader.OpenReason.NORMAL);
        }

        protected Throwable doCommit(Throwable accumulate)
        {
            accumulate = dataWriter.commit(accumulate);
            accumulate = indexWriter.commit(accumulate);
            return accumulate;
        }


        protected Throwable doAbort(Throwable accumulate)
        {
            accumulate = indexWriter.abort(accumulate);
            accumulate = dataWriter.abort(accumulate);
            return accumulate;
        }
    }

    private void writeMetadata(Descriptor desc, Map<MetadataType, MetadataComponent> components)
    {
        File file = new File(desc.filenameFor(Component.STATS));
        try (SequentialWriter out = new SequentialWriter(file, ioOptions.writerOptions))
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
        return dataWriter.position();
    }

    public long getOnDiskFilePointer()
    {
        return dataWriter.getOnDiskFilePointer();
    }

    public long getEstimatedOnDiskBytesWritten()
    {
        return dataWriter.getEstimatedOnDiskBytesWritten();
    }

    /**
     * Encapsulates writing the index and filter for an SSTable. The state of this object is not valid until it has been closed.
     */
    class IndexWriter extends AbstractTransactional implements Transactional
    {
        private final SequentialWriter writer;
        public final FileHandle.Builder builder;
        public final IndexSummaryBuilder summary;
        public final IFilter bf;
        private DataPosition mark;

        IndexWriter(long keyCount)
        {
            writer = new SequentialWriter(new File(descriptor.filenameFor(Component.PRIMARY_INDEX)), ioOptions.writerOptions);
            builder = new FileHandle.Builder(descriptor.fileFor(Component.PRIMARY_INDEX)).mmapped(ioOptions.indexDiskAccessMode);
            builder.withChunkCache(chunkCache);
            summary = new IndexSummaryBuilder(keyCount, metadata().params.minIndexInterval, Downsampling.BASE_SAMPLING_LEVEL);
            bf = FilterFactory.getFilter(keyCount, metadata().params.bloomFilterFpChance);
            // register listeners to be alerted when the data files are flushed
            writer.setPostFlushListener(() -> summary.markIndexSynced(writer.getLastFlushOffset()));
            dataWriter.setPostFlushListener(() -> summary.markDataSynced(dataWriter.getLastFlushOffset()));
        }

        // finds the last (-offset) decorated key that can be guaranteed to occur fully in the flushed portion of the index file
        IndexSummaryBuilder.ReadableBoundary getMaxReadable()
        {
            return summary.getLastReadableBoundary();
        }

        public void append(DecoratedKey key, RowIndexEntry indexEntry, long dataEnd, ByteBuffer indexInfo) throws IOException
        {
            bf.add(key);
            long indexStart = writer.position();
            try
            {
                ByteBufferUtil.writeWithShortLength(key.getKey(), writer);
                rowIndexEntrySerializer.serialize(indexEntry, writer, indexInfo);
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, writer.getPath());
            }
            long indexEnd = writer.position();

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
                try
                {
                    FilterComponent.saveOrDeleteCorrupted(descriptor, bf);
                }
                catch (IOException ex)
                {
                    throw new FSWriteError(ex, descriptor.fileFor(Component.FILTER));
                }
            }
        }

        public void mark()
        {
            mark = writer.mark();
        }

        public void resetAndTruncate()
        {
            // we can't un-set the bloom filter addition, but extra keys in there are harmless.
            // we can't reset dbuilder either, but that is the last thing called in afterappend so
            // we assume that if that worked then we won't be trying to reset.
            writer.resetAndTruncate(mark);
        }

        protected void doPrepare()
        {
            flushBf();

            // truncate index file
            long position = writer.position();
            writer.prepareToCommit();
            FileUtils.truncate(writer.getPath(), position);

            // save summary
            summary.prepareToCommit();
            try (IndexSummary indexSummary = summary.build(getPartitioner()))
            {
                new IndexSummaryComponent(indexSummary, first, last).saveOrDeleteCorrupted(descriptor);
            }
            catch (IOException ex)
            {
                logger.warn("Failed to save index summary", ex);
            }
        }

        protected Throwable doCommit(Throwable accumulate)
        {
            return writer.commit(accumulate);
        }

        protected Throwable doAbort(Throwable accumulate)
        {
            return summary.close(writer.abort(accumulate));
        }

        @Override
        protected Throwable doPostCleanup(Throwable accumulate)
        {
            accumulate = summary.close(accumulate);
            accumulate = bf.close(accumulate);
            return accumulate;
        }
    }

    private static Set<Component> components(TableMetadata metadata)
    {
        Set<Component> components = new HashSet<Component>(Arrays.asList(Component.DATA,
                                                                         Component.PRIMARY_INDEX,
                                                                         Component.STATS,
                                                                         Component.SUMMARY,
                                                                         Component.TOC,
                                                                         Component.DIGEST));

        if (metadata.params.bloomFilterFpChance < 1.0)
            components.add(Component.FILTER);

        if (metadata.params.compression.isEnabled())
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

}