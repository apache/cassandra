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
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.Downsampling;
import org.apache.cassandra.io.sstable.IndexSummary;
import org.apache.cassandra.io.sstable.IndexSummaryBuilder;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReaderBuilder;
import org.apache.cassandra.io.sstable.format.SortedTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileOutputStreamPlus;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.concurrent.Transactional;

public class BigTableWriter extends SortedTableWriter
{
    protected static final Logger logger = LoggerFactory.getLogger(BigTableWriter.class);

    protected final BigTableRowIndexEntry.IndexSerializer<IndexInfo> rowIndexEntrySerializer;
    private final ColumnIndex columnIndexWriter;
    private final IndexWriter iwriter;
    private final TransactionalProxy txnProxy;

    private static final SequentialWriterOption WRITER_OPTION = SequentialWriterOption.newBuilder()
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
        super(descriptor, components(metadata.getLocal(), indexComponents), lifecycleNewTracker, WRITER_OPTION, keyCount, repairedAt, pendingRepair, isTransient, metadata, metadataCollector, header, observers);
        iwriter = new IndexWriter(keyCount);

        this.rowIndexEntrySerializer = new BigTableRowIndexEntry.Serializer(descriptor.version, header);
        columnIndexWriter = new ColumnIndex(this.header, dataFile, descriptor.version, this.observers, rowIndexEntrySerializer.indexInfoSerializer());
        txnProxy = new TransactionalProxy();
    }

    private static Set<Component> components(TableMetadata metadata, Collection<Component> indexComponents)
    {
        Set<Component> components = Sets.newHashSet(Component.DATA,
                                                    Component.PRIMARY_INDEX,
                                                    Component.STATS,
                                                    Component.SUMMARY,
                                                    Component.TOC,
                                                    Component.DIGEST);

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

        components.addAll(indexComponents);

        return components;
    }

    @Override
    public void mark()
    {
        super.mark();
        iwriter.mark();
    }

    @Override
    public void resetAndTruncate()
    {
        super.resetAndTruncate();
        iwriter.resetAndTruncate();
    }

    public boolean startPartition(DecoratedKey key, DeletionTime partitionLevelDeletion) throws IOException
    {
        if (!startPartitionMetadata(key, partitionLevelDeletion))
            return false;

        // Reuse the writer for each row
        columnIndexWriter.reset();

        if (!observers.isEmpty())
            observers.forEach(o -> o.startPartition(key, iwriter.indexFile.position()));
        columnIndexWriter.writePartitionHeader(key, partitionLevelDeletion);
        return true;
    }

    public void addUnfiltered(Unfiltered unfiltered) throws IOException
    {
        addUnfilteredMetadata(unfiltered);
        columnIndexWriter.addUnfiltered(unfiltered);
    }

    public BigTableRowIndexEntry endPartition() throws IOException
    {
        endPartitionMetadata();

        columnIndexWriter.endPartition();
        // afterAppend() writes the partition key before the first RowIndexEntry - so we have to add it's
        // serialized size to the index-writer position
        long indexFilePosition = ByteBufferUtil.serializedSizeWithShortLength(currentKey.getKey()) + iwriter.indexFile.position();

        BigTableRowIndexEntry entry = BigTableRowIndexEntry.create(currentStartPosition, indexFilePosition,
                                                                   currentPartitionLevelDeletion,
                                                                   columnIndexWriter.headerLength,
                                                                   columnIndexWriter.columnIndexCount,
                                                                   columnIndexWriter.indexInfoSerializedSize(),
                                                                   columnIndexWriter.indexSamples(),
                                                                   columnIndexWriter.offsets(),
                                                                   rowIndexEntrySerializer.indexInfoSerializer());

        long endPosition = dataFile.position();
        iwriter.append(currentKey, entry, endPosition, columnIndexWriter.buffer());
        return entry;
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

    public SSTableReader openFinalEarly()
    {
        // we must ensure the data is completely flushed to disk
        dataFile.sync();
        iwriter.indexFile.sync();

        return openFinal(SSTableReader.OpenReason.EARLY);
    }

    @SuppressWarnings("resource")
    protected SSTableReader openFinal(SSTableReader.OpenReason openReason)
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

    protected SortedTableWriter.TransactionalProxy txnProxy()
    {
        return txnProxy;
    }

    class TransactionalProxy extends SortedTableWriter.TransactionalProxy
    {
        // finalise our state on disk, including renaming
        protected void doPrepare()
        {
            iwriter.prepareToCommit();
            super.doPrepare();
        }

        protected Throwable doCommit(Throwable accumulate)
        {
            accumulate = super.doCommit(accumulate);
            accumulate = iwriter.commit(accumulate);
            return accumulate;
        }

        protected Throwable doAbort(Throwable accumulate)
        {
            accumulate = iwriter.abort(accumulate);
            accumulate = super.doAbort(accumulate);
            return accumulate;
        }
    }

    protected SequentialWriterOption writerOption()
    {
        return WRITER_OPTION;
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
            indexFile = new SequentialWriter(descriptor.fileFor(Component.PRIMARY_INDEX), WRITER_OPTION);
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
