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
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.Downsampling;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.DataComponent;
import org.apache.cassandra.io.sstable.format.IndexComponent;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SortedTableWriter;
import org.apache.cassandra.io.sstable.format.big.BigFormat.Components;
import org.apache.cassandra.io.sstable.indexsummary.IndexSummary;
import org.apache.cassandra.io.sstable.indexsummary.IndexSummaryBuilder;
import org.apache.cassandra.io.sstable.keycache.KeyCache;
import org.apache.cassandra.io.sstable.keycache.KeyCacheSupport;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.MmappedRegionsCache;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.service.CacheService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Throwables;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.io.util.FileHandle.Builder.NO_LENGTH_OVERRIDE;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

public class BigTableWriter extends SortedTableWriter<BigFormatPartitionWriter, BigTableWriter.IndexWriter>
{
    private static final Logger logger = LoggerFactory.getLogger(BigTableWriter.class);

    private final RowIndexEntry.IndexSerializer rowIndexEntrySerializer;
    private final Map<DecoratedKey, AbstractRowIndexEntry> cachedKeys = new HashMap<>();
    private final boolean shouldMigrateKeyCache;

    public BigTableWriter(Builder builder, LifecycleNewTracker lifecycleNewTracker, SSTable.Owner owner)
    {
        super(builder, lifecycleNewTracker, owner);

        this.rowIndexEntrySerializer = builder.getRowIndexEntrySerializer();
        checkNotNull(this.rowIndexEntrySerializer);

        this.shouldMigrateKeyCache = DatabaseDescriptor.shouldMigrateKeycacheOnCompaction()
                                     && lifecycleNewTracker instanceof ILifecycleTransaction
                                     && !((ILifecycleTransaction) lifecycleNewTracker).isOffline();
    }

    @Override
    protected void onStartPartition(DecoratedKey key)
    {
        notifyObservers(o -> o.startPartition(key, partitionWriter.getInitialPosition(), indexWriter.writer.position()));
    }

    @Override
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
                                                   rowIndexEntrySerializer.indexInfoSerializer(),
                                                   descriptor.version);

        indexWriter.append(key, entry, dataWriter.position(), partitionWriter.buffer());

        if (shouldMigrateKeyCache)
        {
            for (SSTableReader reader : ((ILifecycleTransaction) lifecycleNewTracker).originals())
            {
                if (reader instanceof KeyCacheSupport<?> && ((KeyCacheSupport<?>) reader).getCachedPosition(key, false) != null)
                {
                    cachedKeys.put(key, entry);
                    break;
                }
            }
        }

        return entry;
    }

    private BigTableReader openInternal(IndexSummaryBuilder.ReadableBoundary boundary, SSTableReader.OpenReason openReason)
    {
        assert boundary == null || (boundary.indexLength > 0 && boundary.dataLength > 0);

        IFilter filter = null;
        IndexSummary indexSummary = null;
        FileHandle dataFile = null;
        FileHandle indexFile = null;

        BigTableReader.Builder builder = unbuildTo(new BigTableReader.Builder(descriptor), true).setMaxDataAge(maxDataAge)
                                                                                                .setSerializationHeader(header)
                                                                                                .setOpenReason(openReason)
                                                                                                .setFirst(first)
                                                                                                .setLast(boundary != null ? boundary.lastKey : last);

        BigTableReader reader;
        try
        {

            builder.setStatsMetadata(statsMetadata());

            EstimatedHistogram partitionSizeHistogram = builder.getStatsMetadata().estimatedPartitionSize;
            if (boundary != null)
            {
                if (partitionSizeHistogram.isOverflowed())
                {
                    logger.warn("Estimated partition size histogram for '{}' is overflowed ({} values greater than {}). " +
                                "Clearing the overflow bucket to allow for degraded mean and percentile calculations...",
                                descriptor, partitionSizeHistogram.overflowCount(), partitionSizeHistogram.getLargestBucketOffset());
                    partitionSizeHistogram.clearOverflow();
                }
            }

            filter = indexWriter.getFilterCopy();
            builder.setFilter(filter);
            indexSummary = indexWriter.summary.build(metadata().partitioner, boundary);
            builder.setIndexSummary(indexSummary);

            long indexFileLength = descriptor.fileFor(Components.PRIMARY_INDEX).length();
            int indexBufferSize = ioOptions.diskOptimizationStrategy.bufferSize(indexFileLength / builder.getIndexSummary().size());
            FileHandle.Builder indexFileBuilder = indexWriter.builder;
            indexFile = indexFileBuilder.bufferSize(indexBufferSize)
                                        .withLengthOverride(boundary != null ? boundary.indexLength : NO_LENGTH_OVERRIDE)
                                        .complete();
            builder.setIndexFile(indexFile);
            dataFile = openDataFile(boundary != null ? boundary.dataLength : NO_LENGTH_OVERRIDE, builder.getStatsMetadata());
            builder.setDataFile(dataFile);
            builder.setKeyCache(metadata().params.caching.cacheKeys() ? new KeyCache(CacheService.instance.keyCache) : KeyCache.NO_CACHE);

            reader = builder.build(owner().orElse(null), true, true);
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            Throwables.closeNonNullAndAddSuppressed(t, dataFile, indexFile, indexSummary, filter);
            throw t;
        }

        try
        {
            for (Map.Entry<DecoratedKey, AbstractRowIndexEntry> cachedKey : cachedKeys.entrySet())
                reader.cacheKey(cachedKey.getKey(), cachedKey.getValue());

            // clearing the collected cache keys so that we will not have to cache them again when opening partial or
            // final later - cache key refer only to the descriptor, not to the particular SSTableReader instance.
            cachedKeys.clear();
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
        }

        return reader;
    }

    @Override
    public void openEarly(Consumer<SSTableReader> doWhenReady)
    {
        // find the max (exclusive) readable key
        IndexSummaryBuilder.ReadableBoundary boundary = indexWriter.getMaxReadable();
        if (boundary == null)
            return;

        doWhenReady.accept(openInternal(boundary, SSTableReader.OpenReason.EARLY));
    }

    @Override
    public SSTableReader openFinalEarly()
    {
        // we must ensure the data is completely flushed to disk
        dataWriter.sync();
        indexWriter.writer.sync();

        return openFinal(SSTableReader.OpenReason.EARLY);
    }

    @Override
    public SSTableReader openFinal(SSTableReader.OpenReason openReason)
    {
        if (maxDataAge < 0)
            maxDataAge = currentTimeMillis();

        return openInternal(null, openReason);
    }

    /**
     * Encapsulates writing the index and filter for an SSTable. The state of this object is not valid until it has been closed.
     */
    protected static class IndexWriter extends SortedTableWriter.AbstractIndexWriter
    {
        private final RowIndexEntry.IndexSerializer rowIndexEntrySerializer;

        final SequentialWriter writer;
        final FileHandle.Builder builder;
        final IndexSummaryBuilder summary;
        private DataPosition mark;
        private DecoratedKey first;
        private DecoratedKey last;

        protected IndexWriter(Builder b, SequentialWriter dataWriter)
        {
            super(b);
            this.rowIndexEntrySerializer = b.getRowIndexEntrySerializer();
            writer = new SequentialWriter(b.descriptor.fileFor(Components.PRIMARY_INDEX), b.getIOOptions().writerOptions);
            builder = IndexComponent.fileBuilder(Components.PRIMARY_INDEX, b).withMmappedRegionsCache(b.getMmappedRegionsCache());
            summary = new IndexSummaryBuilder(b.getKeyCount(), b.getTableMetadataRef().getLocal().params.minIndexInterval, Downsampling.BASE_SAMPLING_LEVEL);
            // register listeners to be alerted when the data files are flushed
            writer.setPostFlushListener(summary::markIndexSynced);
            dataWriter.setPostFlushListener(summary::markDataSynced);
        }

        // finds the last (-offset) decorated key that can be guaranteed to occur fully in the flushed portion of the index file
        IndexSummaryBuilder.ReadableBoundary getMaxReadable()
        {
            return summary.getLastReadableBoundary();
        }

        public void append(DecoratedKey key, RowIndexEntry indexEntry, long dataEnd, ByteBuffer indexInfo) throws IOException
        {
            bf.add(key);
            if (first == null)
                first = key;
            last = key;

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

        @Override
        public void mark()
        {
            mark = writer.mark();
        }

        @Override
        public void resetAndTruncate()
        {
            // we can't un-set the bloom filter addition, but extra keys in there are harmless.
            // we can't reset dbuilder either, but that is the last thing called in afterappend, so
            // we assume that if that worked then we won't be trying to reset.
            writer.resetAndTruncate(mark);
        }

        protected void doPrepare()
        {
            checkNotNull(first);
            checkNotNull(last);

            super.doPrepare();

            // truncate index file
            long position = writer.position();
            writer.prepareToCommit();
            FileUtils.truncate(writer.getPath(), position);

            // save summary
            summary.prepareToCommit();
            try (IndexSummary indexSummary = summary.build(metadata.getLocal().partitioner))
            {
                new IndexSummaryComponent(indexSummary, first, last).save(descriptor.fileFor(Components.SUMMARY), true);
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
            accumulate = super.doPostCleanup(accumulate);
            accumulate = summary.close(accumulate);
            return accumulate;
        }
    }

    public static class Builder extends SortedTableWriter.Builder<BigFormatPartitionWriter, IndexWriter, BigTableWriter, Builder>
    {
        private RowIndexEntry.IndexSerializer rowIndexEntrySerializer;
        private MmappedRegionsCache mmappedRegionsCache;
        private OperationType operationType;

        // Writers are expected to be opened only once during construction of the sstable. The following flags are used
        // to ensure that.
        private boolean indexWriterOpened;
        private boolean dataWriterOpened;
        private boolean partitionWriterOpened;

        public Builder(Descriptor descriptor)
        {
            super(descriptor);
        }

        @Override
        public Builder addDefaultComponents(Collection<Index.Group> indexGroups)
        {
            super.addDefaultComponents(indexGroups);

            addComponents(ImmutableSet.of(Components.PRIMARY_INDEX, Components.SUMMARY));

            return this;
        }

        // The following getters for the resources opened by buildInternal method can be only used during the lifetime of
        // that method - that is, during the construction of the sstable.

        @Override
        public MmappedRegionsCache getMmappedRegionsCache()
        {
            return ensuringInBuildInternalContext(mmappedRegionsCache);
        }

        @Override
        protected SequentialWriter openDataWriter()
        {
            checkState(!dataWriterOpened, "Data writer has been already opened.");

            SequentialWriter dataWriter = DataComponent.buildWriter(descriptor,
                                                                    getTableMetadataRef().getLocal(),
                                                                    getIOOptions().writerOptions,
                                                                    getMetadataCollector(),
                                                                    ensuringInBuildInternalContext(operationType),
                                                                    getIOOptions().flushCompression);
            this.dataWriterOpened = true;
            return dataWriter;
        }

        @Override
        protected IndexWriter openIndexWriter(SequentialWriter dataWriter)
        {
            checkNotNull(dataWriter);
            checkState(!indexWriterOpened, "Index writer has been already opened.");

            IndexWriter indexWriter = new IndexWriter(this, dataWriter);
            this.indexWriterOpened = true;
            return indexWriter;
        }

        @Override
        protected BigFormatPartitionWriter openPartitionWriter(SequentialWriter dataWriter, IndexWriter indexWriter)
        {
            checkNotNull(dataWriter);
            checkNotNull(indexWriter);
            checkState(!partitionWriterOpened, "Partition writer has been already opened.");

            BigFormatPartitionWriter partitionWriter = new BigFormatPartitionWriter(getSerializationHeader(), dataWriter, descriptor.version, getRowIndexEntrySerializer().indexInfoSerializer());
            this.partitionWriterOpened = true;
            return partitionWriter;
        }

        RowIndexEntry.IndexSerializer getRowIndexEntrySerializer()
        {
            return ensuringInBuildInternalContext(rowIndexEntrySerializer);
        }

        private <T> T ensuringInBuildInternalContext(T value)
        {
            checkState(value != null, "The requested resource has not been initialized yet.");
            return value;
        }

        @Override
        protected BigTableWriter buildInternal(LifecycleNewTracker lifecycleNewTracker, Owner owner)
        {
            try
            {
                this.operationType = lifecycleNewTracker.opType();
                this.mmappedRegionsCache = new MmappedRegionsCache();
                this.rowIndexEntrySerializer = new RowIndexEntry.Serializer(descriptor.version, getSerializationHeader(), owner != null ? owner.getMetrics() : null);
                return new BigTableWriter(this, lifecycleNewTracker, owner);
            }
            catch (RuntimeException | Error ex)
            {
                Throwables.closeNonNullAndAddSuppressed(ex, mmappedRegionsCache);
                throw ex;
            }
            finally
            {
                this.rowIndexEntrySerializer = null;
                this.mmappedRegionsCache = null;
                this.operationType = null;
                this.partitionWriterOpened = false;
                this.indexWriterOpened = false;
                this.dataWriterOpened = false;
            }
        }
    }
}
