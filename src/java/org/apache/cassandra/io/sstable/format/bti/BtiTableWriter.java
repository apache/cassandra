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
package org.apache.cassandra.io.sstable.format.bti;

import java.io.IOException;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.DataComponent;
import org.apache.cassandra.io.sstable.format.IndexComponent;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.OpenReason;
import org.apache.cassandra.io.sstable.format.SortedTableWriter;
import org.apache.cassandra.io.sstable.format.bti.BtiFormat.Components;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.MmappedRegionsCache;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Throwables;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.io.util.FileHandle.Builder.NO_LENGTH_OVERRIDE;

/**
 * Writes SSTables in BTI format (see {@link BtiFormat}), which can be read by {@link BtiTableReader}.
 */
@VisibleForTesting
public class BtiTableWriter extends SortedTableWriter<BtiFormatPartitionWriter, BtiTableWriter.IndexWriter>
{
    private static final Logger logger = LoggerFactory.getLogger(BtiTableWriter.class);

    public BtiTableWriter(Builder builder, LifecycleNewTracker lifecycleNewTracker, SSTable.Owner owner)
    {
        super(builder, lifecycleNewTracker, owner);
    }

    @Override
    protected TrieIndexEntry createRowIndexEntry(DecoratedKey key, DeletionTime partitionLevelDeletion, long finishResult) throws IOException
    {
        TrieIndexEntry entry = TrieIndexEntry.create(partitionWriter.getInitialPosition(),
                                                     finishResult,
                                                     partitionLevelDeletion,
                                                     partitionWriter.getRowIndexBlockCount());
        indexWriter.append(key, entry);
        return entry;
    }

    @SuppressWarnings({ "resource", "RedundantSuppression" }) // dataFile is closed along with the reader
    private BtiTableReader openInternal(OpenReason openReason, boolean isFinal, Supplier<PartitionIndex> partitionIndexSupplier)
    {
        IFilter filter = null;
        FileHandle dataFile = null;
        PartitionIndex partitionIndex = null;
        FileHandle rowIndexFile = null;

        BtiTableReader.Builder builder = unbuildTo(new BtiTableReader.Builder(descriptor), true).setMaxDataAge(maxDataAge)
                                                                                                .setSerializationHeader(header)
                                                                                                .setOpenReason(openReason);

        try
        {
            builder.setStatsMetadata(statsMetadata());

            partitionIndex = partitionIndexSupplier.get();
            rowIndexFile = indexWriter.rowIndexFHBuilder.complete();
            dataFile = openDataFile(isFinal ? NO_LENGTH_OVERRIDE : dataWriter.getLastFlushOffset(), builder.getStatsMetadata());
            filter = indexWriter.getFilterCopy();

            return builder.setPartitionIndex(partitionIndex)
                          .setFirst(partitionIndex.firstKey())
                          .setLast(partitionIndex.lastKey())
                          .setRowIndexFile(rowIndexFile)
                          .setDataFile(dataFile)
                          .setFilter(filter)
                          .build(owner().orElse(null), true, true);
        }
        catch (RuntimeException | Error ex)
        {
            JVMStabilityInspector.inspectThrowable(ex);
            Throwables.closeNonNullAndAddSuppressed(ex, filter, dataFile, rowIndexFile, partitionIndex);
            throw ex;
        }
    }

    @Override
    public void openEarly(Consumer<SSTableReader> callWhenReady)
    {
        long dataLength = dataWriter.position();
        indexWriter.buildPartial(dataLength, partitionIndex ->
        {
            indexWriter.rowIndexFHBuilder.withLengthOverride(indexWriter.rowIndexWriter.getLastFlushOffset());
            BtiTableReader reader = openInternal(OpenReason.EARLY, false, () -> partitionIndex);
            callWhenReady.accept(reader);
        });
    }

    @Override
    public SSTableReader openFinalEarly()
    {
        // we must ensure the data is completely flushed to disk
        indexWriter.complete(); // This will be called by completedPartitionIndex() below too, but we want it done now to
        // ensure outstanding openEarly actions are not triggered.
        dataWriter.sync();
        indexWriter.rowIndexWriter.sync();
        // Note: Nothing must be written to any of the files after this point, as the chunk cache could pick up and
        // retain a partially-written page.

        return openFinal(OpenReason.EARLY);
    }

    @Override
    protected SSTableReader openFinal(OpenReason openReason)
    {

        if (maxDataAge < 0)
            maxDataAge = Clock.Global.currentTimeMillis();

        return openInternal(openReason, true, indexWriter::completedPartitionIndex);
    }

    /**
     * Encapsulates writing the index and filter for an SSTable. The state of this object is not valid until it has been closed.
     */
    protected static class IndexWriter extends SortedTableWriter.AbstractIndexWriter
    {
        final SequentialWriter rowIndexWriter;
        private final FileHandle.Builder rowIndexFHBuilder;
        private final SequentialWriter partitionIndexWriter;
        private final FileHandle.Builder partitionIndexFHBuilder;
        private final PartitionIndexBuilder partitionIndex;
        boolean partitionIndexCompleted = false;
        private DataPosition riMark;
        private DataPosition piMark;

        IndexWriter(Builder b, SequentialWriter dataWriter)
        {
            super(b);
            rowIndexWriter = new SequentialWriter(descriptor.fileFor(Components.ROW_INDEX), b.getIOOptions().writerOptions);
            rowIndexFHBuilder = IndexComponent.fileBuilder(Components.ROW_INDEX, b).withMmappedRegionsCache(b.getMmappedRegionsCache());
            partitionIndexWriter = new SequentialWriter(descriptor.fileFor(Components.PARTITION_INDEX), b.getIOOptions().writerOptions);
            partitionIndexFHBuilder = IndexComponent.fileBuilder(Components.PARTITION_INDEX, b).withMmappedRegionsCache(b.getMmappedRegionsCache());
            partitionIndex = new PartitionIndexBuilder(partitionIndexWriter, partitionIndexFHBuilder);
            // register listeners to be alerted when the data files are flushed
            partitionIndexWriter.setPostFlushListener(partitionIndex::markPartitionIndexSynced);
            rowIndexWriter.setPostFlushListener(partitionIndex::markRowIndexSynced);
            dataWriter.setPostFlushListener(partitionIndex::markDataSynced);
        }

        public long append(DecoratedKey key, AbstractRowIndexEntry indexEntry) throws IOException
        {
            bf.add(key);
            long position;
            if (indexEntry.isIndexed())
            {
                long indexStart = rowIndexWriter.position();
                try
                {
                    ByteBufferUtil.writeWithShortLength(key.getKey(), rowIndexWriter);
                    ((TrieIndexEntry) indexEntry).serialize(rowIndexWriter, rowIndexWriter.position(), descriptor.version);
                }
                catch (IOException e)
                {
                    throw new FSWriteError(e, rowIndexWriter.getFile());
                }

                if (logger.isTraceEnabled())
                    logger.trace("wrote index entry: {} at {}", indexEntry, indexStart);
                position = indexStart;
            }
            else
            {
                // Write data position directly in trie.
                position = ~indexEntry.position;
            }
            partitionIndex.addEntry(key, position);
            return position;
        }

        public boolean buildPartial(long dataPosition, Consumer<PartitionIndex> callWhenReady)
        {
            return partitionIndex.buildPartial(callWhenReady, rowIndexWriter.position(), dataPosition);
        }

        public void mark()
        {
            riMark = rowIndexWriter.mark();
            piMark = partitionIndexWriter.mark();
        }

        public void resetAndTruncate()
        {
            // we can't un-set the bloom filter addition, but extra keys in there are harmless.
            // we can't reset dbuilder either, but that is the last thing called in after append, so
            // we assume that if that worked then we won't be trying to reset.
            rowIndexWriter.resetAndTruncate(riMark);
            partitionIndexWriter.resetAndTruncate(piMark);
        }

        protected void doPrepare()
        {
            flushBf();

            // truncate index file
            rowIndexWriter.prepareToCommit();
            rowIndexFHBuilder.withLengthOverride(rowIndexWriter.getLastFlushOffset());

            complete();
        }

        void complete() throws FSWriteError
        {
            if (partitionIndexCompleted)
                return;

            try
            {
                partitionIndex.complete();
                partitionIndexCompleted = true;
            }
            catch (IOException e)
            {
                throw new FSWriteError(e, partitionIndexWriter.getFile());
            }
        }

        PartitionIndex completedPartitionIndex()
        {
            complete();
            rowIndexFHBuilder.withLengthOverride(0);
            partitionIndexFHBuilder.withLengthOverride(0);
            try
            {
                return PartitionIndex.load(partitionIndexFHBuilder, metadata.getLocal().partitioner, false);
            }
            catch (IOException e)
            {
                throw new FSReadError(e, partitionIndexWriter.getFile());
            }
        }

        protected Throwable doCommit(Throwable accumulate)
        {
            return rowIndexWriter.commit(accumulate);
        }

        protected Throwable doAbort(Throwable accumulate)
        {
            return rowIndexWriter.abort(accumulate);
        }

        @Override
        protected Throwable doPostCleanup(Throwable accumulate)
        {
            return Throwables.close(accumulate, bf, partitionIndex, rowIndexWriter, partitionIndexWriter);
        }
    }

    public static class Builder extends SortedTableWriter.Builder<BtiFormatPartitionWriter, IndexWriter, BtiTableWriter, Builder>
    {
        private MmappedRegionsCache mmappedRegionsCache;
        private OperationType operationType;

        private boolean dataWriterOpened;
        private boolean partitionWriterOpened;
        private boolean indexWriterOpened;

        public Builder(Descriptor descriptor)
        {
            super(descriptor);
        }

        @Override
        public Builder addDefaultComponents(Collection<Index.Group> indexGroups)
        {
            super.addDefaultComponents(indexGroups);

            addComponents(ImmutableSet.of(Components.PARTITION_INDEX, Components.ROW_INDEX));

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

            return DataComponent.buildWriter(descriptor,
                                             getTableMetadataRef().getLocal(),
                                             getIOOptions().writerOptions,
                                             getMetadataCollector(),
                                             ensuringInBuildInternalContext(operationType),
                                             getIOOptions().flushCompression);
        }

        @Override
        protected IndexWriter openIndexWriter(SequentialWriter dataWriter)
        {
            checkNotNull(dataWriter);
            checkState(!indexWriterOpened, "Index writer has been already opened.");

            IndexWriter indexWriter = new IndexWriter(this, dataWriter);
            indexWriterOpened = true;
            return indexWriter;
        }

        @Override
        protected BtiFormatPartitionWriter openPartitionWriter(SequentialWriter dataWriter, IndexWriter indexWriter)
        {
            checkNotNull(dataWriter);
            checkNotNull(indexWriter);
            checkState(!partitionWriterOpened, "Partition writer has been already opened.");

            BtiFormatPartitionWriter partitionWriter = new BtiFormatPartitionWriter(getSerializationHeader(),
                                                                                    getTableMetadataRef().getLocal().comparator,
                                                                                    dataWriter,
                                                                                    indexWriter.rowIndexWriter,
                                                                                    descriptor.version);
            partitionWriterOpened = true;
            return partitionWriter;
        }

        private <T> T ensuringInBuildInternalContext(T value)
        {
            checkState(value != null, "The requested resource has not been initialized yet.");
            return value;
        }

        @Override
        protected BtiTableWriter buildInternal(LifecycleNewTracker lifecycleNewTracker, Owner owner)
        {
            try
            {
                this.mmappedRegionsCache = new MmappedRegionsCache();
                this.operationType = lifecycleNewTracker.opType();

                return new BtiTableWriter(this, lifecycleNewTracker, owner);
            }
            catch (RuntimeException | Error ex)
            {
                Throwables.closeAndAddSuppressed(ex, mmappedRegionsCache);
                throw ex;
            }
            finally
            {
                mmappedRegionsCache = null;
                partitionWriterOpened = false;
                indexWriterOpened = false;
                dataWriterOpened = false;
            }
        }
    }
}
