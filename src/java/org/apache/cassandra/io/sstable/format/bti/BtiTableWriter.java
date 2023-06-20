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
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
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
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Transactional;
import org.checkerframework.checker.calledmethods.qual.EnsuresCalledMethods;
import org.checkerframework.checker.mustcall.qual.MustCallAlias;
import org.checkerframework.checker.mustcall.qual.Owning;

import static org.apache.cassandra.io.util.FileHandle.Builder.NO_LENGTH_OVERRIDE;
import static org.apache.cassandra.utils.SuppressionConstants.RESOURCE;

/**
 * Writes SSTables in BTI format (see {@link BtiFormat}), which can be read by {@link BtiTableReader}.
 */
@VisibleForTesting
public class BtiTableWriter extends SortedTableWriter<BtiFormatPartitionWriter>
{
    private static final Logger logger = LoggerFactory.getLogger(BtiTableWriter.class);

    private final BtiFormatPartitionWriter partitionWriter;
    private final IndexWriter iwriter;

    public BtiTableWriter(Builder builder, LifecycleNewTracker lifecycleNewTracker, SSTable.Owner owner)
    {
        super(builder, lifecycleNewTracker, owner);
        this.iwriter = builder.getIndexWriter();
        this.partitionWriter = builder.getPartitionWriter();
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

    @Override
    protected TrieIndexEntry createRowIndexEntry(DecoratedKey key, DeletionTime partitionLevelDeletion, long finishResult) throws IOException
    {
        TrieIndexEntry entry = TrieIndexEntry.create(partitionWriter.getInitialPosition(),
                                                     finishResult,
                                                     partitionLevelDeletion,
                                                     partitionWriter.getRowIndexBlockCount());
        iwriter.append(key, entry);
        return entry;
    }

    @SuppressWarnings({"resource", "RedundantSuppression"})
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
            rowIndexFile = iwriter.rowIndexFHBuilder.complete();
            dataFile = openDataFile(isFinal ? NO_LENGTH_OVERRIDE : dataWriter.getLastFlushOffset(), builder.getStatsMetadata());
            filter = iwriter.getFilterCopy();

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
        iwriter.buildPartial(dataLength, partitionIndex ->
        {
            iwriter.rowIndexFHBuilder.withLengthOverride(iwriter.rowIndexWriter.getLastFlushOffset());
            BtiTableReader reader = openInternal(OpenReason.EARLY, false, () -> partitionIndex);
            callWhenReady.accept(reader);
        });
    }

    @Override
    public SSTableReader openFinalEarly()
    {
        // we must ensure the data is completely flushed to disk
        iwriter.complete(); // This will be called by completedPartitionIndex() below too, but we want it done now to
        // ensure outstanding openEarly actions are not triggered.
        dataWriter.sync();
        iwriter.rowIndexWriter.sync();
        // Note: Nothing must be written to any of the files after this point, as the chunk cache could pick up and
        // retain a partially-written page.

        return openFinal(OpenReason.EARLY);
    }

    @Override
    @SuppressWarnings({"resource", "RedundantSuppression"})
    protected SSTableReader openFinal(OpenReason openReason)
    {

        if (maxDataAge < 0)
            maxDataAge = Clock.Global.currentTimeMillis();

        return openInternal(openReason, true, iwriter::completedPartitionIndex);
    }

    @Override
    protected TransactionalProxy txnProxy()
    {
        return new TransactionalProxy(() -> FBUtilities.immutableListWithFilteredNulls(iwriter, dataWriter));
    }

    private class TransactionalProxy extends SortedTableWriter<BtiFormatPartitionWriter>.TransactionalProxy
    {
        public TransactionalProxy(Supplier<ImmutableList<Transactional>> transactionals)
        {
            super(transactionals);
        }

        @Override
        protected Throwable doPostCleanup(Throwable accumulate)
        {
            accumulate = Throwables.close(accumulate, partitionWriter);
            accumulate = super.doPostCleanup(accumulate);
            return accumulate;
        }
    }

    /**
     * Encapsulates writing the index and filter for an SSTable. The state of this object is not valid until it has been closed.
     */
    static class IndexWriter extends SortedTableWriter.AbstractIndexWriter
    {
        final @Owning SequentialWriter rowIndexWriter;
        private final FileHandle.Builder rowIndexFHBuilder;
        private final @Owning SequentialWriter partitionIndexWriter;
        private final FileHandle.Builder partitionIndexFHBuilder;
        private final PartitionIndexBuilder partitionIndex;
        boolean partitionIndexCompleted = false;
        private DataPosition riMark;
        private DataPosition piMark;

        IndexWriter(Builder b)
        {
            super(b);
            rowIndexWriter = new SequentialWriter(descriptor.fileFor(Components.ROW_INDEX), b.getIOOptions().writerOptions);
            rowIndexFHBuilder = IndexComponent.fileBuilder(Components.ROW_INDEX, b).withMmappedRegionsCache(b.getMmappedRegionsCache());
            partitionIndexWriter = new SequentialWriter(descriptor.fileFor(Components.PARTITION_INDEX), b.getIOOptions().writerOptions);
            partitionIndexFHBuilder = IndexComponent.fileBuilder(Components.PARTITION_INDEX, b).withMmappedRegionsCache(b.getMmappedRegionsCache());
            partitionIndex = new PartitionIndexBuilder(partitionIndexWriter, partitionIndexFHBuilder);
            // register listeners to be alerted when the data files are flushed
            partitionIndexWriter.setPostFlushListener(() -> partitionIndex.markPartitionIndexSynced(partitionIndexWriter.getLastFlushOffset()));
            rowIndexWriter.setPostFlushListener(() -> partitionIndex.markRowIndexSynced(rowIndexWriter.getLastFlushOffset()));
            @SuppressWarnings({"resource", "RedundantSuppression"})
            SequentialWriter dataWriter = b.getDataWriter();
            dataWriter.setPostFlushListener(() -> partitionIndex.markDataSynced(dataWriter.getLastFlushOffset()));
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

        @EnsuresCalledMethods(value = {"this.rowIndexWriter", "this.partitionIndexWriter"}, methods = "close")
        protected Throwable doCommit(Throwable accumulate)
        {
            return rowIndexWriter.commit(accumulate);
        }

        @EnsuresCalledMethods(value = {"this.rowIndexWriter", "this.partitionIndexWriter"}, methods = "close")
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

    public static class Builder extends SortedTableWriter.Builder<BtiFormatPartitionWriter, BtiTableWriter, Builder>
    {
        private SequentialWriter dataWriter;
        private BtiFormatPartitionWriter partitionWriter;
        private IndexWriter indexWriter;
        private MmappedRegionsCache mmappedRegionsCache;

        public Builder(Descriptor descriptor)
        {
            super(descriptor);
        }

        // The following getters for the resources opened by buildInternal method can be only used during the lifetime of
        // that method - that is, during the construction of the sstable.

        @Override
        public @MustCallAlias MmappedRegionsCache getMmappedRegionsCache()
        {
            return ensuringInBuildInternalContext(mmappedRegionsCache);
        }

        @Override
        public @MustCallAlias SequentialWriter getDataWriter()
        {
            return ensuringInBuildInternalContext(dataWriter);
        }

        @Override
        public @MustCallAlias BtiFormatPartitionWriter getPartitionWriter()
        {
            return ensuringInBuildInternalContext(partitionWriter);
        }

        public @MustCallAlias IndexWriter getIndexWriter()
        {
            return ensuringInBuildInternalContext(indexWriter);
        }

        private <T> T ensuringInBuildInternalContext(T value)
        {
            Preconditions.checkState(value != null, "This getter can be used only during the lifetime of the sstable constructor. Do not use it directly.");
            return value;
        }

        @Override
        public Builder addDefaultComponents()
        {
            super.addDefaultComponents();

            addComponents(ImmutableSet.of(Components.PARTITION_INDEX, Components.ROW_INDEX));

            return this;
        }

        @Override
        @SuppressWarnings(RESOURCE)
        protected BtiTableWriter buildInternal(LifecycleNewTracker lifecycleNewTracker, Owner owner)
        {
            try
            {
                mmappedRegionsCache = new MmappedRegionsCache();
                dataWriter = DataComponent.buildWriter(descriptor,
                                                       getTableMetadataRef().getLocal(),
                                                       getIOOptions().writerOptions,
                                                       getMetadataCollector(),
                                                       lifecycleNewTracker.opType(),
                                                       getIOOptions().flushCompression);

                indexWriter = new IndexWriter(this);
                partitionWriter = new BtiFormatPartitionWriter(getSerializationHeader(),
                                                               getTableMetadataRef().getLocal().comparator,
                                                               dataWriter,
                                                               indexWriter.rowIndexWriter,
                                                               descriptor.version);


                return new BtiTableWriter(this, lifecycleNewTracker, owner);
            }
            catch (RuntimeException | Error ex)
            {
                Throwables.closeAndAddSuppressed(ex, partitionWriter, indexWriter, dataWriter, mmappedRegionsCache);
                throw ex;
            }
            finally
            {
                partitionWriter = null;
                indexWriter = null;
                dataWriter = null;
                mmappedRegionsCache = null;
            }
        }
    }
}
