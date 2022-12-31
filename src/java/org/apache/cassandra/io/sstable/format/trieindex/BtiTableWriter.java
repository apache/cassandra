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
package org.apache.cassandra.io.sstable.format.trieindex;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.IndexComponent;
import org.apache.cassandra.io.sstable.format.AbstractRowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReader.OpenReason;
import org.apache.cassandra.io.sstable.format.SortedTableWriter;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Transactional;

import static org.apache.cassandra.io.util.FileHandle.Builder.NO_LENGTH_OVERRIDE;

@VisibleForTesting
public class BtiTableWriter extends SortedTableWriter<BtiFormatPartitionWriter, TrieIndexEntry>
{
    private static final Logger logger = LoggerFactory.getLogger(BtiTableWriter.class);

    private final BtiFormatPartitionWriter partitionWriter;
    private final IndexWriter iwriter;

    public BtiTableWriter(BtiTableWriterBuilder builder, LifecycleNewTracker lifecycleNewTracker)
    {
        super(builder, lifecycleNewTracker);
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
                                                     partitionWriter.getRowIndexCount());
        iwriter.append(key, entry);
        return entry;
    }

    private BtiTableReader openInternal(OpenReason openReason, boolean isFinal, Supplier<PartitionIndex> partitionIndexSupplier)
    {
        IFilter filter = null;
        FileHandle dataFile = null;
        PartitionIndex partitionIndex = null;
        FileHandle rowIndexFile = null;

        BtiTableReaderBuilder builder = unbuildTo(new BtiTableReaderBuilder(descriptor)).setMaxDataAge(maxDataAge)
                                                                                        .setSerializationHeader(header)
                                                                                        .setOpenReason(openReason)
                                                                                        .setFirst(first)
                                                                                        .setLast(last);

        try
        {
            builder.setStatsMetadata(statsMetadata());

            partitionIndex = partitionIndexSupplier.get();
            rowIndexFile = iwriter.rowIndexFHBuilder.complete();
            dataFile = openDataFile(isFinal ? NO_LENGTH_OVERRIDE : dataWriter.getLastFlushOffset(), builder.getStatsMetadata());
            filter = iwriter.getFilterCopy();

            return builder.setPartitionIndex(partitionIndex)
                          .setRowIndexFile(rowIndexFile)
                          .setDataFile(dataFile)
                          .setFilter(filter)
                          .build(true, true);
        }
        catch (RuntimeException | Error ex)
        {
            JVMStabilityInspector.inspectThrowable(ex);
            Throwables.closeAndAddSuppressed(ex, filter, dataFile, rowIndexFile, partitionIndex);
            throw ex;
        }
    }


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

    public SSTableReader openFinalEarly()
    {
        // we must ensure the data is completely flushed to disk
        iwriter.complete(); // This will be called by completedPartitionIndex() below too, but we want it done now to
        // ensure outstanding openEarly actions are not triggered.
        dataWriter.sync();
        iwriter.rowIndexWriter.sync();
        // Note: Nothing must be written to any of the files after this point, as the chunk cache could pick up and
        // retain a partially-written page (see DB-2446).

        return openFinal(OpenReason.EARLY);
    }

    @SuppressWarnings("resource")
    protected SSTableReader openFinal(OpenReason openReason)
    {

        if (maxDataAge < 0)
            maxDataAge = Clock.Global.currentTimeMillis();

        return openInternal(openReason, true, iwriter::completedPartitionIndex);
    }

    protected TransactionalProxy txnProxy()
    {
        return new TransactionalProxy(() -> FBUtilities.immutableListWithFilteredNulls(iwriter, dataWriter));
    }

    private class TransactionalProxy extends SortedTableWriter<BtiFormatPartitionWriter, TrieIndexEntry>.TransactionalProxy
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
        final SequentialWriter rowIndexWriter;
        private final FileHandle.Builder rowIndexFHBuilder;
        private final SequentialWriter partitionIndexWriter;
        private final FileHandle.Builder partitionIndexFHBuilder;
        private final PartitionIndexBuilder partitionIndex;
        boolean partitionIndexCompleted = false;
        private DataPosition riMark;
        private DataPosition piMark;

        IndexWriter(BtiTableWriterBuilder b)
        {
            super(b);
            rowIndexWriter = new SequentialWriter(descriptor.fileFor(Component.ROW_INDEX), b.getIOOptions().writerOptions);
            rowIndexFHBuilder = IndexComponent.fileBuilder(Component.ROW_INDEX, b);
            partitionIndexWriter = new SequentialWriter(descriptor.fileFor(Component.PARTITION_INDEX), b.getIOOptions().writerOptions);
            partitionIndexFHBuilder = IndexComponent.fileBuilder(Component.PARTITION_INDEX, b);
            partitionIndex = new PartitionIndexBuilder(partitionIndexWriter, partitionIndexFHBuilder);
            // register listeners to be alerted when the data files are flushed
            partitionIndexWriter.setPostFlushListener(() -> partitionIndex.markPartitionIndexSynced(partitionIndexWriter.getLastFlushOffset()));
            rowIndexWriter.setPostFlushListener(() -> partitionIndex.markRowIndexSynced(rowIndexWriter.getLastFlushOffset()));
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
                    ((TrieIndexEntry) indexEntry).serialize(rowIndexWriter, rowIndexWriter.position());
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
            // we can't reset dbuilder either, but that is the last thing called in afterappend so
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
}
