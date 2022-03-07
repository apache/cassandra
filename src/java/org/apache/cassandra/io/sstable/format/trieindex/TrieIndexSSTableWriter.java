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
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReaderBuilder;
import org.apache.cassandra.io.sstable.format.SortedTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.DataPosition;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.io.util.SequentialWriterOption;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FilterFactory;
import org.apache.cassandra.utils.IFilter;
import org.apache.cassandra.utils.SyncUtil;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Transactional;

@VisibleForTesting
public class TrieIndexSSTableWriter extends SortedTableWriter
{
    private static final Logger logger = LoggerFactory.getLogger(TrieIndexSSTableWriter.class);

    private final PartitionWriter partitionWriter;
    private final IndexWriter iwriter;
    private final TransactionalProxy txnProxy;

    private static final SequentialWriterOption WRITER_OPTION = SequentialWriterOption.newBuilder()
                                                                                      .trickleFsync(DatabaseDescriptor.getTrickleFsync())
                                                                                      .trickleFsyncByteInterval(DatabaseDescriptor.getTrickleFsyncIntervalInKb() * 1024)
                                                                                      .bufferType(BufferType.OFF_HEAP)
                                                                                      .build();

    public TrieIndexSSTableWriter(Descriptor descriptor,
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

        iwriter = new IndexWriter(metadata.get());
        partitionWriter = new PartitionWriter(this.header, metadata().comparator, dataFile, iwriter.rowIndexFile, descriptor.version, this.observers);
        txnProxy = new TransactionalProxy();
    }

    private static Set<Component> components(TableMetadata metadata, Collection<Component> indexComponents)
    {
        Set<Component> components = Sets.newHashSet(Component.DATA,
                                                    Component.PARTITION_INDEX,
                                                    Component.ROW_INDEX,
                                                    Component.STATS,
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
        partitionWriter.reset();

        if (!observers.isEmpty())
            observers.forEach(o -> o.startPartition(key, currentStartPosition));
        partitionWriter.writePartitionHeader(key, partitionLevelDeletion);
        return true;
    }

    public void addUnfiltered(Unfiltered unfiltered) throws IOException
    {
        addUnfilteredMetadata(unfiltered);
        partitionWriter.addUnfiltered(unfiltered);
    }

    public RowIndexEntry endPartition() throws IOException
    {
        endPartitionMetadata();
        long trieRoot = partitionWriter.finish();
        RowIndexEntry entry = TrieIndexEntry.create(currentStartPosition,
                                                    trieRoot,
                                                    currentPartitionLevelDeletion,
                                                    partitionWriter.rowIndexCount);
        iwriter.append(currentKey, entry);
        return entry;
    }

    @SuppressWarnings("resource")
    public boolean openEarly(Consumer<SSTableReader> callWhenReady)
    {
        long dataLength = dataFile.position();

        return iwriter.buildPartial(dataLength, partitionIndex ->
        {
            StatsMetadata stats = statsMetadata();
            FileHandle ifile = iwriter.rowIndexFHBuilder.complete(iwriter.rowIndexFile.getLastFlushOffset());
            if (compression)
                dbuilder.withCompressionMetadata(((CompressedSequentialWriter) dataFile).open(dataFile.getLastFlushOffset()));
            int dataBufferSize = optimizationStrategy.bufferSize(stats.estimatedPartitionSize.percentile(DatabaseDescriptor.getDiskOptimizationEstimatePercentile()));
            FileHandle dfile = dbuilder.bufferSize(dataBufferSize).complete(dataFile.getLastFlushOffset());
            invalidateCacheAtBoundary(dfile);
            SSTableReader sstable = TrieIndexSSTableReader.internalOpen(descriptor,
                                                               components, metadata,
                                                               ifile, dfile, partitionIndex, iwriter.bf.sharedCopy(),
                                                               maxDataAge, stats, SSTableReader.OpenReason.EARLY, header);

            sstable.first = getMinimalKey(partitionIndex.firstKey());
            sstable.last = getMinimalKey(partitionIndex.lastKey());
            sstable.setup(true);
            callWhenReady.accept(sstable);
        });
    }

    public SSTableReader openFinalEarly()
    {
        // we must ensure the data is completely flushed to disk
        iwriter.complete(); // This will be called by completedPartitionIndex() below too, but we want it done now to
                            // ensure outstanding openEarly actions are not triggered.
        dataFile.sync();
        iwriter.rowIndexFile.sync();
        // Note: Nothing must be written to any of the files after this point, as the chunk cache could pick up and
        // retain a partially-written page (see DB-2446).

        return openFinal(SSTableReader.OpenReason.EARLY);
    }

    @SuppressWarnings("resource")
    protected SSTableReader openFinal(SSTableReader.OpenReason openReason)
    {
        if (maxDataAge < 0)
            maxDataAge = System.currentTimeMillis();

        StatsMetadata stats = statsMetadata();
        // finalize in-memory state for the reader
        PartitionIndex partitionIndex = iwriter.completedPartitionIndex();
        FileHandle rowIndexFile = iwriter.rowIndexFHBuilder.complete();
        int dataBufferSize = optimizationStrategy.bufferSize(stats.estimatedPartitionSize.percentile(DatabaseDescriptor.getDiskOptimizationEstimatePercentile()));
        if (compression)
            dbuilder.withCompressionMetadata(((CompressedSequentialWriter) dataFile).open(dataFile.getLastFlushOffset()));
        FileHandle dfile = dbuilder.bufferSize(dataBufferSize).complete();
        invalidateCacheAtBoundary(dfile);
        SSTableReader sstable = TrieIndexSSTableReader.internalOpen(descriptor,
                                                            components,
                                                            this.metadata,
                                                            rowIndexFile,
                                                            dfile,
                                                            partitionIndex,
                                                            iwriter.bf.sharedCopy(),
                                                            maxDataAge,
                                                            stats,
                                                            openReason,
                                                            header);
        sstable.first = getMinimalKey(first);
        sstable.last = getMinimalKey(last);
        sstable.setup(true);
        return sstable;
    }

    protected SortedTableWriter.TransactionalProxy txnProxy()
    {
        return txnProxy;
    }

    class TransactionalProxy extends SortedTableWriter.TransactionalProxy
    {
        // finalise our state on disk, including renaming
        @Override
        protected void doPrepare()
        {
            iwriter.prepareToCommit();
            super.doPrepare();
        }

        @Override
        protected Throwable doCommit(Throwable accumulate)
        {
            accumulate = super.doCommit(accumulate);
            accumulate = iwriter.commit(accumulate);
            return accumulate;
        }

        @Override
        protected Throwable doPostCleanup(Throwable accumulate)
        {
            accumulate = Throwables.close(accumulate, partitionWriter);
            accumulate = super.doPostCleanup(accumulate);
            return accumulate;
        }

        @Override
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
        private final SequentialWriter rowIndexFile;
        public final FileHandle.Builder rowIndexFHBuilder;
        private final SequentialWriter partitionIndexFile;
        public final FileHandle.Builder partitionIndexFHBuilder;
        public final PartitionIndexBuilder partitionIndex;
        public final IFilter bf;
        boolean partitionIndexCompleted = false;
        private DataPosition riMark;
        private DataPosition piMark;

        IndexWriter(TableMetadata table)
        {
            rowIndexFile = new SequentialWriter(descriptor.fileFor(Component.ROW_INDEX), WRITER_OPTION);
            rowIndexFHBuilder = SSTableReaderBuilder.defaultIndexHandleBuilder(descriptor, Component.ROW_INDEX);
            partitionIndexFile = new SequentialWriter(descriptor.fileFor(Component.PARTITION_INDEX), WRITER_OPTION);
            partitionIndexFHBuilder = SSTableReaderBuilder.defaultIndexHandleBuilder(descriptor, Component.PARTITION_INDEX);
            partitionIndex = new PartitionIndexBuilder(partitionIndexFile, partitionIndexFHBuilder);
            bf = FilterFactory.getFilter(keyCount, table.params.bloomFilterFpChance);
            // register listeners to be alerted when the data files are flushed
            partitionIndexFile.setPostFlushListener(() -> partitionIndex.markPartitionIndexSynced(partitionIndexFile.getLastFlushOffset()));
            rowIndexFile.setPostFlushListener(() -> partitionIndex.markRowIndexSynced(rowIndexFile.getLastFlushOffset()));
            dataFile.setPostFlushListener(() -> partitionIndex.markDataSynced(dataFile.getLastFlushOffset()));
        }

        public long append(DecoratedKey key, RowIndexEntry indexEntry) throws IOException
        {
            bf.add(key);
            long position;
            if (indexEntry.isIndexed())
            {
                long indexStart = rowIndexFile.position();
                try
                {
                    ByteBufferUtil.writeWithShortLength(key.getKey(), rowIndexFile);
                    ((TrieIndexEntry) indexEntry).serialize(rowIndexFile, rowIndexFile.position());
                }
                catch (IOException e)
                {
                    throw new FSWriteError(e, rowIndexFile.getFile());
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
            return partitionIndex.buildPartial(callWhenReady, rowIndexFile.position(), dataPosition);
        }

        /**
         * Closes the index and bloomfilter, making the public state of this writer valid for consumption.
         */
        void flushBf()
        {
            if (components.contains(Component.FILTER))
            {
                File path = descriptor.fileFor(Component.FILTER);
                try (SeekableByteChannel fos = Files.newByteChannel(path.toPath(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
                     DataOutputStreamPlus stream = new BufferedDataOutputStreamPlus(fos))
                {
                    // bloom filter
                    BloomFilter.serializer.serialize((BloomFilter) bf, stream);
                    stream.flush();
                    SyncUtil.sync((FileChannel) fos);
                }
                catch (IOException e)
                {
                    throw new FSWriteError(e, path);
                }
            }
        }

        public void mark()
        {
            riMark = rowIndexFile.mark();
            piMark = partitionIndexFile.mark();
        }

        public void resetAndTruncate()
        {
            // we can't un-set the bloom filter addition, but extra keys in there are harmless.
            // we can't reset dbuilder either, but that is the last thing called in afterappend so
            // we assume that if that worked then we won't be trying to reset.
            rowIndexFile.resetAndTruncate(riMark);
            partitionIndexFile.resetAndTruncate(piMark);
        }

        protected void doPrepare()
        {
            flushBf();

            // truncate index file
            rowIndexFile.prepareToCommit();
            rowIndexFHBuilder.withLength(rowIndexFile.getLastFlushOffset());

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
                throw new FSWriteError(e, partitionIndexFile.getFile());
            }
        }

        PartitionIndex completedPartitionIndex()
        {
            complete();
            try
            {
                return PartitionIndex.load(partitionIndexFHBuilder, getPartitioner(), false);
            }
            catch (IOException e)
            {
                throw new FSReadError(e, partitionIndexFile.getFile());
            }
        }

        protected Throwable doCommit(Throwable accumulate)
        {
            return rowIndexFile.commit(accumulate);
        }

        protected Throwable doAbort(Throwable accumulate)
        {
            return rowIndexFile.abort(accumulate);
        }

        @Override
        protected Throwable doPostCleanup(Throwable accumulate)
        {
            return Throwables.close(accumulate, bf, partitionIndex, rowIndexFile, rowIndexFHBuilder, partitionIndexFile, partitionIndexFHBuilder);
        }
    }
}
