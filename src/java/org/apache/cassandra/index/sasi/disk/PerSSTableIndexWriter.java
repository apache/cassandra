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
package org.apache.cassandra.index.sasi.disk;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.index.sasi.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sasi.conf.ColumnIndex;
import org.apache.cassandra.index.sasi.utils.CombinedTermIterator;
import org.apache.cassandra.index.sasi.utils.TypeUtil;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableFlushObserver;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.CountDownLatch;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.concurrent.CountDownLatch.newCountDownLatch;

public class PerSSTableIndexWriter implements SSTableFlushObserver
{
    private static final Logger logger = LoggerFactory.getLogger(PerSSTableIndexWriter.class);

    private static final int POOL_SIZE = 8;
    private static final ExecutorPlus INDEX_FLUSHER_MEMTABLE;
    private static final ExecutorPlus INDEX_FLUSHER_GENERAL;

    static
    {
        INDEX_FLUSHER_GENERAL = executorFactory().withJmxInternal()
                                                 .pooled("SASI-General", POOL_SIZE);

        INDEX_FLUSHER_MEMTABLE = executorFactory().withJmxInternal()
                                                  .pooled("SASI-Memtable", POOL_SIZE);
    }

    private final long nowInSec = FBUtilities.nowInSeconds();

    private final Descriptor descriptor;
    private final OperationType source;

    private final AbstractType<?> keyValidator;

    @VisibleForTesting
    protected final Map<ColumnMetadata, Index> indexes;

    private DecoratedKey currentKey;
    private long currentKeyPosition;
    private boolean isComplete;

    public PerSSTableIndexWriter(AbstractType<?> keyValidator,
                                 Descriptor descriptor,
                                 OperationType source,
                                 Map<ColumnMetadata, ColumnIndex> supportedIndexes)
    {
        this.keyValidator = keyValidator;
        this.descriptor = descriptor;
        this.source = source;
        this.indexes = Maps.newHashMapWithExpectedSize(supportedIndexes.size());
        for (Map.Entry<ColumnMetadata, ColumnIndex> entry : supportedIndexes.entrySet())
            indexes.put(entry.getKey(), newIndex(entry.getValue()));
    }

    @Override
    public void begin()
    {}

    @Override
    public void startPartition(DecoratedKey key, long keyPosition, long KeyPositionForSASI)
    {
        currentKey = key;
        currentKeyPosition = KeyPositionForSASI;
    }

    @Override
    public void staticRow(Row staticRow)
    {
        nextUnfilteredCluster(staticRow);
    }

    @Override
    public void nextUnfilteredCluster(Unfiltered unfiltered)
    {
        if (!unfiltered.isRow())
            return;

        Row row = (Row) unfiltered;

        indexes.forEach((column, index) -> {
            ByteBuffer value = ColumnIndex.getValueOf(column, row, nowInSec);
            if (value == null)
                return;

            if (index == null)
                throw new IllegalArgumentException("No index exists for column " + column.name.toString());

            index.add(value.duplicate(), currentKey, currentKeyPosition);
        });
    }

    @Override
    public void complete()
    {
        if (isComplete)
            return;

        currentKey = null;

        try
        {
            CountDownLatch latch = newCountDownLatch(indexes.size());
            for (Index index : indexes.values())
                index.complete(latch);

            latch.awaitUninterruptibly();
        }
        finally
        {
            indexes.clear();
            isComplete = true;
        }
    }

    public Index getIndex(ColumnMetadata columnDef)
    {
        return indexes.get(columnDef);
    }

    public Descriptor getDescriptor()
    {
        return descriptor;
    }

    @VisibleForTesting
    protected Index newIndex(ColumnIndex columnIndex)
    {
        return new Index(columnIndex);
    }

    @VisibleForTesting
    protected class Index
    {
        @VisibleForTesting
        protected final File outputFile;

        private final ColumnIndex columnIndex;
        private final AbstractAnalyzer analyzer;
        private final long maxMemorySize;

        @VisibleForTesting
        protected final Set<Future<OnDiskIndex>> segments;
        private int segmentNumber = 0;

        private OnDiskIndexBuilder currentBuilder;

        public Index(ColumnIndex columnIndex)
        {
            this.columnIndex = columnIndex;
            this.outputFile = descriptor.fileFor(columnIndex.getComponent());
            this.analyzer = columnIndex.getAnalyzer();
            this.segments = new HashSet<>();
            this.maxMemorySize = maxMemorySize(columnIndex);
            this.currentBuilder = newIndexBuilder();
        }

        public void add(ByteBuffer term, DecoratedKey key, long keyPosition)
        {
            if (term.remaining() == 0)
                return;

            boolean isAdded = false;

            analyzer.reset(term);
            while (analyzer.hasNext())
            {
                ByteBuffer token = analyzer.next();
                int size = token.remaining();

                if (token.remaining() >= OnDiskIndexBuilder.MAX_TERM_SIZE)
                {
                    logger.info("Rejecting value (size {}, maximum {}) for column {} (analyzed {}) at {} SSTable.",
                            FBUtilities.prettyPrintMemory(term.remaining()),
                            FBUtilities.prettyPrintMemory(OnDiskIndexBuilder.MAX_TERM_SIZE),
                            columnIndex.getColumnName(),
                            columnIndex.getMode().isAnalyzed,
                            descriptor);
                    continue;
                }

                if (!TypeUtil.isValid(token, columnIndex.getValidator()))
                {
                    if ((token = TypeUtil.tryUpcast(token, columnIndex.getValidator())) == null)
                    {
                        logger.info("({}) Failed to add {} to index for key: {}, value size was {}, validator is {}.",
                                    outputFile,
                                    columnIndex.getColumnName(),
                                    keyValidator.getString(key.getKey()),
                                    FBUtilities.prettyPrintMemory(size),
                                    columnIndex.getValidator());
                        continue;
                    }
                }

                currentBuilder.add(token, key, keyPosition);
                isAdded = true;
            }

            if (!isAdded || currentBuilder.estimatedMemoryUse() < maxMemorySize)
                return; // non of the generated tokens were added to the index or memory size wasn't reached

            segments.add(getExecutor().submit(scheduleSegmentFlush(false)));
        }

        @VisibleForTesting
        protected Callable<OnDiskIndex> scheduleSegmentFlush(final boolean isFinal)
        {
            final OnDiskIndexBuilder builder = currentBuilder;
            currentBuilder = newIndexBuilder();

            final File segmentFile = file(isFinal);

            return () -> {
                long start = nanoTime();

                try
                {
                    return builder.finish(segmentFile) ? new OnDiskIndex(segmentFile, columnIndex.getValidator(), null) : null;
                }
                catch (Exception | FSError e)
                {
                    logger.error("Failed to build index segment {}", segmentFile, e);
                    return null;
                }
                finally
                {
                    if (!isFinal)
                        logger.info("Flushed index segment {}, took {} ms.", segmentFile, TimeUnit.NANOSECONDS.toMillis(nanoTime() - start));
                }
            };
        }

        public void complete(final CountDownLatch latch)
        {
            logger.info("Scheduling index flush to {}", outputFile);

            getExecutor().submit(() -> {
                long start1 = nanoTime();

                OnDiskIndex[] parts = new OnDiskIndex[segments.size() + 1];

                try
                {
                    // no parts present, build entire index from memory
                    if (segments.isEmpty())
                    {
                        scheduleSegmentFlush(true).call();
                        return;
                    }

                    // parts are present but there is something still in memory, let's flush that inline
                    if (!currentBuilder.isEmpty())
                    {
                        OnDiskIndex last = scheduleSegmentFlush(false).call();
                        segments.add(ImmediateFuture.success(last));
                    }

                    int index = 0;
                    ByteBuffer combinedMin = null, combinedMax = null;

                    for (Future<OnDiskIndex> f : segments)
                    {
                        OnDiskIndex part = f.get();
                        if (part == null)
                            continue;

                        parts[index++] = part;
                        combinedMin = (combinedMin == null || keyValidator.compare(combinedMin, part.minKey()) > 0) ? part.minKey() : combinedMin;
                        combinedMax = (combinedMax == null || keyValidator.compare(combinedMax, part.maxKey()) < 0) ? part.maxKey() : combinedMax;
                    }

                    OnDiskIndexBuilder builder = newIndexBuilder();
                    builder.finish(Pair.create(combinedMin, combinedMax),
                                   outputFile,
                                   new CombinedTermIterator(parts));
                }
                catch (Exception | FSError e)
                {
                    logger.error("Failed to flush index {}.", outputFile, e);
                    outputFile.tryDelete();
                }
                finally
                {
                    logger.info("Index flush to {} took {} ms.", outputFile, TimeUnit.NANOSECONDS.toMillis(nanoTime() - start1));

                    for (int segment = 0; segment < segmentNumber; segment++)
                    {
                        OnDiskIndex part = parts[segment];

                        if (part != null)
                            FileUtils.closeQuietly(part);

                        outputFile.withSuffix("_" + segment).tryDelete();
                    }

                    latch.decrement();
                }
            });
        }

        private ExecutorService getExecutor()
        {
            return source == OperationType.FLUSH ? INDEX_FLUSHER_MEMTABLE : INDEX_FLUSHER_GENERAL;
        }

        private OnDiskIndexBuilder newIndexBuilder()
        {
            return new OnDiskIndexBuilder(keyValidator, columnIndex.getValidator(), columnIndex.getMode().mode);
        }

        public File file(boolean isFinal)
        {
            return isFinal ? outputFile : outputFile.withSuffix("_" + segmentNumber++);
        }
    }

    protected long maxMemorySize(ColumnIndex columnIndex)
    {
        // 1G for memtable and configuration for compaction
        return source == OperationType.FLUSH ? 1073741824L : columnIndex.getMode().maxCompactionFlushMemoryInBytes;
    }

    public int hashCode()
    {
        return descriptor.hashCode();
    }

    public boolean equals(Object o)
    {
        return o instanceof PerSSTableIndexWriter && descriptor.equals(((PerSSTableIndexWriter) o).descriptor);
    }
}
