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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.index.sasi.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sasi.conf.ColumnIndex;
import org.apache.cassandra.index.sasi.utils.CombinedTermIterator;
import org.apache.cassandra.index.sasi.utils.TypeUtil;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerSSTableIndexWriter implements SSTableFlushObserver
{
    private static final Logger logger = LoggerFactory.getLogger(PerSSTableIndexWriter.class);

    private static final int POOL_SIZE = 8;
    private static final ThreadPoolExecutor INDEX_FLUSHER_MEMTABLE;
    private static final ThreadPoolExecutor INDEX_FLUSHER_GENERAL;

    static
    {
        INDEX_FLUSHER_GENERAL = new JMXEnabledThreadPoolExecutor(POOL_SIZE, POOL_SIZE, 1, TimeUnit.MINUTES,
                                                                 new LinkedBlockingQueue<>(),
                                                                 new NamedThreadFactory("SASI-General"),
                                                                 "internal");
        INDEX_FLUSHER_GENERAL.allowCoreThreadTimeOut(true);

        INDEX_FLUSHER_MEMTABLE = new JMXEnabledThreadPoolExecutor(POOL_SIZE, POOL_SIZE, 1, TimeUnit.MINUTES,
                                                                  new LinkedBlockingQueue<>(),
                                                                  new NamedThreadFactory("SASI-Memtable"),
                                                                  "internal");
        INDEX_FLUSHER_MEMTABLE.allowCoreThreadTimeOut(true);
    }

    private final int nowInSec = FBUtilities.nowInSeconds();

    private final Descriptor descriptor;
    private final OperationType source;

    private final AbstractType<?> keyValidator;

    @VisibleForTesting
    protected final Map<ColumnDefinition, Index> indexes;

    private DecoratedKey currentKey;
    private long currentKeyPosition;
    private boolean isComplete;

    public PerSSTableIndexWriter(AbstractType<?> keyValidator,
                                 Descriptor descriptor,
                                 OperationType source,
                                 Map<ColumnDefinition, ColumnIndex> supportedIndexes)
    {
        this.keyValidator = keyValidator;
        this.descriptor = descriptor;
        this.source = source;
        this.indexes = new HashMap<>();
        for (Map.Entry<ColumnDefinition, ColumnIndex> entry : supportedIndexes.entrySet())
            indexes.put(entry.getKey(), newIndex(entry.getValue()));
    }

    public void begin()
    {}

    public void startPartition(DecoratedKey key, long curPosition)
    {
        currentKey = key;
        currentKeyPosition = curPosition;
    }

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

    public void complete()
    {
        if (isComplete)
            return;

        currentKey = null;

        try
        {
            CountDownLatch latch = new CountDownLatch(indexes.size());
            for (Index index : indexes.values())
                index.complete(latch);

            Uninterruptibles.awaitUninterruptibly(latch);
        }
        finally
        {
            indexes.clear();
            isComplete = true;
        }
    }

    public Index getIndex(ColumnDefinition columnDef)
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
        protected final String outputFile;

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
            this.outputFile = descriptor.filenameFor(columnIndex.getComponent());
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

            final String segmentFile = filename(isFinal);

            return () -> {
                long start = System.nanoTime();

                try
                {
                    File index = new File(segmentFile);
                    return builder.finish(index) ? new OnDiskIndex(index, columnIndex.getValidator(), null) : null;
                }
                catch (Exception | FSError e)
                {
                    logger.error("Failed to build index segment {}", segmentFile, e);
                    return null;
                }
                finally
                {
                    if (!isFinal)
                        logger.info("Flushed index segment {}, took {} ms.", segmentFile, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
                }
            };
        }

        public void complete(final CountDownLatch latch)
        {
            logger.info("Scheduling index flush to {}", outputFile);

            getExecutor().submit((Runnable) () -> {
                long start1 = System.nanoTime();

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
                        @SuppressWarnings("resource")
                        OnDiskIndex last = scheduleSegmentFlush(false).call();
                        segments.add(Futures.immediateFuture(last));
                    }

                    int index = 0;
                    ByteBuffer combinedMin = null, combinedMax = null;

                    for (Future<OnDiskIndex> f : segments)
                    {
                        @SuppressWarnings("resource")
                        OnDiskIndex part = f.get();
                        if (part == null)
                            continue;

                        parts[index++] = part;
                        combinedMin = (combinedMin == null || keyValidator.compare(combinedMin, part.minKey()) > 0) ? part.minKey() : combinedMin;
                        combinedMax = (combinedMax == null || keyValidator.compare(combinedMax, part.maxKey()) < 0) ? part.maxKey() : combinedMax;
                    }

                    OnDiskIndexBuilder builder = newIndexBuilder();
                    builder.finish(Pair.create(combinedMin, combinedMax),
                                   new File(outputFile),
                                   new CombinedTermIterator(parts));
                }
                catch (Exception | FSError e)
                {
                    logger.error("Failed to flush index {}.", outputFile, e);
                    FileUtils.delete(outputFile);
                }
                finally
                {
                    logger.info("Index flush to {} took {} ms.", outputFile, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start1));

                    for (int segment = 0; segment < segmentNumber; segment++)
                    {
                        @SuppressWarnings("resource")
                        OnDiskIndex part = parts[segment];

                        if (part != null)
                            FileUtils.closeQuietly(part);

                        FileUtils.delete(outputFile + "_" + segment);
                    }

                    latch.countDown();
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

        public String filename(boolean isFinal)
        {
            return outputFile + (isFinal ? "" : "_" + segmentNumber++);
        }
    }

    protected long maxMemorySize(ColumnIndex columnIndex)
    {
        // 1G for memtable and configuration for compaction
        return source == OperationType.FLUSH ? 1073741824L : columnIndex.getMode().maxCompactionFlushMemoryInMb;
    }

    public int hashCode()
    {
        return descriptor.hashCode();
    }

    public boolean equals(Object o)
    {
        return !(o == null || !(o instanceof PerSSTableIndexWriter)) && descriptor.equals(((PerSSTableIndexWriter) o).descriptor);
    }
}
