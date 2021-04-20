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
package org.apache.cassandra.index.sai.disk;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.ColumnContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.MetadataWriter;
import org.apache.cassandra.index.sai.utils.NamedMemoryLimiter;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;

/**
 * Column index writer that accumulates (on-heap) indexed data from a compacted SSTable as it's being flushed to disk.
 */
@NotThreadSafe
public class SSTableIndexWriter implements ColumnIndexWriter
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableIndexWriter.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);

    public static final int MAX_STRING_TERM_SIZE = Integer.getInteger("cassandra.sai.max_string_term_size_kb", 1) * 1024;
    public static final int MAX_FROZEN_TERM_SIZE = Integer.getInteger("cassandra.sai.max_frozen_term_size_kb", 5) * 1024;
    public static final String TERM_OVERSIZE_MESSAGE =
            "Can't add term of column {} to index for key: {}, term size {} " +
                    "max allowed size {}, use analyzed = true (if not yet set) for that column.";

    private final int nowInSec = FBUtilities.nowInSeconds();
    private final ColumnContext columnContext;
    private final Descriptor descriptor;
    private final IndexComponents indexComponents;
    private final AbstractAnalyzer analyzer;
    private final NamedMemoryLimiter limiter;
    private final int maxTermSize;
    private final BooleanSupplier isIndexValid;

    private boolean aborted = false;

    // segment writer
    private SegmentBuilder currentBuilder;
    private final List<SegmentMetadata> segments = new ArrayList<>();
    private long maxSSTableRowId;

    public SSTableIndexWriter(Descriptor descriptor, ColumnContext columnContext, NamedMemoryLimiter limiter,
                              BooleanSupplier isIndexValid, CompressionParams compressionParams)
    {
        this.columnContext = columnContext;
        this.descriptor = descriptor;
        this.indexComponents = IndexComponents.create(columnContext.getIndexName(), descriptor, compressionParams);
        this.analyzer = columnContext.getAnalyzer();
        this.limiter = limiter;
        this.isIndexValid = isIndexValid;
        this.maxTermSize = columnContext.isFrozen() ? MAX_FROZEN_TERM_SIZE : MAX_STRING_TERM_SIZE;

    }

    @Override
    public void addRow(DecoratedKey rowKey, long sstableRowId, Row row) throws IOException
    {
        if (maybeAbort())
            return;

        if (columnContext.isNonFrozenCollection())
        {
            Iterator<ByteBuffer> valueIterator = columnContext.getValuesOf(row, nowInSec);
            if (valueIterator != null)
            {
                while (valueIterator.hasNext())
                {
                    ByteBuffer value = valueIterator.next();
                    addTerm(TypeUtil.encode(value.duplicate(), columnContext.getValidator()), rowKey, sstableRowId, columnContext.getValidator());
                }
            }
        }
        else
        {
            ByteBuffer value = columnContext.getValueOf(rowKey, row, nowInSec);
            if (value != null)
                addTerm(TypeUtil.encode(value.duplicate(), columnContext.getValidator()), rowKey, sstableRowId, columnContext.getValidator());
        }
        maxSSTableRowId = sstableRowId;
    }

    /**
     * abort current write if index is dropped
     *
     * @return true if current write is aborted.
     */
    private boolean maybeAbort()
    {
        if (aborted)
            return true;

        if (isIndexValid.getAsBoolean())
            return false;

        abort(new RuntimeException(String.format("index %s is dropped", columnContext.getIndexName())));
        return true;
    }

    private void addTerm(ByteBuffer term, DecoratedKey key, long sstableRowId, AbstractType<?> type) throws IOException
    {
        if (term.remaining() >= maxTermSize)
        {
            noSpamLogger.warn(columnContext.logMessage(TERM_OVERSIZE_MESSAGE),
                              columnContext.getColumnName(),
                              columnContext.keyValidator().getString(key.getKey()),
                              FBUtilities.prettyPrintMemory(term.remaining()),
                              FBUtilities.prettyPrintMemory(maxTermSize));
            return;
        }

        if (currentBuilder == null)
        {
            currentBuilder = newSegmentBuilder();
        }
        else if (shouldFlush(sstableRowId))
        {
            flushSegment();
            currentBuilder = newSegmentBuilder();
        }

        if (term.remaining() == 0) return;

        if (!TypeUtil.isLiteral(type))
        {
            limiter.increment(currentBuilder.add(term, key, sstableRowId));
        }
        else
        {
            analyzer.reset(term);
            while (analyzer.hasNext())
            {
                ByteBuffer token = analyzer.next();
                limiter.increment(currentBuilder.add(token, key, sstableRowId));
            }
        }
    }

    private boolean shouldFlush(long sstableRowId)
    {
        // If we've hit the minimum flush size and we've breached the global limit, flush a new segment:
        boolean reachMemoryLimit = limiter.usageExceedsLimit() && currentBuilder.hasReachedMinimumFlushSize();

        if (reachMemoryLimit)
        {
            logger.debug(columnContext.logMessage("Global limit of {} and minimum flush size of {} exceeded. " +
                                            "Current builder usage is {} for {} cells. Global Usage is {}. Flushing..."),
                         FBUtilities.prettyPrintMemory(limiter.limitBytes()),
                         FBUtilities.prettyPrintMemory(currentBuilder.getMinimumFlushBytes()),
                         FBUtilities.prettyPrintMemory(currentBuilder.totalBytesAllocated()),
                         currentBuilder.getRowCount(),
                         FBUtilities.prettyPrintMemory(limiter.currentBytesUsed()));
        }

        return reachMemoryLimit || currentBuilder.exceedsSegmentLimit(sstableRowId);
    }

    private void flushSegment() throws IOException
    {
        long start = System.nanoTime();

        try
        {
            long bytesAllocated = currentBuilder.totalBytesAllocated();

            SegmentMetadata segmentMetadata = currentBuilder.flush(indexComponents);

            long flushMillis = Math.max(1, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

            if (segmentMetadata != null)
            {
                segments.add(segmentMetadata);

                //TODO Need to look at some of these metrics
                double rowCount = segmentMetadata.numRows;
                if (columnContext.getIndexMetrics() != null)
                    columnContext.getIndexMetrics().compactionSegmentCellsPerSecond.update((long)(rowCount / flushMillis * 1000.0));

                double segmentBytes = segmentMetadata.componentMetadatas.indexSize();
                if (columnContext.getIndexMetrics() != null)
                    columnContext.getIndexMetrics().compactionSegmentBytesPerSecond.update((long)(segmentBytes / flushMillis * 1000.0));

                logger.debug(columnContext.logMessage("Flushed segment with {} cells for a total of {} to {} in {} ms."),
                             (long) rowCount, FBUtilities.prettyPrintMemory((long) segmentBytes), indexComponents, flushMillis);
            }

            // Builder memory is released against the limiter at the conclusion of a successful
            // flush. Note that any failure that occurs before this (even in term addition) will
            // actuate this column writer's abort logic from the parent SSTable-level writer, and
            // that abort logic will release the current builder's memory against the limiter.
            long globalBytesUsed = currentBuilder.release(indexComponents);
            currentBuilder = null;
            logger.debug(columnContext.logMessage("Flushing index segment for SSTable {} released {}. Global segment memory usage now at {}."),
                         descriptor, FBUtilities.prettyPrintMemory(bytesAllocated), FBUtilities.prettyPrintMemory(globalBytesUsed));

        }
        catch (Throwable t)
        {
            logger.error(columnContext.logMessage("Failed to build index for SSTable {}."), descriptor, t);
            indexComponents.deleteColumnIndex();

            columnContext.getIndexMetrics().segmentFlushErrors.inc();

            throw t;
        }
    }

    @Override
    public void flush() throws IOException
    {
        if (maybeAbort())
            return;

        boolean emptySegment = currentBuilder == null || currentBuilder.isEmpty();
        logger.debug(columnContext.logMessage("Completing index flush with {}buffered data..."), emptySegment ? "no " : "");

        try
        {
            // parts are present but there is something still in memory, let's flush that inline
            if (!emptySegment)
            {
                flushSegment();
            }

            // Even an empty segment may carry some fixed memory, so remove it:
            if (currentBuilder != null)
            {
                long bytesAllocated = currentBuilder.totalBytesAllocated();
                long globalBytesUsed = currentBuilder.release(indexComponents);
                logger.debug(columnContext.logMessage("Flushing final segment for SSTable {} released {}. Global segment memory usage now at {}."),
                             descriptor, FBUtilities.prettyPrintMemory(bytesAllocated), FBUtilities.prettyPrintMemory(globalBytesUsed));
            }

            compactSegments();

            writeSegmentsMetadata();
            indexComponents.createColumnCompletionMarker();
        }
        finally
        {
            if (columnContext.getIndexMetrics() != null)
            {
                columnContext.getIndexMetrics().segmentsPerCompaction.update(segments.size());
                segments.clear();
                columnContext.getIndexMetrics().compactionCount.inc();
            }
        }
    }

    @Override
    public void abort(Throwable cause)
    {
        aborted = true;

        logger.warn(columnContext.logMessage("Aborting SSTable index flush for {}..."), descriptor, cause);

        // It's possible for the current builder to be unassigned after we flush a final segment.
        if (currentBuilder != null)
        {
            // If an exception is thrown out of any writer operation prior to successful segment
            // flush, we will end up here, and we need to free up builder memory tracked by the limiter:
            long allocated = currentBuilder.totalBytesAllocated();
            long globalBytesUsed = currentBuilder.release(indexComponents);
            logger.debug(columnContext.logMessage("Aborting index writer for SSTable {} released {}. Global segment memory usage now at {}."),
                         descriptor, FBUtilities.prettyPrintMemory(allocated), FBUtilities.prettyPrintMemory(globalBytesUsed));
        }

        indexComponents.deleteColumnIndex();
    }

    private void compactSegments() throws IOException
    {
        if (segments.isEmpty())
            return;

        DecoratedKey minKey = segments.get(0).minKey;
        DecoratedKey maxKey = segments.get(segments.size() - 1).maxKey;

        try (SegmentMerger segmentMerger = SegmentMerger.newSegmentMerger(columnContext.isLiteral());
             SSTableIndex.PerIndexFiles perIndexFiles = new SSTableIndex.PerIndexFiles(indexComponents, columnContext.isLiteral(), true))
        {
            for (final SegmentMetadata segment : segments)
            {
                segmentMerger.addSegment(columnContext, segment, perIndexFiles);
            }
            segments.clear();
            segments.add(segmentMerger.merge(columnContext, indexComponents, minKey, maxKey, maxSSTableRowId));
        }
        finally
        {
            indexComponents.deleteTemporaryComponents();
        }
    }

    private void writeSegmentsMetadata() throws IOException
    {
        if (segments.isEmpty())
            return;

        try (final MetadataWriter writer = new MetadataWriter(indexComponents.createOutput(indexComponents.meta)))
        {
            SegmentMetadata.write(writer, segments, null);
        }
        catch (IOException e)
        {
            abort(e);
            throw e;
        }
    }

    private SegmentBuilder newSegmentBuilder()
    {
        SegmentBuilder builder = TypeUtil.isLiteral(columnContext.getValidator())
                                 ? new SegmentBuilder.RAMStringSegmentBuilder(columnContext.getValidator(), limiter)
                                 : new SegmentBuilder.KDTreeSegmentBuilder(columnContext.getValidator(), limiter, columnContext.getIndexWriterConfig());

        long globalBytesUsed = limiter.increment(builder.totalBytesAllocated());
        logger.debug(columnContext.logMessage("Created new segment builder while flushing SSTable {}. Global segment memory usage now at {}."),
                     descriptor, FBUtilities.prettyPrintMemory(globalBytesUsed));

        return builder;
    }
}
