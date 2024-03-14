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
package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sai.disk.PerColumnIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentBuilder;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.utils.NamedMemoryLimiter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Column index writer that accumulates (on-heap) indexed data from a compacted SSTable as it's being flushed to disk.
 */
@NotThreadSafe
public class SSTableIndexWriter implements PerColumnIndexWriter
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableIndexWriter.class);

    private final IndexDescriptor indexDescriptor;
    private final StorageAttachedIndex index;
    private final long nowInSec = FBUtilities.nowInSeconds();
    private final AbstractAnalyzer analyzer;
    private final NamedMemoryLimiter limiter;
    private final BooleanSupplier isIndexValid;
    private final List<SegmentMetadata> segments = new ArrayList<>();

    private boolean aborted = false;
    private SegmentBuilder currentBuilder;

    public SSTableIndexWriter(IndexDescriptor indexDescriptor,
                              StorageAttachedIndex index,
                              NamedMemoryLimiter limiter,
                              BooleanSupplier isIndexValid)
    {
        this.indexDescriptor = indexDescriptor;
        this.index = index;
        this.analyzer = index.hasAnalyzer() ? index.analyzer() : null;
        this.limiter = limiter;
        this.isIndexValid = isIndexValid;
    }

    @Override
    public void addRow(PrimaryKey key, Row row, long sstableRowId) throws IOException
    {
        if (maybeAbort())
            return;

        if (index.termType().isNonFrozenCollection())
        {
            Iterator<ByteBuffer> valueIterator = index.termType().valuesOf(row, nowInSec);
            if (valueIterator != null)
            {
                while (valueIterator.hasNext())
                {
                    ByteBuffer value = valueIterator.next();
                    addTerm(index.termType().asIndexBytes(value.duplicate()), key, sstableRowId);
                }
            }
        }
        else
        {
            ByteBuffer value = index.termType().valueOf(key.partitionKey(), row, nowInSec);
            if (value != null)
                addTerm(index.termType().asIndexBytes(value.duplicate()), key, sstableRowId);
        }
    }

    @Override
    public void complete(Stopwatch stopwatch) throws IOException
    {
        if (maybeAbort())
            return;

        long start = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        long elapsed;

        boolean emptySegment = currentBuilder == null || currentBuilder.isEmpty();
        logger.debug(index.identifier().logMessage("Completing index flush with {}buffered data..."), emptySegment ? "no " : "");

        try
        {
            // parts are present but there is something still in memory, let's flush that inline
            if (!emptySegment)
            {
                flushSegment();
                elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
                logger.debug(index.identifier().logMessage("Completed flush of final segment for SSTable {}. Duration: {} ms. Total elapsed: {} ms"),
                             indexDescriptor.sstableDescriptor,
                             elapsed - start,
                             elapsed);
            }

            // Even an empty segment may carry some fixed memory, so remove it:
            if (currentBuilder != null)
            {
                long bytesAllocated = currentBuilder.totalBytesAllocated();
                long globalBytesUsed = currentBuilder.release(index.identifier());
                logger.debug(index.identifier().logMessage("Flushing final segment for SSTable {} released {}. Global segment memory usage now at {}."),
                             indexDescriptor.sstableDescriptor, FBUtilities.prettyPrintMemory(bytesAllocated), FBUtilities.prettyPrintMemory(globalBytesUsed));
            }

            writeSegmentsMetadata();

            // write column index completion marker, indicating whether the index is empty
            ColumnCompletionMarkerUtil.create(indexDescriptor, index.identifier(), segments.isEmpty());
        }
        finally
        {
            index.indexMetrics().segmentsPerCompaction.update(segments.size());
            segments.clear();
            index.indexMetrics().compactionCount.inc();
        }
    }

    @Override
    public void abort(Throwable cause)
    {
        aborted = true;

        logger.warn(index.identifier().logMessage("Aborting SSTable index flush for {}..."), indexDescriptor.sstableDescriptor, cause);

        // It's possible for the current builder to be unassigned after we flush a final segment.
        if (currentBuilder != null)
        {
            // If an exception is thrown out of any writer operation prior to successful segment
            // flush, we will end up here, and we need to free up builder memory tracked by the limiter:
            long allocated = currentBuilder.totalBytesAllocated();
            long globalBytesUsed = currentBuilder.release(index.identifier());
            logger.debug(index.identifier().logMessage("Aborting index writer for SSTable {} released {}. Global segment memory usage now at {}."),
                         indexDescriptor.sstableDescriptor, FBUtilities.prettyPrintMemory(allocated), FBUtilities.prettyPrintMemory(globalBytesUsed));
        }

        indexDescriptor.deleteColumnIndex(index.termType(), index.identifier());
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

        abort(new RuntimeException(String.format("index %s is dropped", index.identifier())));
        return true;
    }

    private void addTerm(ByteBuffer term, PrimaryKey key, long sstableRowId) throws IOException
    {
        if (!index.validateMaxTermSize(key.partitionKey(), term, false))
            return;

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

        if (analyzer == null || !index.termType().isLiteral())
        {
            limiter.increment(currentBuilder.add(term, key, sstableRowId));
        }
        else
        {
            analyzer.reset(term);
            try
            {
                while (analyzer.hasNext())
                {
                    ByteBuffer tokenTerm = analyzer.next();
                    limiter.increment(currentBuilder.add(tokenTerm, key, sstableRowId));
                }
            }
            finally
            {
                analyzer.end();
            }
        }
    }

    private boolean shouldFlush(long sstableRowId)
    {
        // If we've hit the minimum flush size and, we've breached the global limit, flush a new segment:
        boolean reachMemoryLimit = limiter.usageExceedsLimit() && currentBuilder.hasReachedMinimumFlushSize();

        if (reachMemoryLimit)
        {
            logger.debug(index.identifier().logMessage("Global limit of {} and minimum flush size of {} exceeded. " +
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
        long start = Clock.Global.nanoTime();

        try
        {
            long bytesAllocated = currentBuilder.totalBytesAllocated();

            SegmentMetadata segmentMetadata = currentBuilder.flush(indexDescriptor, index.identifier());

            long flushMillis = Math.max(1, TimeUnit.NANOSECONDS.toMillis(Clock.Global.nanoTime() - start));

            if (segmentMetadata != null)
            {
                segments.add(segmentMetadata);

                double rowCount = segmentMetadata.numRows;
                index.indexMetrics().compactionSegmentCellsPerSecond.update((long)(rowCount / flushMillis * 1000.0));

                double segmentBytes = segmentMetadata.componentMetadatas.indexSize();
                index.indexMetrics().compactionSegmentBytesPerSecond.update((long)(segmentBytes / flushMillis * 1000.0));

                logger.debug(index.identifier().logMessage("Flushed segment with {} cells for a total of {} in {} ms."),
                             (long) rowCount, FBUtilities.prettyPrintMemory((long) segmentBytes), flushMillis);
            }

            // Builder memory is released against the limiter at the conclusion of a successful
            // flush. Note that any failure that occurs before this (even in term addition) will
            // actuate this column writer's abort logic from the parent SSTable-level writer, and
            // that abort logic will release the current builder's memory against the limiter.
            long globalBytesUsed = currentBuilder.release(index.identifier());
            currentBuilder = null;
            logger.debug(index.identifier().logMessage("Flushing index segment for SSTable {} released {}. Global segment memory usage now at {}."),
                         indexDescriptor.sstableDescriptor, FBUtilities.prettyPrintMemory(bytesAllocated), FBUtilities.prettyPrintMemory(globalBytesUsed));

        }
        catch (Throwable t)
        {
            logger.error(index.identifier().logMessage("Failed to build index for SSTable {}."), indexDescriptor.sstableDescriptor, t);
            indexDescriptor.deleteColumnIndex(index.termType(), index.identifier());
            index.indexMetrics().segmentFlushErrors.inc();
            throw t;
        }
    }

    private void writeSegmentsMetadata() throws IOException
    {
        if (segments.isEmpty())
            return;

        try (MetadataWriter writer = new MetadataWriter(indexDescriptor.openPerIndexOutput(IndexComponent.META, index.identifier())))
        {
            SegmentMetadata.write(writer, segments);
        }
        catch (IOException e)
        {
            abort(e);
            throw e;
        }
    }

    private SegmentBuilder newSegmentBuilder()
    {
        SegmentBuilder builder;

        if (index.termType().isVector())
            builder = new SegmentBuilder.VectorSegmentBuilder(index.termType(), limiter, index.indexWriterConfig());
        else if (index.termType().isLiteral())
            builder = new SegmentBuilder.RAMStringSegmentBuilder(index.termType(), limiter);
        else
            builder = new SegmentBuilder.BlockBalancedTreeSegmentBuilder(index.termType(), limiter);

        long globalBytesUsed = limiter.increment(builder.totalBytesAllocated());
        logger.debug(index.identifier().logMessage("Created new segment builder while flushing SSTable {}. Global segment memory usage now at {}."),
                     indexDescriptor.sstableDescriptor,
                     FBUtilities.prettyPrintMemory(globalBytesUsed));

        return builder;
    }
}
