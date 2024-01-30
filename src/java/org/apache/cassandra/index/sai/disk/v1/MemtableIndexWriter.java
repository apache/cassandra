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
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.PerColumnIndexWriter;
import org.apache.cassandra.index.sai.disk.RowMapping;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.metrics.IndexMetrics;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

/**
 * Column index writer that flushes indexed data directly from the corresponding Memtable index, without buffering index
 * data in memory.
 */
public class MemtableIndexWriter implements PerColumnIndexWriter
{
    private static final Logger logger = LoggerFactory.getLogger(MemtableIndexWriter.class);

    private final IndexDescriptor indexDescriptor;
    private final IndexTermType indexTermType;
    private final IndexIdentifier indexIdentifier;
    private final IndexMetrics indexMetrics;
    private final MemtableIndex memtable;
    private final StorageAttachedIndex.Flusher flusher;
    private final RowMapping rowMapping;

    public MemtableIndexWriter(MemtableIndex memtable,
                               IndexDescriptor indexDescriptor,
                               IndexTermType indexTermType,
                               IndexIdentifier indexIdentifier,
                               IndexMetrics indexMetrics,
                               StorageAttachedIndex.Flusher flusher,
                               RowMapping rowMapping)
    {
        assert rowMapping != null && rowMapping != RowMapping.DUMMY : "Row mapping must exist during FLUSH.";

        this.indexDescriptor = indexDescriptor;
        this.indexTermType = indexTermType;
        this.indexIdentifier = indexIdentifier;
        this.indexMetrics = indexMetrics;
        this.memtable = memtable;
        this.flusher = flusher;
        this.rowMapping = rowMapping;
    }

    @Override
    public void addRow(PrimaryKey key, Row row, long sstableRowId)
    {
        // Memtable indexes are flushed directly to disk with the aid of a mapping between primary
        // keys and row IDs in the flushing SSTable. This writer, therefore, does nothing in
        // response to the flushing of individual rows.
    }

    @Override
    public void abort(Throwable cause)
    {
        logger.warn(indexIdentifier.logMessage("Aborting index memtable flush for {}..."), indexDescriptor.sstableDescriptor, cause);
        indexDescriptor.deleteColumnIndex(indexTermType, indexIdentifier);
    }

    @Override
    public void complete(Stopwatch stopwatch) throws IOException
    {
        assert rowMapping.isComplete() : "Cannot complete the memtable index writer because the row mapping is not complete";

        long start = stopwatch.elapsed(TimeUnit.MILLISECONDS);

        try
        {
            if (!rowMapping.hasRows() || memtable == null || memtable.isEmpty())
            {
                logger.debug(indexIdentifier.logMessage("No indexed rows to flush from SSTable {}."), indexDescriptor.sstableDescriptor);
                // Write a completion marker even though we haven't written anything to the index,
                // so we won't try to build the index again for the SSTable
                ColumnCompletionMarkerUtil.create(indexDescriptor, indexIdentifier, true);

                return;
            }

            SegmentMetadata metadata = flusher.flush(memtable, indexDescriptor, rowMapping);
            if (metadata != null)
            {
                try (MetadataWriter writer = new MetadataWriter(indexDescriptor.openPerIndexOutput(IndexComponent.META, indexIdentifier)))
                {
                    SegmentMetadata.write(writer, Collections.singletonList(metadata));
                }
                completeIndexFlush(metadata.numRows, start, stopwatch);
            }
            else
            {
                completeIndexFlush(0, start, stopwatch);
            }
        }
        catch (Throwable t)
        {
            logger.error(indexIdentifier.logMessage("Error while flushing index {}"), t.getMessage(), t);
            indexMetrics.memtableIndexFlushErrors.inc();

            throw t;
        }
    }

    private void completeIndexFlush(long cellCount, long startTime, Stopwatch stopwatch) throws IOException
    {
        // create a completion marker indicating that the index is complete and not-empty
        ColumnCompletionMarkerUtil.create(indexDescriptor, indexIdentifier, false);

        indexMetrics.memtableIndexFlushCount.inc();

        long elapsedTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);

        logger.debug(indexIdentifier.logMessage("Completed flushing {} memtable index cells to SSTable {}. Duration: {} ms. Total elapsed: {} ms"),
                     cellCount,
                     indexDescriptor.sstableDescriptor,
                     elapsedTime - startTime,
                     elapsedTime);

        indexMetrics.memtableFlushCellsPerSecond.update((long) (cellCount * 1000.0 / Math.max(1, elapsedTime - startTime)));
    }
}
