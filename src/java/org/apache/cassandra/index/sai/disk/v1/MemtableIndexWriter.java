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
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.LongArrayList;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.disk.PerColumnIndexWriter;
import org.apache.cassandra.index.sai.disk.RowMapping;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.bbtree.NumericIndexWriter;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentWriter;
import org.apache.cassandra.index.sai.disk.v1.trie.LiteralIndexWriter;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.memory.MemtableTermsIterator;
import org.apache.cassandra.index.sai.metrics.IndexMetrics;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

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
    private final RowMapping rowMapping;

    public MemtableIndexWriter(MemtableIndex memtable,
                               IndexDescriptor indexDescriptor,
                               IndexTermType indexTermType,
                               IndexIdentifier indexIdentifier,
                               IndexMetrics indexMetrics,
                               RowMapping rowMapping)
    {
        assert rowMapping != null && rowMapping != RowMapping.DUMMY : "Row mapping must exist during FLUSH.";

        this.indexDescriptor = indexDescriptor;
        this.indexTermType = indexTermType;
        this.indexIdentifier = indexIdentifier;
        this.indexMetrics = indexMetrics;
        this.memtable = memtable;
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

            if (indexTermType.isVector())
            {
                flushVectorIndex(start, stopwatch);
            }
            else
            {
                final Iterator<Pair<ByteComparable, LongArrayList>> iterator = rowMapping.merge(memtable);

                try (MemtableTermsIterator terms = new MemtableTermsIterator(memtable.getMinTerm(), memtable.getMaxTerm(), iterator))
                {
                    long cellCount = flush(terms);

                    completeIndexFlush(cellCount, start, stopwatch);
                }
            }
        }
        catch (Throwable t)
        {
            logger.error(indexIdentifier.logMessage("Error while flushing index {}"), t.getMessage(), t);
            indexMetrics.memtableIndexFlushErrors.inc();

            throw t;
        }
    }

    private long flush(MemtableTermsIterator terms) throws IOException
    {
        SegmentWriter writer = indexTermType.isLiteral() ? new LiteralIndexWriter(indexDescriptor, indexIdentifier)
                                                         : new NumericIndexWriter(indexDescriptor,
                                                                                  indexIdentifier,
                                                                                  indexTermType.fixedSizeOf());

        SegmentMetadata.ComponentMetadataMap indexMetas = writer.writeCompleteSegment(terms);
        long numRows = writer.getNumberOfRows();

        // If no rows were written we need to delete any created column index components
        // so that the index is correctly identified as being empty (only having a completion marker)
        if (numRows == 0)
        {
            indexDescriptor.deleteColumnIndex(indexTermType, indexIdentifier);
            return 0;
        }

        PrimaryKey minKey = indexTermType.columnMetadata().isStatic() ? rowMapping.minStaticKey : rowMapping.minKey;
        PrimaryKey maxKey = indexTermType.columnMetadata().isStatic() ? rowMapping.maxStaticKey : rowMapping.maxKey;

        // During index memtable flush, the data is sorted based on terms.
        SegmentMetadata metadata = new SegmentMetadata(0,
                                                       numRows,
                                                       terms.getMinSSTableRowId(), terms.getMaxSSTableRowId(),
                                                       minKey, maxKey, 
                                                       terms.getMinTerm(), terms.getMaxTerm(),
                                                       indexMetas);

        try (MetadataWriter metadataWriter = new MetadataWriter(indexDescriptor.openPerIndexOutput(IndexComponent.META, indexIdentifier)))
        {
            SegmentMetadata.write(metadataWriter, Collections.singletonList(metadata));
        }

        return numRows;
    }

    private void flushVectorIndex(long startTime, Stopwatch stopwatch) throws IOException
    {
        int rowCount = indexTermType.columnMetadata().isStatic() ? rowMapping.staticRowCount : rowMapping.rowCount;
        PrimaryKey minKey = indexTermType.columnMetadata().isStatic() ? rowMapping.minStaticKey : rowMapping.minKey;
        PrimaryKey maxKey = indexTermType.columnMetadata().isStatic() ? rowMapping.maxStaticKey : rowMapping.maxKey;
        long maxSSTableRowId = indexTermType.columnMetadata().isStatic() ? rowMapping.maxStaticSSTableRowId : rowMapping.maxSSTableRowId;

        SegmentMetadata.ComponentMetadataMap metadataMap = memtable.writeDirect(indexDescriptor, indexIdentifier, rowMapping::get);
        completeIndexFlush(rowCount, startTime, stopwatch);

        SegmentMetadata metadata = new SegmentMetadata(0,
                                                       rowCount,
                                                       0, maxSSTableRowId,
                                                       minKey, maxKey, 
                                                       ByteBufferUtil.bytes(0), ByteBufferUtil.bytes(0),
                                                       metadataMap);

        try (MetadataWriter writer = new MetadataWriter(indexDescriptor.openPerIndexOutput(IndexComponent.META, indexIdentifier)))
        {
            SegmentMetadata.write(writer, Collections.singletonList(metadata));
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
