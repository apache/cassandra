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
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.PerColumnIndexWriter;
import org.apache.cassandra.index.sai.disk.RowMapping;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.bbtree.BlockBalancedTreeIterator;
import org.apache.cassandra.index.sai.disk.v1.bbtree.NumericIndexWriter;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.trie.LiteralIndexWriter;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.memory.MemtableTermsIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.TypeUtil;
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
    private final IndexContext indexContext;
    private final MemtableIndex memtable;
    private final RowMapping rowMapping;

    public MemtableIndexWriter(MemtableIndex memtable,
                               IndexDescriptor indexDescriptor,
                               IndexContext indexContext,
                               RowMapping rowMapping)
    {
        assert rowMapping != null && rowMapping != RowMapping.DUMMY : "Row mapping must exist during FLUSH.";

        this.indexDescriptor = indexDescriptor;
        this.indexContext = indexContext;
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
        logger.warn(indexContext.logMessage("Aborting index memtable flush for {}..."), indexDescriptor.sstableDescriptor, cause);
        indexDescriptor.deleteColumnIndex(indexContext);
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
                logger.debug(indexContext.logMessage("No indexed rows to flush from SSTable {}."), indexDescriptor.sstableDescriptor);
                // Write a completion marker even though we haven't written anything to the index,
                // so we won't try to build the index again for the SSTable
                indexDescriptor.createComponentOnDisk(IndexComponent.COLUMN_COMPLETION_MARKER, indexContext);
                return;
            }

            final DecoratedKey minKey = rowMapping.minKey.partitionKey();
            final DecoratedKey maxKey = rowMapping.maxKey.partitionKey();

            final Iterator<Pair<ByteComparable, LongArrayList>> iterator = rowMapping.merge(memtable);

            try (MemtableTermsIterator terms = new MemtableTermsIterator(memtable.getMinTerm(), memtable.getMaxTerm(), iterator))
            {
                long cellCount = flush(minKey, maxKey, indexContext.getValidator(), terms, rowMapping.maxSSTableRowId);

                indexDescriptor.createComponentOnDisk(IndexComponent.COLUMN_COMPLETION_MARKER, indexContext);

                indexContext.getIndexMetrics().memtableIndexFlushCount.inc();

                long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);

                logger.debug(indexContext.logMessage("Completed flushing {} memtable index cells to SSTable {}. Duration: {} ms. Total elapsed: {} ms"),
                             cellCount, indexDescriptor.sstableDescriptor, elapsed - start, elapsed);

                indexContext.getIndexMetrics().memtableFlushCellsPerSecond.update((long) (cellCount * 1000.0 / Math.max(1, elapsed - start)));
            }
        }
        catch (Throwable t)
        {
            logger.error(indexContext.logMessage("Error while flushing index {}"), t.getMessage(), t);
            indexContext.getIndexMetrics().memtableIndexFlushErrors.inc();

            throw t;
        }
    }

    private long flush(DecoratedKey minKey,
                       DecoratedKey maxKey,
                       AbstractType<?> termComparator,
                       MemtableTermsIterator terms,
                       long maxSSTableRowId) throws IOException
    {
        long numRows;
        SegmentMetadata.ComponentMetadataMap indexMetas;

        if (TypeUtil.isLiteral(termComparator))
        {
            try (LiteralIndexWriter writer = new LiteralIndexWriter(indexDescriptor, indexContext))
            {
                indexMetas = writer.writeCompleteSegment(terms);
                numRows = writer.getPostingsCount();
            }
        }
        else
        {
            NumericIndexWriter writer = new NumericIndexWriter(indexDescriptor,
                                                               indexContext,
                                                               TypeUtil.fixedSizeOf(termComparator),
                                                               maxSSTableRowId);
            indexMetas = writer.writeCompleteSegment(BlockBalancedTreeIterator.fromTermsIterator(terms, termComparator));
            numRows = writer.getValueCount();
        }

        // If no rows were written we need to delete any created column index components
        // so that the index is correctly identified as being empty (only having a completion marker)
        if (numRows == 0)
        {
            indexDescriptor.deleteColumnIndex(indexContext);
            return 0;
        }

        // During index memtable flush, the data is sorted based on terms.
        SegmentMetadata metadata = new SegmentMetadata(0,
                                                       numRows,
                                                       terms.getMinSSTableRowId(),
                                                       terms.getMaxSSTableRowId(),
                                                       indexDescriptor.primaryKeyFactory.createPartitionKeyOnly(minKey),
                                                       indexDescriptor.primaryKeyFactory.createPartitionKeyOnly(maxKey),
                                                       terms.getMinTerm(),
                                                       terms.getMaxTerm(),
                                                       indexMetas);

        try (MetadataWriter writer = new MetadataWriter(indexDescriptor.openPerIndexOutput(IndexComponent.META, indexContext)))
        {
            SegmentMetadata.write(writer, Collections.singletonList(metadata));
        }

        return numRows;
    }
}
