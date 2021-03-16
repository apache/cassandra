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
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.IntArrayList;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.ColumnContext;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.disk.v1.InvertedIndexWriter;
import org.apache.cassandra.index.sai.disk.v1.MetadataWriter;
import org.apache.cassandra.index.sai.disk.v1.NumericIndexWriter;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Column index writer that flushes indexed data directly from the corresponding Memtable index, without buffering index
 * data in memory.
 */
public class MemtableIndexWriter implements ColumnIndexWriter
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final MemtableIndex memtable;
    private final RowMapping rowMapping;
    private final ColumnContext context;
    private final Descriptor descriptor;
    private final IndexComponents indexComponents;

    public MemtableIndexWriter(MemtableIndex memtable,
                               Descriptor descriptor,
                               ColumnContext context,
                               RowMapping rowMapping,
                               CompressionParams compressionParams)
    {
        assert rowMapping != null && rowMapping != RowMapping.DUMMY : "Row mapping must exist during FLUSH.";

        this.memtable = memtable;
        this.rowMapping = rowMapping;
        this.context = context;
        this.descriptor = descriptor;

        this.indexComponents = IndexComponents.create(context.getIndexName(), descriptor, compressionParams);
    }

    @Override
    public void addRow(DecoratedKey rowKey, long ssTableRowId, Row row)
    {
        // Memtable indexes are flushed directly to disk with the aid of a mapping between primary
        // keys and row IDs in the flushing SSTable. This writer, therefore, does nothing in
        // response to the flushing of individual rows.
    }

    @Override
    public void abort(Throwable cause)
    {
        logger.warn(context.logMessage("Aborting index memtable flush for {}..."), descriptor, cause);
        indexComponents.deleteColumnIndex();
    }

    @Override
    public void flush() throws IOException
    {
        long start = System.nanoTime();

        try
        {
            if (!rowMapping.hasRows() || (memtable == null) || memtable.isEmpty())
            {
                logger.debug(context.logMessage("No indexed rows to flush from SSTable {}."), descriptor);
                // Write a completion marker even though we haven't written anything to the index
                // so we won't try to build the index again for the SSTable
                indexComponents.createColumnCompletionMarker();
                return;
            }

            final DecoratedKey minKey = rowMapping.minKey;
            final DecoratedKey maxKey = rowMapping.maxKey;

            final Iterator<Pair<ByteComparable, IntArrayList>> iterator = rowMapping.merge(memtable);

            try (MemtableTermsIterator terms = new MemtableTermsIterator(memtable.getMinTerm(), memtable.getMaxTerm(), iterator))
            {
                long cellCount = flush(minKey, maxKey, context.getValidator(), terms, rowMapping.maxSegmentRowId);

                indexComponents.createColumnCompletionMarker();

                context.getIndexMetrics().memtableIndexFlushCount.inc();

                long durationMillis = Math.max(1, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

                if (logger.isTraceEnabled())
                {
                    logger.trace(context.logMessage("Flushed {} Memtable index cells for {} in {} ms."), cellCount, descriptor, durationMillis);
                }

                context.getIndexMetrics().memtableFlushCellsPerSecond.update((long) (cellCount * 1000.0 / durationMillis));
            }
        }
        catch (Throwable t)
        {
            logger.error(context.logMessage("Error while flushing index {}"), t.getMessage(), t);
            context.getIndexMetrics().memtableIndexFlushErrors.inc();

            throw t;
        }
    }

    private long flush(DecoratedKey minKey, DecoratedKey maxKey, AbstractType<?> termComparator, MemtableTermsIterator terms, int maxSegmentRowId) throws IOException
    {
        long numRows;
        SegmentMetadata.ComponentMetadataMap indexMetas;

        if (TypeUtil.isLiteral(termComparator))
        {
            try (InvertedIndexWriter writer = new InvertedIndexWriter(indexComponents, false))
            {
                indexMetas = writer.writeAll(terms);
                numRows = writer.getPostingsCount();
            }
        }
        else
        {
            try (NumericIndexWriter writer = new NumericIndexWriter(indexComponents,
                                                                    TypeUtil.fixedSizeOf(termComparator),
                                                                    maxSegmentRowId,
                                                                    // Due to stale entries in IndexMemtable, we may have more indexed rows than num of rowIds.
                                                                    Integer.MAX_VALUE,
                                                                    context.getIndexWriterConfig(),
                                                                    false))
            {
                indexMetas = writer.writeAll(ImmutableOneDimPointValues.fromTermEnum(terms, termComparator));
                numRows = writer.getPointCount();
            }
        }

        // If no rows were written we need to delete any created column index components
        // so that the index is correctly identified as being empty (only having a completion marker)
        if (numRows == 0)
        {
            indexComponents.deleteColumnIndex();
            return 0;
        }

        // During index memtable flush, the data is sorted based on terms.
        SegmentMetadata metadata = new SegmentMetadata(0, numRows, terms.getMinSSTableRowId(), terms.getMaxSSTableRowId(),
                                                       minKey, maxKey, terms.getMinTerm(), terms.getMaxTerm(), indexMetas);

        try (MetadataWriter writer = new MetadataWriter(indexComponents.createOutput(indexComponents.meta)))
        {
            SegmentMetadata.write(writer, Collections.singletonList(metadata), null);
        }

        return numRows;
    }
}
