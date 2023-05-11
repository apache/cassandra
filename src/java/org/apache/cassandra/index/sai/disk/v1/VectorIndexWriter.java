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
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.PerColumnIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.hnsw.VectorMemtableIndex;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.lucene.codecs.lucene95.Lucene95Codec;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;

/**
 * Column index writer that HNSW graph writer
 */
public class VectorIndexWriter implements PerColumnIndexWriter
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final VectorMemtableIndex memtableIndex;
    private final IndexDescriptor indexDescriptor;
    private final IndexContext indexContext;
    private final int nowInSec = FBUtilities.nowInSeconds();

    private final Map<PrimaryKey, Integer> keyToRowId = new HashMap<>(); // TODO can we know ahead of time how many rows there will be?

    private final byte[] segmentId;
    private final boolean needsBuilding;

    private long maxRowId = -1;

    private PrimaryKey minKey;
    private PrimaryKey maxKey;

    public VectorIndexWriter(VectorMemtableIndex memtableIndex, IndexDescriptor indexDescriptor, IndexContext indexContext)
    {
        this.needsBuilding = memtableIndex == null;
        this.memtableIndex = needsBuilding ? new VectorMemtableIndex(indexContext) : memtableIndex;
        Preconditions.checkState(indexContext.isVector());

        this.indexDescriptor = indexDescriptor;
        this.indexContext = indexContext;

        segmentId = StringHelper.randomId();
    }

    public IndexContext indexContext()
    {
        return indexContext;
    }

    @Override
    public void addRow(PrimaryKey key, Row row, long sstableRowId)
    {
        assert sstableRowId > maxRowId : "Row IDs must be added in ascending order";
        keyToRowId.put(key, Math.toIntExact(sstableRowId));
        if (needsBuilding) {
            ByteBuffer value = indexContext.getValueOf(key.partitionKey(), row, nowInSec);
            if (value != null)
            {
                float[] vector = (float[]) indexContext.getValidator().getSerializer().deserialize(value.duplicate());
                memtableIndex.index(key, vector);
            }
        }

        if (minKey == null)
            minKey = key;
        maxKey = key;
        maxRowId = sstableRowId;
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
        try
        {
            if (keyToRowId.isEmpty())
            {
                logger.debug(indexContext.logMessage("No indexed rows to flush from SSTable {}."), indexDescriptor.sstableDescriptor);
                // Write a completion marker even though we haven't written anything to the index
                // so we won't try to build the index again for the SSTable
            }
            else
            {
                flushVectorIndex(stopwatch);
            }

            indexDescriptor.createComponentOnDisk(IndexComponent.COLUMN_COMPLETION_MARKER, indexContext);
        }
        catch (Throwable t)
        {
            logger.error(indexContext.logMessage("Error while flushing index {}"), t.getMessage(), t);
            indexContext.getIndexMetrics().memtableIndexFlushErrors.inc();

            throw t;
        }
    }

    private void flushVectorIndex(Stopwatch stopwatch) throws IOException
    {
        long start = stopwatch.elapsed(TimeUnit.MILLISECONDS);

        memtableIndex.writeData(indexDescriptor, indexContext, keyToRowId);

        long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);

        logger.debug(indexContext.logMessage("Completed flushing {} memtable index vectors to SSTable {}. Duration: {} ms. Total elapsed: {} ms"),
                     keyToRowId.size(),
                     indexDescriptor.sstableDescriptor,
                     elapsed - start,
                     elapsed);

        indexContext.getIndexMetrics().memtableFlushCellsPerSecond.update((long) (keyToRowId.size() * 1000.0 / Math.max(1, elapsed - start)));

        SegmentMetadata.ComponentMetadataMap metadataMap = new SegmentMetadata.ComponentMetadataMap();

        // we don't care about root/offset/length for vector. segmentId is used in searcher
        Map<String, String> vectorConfigs = Map.of("SEGMENT_ID", ByteBufferUtil.bytesToHex(ByteBuffer.wrap(segmentId)));
        metadataMap.put(IndexComponent.VECTOR, 0, 0, 0, vectorConfigs);

        SegmentMetadata metadata = new SegmentMetadata(0,
                                                       keyToRowId.size(),
                                                       0,
                                                       maxRowId,
                                                       indexDescriptor.primaryKeyFactory.createPartitionKeyOnly(minKey.partitionKey()),
                                                       indexDescriptor.primaryKeyFactory.createPartitionKeyOnly(maxKey.partitionKey()),
                                                       ByteBufferUtil.bytes(0), // TODO by pass min max terms for vectors
                                                       ByteBufferUtil.bytes(0), // TODO by pass min max terms for vectors
                                                       metadataMap);

        try (MetadataWriter writer = new MetadataWriter(indexDescriptor.openPerIndexOutput(IndexComponent.META, indexContext)))
        {
            SegmentMetadata.write(writer, Collections.singletonList(metadata));
        }
    }
}
