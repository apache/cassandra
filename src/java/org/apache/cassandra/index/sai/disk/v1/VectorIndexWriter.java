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
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.PerIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.lucene95.Lucene95Codec;
import org.apache.lucene.codecs.lucene95.Lucene95HnswVectorsFormat;
import org.apache.lucene.codecs.lucene95.Lucene95HnswVectorsWriter;
import org.apache.lucene.index.FieldInfo;
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
public class VectorIndexWriter implements PerIndexWriter
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final IndexDescriptor indexDescriptor;
    private final IndexContext indexContext;
    private final int nowInSec = FBUtilities.nowInSeconds();

    private final Lucene95HnswVectorsWriter writer;
    private final KnnFieldVectorsWriter fieldWriter;

    private final int dimension;
    private byte[] segmentId;

    private int maxDocId = -1;
    private int vectors = 0;

    private PrimaryKey minKey;
    private PrimaryKey maxKey;

    public VectorIndexWriter(IndexDescriptor indexDescriptor, IndexContext indexContext)
    {
        Preconditions.checkState(indexContext.isVector());

        this.indexDescriptor = indexDescriptor;
        this.indexContext = indexContext;
        this.dimension = ((VectorType) indexContext.getValidator()).getDimensions();

        try
        {
            // full path in SAI naming pattern, e.g. .../<ks>/<cf>/ca-3g5d_1t56_18d8122br2d3mg6twm-bti-SAI+ba+table_00_val_idx+Vector.db
            File vectorPath = indexDescriptor.fileFor(IndexComponent.VECTOR, indexContext);

            // table directory
            Directory directory = FSDirectory.open(vectorPath.toPath().getParent());
            // segment name in SAI naming pattern, e.g. ca-3g5d_1t56_18d8122br2d3mg6twm-bti-SAI+ba+table_00_val_idx+Vector.db
            String segmentName = vectorPath.name();

            segmentId = StringHelper.randomId();
            SegmentInfo segmentInfo = new SegmentInfo(directory, Version.LATEST, Version.LATEST, segmentName, -1, false, Lucene95Codec.getDefault(), Collections.emptyMap(), segmentId, Collections.emptyMap(), null);
            SegmentWriteState state = new SegmentWriteState(InfoStream.getDefault(), directory, segmentInfo, null, null, IOContext.DEFAULT);
            writer = (Lucene95HnswVectorsWriter) new Lucene95HnswVectorsFormat(indexContext.getIndexWriterConfig().getMaximumNodeConnections(),
                                                                               indexContext.getIndexWriterConfig().getConstructionBeamWidth()).fieldsWriter(state);

            FieldInfo fieldInfo = indexContext.createFieldInfoForVector(dimension);
            // lucene creates 3 vectors files, e.g.:
            // ca-3g5d_1t56_18d8122br2d3mg6twm-bti-SAI+ba+table_00_val_idx+Vector.db.vex
            // ca-3g5d_1t56_18d8122br2d3mg6twm-bti-SAI+ba+table_00_val_idx+Vector.db.vec
            // ca-3g5d_1t56_18d8122br2d3mg6twm-bti-SAI+ba+table_00_val_idx+Vector.db.vem
            fieldWriter = writer.addField(fieldInfo);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public IndexContext indexContext()
    {
        return indexContext;
    }

    @Override
    public void addRow(PrimaryKey key, Row row, long sstableRowId)
    {
        // Memtable indexes are flushed directly to disk with the aid of a mapping between primary
        // keys and row IDs in the flushing SSTable. This writer, therefore, does nothing in
        // response to the flushing of individual rows.


        ByteBuffer value = indexContext.getValueOf(key.partitionKey(), row, nowInSec);
        if (value != null)
        {
            float[] vector = VectorType.Serializer.instance.deserialize(value.duplicate());
            try
            {
                // we should not have more than 2.1B rows in memtable
                int docId = Math.toIntExact(sstableRowId);
                fieldWriter.addValue(docId, vector);

                if (minKey == null)
                    minKey = key;

                maxKey = key;
                vectors++;
                maxDocId = Math.max(maxDocId, docId);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void abort(Throwable cause)
    {
        logger.warn(indexContext.logMessage("Aborting index memtable flush for {}..."), indexDescriptor.descriptor, cause);
        indexDescriptor.deleteColumnIndex(indexContext);
    }

    @Override
    public void complete(Stopwatch stopwatch) throws IOException
    {
        long start = stopwatch.elapsed(TimeUnit.MILLISECONDS);

        try
        {
            if (vectors == 0)
            {
                logger.debug(indexContext.logMessage("No indexed rows to flush from SSTable {}."), indexDescriptor.descriptor);
                // Write a completion marker even though we haven't written anything to the index
                // so we won't try to build the index again for the SSTable
                indexDescriptor.createComponentOnDisk(IndexComponent.COLUMN_COMPLETION_MARKER, indexContext);
                return;
            }

            flushVectorIndex(stopwatch, start);

            indexDescriptor.createComponentOnDisk(IndexComponent.COLUMN_COMPLETION_MARKER, indexContext);
        }
        catch (Throwable t)
        {
            logger.error(indexContext.logMessage("Error while flushing index {}"), t.getMessage(), t);
            indexContext.getIndexMetrics().memtableIndexFlushErrors.inc();

            throw t;
        }
    }

    private void flushVectorIndex(Stopwatch stopwatch, long start) throws IOException
    {
        writer.flush(maxDocId, null); // no need to reorder with DocMap
        writer.finish();
        writer.close();

        long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);

        logger.debug(indexContext.logMessage("Completed flushing {} memtable index vectors to SSTable {}. Duration: {} ms. Total elapsed: {} ms"),
                     vectors,
                     indexDescriptor.descriptor,
                     elapsed - start,
                     elapsed);

        indexContext.getIndexMetrics().memtableFlushCellsPerSecond.update((long) (vectors * 1000.0 / Math.max(1, elapsed - start)));

        SegmentMetadata.ComponentMetadataMap metadataMap = new SegmentMetadata.ComponentMetadataMap();

        // we don't care about root/offset/length for vector. segmentId is used in searcher
        Map<String, String> vectorConfigs = Map.of("SEGMENT_ID", ByteBufferUtil.bytesToHex(ByteBuffer.wrap(segmentId)));
        metadataMap.put(IndexComponent.VECTOR, 0, 0, 0, vectorConfigs);

        SegmentMetadata metadata = new SegmentMetadata(0,
                                                       vectors,
                                                       0,
                                                       maxDocId,
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
