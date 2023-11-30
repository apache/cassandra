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
import java.io.UncheckedIOException;
import java.util.EnumSet;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.PerColumnIndexWriter;
import org.apache.cassandra.index.sai.disk.PerSSTableIndexWriter;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.RowMapping;
import org.apache.cassandra.index.sai.disk.SSTableIndex;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentBuilder;
import org.apache.cassandra.index.sai.metrics.AbstractMetrics;
import org.apache.cassandra.index.sai.utils.IndexIdentifier;
import org.apache.cassandra.index.sai.utils.IndexTermType;
import org.apache.cassandra.index.sai.utils.NamedMemoryLimiter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.utils.Throwables;
import org.apache.lucene.store.IndexInput;

import static org.apache.cassandra.utils.FBUtilities.prettyPrintMemory;

public class V1OnDiskFormat implements OnDiskFormat
{
    private static final Logger logger = LoggerFactory.getLogger(V1OnDiskFormat.class);

    @VisibleForTesting
    public static final Set<IndexComponent> SKINNY_PER_SSTABLE_COMPONENTS = EnumSet.of(IndexComponent.GROUP_COMPLETION_MARKER,
                                                                                       IndexComponent.GROUP_META,
                                                                                       IndexComponent.ROW_TO_TOKEN,
                                                                                       IndexComponent.ROW_TO_PARTITION,
                                                                                       IndexComponent.PARTITION_KEY_BLOCKS,
                                                                                       IndexComponent.PARTITION_KEY_BLOCK_OFFSETS);

    @VisibleForTesting
    public static final Set<IndexComponent> WIDE_PER_SSTABLE_COMPONENTS = EnumSet.of(IndexComponent.GROUP_COMPLETION_MARKER,
                                                                                     IndexComponent.GROUP_META,
                                                                                     IndexComponent.ROW_TO_TOKEN,
                                                                                     IndexComponent.ROW_TO_PARTITION,
                                                                                     IndexComponent.PARTITION_TO_SIZE,
                                                                                     IndexComponent.PARTITION_KEY_BLOCKS,
                                                                                     IndexComponent.PARTITION_KEY_BLOCK_OFFSETS,
                                                                                     IndexComponent.CLUSTERING_KEY_BLOCKS,
                                                                                     IndexComponent.CLUSTERING_KEY_BLOCK_OFFSETS);

    @VisibleForTesting
    public static final Set<IndexComponent> LITERAL_COMPONENTS = EnumSet.of(IndexComponent.COLUMN_COMPLETION_MARKER,
                                                                            IndexComponent.META,
                                                                            IndexComponent.TERMS_DATA,
                                                                            IndexComponent.POSTING_LISTS);
    @VisibleForTesting
    public static final Set<IndexComponent> NUMERIC_COMPONENTS = EnumSet.of(IndexComponent.COLUMN_COMPLETION_MARKER,
                                                                            IndexComponent.META,
                                                                            IndexComponent.BALANCED_TREE,
                                                                            IndexComponent.POSTING_LISTS);

    @VisibleForTesting
    public static final Set<IndexComponent> VECTOR_COMPONENTS = EnumSet.of(IndexComponent.COLUMN_COMPLETION_MARKER,
                                                                           IndexComponent.META,
                                                                           IndexComponent.COMPRESSED_VECTORS,
                                                                           IndexComponent.TERMS_DATA,
                                                                           IndexComponent.POSTING_LISTS);

    /**
     * Global limit on heap consumed by all index segment building that occurs outside the context of Memtable flush.
     * <p>
     * Note that to avoid flushing small index segments, a segment is only flushed when
     * both the global size of all building segments has breached the limit and the size of the
     * segment in question reaches (segment_write_buffer_space_mb / # currently building column indexes).
     * <p>
     * ex. If there is only one column index building, it can buffer up to segment_write_buffer_space_mb.
     * <p>
     * ex. If there is one column index building per table across 8 compactors, each index will be
     *     eligible to flush once it reaches (segment_write_buffer_space_mb / 8) MBs.
     */
    public static final long SEGMENT_BUILD_MEMORY_LIMIT = DatabaseDescriptor.getSAISegmentWriteBufferSpace().toBytes();

    public static final NamedMemoryLimiter SEGMENT_BUILD_MEMORY_LIMITER = new NamedMemoryLimiter(SEGMENT_BUILD_MEMORY_LIMIT,
                                                                                                 "Storage Attached Index Segment Builder");

    static
    {
        CassandraMetricsRegistry.MetricName bufferSpaceUsed = DefaultNameFactory.createMetricName(AbstractMetrics.TYPE, "SegmentBufferSpaceUsedBytes", null);
        CassandraMetricsRegistry.Metrics.register(bufferSpaceUsed, (Gauge<Long>) SEGMENT_BUILD_MEMORY_LIMITER::currentBytesUsed);

        CassandraMetricsRegistry.MetricName bufferSpaceLimit = DefaultNameFactory.createMetricName(AbstractMetrics.TYPE, "SegmentBufferSpaceLimitBytes", null);
        CassandraMetricsRegistry.Metrics.register(bufferSpaceLimit, (Gauge<Long>) () -> SEGMENT_BUILD_MEMORY_LIMIT);

        CassandraMetricsRegistry.MetricName buildsInProgress = DefaultNameFactory.createMetricName(AbstractMetrics.TYPE, "ColumnIndexBuildsInProgress", null);
        CassandraMetricsRegistry.Metrics.register(buildsInProgress, (Gauge<Integer>) SegmentBuilder::getActiveBuilderCount);
    }

    public static final V1OnDiskFormat instance = new V1OnDiskFormat();

    protected V1OnDiskFormat()
    {}

    @Override
    public PrimaryKeyMap.Factory newPrimaryKeyMapFactory(IndexDescriptor indexDescriptor, SSTableReader sstable)
    {
        return indexDescriptor.hasClustering() ? new WidePrimaryKeyMap.Factory(indexDescriptor, sstable)
                                               : new SkinnyPrimaryKeyMap.Factory(indexDescriptor);
    }

    @Override
    public SSTableIndex newSSTableIndex(SSTableContext sstableContext, StorageAttachedIndex index)
    {
        return new V1SSTableIndex(sstableContext, index);
    }

    @Override
    public PerSSTableIndexWriter newPerSSTableIndexWriter(IndexDescriptor indexDescriptor) throws IOException
    {
        return new SSTableComponentsWriter(indexDescriptor);
    }

    @Override
    public PerColumnIndexWriter newPerColumnIndexWriter(StorageAttachedIndex index,
                                                        IndexDescriptor indexDescriptor,
                                                        LifecycleNewTracker tracker,
                                                        RowMapping rowMapping)
    {
        // If we're not flushing, or we haven't yet started the initialization build, flush from SSTable contents.
        if (tracker.opType() != OperationType.FLUSH || !index.isInitBuildStarted())
        {
            NamedMemoryLimiter limiter = SEGMENT_BUILD_MEMORY_LIMITER;
            logger.info(index.identifier().logMessage("Starting a compaction index build. Global segment memory usage: {}"),
                        prettyPrintMemory(limiter.currentBytesUsed()));

            return new SSTableIndexWriter(indexDescriptor, index, limiter, index.isIndexValid());
        }

        return new MemtableIndexWriter(index.memtableIndexManager().getPendingMemtableIndex(tracker),
                                       indexDescriptor,
                                       index.termType(),
                                       index.identifier(),
                                       index.indexMetrics(),
                                       rowMapping);
    }

    @Override
    public boolean isPerSSTableIndexBuildComplete(IndexDescriptor indexDescriptor)
    {
        return indexDescriptor.hasComponent(IndexComponent.GROUP_COMPLETION_MARKER);
    }

    @Override
    public boolean isPerColumnIndexBuildComplete(IndexDescriptor indexDescriptor, IndexIdentifier indexIdentifier)
    {
        return indexDescriptor.hasComponent(IndexComponent.GROUP_COMPLETION_MARKER) &&
               indexDescriptor.hasComponent(IndexComponent.COLUMN_COMPLETION_MARKER, indexIdentifier);
    }

    @Override
    public void validatePerSSTableIndexComponents(IndexDescriptor indexDescriptor, boolean checksum)
    {
        for (IndexComponent indexComponent : perSSTableIndexComponents(indexDescriptor.hasClustering()))
        {
            if (isNotBuildCompletionMarker(indexComponent))
            {
                validateIndexComponent(indexDescriptor, null, indexComponent, checksum);
            }
        }
    }

    @Override
    public void validatePerColumnIndexComponents(IndexDescriptor indexDescriptor, IndexTermType indexTermType, IndexIdentifier indexIdentifier, boolean checksum)
    {
        // determine if the index is empty, which would be encoded in the column completion marker
        boolean isEmptyIndex = false;
        if (indexDescriptor.hasComponent(IndexComponent.COLUMN_COMPLETION_MARKER, indexIdentifier))
        {
            // first validate the file...
            validateIndexComponent(indexDescriptor, indexIdentifier, IndexComponent.COLUMN_COMPLETION_MARKER, checksum);

            // ...then read to check if the index is empty
            try
            {
                isEmptyIndex = ColumnCompletionMarkerUtil.isEmptyIndex(indexDescriptor, indexIdentifier);
            }
            catch (IOException e)
            {
                rethrowIOException(e);
            }
        }

        for (IndexComponent indexComponent : perColumnIndexComponents(indexTermType))
        {
            if (!isEmptyIndex && isNotBuildCompletionMarker(indexComponent))
            {
                validateIndexComponent(indexDescriptor, indexIdentifier, indexComponent, checksum);
            }
        }
    }

    private static void validateIndexComponent(IndexDescriptor indexDescriptor,
                                               IndexIdentifier indexContext,
                                               IndexComponent indexComponent,
                                               boolean checksum)
    {
        try (IndexInput input = indexContext == null
                                ? indexDescriptor.openPerSSTableInput(indexComponent)
                                : indexDescriptor.openPerIndexInput(indexComponent, indexContext))
        {
            if (checksum)
                SAICodecUtils.validateChecksum(input);
            else
                SAICodecUtils.validate(input);
        }
        catch (Exception e)
        {
            logger.warn(indexDescriptor.logMessage("{} failed for index component {} on SSTable {}"),
                        checksum ? "Checksum validation" : "Validation",
                        indexComponent,
                        indexDescriptor.sstableDescriptor);
            rethrowIOException(e);
        }
    }

    private static void rethrowIOException(Exception e)
    {
        if (e instanceof IOException)
            throw new UncheckedIOException((IOException) e);
        if (e.getCause() instanceof IOException)
            throw new UncheckedIOException((IOException) e.getCause());
        throw Throwables.unchecked(e);
    }

    @Override
    public Set<IndexComponent> perSSTableIndexComponents(boolean hasClustering)
    {
        return hasClustering ? WIDE_PER_SSTABLE_COMPONENTS : SKINNY_PER_SSTABLE_COMPONENTS;
    }

    @Override
    public Set<IndexComponent> perColumnIndexComponents(IndexTermType indexTermType)
    {
        return indexTermType.isVector() ? VECTOR_COMPONENTS : indexTermType.isLiteral() ? LITERAL_COMPONENTS : NUMERIC_COMPONENTS;
    }

    @Override
    public int openFilesPerSSTableIndex(boolean hasClustering)
    {
        // For the V1 format the number of open files depends on whether the table has clustering. For wide tables
        // the number of open files will be 6 per SSTable - token values, partition sizes index, partition key blocks,
        // partition key block offsets, clustering key blocks & clustering key block offsets and for skinny tables
        // the number of files will be 4 per SSTable - token values, partition key sizes, partition key blocks &
        // partition key block offsets.
        return hasClustering ? 6 : 4;
    }

    @Override
    public int openFilesPerColumnIndex()
    {
        // For the V1 format there are always 2 open files per index - index (balanced tree or terms) + auxiliary postings
        // for the balanced tree and postings for the literal terms
        return 2;
    }

    protected boolean isNotBuildCompletionMarker(IndexComponent indexComponent)
    {
        return indexComponent != IndexComponent.GROUP_COMPLETION_MARKER &&
               indexComponent != IndexComponent.COLUMN_COMPLETION_MARKER;
    }
}
