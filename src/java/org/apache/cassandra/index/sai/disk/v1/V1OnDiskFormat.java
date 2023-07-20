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
import java.util.EnumSet;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.index.sai.IndexContext;
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
import org.apache.cassandra.index.sai.metrics.AbstractMetrics;
import org.apache.cassandra.index.sai.utils.SegmentMemoryLimiter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.lucene.store.IndexInput;

import static org.apache.cassandra.utils.FBUtilities.prettyPrintMemory;

public class V1OnDiskFormat implements OnDiskFormat
{
    private static final Logger logger = LoggerFactory.getLogger(V1OnDiskFormat.class);

    @VisibleForTesting
    public static final Set<IndexComponent> SKINNY_PER_SSTABLE_COMPONENTS = EnumSet.of(IndexComponent.GROUP_COMPLETION_MARKER,
                                                                                       IndexComponent.GROUP_META,
                                                                                       IndexComponent.TOKEN_VALUES,
                                                                                       IndexComponent.PRIMARY_KEY_BLOCKS,
                                                                                       IndexComponent.PRIMARY_KEY_BLOCK_OFFSETS);

    @VisibleForTesting
    public static final Set<IndexComponent> WIDE_PER_SSTABLE_COMPONENTS = EnumSet.of(IndexComponent.GROUP_COMPLETION_MARKER,
                                                                                     IndexComponent.GROUP_META,
                                                                                     IndexComponent.TOKEN_VALUES,
                                                                                     IndexComponent.PRIMARY_KEY_TRIE,
                                                                                     IndexComponent.PRIMARY_KEY_BLOCKS,
                                                                                     IndexComponent.PRIMARY_KEY_BLOCK_OFFSETS);

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

    static
    {
        CassandraMetricsRegistry.MetricName bufferSpaceUsed = DefaultNameFactory.createMetricName(AbstractMetrics.TYPE, "SegmentBufferSpaceUsedBytes", null);
        CassandraMetricsRegistry.Metrics.register(bufferSpaceUsed, (Gauge<Long>) SegmentMemoryLimiter::currentBytesUsed);

        CassandraMetricsRegistry.MetricName bufferSpaceLimit = DefaultNameFactory.createMetricName(AbstractMetrics.TYPE, "SegmentBufferSpaceLimitBytes", null);
        CassandraMetricsRegistry.Metrics.register(bufferSpaceLimit, (Gauge<Long>) () -> SegmentMemoryLimiter.DEFAULT_SEGMENT_BUILD_MEMORY_LIMIT);

        CassandraMetricsRegistry.MetricName buildsInProgress = DefaultNameFactory.createMetricName(AbstractMetrics.TYPE, "ColumnIndexBuildsInProgress", null);
        CassandraMetricsRegistry.Metrics.register(buildsInProgress, (Gauge<Integer>) SegmentMemoryLimiter::getActiveBuilderCount);
    }

    public static final V1OnDiskFormat instance = new V1OnDiskFormat();

    protected V1OnDiskFormat()
    {}

    @Override
    public PrimaryKeyMap.Factory newPrimaryKeyMapFactory(IndexDescriptor indexDescriptor, SSTableReader sstable)
    {
        return indexDescriptor.hasClustering() ? new WideRowAwarePrimaryKeyMap.WideRowAwarePrimaryKeyMapFactory(indexDescriptor, sstable)
                                               : new SkinnyRowAwarePrimaryKeyMap.SkinnyRowAwarePrimaryKeyMapFactory(indexDescriptor, sstable);
    }

    @Override
    public SSTableIndex newSSTableIndex(SSTableContext sstableContext, IndexContext indexContext)
    {
        return new V1SSTableIndex(sstableContext, indexContext);
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
            logger.info(index.getIndexContext().logMessage("Starting a compaction index build. Global segment memory usage: {}"),
                        prettyPrintMemory(SegmentMemoryLimiter.currentBytesUsed()));

            return new SSTableIndexWriter(indexDescriptor, index.getIndexContext(), index.isIndexValid());
        }

        return new MemtableIndexWriter(index.getIndexContext().getMemtableIndexManager().getPendingMemtableIndex(tracker),
                                       indexDescriptor,
                                       index.getIndexContext(),
                                       rowMapping);
    }

    @Override
    public boolean isPerSSTableIndexBuildComplete(IndexDescriptor indexDescriptor)
    {
        return indexDescriptor.hasComponent(IndexComponent.GROUP_COMPLETION_MARKER);
    }

    @Override
    public boolean isPerColumnIndexBuildComplete(IndexDescriptor indexDescriptor, IndexContext indexContext)
    {
        return indexDescriptor.hasComponent(IndexComponent.GROUP_COMPLETION_MARKER) &&
               indexDescriptor.hasComponent(IndexComponent.COLUMN_COMPLETION_MARKER, indexContext);
    }

    @Override
    public boolean validatePerSSTableIndexComponents(IndexDescriptor indexDescriptor, boolean checksum)
    {
        for (IndexComponent indexComponent : perSSTableIndexComponents(indexDescriptor.hasClustering()))
        {
            if (isNotBuildCompletionMarker(indexComponent))
            {
                try (IndexInput input = indexDescriptor.openPerSSTableInput(indexComponent))
                {
                    if (checksum)
                        SAICodecUtils.validateChecksum(input);
                    else
                        SAICodecUtils.validate(input);
                }
                catch (Throwable e)
                {
                    logger.error(indexDescriptor.logMessage("{} failed for index component {} on SSTable {}. Error: {}"),
                                 checksum ? "Checksum validation" : "Validation",
                                 indexComponent,
                                 indexDescriptor.sstableDescriptor,
                                 e);
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public boolean validatePerColumnIndexComponents(IndexDescriptor indexDescriptor, IndexContext indexContext, boolean checksum)
    {
        for (IndexComponent indexComponent : perColumnIndexComponents(indexContext))
        {
            if (isNotBuildCompletionMarker(indexComponent))
            {
                try (IndexInput input = indexDescriptor.openPerIndexInput(indexComponent, indexContext))
                {
                    if (checksum)
                        SAICodecUtils.validateChecksum(input);
                    else
                        SAICodecUtils.validate(input);
                }
                catch (Throwable e)
                {
                    if (logger.isDebugEnabled())
                    {
                        logger.debug(indexDescriptor.logMessage("{} failed for index component {} on SSTable {}"),
                                     (checksum ? "Checksum validation" : "Validation"),
                                     indexComponent,
                                     indexDescriptor.sstableDescriptor);
                    }
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public Set<IndexComponent> perSSTableIndexComponents(boolean hasClustering)
    {
        return hasClustering ? WIDE_PER_SSTABLE_COMPONENTS : SKINNY_PER_SSTABLE_COMPONENTS;
    }

    @Override
    public Set<IndexComponent> perColumnIndexComponents(IndexContext indexContext)
    {
        return indexContext.isLiteral() ? LITERAL_COMPONENTS : NUMERIC_COMPONENTS;
    }

    @Override
    public int openFilesPerSSTableIndex(boolean hasClustering)
    {
        // For the V1 format the number of files depends on whether the table has clustering. The primary key trie
        // is only built for clustering columns so the number of files will be 4 per SSTable - token values, primary key trie,
        // primary key blocks, primary key block offsets for wide tables and 3 per SSTable - token values,
        // primary key blocks & primary key block offsets for skinny tables.
        return hasClustering ? 4 : 3;
    }

    @Override
    public int openFilesPerColumnIndex(IndexContext indexContext)
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
