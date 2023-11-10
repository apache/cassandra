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
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.SSTableIndex;
import org.apache.cassandra.index.sai.disk.v1.segment.Segment;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.iterators.KeyRangeIterator;
import org.apache.cassandra.index.sai.iterators.KeyRangeUnionIterator;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.index.sai.virtual.SegmentsSystemView.CELL_COUNT;
import static org.apache.cassandra.index.sai.virtual.SegmentsSystemView.COLUMN_NAME;
import static org.apache.cassandra.index.sai.virtual.SegmentsSystemView.COMPONENT_METADATA;
import static org.apache.cassandra.index.sai.virtual.SegmentsSystemView.END_TOKEN;
import static org.apache.cassandra.index.sai.virtual.SegmentsSystemView.MAX_SSTABLE_ROW_ID;
import static org.apache.cassandra.index.sai.virtual.SegmentsSystemView.MAX_TERM;
import static org.apache.cassandra.index.sai.virtual.SegmentsSystemView.MIN_SSTABLE_ROW_ID;
import static org.apache.cassandra.index.sai.virtual.SegmentsSystemView.MIN_TERM;
import static org.apache.cassandra.index.sai.virtual.SegmentsSystemView.START_TOKEN;
import static org.apache.cassandra.index.sai.virtual.SegmentsSystemView.TABLE_NAME;

/**
 * A version specific implementation of the {@link SSTableIndex} where the
 * index is segmented
 */
public class V1SSTableIndex extends SSTableIndex
{
    private final ImmutableList<Segment> segments;
    private final List<SegmentMetadata> metadatas;
    private final AbstractBounds<PartitionPosition> bounds;
    private final ByteBuffer minTerm;
    private final ByteBuffer maxTerm;
    private final long minSSTableRowId, maxSSTableRowId;
    private final long numRows;

    private PerColumnIndexFiles indexFiles;

    public V1SSTableIndex(SSTableContext sstableContext, StorageAttachedIndex index)
    {
        super(sstableContext, index);

        try
        {
            this.indexFiles = new PerColumnIndexFiles(sstableContext.indexDescriptor, indexTermType, indexIdentifier);

            ImmutableList.Builder<Segment> segmentsBuilder = ImmutableList.builder();

            final MetadataSource source = MetadataSource.loadColumnMetadata(sstableContext.indexDescriptor, indexIdentifier);

            metadatas = SegmentMetadata.load(source, sstableContext.indexDescriptor.primaryKeyFactory);

            for (SegmentMetadata metadata : metadatas)
            {
                segmentsBuilder.add(new Segment(index, sstableContext, indexFiles, metadata));
            }

            segments = segmentsBuilder.build();
            assert !segments.isEmpty();

            DecoratedKey minKey = metadatas.get(0).minKey.partitionKey();
            DecoratedKey maxKey = metadatas.get(metadatas.size() - 1).maxKey.partitionKey();

            this.bounds = AbstractBounds.bounds(minKey, true, maxKey, true);

            this.minTerm = metadatas.stream().map(m -> m.minTerm).min(indexTermType.comparator()).orElse(null);
            this.maxTerm = metadatas.stream().map(m -> m.maxTerm).max(indexTermType.comparator()).orElse(null);

            this.numRows = metadatas.stream().mapToLong(m -> m.numRows).sum();

            this.minSSTableRowId = metadatas.get(0).minSSTableRowId;
            this.maxSSTableRowId = metadatas.get(metadatas.size() - 1).maxSSTableRowId;
        }
        catch (Throwable t)
        {
            FileUtils.closeQuietly(indexFiles);
            FileUtils.closeQuietly(sstableContext);
            throw Throwables.unchecked(t);
        }
    }

    @Override
    public long indexFileCacheSize()
    {
        return segments.stream().mapToLong(Segment::indexFileCacheSize).sum();
    }

    @Override
    public long getRowCount()
    {
        return numRows;
    }

    @Override
    public long minSSTableRowId()
    {
        return minSSTableRowId;
    }

    @Override
    public long maxSSTableRowId()
    {
        return maxSSTableRowId;
    }

    @Override
    public ByteBuffer minTerm()
    {
        return minTerm;
    }

    @Override
    public ByteBuffer maxTerm()
    {
        return maxTerm;
    }

    @Override
    public AbstractBounds<PartitionPosition> bounds()
    {
        return bounds;
    }

    @Override
    public List<KeyRangeIterator> search(Expression expression,
                                         AbstractBounds<PartitionPosition> keyRange,
                                         QueryContext context) throws IOException
    {
        List<KeyRangeIterator> segmentIterators = new ArrayList<>();

        for (Segment segment : segments)
        {
            if (segment.intersects(keyRange))
            {
                segmentIterators.add(segment.search(expression, keyRange, context));
            }
        }

        return segmentIterators;
    }

    @Override
    public KeyRangeIterator limitToTopKResults(QueryContext context, List<PrimaryKey> primaryKeys, Expression expression) throws IOException
    {
        KeyRangeUnionIterator.Builder unionIteratorBuilder = KeyRangeUnionIterator.builder(segments.size());
        for (Segment segment : segments)
            unionIteratorBuilder.add(segment.limitToTopKResults(context, primaryKeys, expression));

        return unionIteratorBuilder.build();
    }

    @Override
    public void populateSegmentView(SimpleDataSet dataset)
    {
        SSTableReader sstable = getSSTable();
        Token.TokenFactory tokenFactory = sstable.metadata().partitioner.getTokenFactory();

        for (SegmentMetadata metadata : metadatas)
        {
            dataset.row(sstable.metadata().keyspace, indexIdentifier.indexName, sstable.getFilename(), metadata.rowIdOffset)
                   .column(TABLE_NAME, sstable.descriptor.cfname)
                   .column(COLUMN_NAME, indexTermType.columnName())
                   .column(CELL_COUNT, metadata.numRows)
                   .column(MIN_SSTABLE_ROW_ID, metadata.minSSTableRowId)
                   .column(MAX_SSTABLE_ROW_ID, metadata.maxSSTableRowId)
                   .column(START_TOKEN, tokenFactory.toString(metadata.minKey.token()))
                   .column(END_TOKEN, tokenFactory.toString(metadata.maxKey.token()))
                   .column(MIN_TERM, indexTermType.indexType().getSerializer().deserialize(metadata.minTerm).toString())
                   .column(MAX_TERM, indexTermType.indexType().getSerializer().deserialize(metadata.maxTerm).toString())
                   .column(COMPONENT_METADATA, metadata.componentMetadatas.asMap());
        }
    }

    @Override
    protected void internalRelease()
    {
        FileUtils.closeQuietly(indexFiles);
        FileUtils.closeQuietly(segments);
    }
}
