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

package org.apache.cassandra.index.sai;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.disk.Segment;
import org.apache.cassandra.index.sai.disk.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.disk.io.IndexComponents.IndexComponent;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.RangeConcatIterator;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;


/**
 * SSTableIndex is created for each column index on individual sstable to track per-column indexer.
 */
public class SSTableIndex
{
    // sort sstable index by first key then last key
    public static final Comparator<SSTableIndex> COMPARATOR = Comparator.comparing((SSTableIndex s) -> s.getSSTable().first)
                                                                        .thenComparing(s -> s.getSSTable().last)
                                                                        .thenComparing(s -> s.getSSTable().descriptor.generation);

    private final Version version;
    private final SSTableContext sstableContext;
    private final ColumnContext columnContext;
    private final SSTableReader sstable;
    private final IndexComponents components;

    private final ImmutableList<Segment> segments;
    private PerIndexFiles indexFiles;

    private final List<SegmentMetadata> metadatas;
    private final DecoratedKey minKey, maxKey; // in token order
    private final ByteBuffer minTerm, maxTerm;
    private final long minSSTableRowId, maxSSTableRowId;
    private final long numRows;

    private final AtomicInteger references = new AtomicInteger(1);
    private final AtomicBoolean obsolete = new AtomicBoolean(false);

    public SSTableIndex(SSTableContext sstableContext, ColumnContext columnContext, IndexComponents components)
    {
        this.sstableContext = sstableContext.sharedCopy();
        this.columnContext = columnContext;
        this.sstable = sstableContext.sstable;
        this.components = components;

        final AbstractType<?> validator = columnContext.getValidator();
        assert validator != null;

        try
        {
            this.indexFiles = new PerIndexFiles(components, columnContext.isLiteral());

            ImmutableList.Builder<Segment> segmentsBuilder = ImmutableList.builder();

            final MetadataSource source = MetadataSource.loadColumnMetadata(components);
            version = source.getVersion();
            metadatas = SegmentMetadata.load(source, null);

            for (SegmentMetadata metadata : metadatas)
            {
                segmentsBuilder.add(new Segment(columnContext, sstableContext, indexFiles, metadata));
            }

            segments = segmentsBuilder.build();
            assert !segments.isEmpty();

            this.minKey = metadatas.get(0).minKey;
            this.maxKey = metadatas.get(metadatas.size() - 1).maxKey;

            this.minTerm = metadatas.stream().map(m -> m.minTerm).min(TypeUtil.comparator(validator)).orElse(null);
            this.maxTerm = metadatas.stream().map(m -> m.maxTerm).max(TypeUtil.comparator(validator)).orElse(null);

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

    public ColumnContext getColumnContext()
    {
        return columnContext;
    }

    public SSTableContext getSSTableContext()
    {
        return sstableContext;
    }

    public long indexFileCacheSize()
    {
        return segments.stream().mapToLong(Segment::indexFileCacheSize).sum();
    }

    /**
     * @return number of indexed rows, note that rows may have been updated or removed in sstable.
     */
    public long getRowCount()
    {
        return numRows;
    }

    /**
     * @return total size of per-column index components, in bytes
     */
    public long sizeOfPerColumnComponents()
    {
        return components.sizeOfPerColumnComponents();
    }

    /**
     * @return the smallest possible sstable row id in this index.
     */
    public long minSSTableRowId()
    {
        return minSSTableRowId;
    }

    /**
     * @return the largest possible sstable row id in this index.
     */
    public long maxSSTableRowId()
    {
        return maxSSTableRowId;
    }

    public ByteBuffer minTerm()
    {
        return minTerm;
    }

    public ByteBuffer maxTerm()
    {
        return maxTerm;
    }

    public DecoratedKey minKey()
    {
        return minKey;
    }

    public DecoratedKey maxKey()
    {
        return maxKey;
    }

    public RangeIterator search(Expression expression, AbstractBounds<PartitionPosition> keyRange, SSTableQueryContext context, boolean defer)
    {
        RangeConcatIterator.Builder builder = RangeConcatIterator.builder();

        for (Segment segment : segments)
        {
            if (segment.intersects(keyRange))
            {
                builder.add(segment.search(expression, context, defer));
            }
        }

        return builder.build();
    }

    public int getSegmentSize()
    {
        return segments.size();
    }

    public List<SegmentMetadata> segments()
    {
        return metadatas;
    }

    public Version getVersion()
    {
        return version;
    }

    /**
     * container to share per-index file handles(kdtree, terms data, posting lists) among segments.
     */
    public static class PerIndexFiles implements Closeable
    {
        private final Map<IndexComponent, FileHandle> files = new HashMap<>(2);
        private final IndexComponents components;

        public PerIndexFiles(IndexComponents components, boolean isStringIndex)
        {
            this(components, isStringIndex, false);
        }

        public PerIndexFiles(IndexComponents components, boolean isStringIndex, boolean temporary)
        {
            this.components = components;
            if (isStringIndex)
            {
                files.put(components.postingLists, components.createFileHandle(components.postingLists, temporary));
                files.put(components.termsData, components.createFileHandle(components.termsData, temporary));
            }
            else
            {
                files.put(components.kdTree, components.createFileHandle(components.kdTree, temporary));
                files.put(components.kdTreePostingLists, components.createFileHandle(components.kdTreePostingLists, temporary));
            }
        }

        public FileHandle kdtree()
        {
            return getFile(components.kdTree);
        }

        public FileHandle postingLists()
        {
            return getFile(components.postingLists);
        }

        public FileHandle termsData()
        {
            return getFile(components.termsData);
        }

        public FileHandle kdtreePostingLists()
        {
            return getFile(components.kdTreePostingLists);
        }

        private FileHandle getFile(IndexComponent type)
        {
            FileHandle file = files.get(type);
            if (file == null)
                throw new IllegalArgumentException(String.format("Component %s not found for SSTable %s", type.name, components.descriptor));

            return file;
        }

        public IndexComponents components()
        {
            return this.components;
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(files.values());
        }
    }

    public SSTableReader getSSTable()
    {
        return sstable;
    }

    public boolean reference()
    {
        while (true)
        {
            int n = references.get();
            if (n <= 0)
                return false;
            if (references.compareAndSet(n, n + 1))
            {
                return true;
            }
        }
    }

    public boolean isReleased()
    {
        return references.get() <= 0;
    }

    public void release()
    {
        int n = references.decrementAndGet();

        if (n == 0)
        {
            FileUtils.closeQuietly(indexFiles);
            FileUtils.closeQuietly(segments);
            sstableContext.close();

            /*
             * When SSTable is removed, storage-attached index components will be automatically removed by LogTransaction.
             * We only remove index components explicitly in case of index corruption or index rebuild.
             */
            if (obsolete.get())
            {
                components.deleteColumnIndex();
            }
        }
    }

    public void markObsolete()
    {
        obsolete.getAndSet(true);
        release();
    }

    public boolean equals(Object o)
    {
        return o instanceof SSTableIndex && components.equals(((SSTableIndex) o).components);
    }

    public int hashCode()
    {
        return new HashCodeBuilder().append(components.hashCode()).build();
    }

    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("column", columnContext.getColumnName())
                          .add("sstable", sstable.descriptor)
                          .add("totalRows", sstable.getTotalRows())
                          .toString();
    }
}
