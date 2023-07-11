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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.disk.SearchableIndex;
import org.apache.cassandra.index.sai.disk.format.IndexFeatureSet;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.SegmentOrdering;
import org.apache.cassandra.io.sstable.SSTableIdFactory;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;

/**
 * SSTableIndex is created for each column index on individual sstable to track per-column indexer.
 */
public class SSTableIndex implements SegmentOrdering
{
    // sort sstable index by first key then last key
    public static final Comparator<SSTableIndex> COMPARATOR = Comparator.comparing((SSTableIndex s) -> s.getSSTable().first)
                                                                        .thenComparing(s -> s.getSSTable().last)
                                                                        .thenComparing(s -> s.getSSTable().descriptor.id, SSTableIdFactory.COMPARATOR);

    private final SSTableContext sstableContext;
    private final IndexContext indexContext;
    private final SSTableReader sstable;
    private final SearchableIndex searchableIndex;

    private final AtomicInteger references = new AtomicInteger(1);
    private final AtomicBoolean obsolete = new AtomicBoolean(false);

    public SSTableIndex(SSTableContext sstableContext, IndexContext indexContext)
    {
        assert indexContext.getValidator() != null;
        this.searchableIndex = sstableContext.indexDescriptor.newSearchableIndex(sstableContext, indexContext);

        this.sstableContext = sstableContext.sharedCopy(); // this line must not be before any code that may throw
        this.indexContext = indexContext;
        this.sstable = sstableContext.sstable;
    }

    public IndexContext getIndexContext()
    {
        return indexContext;
    }

    public SSTableContext getSSTableContext()
    {
        return sstableContext;
    }

    public long indexFileCacheSize()
    {
        return searchableIndex.indexFileCacheSize();
    }

    /**
     * @return number of indexed rows, note that rows may have been updated or removed in sstable.
     */
    public long getRowCount()
    {
        return searchableIndex.getRowCount();
    }

    /**
     * @return total size of per-column index components, in bytes
     */
    public long sizeOfPerColumnComponents()
    {
        return sstableContext.indexDescriptor.sizeOnDiskOfPerIndexComponents(indexContext);
    }

    /**
     * @return the smallest possible sstable row id in this index.
     */
    public long minSSTableRowId()
    {
        return searchableIndex.minSSTableRowId();
    }

    /**
     * @return the largest possible sstable row id in this index.
     */
    public long maxSSTableRowId()
    {
        return searchableIndex.maxSSTableRowId();
    }

    public ByteBuffer minTerm()
    {
        return searchableIndex.minTerm();
    }

    public ByteBuffer maxTerm()
    {
        return searchableIndex.maxTerm();
    }

    public DecoratedKey minKey()
    {
        return searchableIndex.minKey();
    }

    public DecoratedKey maxKey()
    {
        return searchableIndex.maxKey();
    }

    public List<RangeIterator<Long>> searchSSTableRowIds(Expression expression,
                                      AbstractBounds<PartitionPosition> keyRange,
                                      QueryContext context,
                                      boolean defer,
                                      int limit) throws IOException
    {
        return searchableIndex.search(expression, keyRange, context, defer, limit);
    }

    public void populateSegmentView(SimpleDataSet dataSet)
    {
        searchableIndex.populateSystemView(dataSet, sstable);
    }

    public Version getVersion()
    {
        return sstableContext.indexDescriptor.version;
    }

    public IndexFeatureSet indexFeatureSet()
    {
        return sstableContext.indexDescriptor.version.onDiskFormat().indexFeatureSet();
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
            FileUtils.closeQuietly(searchableIndex);
            sstableContext.close();

            /*
             * When SSTable is removed, storage-attached index components will be automatically removed by LogTransaction.
             * We only remove index components explicitly in case of index corruption or index rebuild.
             */
            if (obsolete.get())
            {
                sstableContext.indexDescriptor.deleteColumnIndex(indexContext);
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
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SSTableIndex other = (SSTableIndex)o;
        return Objects.equal(sstableContext, other.sstableContext) && Objects.equal(indexContext, other.indexContext);
    }

    public int hashCode()
    {
        return Objects.hashCode(sstableContext, indexContext);
    }

    @Override
    public RangeIterator<PrimaryKey> limitToTopResults(QueryContext context, RangeIterator<Long> iterator, Expression exp, int limit) throws IOException
    {
        return searchableIndex.limitToTopResults(context, iterator, exp, limit);
    }

    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("column", indexContext.getColumnName())
                          .add("sstable", sstable.descriptor)
                          .add("totalRows", sstable.getTotalRows())
                          .toString();
    }
}
