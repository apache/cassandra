/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.db.rows;

import java.util.Comparator;
import java.util.Optional;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.db.Clusterable;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.transform.RTBoundValidator;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.keycache.KeyCacheSupport;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.IteratorWithLowerBound;

/**
 * An unfiltered row iterator with a lower bound retrieved from either the global
 * sstable statistics or the row index lower bounds (if available in the cache).
 * Before initializing the sstable unfiltered row iterator, we return an empty row
 * with the clustering set to the lower bound. The empty row will be filtered out and
 * the result is that if we don't need to access this sstable, i.e. due to the LIMIT conditon,
 * then we will not. See CASSANDRA-8180 for examples of why this is useful.
 */
public class UnfilteredRowIteratorWithLowerBound extends LazilyInitializedUnfilteredRowIterator implements IteratorWithLowerBound<Unfiltered>
{
    private final SSTableReader sstable;
    private final Slices slices;
    private final boolean isReverseOrder;
    private final ColumnFilter selectedColumns;
    private final SSTableReadsListener listener;
    private Optional<Unfiltered> lowerBoundMarker;
    private boolean firstItemRetrieved;

    public UnfilteredRowIteratorWithLowerBound(DecoratedKey partitionKey,
                                               SSTableReader sstable,
                                               ClusteringIndexFilter filter,
                                               ColumnFilter selectedColumns,
                                               SSTableReadsListener listener)
    {
        this(partitionKey, sstable, filter.getSlices(sstable.metadata()), filter.isReversed(), selectedColumns, listener);
    }

    @VisibleForTesting
    public UnfilteredRowIteratorWithLowerBound(DecoratedKey partitionKey,
                                               SSTableReader sstable,
                                               Slices slices,
                                               boolean isReverseOrder,
                                               ColumnFilter selectedColumns,
                                               SSTableReadsListener listener)
    {
        super(partitionKey);
        this.sstable = sstable;
        this.slices = slices;
        this.isReverseOrder = isReverseOrder;
        this.selectedColumns = selectedColumns;
        this.listener = listener;
        this.firstItemRetrieved = false;
    }

    public Unfiltered lowerBound()
    {
        if (lowerBoundMarker != null)
            return lowerBoundMarker.orElse(null);

        // lower bound from cache may be more accurate as it stores information about clusterings range for that exact
        // row, so we try it first (without initializing iterator)
        ClusteringBound<?> lowerBound = maybeGetLowerBoundFromKeyCache();
        if (lowerBound == null)
            // If we couldn't get the lower bound from cache, we try with metadata
            lowerBound = maybeGetLowerBoundFromMetadata();

        if (lowerBound != null)
            lowerBoundMarker = Optional.of(makeBound(lowerBound));
        else
            lowerBoundMarker = Optional.empty();

        return lowerBoundMarker.orElse(null);
    }

    private Unfiltered makeBound(ClusteringBound<?> bound)
    {
        if (bound == null)
            return null;

        return new ArtificialBoundMarker(bound);
    }

    @Override
    protected UnfilteredRowIterator initializeIterator()
    {
        UnfilteredRowIterator iter = RTBoundValidator.validate(sstable.rowIterator(partitionKey(), slices, selectedColumns, isReverseOrder, listener),
                                                               RTBoundValidator.Stage.SSTABLE, false);
        return iter;
    }

    @Override
    protected Unfiltered computeNext()
    {
        Unfiltered ret = super.computeNext();
        if (firstItemRetrieved)
            return ret;

        // Check that the lower bound is not bigger than the first item retrieved
        firstItemRetrieved = true;
        Unfiltered lowerBound = lowerBound();
        if (lowerBound != null && ret != null)
            assert comparator().compare(lowerBound.clustering(), ret.clustering()) <= 0
            : String.format("Lower bound [%s ]is bigger than first returned value [%s] for sstable %s",
                            lowerBound.clustering().toString(metadata()),
                            ret.toString(metadata()),
                            sstable.getFilename());

        return ret;
    }

    private Comparator<Clusterable> comparator()
    {
        return isReverseOrder ? metadata().comparator.reversed() : metadata().comparator;
    }

    @Override
    public TableMetadata metadata()
    {
        return sstable.metadata();
    }

    @Override
    public boolean isReverseOrder()
    {
        return isReverseOrder;
    }

    @Override
    public RegularAndStaticColumns columns()
    {
        return selectedColumns.fetchedColumns();
    }

    @Override
    public EncodingStats stats()
    {
        return sstable.stats();
    }

    @Override
    public DeletionTime partitionLevelDeletion()
    {
        if (!sstable.getSSTableMetadata().hasPartitionLevelDeletions)
            return DeletionTime.LIVE;

        return super.partitionLevelDeletion();
    }

    @Override
    public Row staticRow()
    {
        if (columns().statics.isEmpty())
            return Rows.EMPTY_STATIC_ROW;

        return super.staticRow();
    }

    /**
     * @return the lower bound stored on the index entry for this partition, if available.
     */
    private ClusteringBound<?> maybeGetLowerBoundFromKeyCache()
    {
        if (sstable instanceof KeyCacheSupport<?>)
            return ((KeyCacheSupport<?>) sstable).getLowerBoundPrefixFromCache(partitionKey(), isReverseOrder);

        return null;
    }

    /**
     * Whether we can use the clustering values in the stats of the sstable to build the lower bound.
     */
    private boolean canUseMetadataLowerBound()
    {
        if (sstable.metadata().isCompactTable())
            return false;

        Slices requestedSlices = slices;

        if (requestedSlices.isEmpty())
            return true;

        // Simply exclude the cases where lower bound would not be used anyway, that is, the start of covered range of
        // clusterings in sstable is lower than the requested slice. In such case, we need to access that sstable's
        // iterator anyway so there is no need to use a lower bound optimization extra complexity.
        if (!isReverseOrder())
        {
            return !requestedSlices.hasLowerBound() ||
                   metadata().comparator.compare(requestedSlices.start(), sstable.getSSTableMetadata().coveredClustering.start()) < 0;
        }
        else
        {
            return !requestedSlices.hasUpperBound() ||
                   metadata().comparator.compare(requestedSlices.end(), sstable.getSSTableMetadata().coveredClustering.end()) > 0;
        }
    }

    /**
     * @return a global lower bound made from the clustering values stored in the sstable metadata, note that
     * this currently does not correctly compare tombstone bounds, especially ranges.
     */
    private ClusteringBound<?> maybeGetLowerBoundFromMetadata()
    {
        if (!canUseMetadataLowerBound())
            return null;

        final StatsMetadata m = sstable.getSSTableMetadata();
        ClusteringBound<?> bound = m.coveredClustering.open(isReverseOrder);
        assertBoundSize(bound, sstable);
        return bound.artificialLowerBound(isReverseOrder);
    }

    public static void assertBoundSize(ClusteringPrefix<?> lowerBound, SSTable sstable)
    {
        assert lowerBound.size() <= sstable.metadata().comparator.size() :
        String.format("Unexpected number of clustering values %d, expected %d or fewer for %s",
                      lowerBound.size(),
                      sstable.metadata().comparator.size(),
                      sstable.getFilename());
    }
}