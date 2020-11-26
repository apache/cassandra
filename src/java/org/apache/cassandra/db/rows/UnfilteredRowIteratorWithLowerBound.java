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

import java.io.IOException;
import java.util.Comparator;
import javax.annotation.Nonnull;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.transform.RTBoundValidator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReadsListener;
import org.apache.cassandra.io.sstable.format.big.BigTableRowIndexEntry;
import org.apache.cassandra.io.sstable.format.big.IndexInfo;
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
    private Unfiltered lowerBoundMarker;
    private boolean firstItemRetrieved;

    public UnfilteredRowIteratorWithLowerBound(DecoratedKey partitionKey,
                                               SSTableReader sstable,
                                               ClusteringIndexFilter filter,
                                               ColumnFilter selectedColumns,
                                               SSTableReadsListener listener)
    {
        this(partitionKey, sstable, filter.getSlices(sstable.metadata()), filter.isReversed(), selectedColumns, listener);
    }

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
            return lowerBoundMarker;

        // The partition index lower bound is more accurate than the sstable metadata lower bound but it is only
        // present if the iterator has already been initialized
        ClusteringBound<?> lowerBound = getKeyCacheLowerBound();

        if (lowerBound == null && canUseMetadataLowerBound())
            // If we coudn't get the lower bound from key cache, we try with metadata
            lowerBound = getMetadataLowerBound();

        lowerBoundMarker = makeBound(lowerBound);
        return lowerBoundMarker;
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
        @SuppressWarnings("resource") // 'iter' is added to iterators which is closed on exception, or through the closing of the final merged iterator
        UnfilteredRowIterator iter = RTBoundValidator.validate(
            sstable.iterator(partitionKey(), slices, selectedColumns, isReverseOrder, listener),
            RTBoundValidator.Stage.SSTABLE,
            false
        );
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
        if (lowerBoundMarker != null && ret != null)
            assert comparator().compare(lowerBoundMarker.clustering(), ret.clustering()) <= 0
                : String.format("Lower bound [%s ]is bigger than first returned value [%s] for sstable %s",
                                lowerBoundMarker.clustering().toString(metadata()),
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

    private static <V> ClusteringBound<V> createArtificialLowerBound(boolean isReversed, ClusteringPrefix<V> from)
    {
        return !isReversed
               ? from.accessor().factory().inclusiveOpen(false, from.getRawValues()).artificialLowerBound()
               : from.accessor().factory().inclusiveOpen(true, from.getRawValues()).artificialUpperBound();
    }

    /**
     * @return the lower bound stored on the index entry for this partition, if available.
     */
    private ClusteringBound<?> getKeyCacheLowerBound()
    {
        BigTableRowIndexEntry rowIndexEntry = sstable.getCachedPosition(partitionKey(), false);
        if (rowIndexEntry == null || !rowIndexEntry.indexOnHeap())
            return null;

        try (BigTableRowIndexEntry.IndexInfoRetriever onHeapRetriever = rowIndexEntry.openWithIndex(null))
        {
            IndexInfo column = onHeapRetriever.columnsIndex(isReverseOrder ? rowIndexEntry.columnsIndexCount() - 1 : 0);
            ClusteringPrefix<?> lowerBoundPrefix = isReverseOrder ? column.lastName : column.firstName;

            assert lowerBoundPrefix.getRawValues().length <= metadata().comparator.size() :
            String.format("Unexpected number of clustering values %d, expected %d or fewer for %s",
                          lowerBoundPrefix.getRawValues().length,
                          metadata().comparator.size(),
                          sstable.getFilename());

            return createArtificialLowerBound(isReverseOrder, lowerBoundPrefix);
        }
        catch (IOException e)
        {
            throw new RuntimeException("should never occur", e);
        }
    }

    /**
     * Whether we can use the clustering values in the stats of the sstable to build the lower bound.
     * <p>
     * Currently, the clustering values of the stats file records for each clustering component the min and max
     * value seen, null excluded. In other words, having a non-null value for a component in those min/max clustering
     * values does _not_ guarantee that there isn't an unfiltered in the sstable whose clustering has either no value for
     * that component (it's a prefix) or a null value.
     * <p>
     * This is problematic as this means we can't in general build a lower bound from those values since the "min"
     * values doesn't actually guarantee minimality.
     * <p>
     * However, we can use those values if we can guarantee that no clustering in the sstable 1) is a true prefix and
     * 2) uses null values. Nat having true prefixes means having no range tombstone markers since rows use
     * {@link Clustering} which is always "full" (all components are always present). As for null values, we happen to
     * only allow those in compact tables (for backward compatibility), so we can simply exclude those tables.
     * <p>
     * Note that the information we currently have at our disposal make this condition less precise that it could be.
     * In particular, {@link SSTableReader#mayHaveTombstones} could return {@code true} (making us not use the stats)
     * because of cell tombstone or even expiring cells even if the sstable has no range tombstone markers, even though
     * it's really only markers we want to exclude here (more precisely, as said above, we want to exclude anything
     * whose clustering is not "full", but that's only markers). It wouldn't be very hard to collect whether a sstable
     * has any range tombstone marker however so it's a possible improvement.
     */
    private boolean canUseMetadataLowerBound()
    {
        if (sstable.metadata().isCompactTable())
            return false;

        Slices requestedSlices = slices;

        if (requestedSlices.isEmpty())
            return true;

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
    private @Nonnull ClusteringBound<?> getMetadataLowerBound()
    {
        final StatsMetadata m = sstable.getSSTableMetadata();
        ClusteringBound<?> bound = m.coveredClustering.open(isReverseOrder);
        assert bound.size() <= metadata().comparator.size() :
        String.format("Unexpected number of clustering values %d, expected %d or fewer for %s",
                      bound.size(),
                      metadata().comparator.size(),
                      sstable.getFilename());
        return !isReverseOrder ? bound.artificialLowerBound() : bound.artificialUpperBound();
    }
}
