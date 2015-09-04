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
package org.apache.cassandra.db.rows;

import java.util.*;
import java.security.MessageDigest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.AbstractIterator;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.IMergeIterator;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.memory.AbstractAllocator;

/**
 * Static methods to work with atom iterators.
 */
public abstract class UnfilteredRowIterators
{
    private static final Logger logger = LoggerFactory.getLogger(UnfilteredRowIterators.class);

    private UnfilteredRowIterators() {}

    public interface MergeListener
    {
        public void onMergedPartitionLevelDeletion(DeletionTime mergedDeletion, DeletionTime[] versions);

        public void onMergedRows(Row merged, Row[] versions);
        public void onMergedRangeTombstoneMarkers(RangeTombstoneMarker merged, RangeTombstoneMarker[] versions);

        public void close();
    }

    /**
     * Returns a iterator that only returns rows with only live content.
     *
     * This is mainly used in the CQL layer when we know we don't care about deletion
     * infos (and since an UnfilteredRowIterator cannot shadow it's own data, we know everyting
     * returned isn't shadowed by a tombstone).
     */
    public static RowIterator filter(UnfilteredRowIterator iter, int nowInSec)
    {
        return new FilteringIterator(iter, nowInSec);

    }

    /**
     * Returns an iterator that is the result of merging other iterators.
     */
    public static UnfilteredRowIterator merge(List<UnfilteredRowIterator> iterators, int nowInSec)
    {
        assert !iterators.isEmpty();
        if (iterators.size() == 1)
            return iterators.get(0);

        return UnfilteredRowMergeIterator.create(iterators, nowInSec, null);
    }

    /**
     * Returns an iterator that is the result of merging other iterators, and (optionally) using
     * specific MergeListener.
     *
     * Note that this method assumes that there is at least 2 iterators to merge.
     */
    public static UnfilteredRowIterator merge(List<UnfilteredRowIterator> iterators, int nowInSec, MergeListener mergeListener)
    {
        return UnfilteredRowMergeIterator.create(iterators, nowInSec, mergeListener);
    }

    public static UnfilteredRowIterator emptyIterator(final CFMetaData cfm, final DecoratedKey partitionKey, final boolean isReverseOrder)
    {
        return noRowsIterator(cfm, partitionKey, Rows.EMPTY_STATIC_ROW, DeletionTime.LIVE, isReverseOrder);
    }
    /**
     * Returns an empty atom iterator for a given partition.
     */
    public static UnfilteredRowIterator noRowsIterator(final CFMetaData cfm, final DecoratedKey partitionKey, final Row staticRow, final DeletionTime partitionDeletion, final boolean isReverseOrder)
    {
        PartitionColumns columns = staticRow == null ? PartitionColumns.NONE
                                                     : new PartitionColumns(Columns.from(staticRow.columns()), Columns.NONE);
        return new UnfilteredRowIterator()
        {
            public CFMetaData metadata()
            {
                return cfm;
            }

            public boolean isReverseOrder()
            {
                return isReverseOrder;
            }

            public PartitionColumns columns()
            {
                return columns;
            }

            public DecoratedKey partitionKey()
            {
                return partitionKey;
            }

            public DeletionTime partitionLevelDeletion()
            {
                return partitionDeletion;
            }

            public Row staticRow()
            {
                return staticRow;
            }

            public EncodingStats stats()
            {
                return EncodingStats.NO_STATS;
            }

            public boolean hasNext()
            {
                return false;
            }

            public Unfiltered next()
            {
                throw new NoSuchElementException();
            }

            public void remove()
            {
            }

            public void close()
            {
            }
        };
    }

    /**
     * Digests the partition represented by the provided iterator.
     *
     * @param iterator the iterator to digest.
     * @param digest the {@code MessageDigest} to use for the digest.
     * @param version the messaging protocol to use when producing the digest.
     */
    public static void digest(UnfilteredRowIterator iterator, MessageDigest digest, int version)
    {
        if (version < MessagingService.VERSION_30)
        {
            LegacyLayout.fromUnfilteredRowIterator(iterator).digest(iterator.metadata(), digest);
            return;
        }

        digest.update(iterator.partitionKey().getKey().duplicate());
        iterator.partitionLevelDeletion().digest(digest);
        iterator.columns().digest(digest);
        FBUtilities.updateWithBoolean(digest, iterator.isReverseOrder());
        iterator.staticRow().digest(digest);

        while (iterator.hasNext())
        {
            Unfiltered unfiltered = iterator.next();
            unfiltered.digest(digest);
        }
    }

    /**
     * Returns an iterator that concatenate two atom iterators.
     * This method assumes that both iterator are from the same partition and that the atom from
     * {@code iter2} come after the ones of {@code iter1} (that is, that concatenating the iterator
     * make sense).
     */
    public static UnfilteredRowIterator concat(final UnfilteredRowIterator iter1, final UnfilteredRowIterator iter2)
    {
        assert iter1.metadata().cfId.equals(iter2.metadata().cfId)
            && iter1.partitionKey().equals(iter2.partitionKey())
            && iter1.partitionLevelDeletion().equals(iter2.partitionLevelDeletion())
            && iter1.isReverseOrder() == iter2.isReverseOrder()
            && iter1.columns().equals(iter2.columns())
            && iter1.staticRow().equals(iter2.staticRow());

        return new AbstractUnfilteredRowIterator(iter1.metadata(),
                                                 iter1.partitionKey(),
                                                 iter1.partitionLevelDeletion(),
                                                 iter1.columns(),
                                                 iter1.staticRow(),
                                                 iter1.isReverseOrder(),
                                                 iter1.stats())
        {
            protected Unfiltered computeNext()
            {
                if (iter1.hasNext())
                    return iter1.next();

                return iter2.hasNext() ? iter2.next() : endOfData();
            }

            @Override
            public void close()
            {
                try
                {
                    iter1.close();
                }
                finally
                {
                    iter2.close();
                }
            }
        };
    }

    public static UnfilteredRowIterator cloningIterator(UnfilteredRowIterator iterator, final AbstractAllocator allocator)
    {
        return new AlteringUnfilteredRowIterator(iterator)
        {
            private Row.Builder regularBuilder;

            @Override
            protected Row computeNextStatic(Row row)
            {
                Row.Builder staticBuilder = allocator.cloningBTreeRowBuilder();
                return Rows.copy(row, staticBuilder).build();
            }

            @Override
            protected Row computeNext(Row row)
            {
                if (regularBuilder == null)
                    regularBuilder = allocator.cloningBTreeRowBuilder();

                return Rows.copy(row, regularBuilder).build();
            }

            @Override
            protected RangeTombstoneMarker computeNext(RangeTombstoneMarker marker)
            {
                return marker.copy(allocator);
            }
        };
    }

    /**
     * Validate that the data of the provided iterator is valid, that is that the values
     * it contains are valid for the type they represent, and more generally that the
     * infos stored are sensible.
     *
     * This is mainly used by scrubber to detect problems in sstables.
     *
     * @param iterator the partition to check.
     * @param filename the name of the file the data is comming from.
     * @return an iterator that returns the same data than {@code iterator} but that
     * checks said data and throws a {@code CorruptedSSTableException} if it detects
     * invalid data.
     */
    public static UnfilteredRowIterator withValidation(UnfilteredRowIterator iterator, final String filename)
    {
        return new AlteringUnfilteredRowIterator(iterator)
        {
            @Override
            protected Row computeNextStatic(Row row)
            {
                validate(row);
                return row;
            }

            @Override
            protected Row computeNext(Row row)
            {
                validate(row);
                return row;
            }

            @Override
            protected RangeTombstoneMarker computeNext(RangeTombstoneMarker marker)
            {
                validate(marker);
                return marker;
            }

            private void validate(Unfiltered unfiltered)
            {
                try
                {
                    unfiltered.validateData(iterator.metadata());
                }
                catch (MarshalException me)
                {
                    throw new CorruptSSTableException(me, filename);
                }
            }
        };
    }

    /**
     * Wraps the provided iterator so it logs the returned atoms for debugging purposes.
     * <p>
     * Note that this is only meant for debugging as this can log a very large amount of
     * logging at INFO.
     */
    public static UnfilteredRowIterator loggingIterator(UnfilteredRowIterator iterator, final String id, final boolean fullDetails)
    {
        CFMetaData metadata = iterator.metadata();
        logger.info("[{}] Logging iterator on {}.{}, partition key={}, reversed={}, deletion={}",
                    id,
                    metadata.ksName,
                    metadata.cfName,
                    metadata.getKeyValidator().getString(iterator.partitionKey().getKey()),
                    iterator.isReverseOrder(),
                    iterator.partitionLevelDeletion().markedForDeleteAt());

        return new AlteringUnfilteredRowIterator(iterator)
        {
            @Override
            protected Row computeNextStatic(Row row)
            {
                if (!row.isEmpty())
                    logger.info("[{}] {}", id, row.toString(metadata(), fullDetails));
                return row;
            }

            @Override
            protected Row computeNext(Row row)
            {
                logger.info("[{}] {}", id, row.toString(metadata(), fullDetails));
                return row;
            }

            @Override
            protected RangeTombstoneMarker computeNext(RangeTombstoneMarker marker)
            {
                logger.info("[{}] {}", id, marker.toString(metadata()));
                return marker;
            }
        };
    }

    /**
     * A wrapper over MergeIterator to implement the UnfilteredRowIterator interface.
     */
    private static class UnfilteredRowMergeIterator extends AbstractUnfilteredRowIterator
    {
        private final IMergeIterator<Unfiltered, Unfiltered> mergeIterator;
        private final MergeListener listener;

        private UnfilteredRowMergeIterator(CFMetaData metadata,
                                           List<UnfilteredRowIterator> iterators,
                                           PartitionColumns columns,
                                           DeletionTime partitionDeletion,
                                           int nowInSec,
                                           boolean reversed,
                                           MergeListener listener)
        {
            super(metadata,
                  iterators.get(0).partitionKey(),
                  partitionDeletion,
                  columns,
                  mergeStaticRows(iterators, columns.statics, nowInSec, listener, partitionDeletion),
                  reversed,
                  mergeStats(iterators));

            this.mergeIterator = MergeIterator.get(iterators,
                                                   reversed ? metadata.comparator.reversed() : metadata.comparator,
                                                   new MergeReducer(iterators.size(), reversed, nowInSec, listener));
            this.listener = listener;
        }

        private static UnfilteredRowMergeIterator create(List<UnfilteredRowIterator> iterators, int nowInSec, MergeListener listener)
        {
            try
            {
                checkForInvalidInput(iterators);
                return new UnfilteredRowMergeIterator(iterators.get(0).metadata(),
                                                      iterators,
                                                      collectColumns(iterators),
                                                      collectPartitionLevelDeletion(iterators, listener),
                                                      nowInSec,
                                                      iterators.get(0).isReverseOrder(),
                                                      listener);
            }
            catch (RuntimeException | Error e)
            {
                try
                {
                    FBUtilities.closeAll(iterators);
                }
                catch (Exception suppressed)
                {
                    e.addSuppressed(suppressed);
                }
                throw e;
            }
        }

        @SuppressWarnings("resource") // We're not really creating any resource here
        private static void checkForInvalidInput(List<UnfilteredRowIterator> iterators)
        {
            if (iterators.isEmpty())
                return;

            UnfilteredRowIterator first = iterators.get(0);
            for (int i = 1; i < iterators.size(); i++)
            {
                UnfilteredRowIterator iter = iterators.get(i);
                assert first.metadata().cfId.equals(iter.metadata().cfId);
                assert first.partitionKey().equals(iter.partitionKey());
                assert first.isReverseOrder() == iter.isReverseOrder();
            }
        }

        @SuppressWarnings("resource") // We're not really creating any resource here
        private static DeletionTime collectPartitionLevelDeletion(List<UnfilteredRowIterator> iterators, MergeListener listener)
        {
            DeletionTime[] versions = listener == null ? null : new DeletionTime[iterators.size()];

            DeletionTime delTime = DeletionTime.LIVE;
            for (int i = 0; i < iterators.size(); i++)
            {
                UnfilteredRowIterator iter = iterators.get(i);
                DeletionTime iterDeletion = iter.partitionLevelDeletion();
                if (listener != null)
                    versions[i] = iterDeletion;
                if (!delTime.supersedes(iterDeletion))
                    delTime = iterDeletion;
            }
            if (listener != null && !delTime.isLive())
                listener.onMergedPartitionLevelDeletion(delTime, versions);
            return delTime;
        }

        private static Row mergeStaticRows(List<UnfilteredRowIterator> iterators,
                                           Columns columns,
                                           int nowInSec,
                                           MergeListener listener,
                                           DeletionTime partitionDeletion)
        {
            if (columns.isEmpty())
                return Rows.EMPTY_STATIC_ROW;

            if (iterators.stream().allMatch(iter -> iter.staticRow().isEmpty()))
                return Rows.EMPTY_STATIC_ROW;

            Row.Merger merger = new Row.Merger(iterators.size(), nowInSec, columns.hasComplex());
            for (int i = 0; i < iterators.size(); i++)
                merger.add(i, iterators.get(i).staticRow());

            Row merged = merger.merge(partitionDeletion);
            if (merged == null)
                merged = Rows.EMPTY_STATIC_ROW;
            if (listener != null)
                listener.onMergedRows(merged, merger.mergedRows());
            return merged;
        }

        private static PartitionColumns collectColumns(List<UnfilteredRowIterator> iterators)
        {
            PartitionColumns first = iterators.get(0).columns();
            Columns statics = first.statics;
            Columns regulars = first.regulars;
            for (int i = 1; i < iterators.size(); i++)
            {
                PartitionColumns cols = iterators.get(i).columns();
                statics = statics.mergeTo(cols.statics);
                regulars = regulars.mergeTo(cols.regulars);
            }
            return statics == first.statics && regulars == first.regulars
                 ? first
                 : new PartitionColumns(statics, regulars);
        }

        private static EncodingStats mergeStats(List<UnfilteredRowIterator> iterators)
        {
            EncodingStats stats = EncodingStats.NO_STATS;
            for (UnfilteredRowIterator iter : iterators)
                stats = stats.mergeWith(iter.stats());
            return stats;
        }

        protected Unfiltered computeNext()
        {
            while (mergeIterator.hasNext())
            {
                Unfiltered merged = mergeIterator.next();
                if (merged != null)
                    return merged;
            }
            return endOfData();
        }

        public void close()
        {
            // This will close the input iterators
            FileUtils.closeQuietly(mergeIterator);

            if (listener != null)
                listener.close();
        }

        private class MergeReducer extends MergeIterator.Reducer<Unfiltered, Unfiltered>
        {
            private final MergeListener listener;

            private Unfiltered.Kind nextKind;

            private final Row.Merger rowMerger;
            private final RangeTombstoneMarker.Merger markerMerger;

            private MergeReducer(int size, boolean reversed, int nowInSec, MergeListener listener)
            {
                this.rowMerger = new Row.Merger(size, nowInSec, columns().regulars.hasComplex());
                this.markerMerger = new RangeTombstoneMarker.Merger(size, partitionLevelDeletion(), reversed);
                this.listener = listener;
            }

            @Override
            public boolean trivialReduceIsTrivial()
            {
                // If we have a listener, we must signal it even when we have a single version
                return listener == null;
            }

            public void reduce(int idx, Unfiltered current)
            {
                nextKind = current.kind();
                if (nextKind == Unfiltered.Kind.ROW)
                    rowMerger.add(idx, (Row)current);
                else
                    markerMerger.add(idx, (RangeTombstoneMarker)current);
            }

            protected Unfiltered getReduced()
            {
                if (nextKind == Unfiltered.Kind.ROW)
                {
                    Row merged = rowMerger.merge(markerMerger.activeDeletion());
                    if (listener != null)
                        listener.onMergedRows(merged == null ? BTreeRow.emptyRow(rowMerger.mergedClustering()) : merged, rowMerger.mergedRows());
                    return merged;
                }
                else
                {
                    RangeTombstoneMarker merged = markerMerger.merge();
                    if (merged != null && listener != null)
                        listener.onMergedRangeTombstoneMarkers(merged, markerMerger.mergedMarkers());
                    return merged;
                }
            }

            protected void onKeyChange()
            {
                if (nextKind == Unfiltered.Kind.ROW)
                    rowMerger.clear();
                else
                    markerMerger.clear();
            }
        }
    }

    private static class FilteringIterator extends AbstractIterator<Row> implements RowIterator
    {
        private final UnfilteredRowIterator iter;
        private final int nowInSec;

        public FilteringIterator(UnfilteredRowIterator iter, int nowInSec)
        {
            this.iter = iter;
            this.nowInSec = nowInSec;
        }

        public CFMetaData metadata()
        {
            return iter.metadata();
        }

        public boolean isReverseOrder()
        {
            return iter.isReverseOrder();
        }

        public PartitionColumns columns()
        {
            return iter.columns();
        }

        public DecoratedKey partitionKey()
        {
            return iter.partitionKey();
        }

        public Row staticRow()
        {
            Row row = iter.staticRow();
            if (row.isEmpty())
                return Rows.EMPTY_STATIC_ROW;

            row = row.purge(DeletionPurger.PURGE_ALL, nowInSec);
            return row == null ? Rows.EMPTY_STATIC_ROW : row;
        }

        protected Row computeNext()
        {
            while (iter.hasNext())
            {
                Unfiltered next = iter.next();
                if (next.isRangeTombstoneMarker())
                    continue;

                Row row = ((Row)next).purge(DeletionPurger.PURGE_ALL, nowInSec);
                if (row != null)
                    return row;
            }
            return endOfData();
        }

        public void close()
        {
            iter.close();
        }
    }
}
