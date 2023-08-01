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

import java.util.Comparator;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;
import org.apache.cassandra.utils.caching.TinyThreadLocalPool;
import org.apache.cassandra.utils.memory.Cloner;

/**
 * Generic interface for the data of a given column (inside a row).
 *
 * In practice, there is only 2 implementations of this: either {@link Cell} for simple columns
 * or {@code ComplexColumnData} for complex columns.
 */
public abstract class ColumnData implements IMeasurableMemory
{
    public static final Comparator<ColumnData> comparator = (cd1, cd2) -> cd1.column().compareTo(cd2.column());


    /**
     * Construct an UpdateFunction for reconciling normal ColumnData
     * (i.e. not suitable for ComplexColumnDeletion sentinels, but suitable ComplexColumnData or Cell)
     *
     * @param updateF a consumer receiving all pairs of reconciled cells
     * @param activeDeletion the row or partition deletion time to use for purging
     */
    public static Reconciler reconciler(PostReconciliationFunction updateF, DeletionTime activeDeletion)
    {
        TinyThreadLocalPool.TinyPool<Reconciler> pool = Reconciler.POOL.get();
        Reconciler reconciler = pool.poll();
        if (reconciler == null)
            reconciler = new Reconciler();
        reconciler.init(updateF, activeDeletion);
        reconciler.pool = pool;
        return reconciler;
    }

    public static PostReconciliationFunction noOp = new PostReconciliationFunction()
    {
        @Override
        public Cell<?> merge(Cell<?> previous, Cell<?> insert)
        {
            return insert;
        }

        @Override
        public ColumnData insert(ColumnData insert)
        {
            return insert;
        }

        @Override
        public void delete(ColumnData existing)
        {
        }

        public void onAllocatedOnHeap(long delta)
        {
        }
    };

    public interface PostReconciliationFunction
    {

        ColumnData insert(ColumnData insert);

        Cell<?> merge(Cell<?> previous, Cell<?> insert);

        void delete(ColumnData existing);

        void onAllocatedOnHeap(long delta);
    }

    public static class Reconciler implements UpdateFunction<ColumnData, ColumnData>, AutoCloseable
    {
        private static final TinyThreadLocalPool<Reconciler> POOL = new TinyThreadLocalPool<>();
        private PostReconciliationFunction postReconcile;
        private DeletionTime activeDeletion;
        private TinyThreadLocalPool.TinyPool<Reconciler> pool;

        private void init(PostReconciliationFunction postReconcile, DeletionTime activeDeletion)
        {
            this.postReconcile = postReconcile;
            this.activeDeletion = activeDeletion;
        }

        public ColumnData merge(ColumnData existing, ColumnData update)
        {
            if (!(existing instanceof ComplexColumnData))
            {
                Cell<?> existingCell = (Cell<?>) existing, updateCell = (Cell<?>) update;
                Cell<?> result = Cells.reconcile(existingCell, updateCell);

                return postReconcile.merge(existingCell, result);
            }
            else
            {
                ComplexColumnData existingComplex = (ComplexColumnData) existing;
                ComplexColumnData updateComplex = (ComplexColumnData) update;

                DeletionTime existingDeletion = existingComplex.complexDeletion();
                DeletionTime updateDeletion = updateComplex.complexDeletion();
                DeletionTime maxComplexDeletion = existingDeletion.supersedes(updateDeletion) ? existingDeletion : updateDeletion;

                Object[] existingTree = existingComplex.tree();
                Object[] updateTree = updateComplex.tree();

                Object[] cells;

                try (Reconciler reconciler = reconciler(postReconcile, maxComplexDeletion))
                {
                    if (!maxComplexDeletion.isLive())
                    {
                        if (maxComplexDeletion == existingDeletion)
                        {
                            updateTree = BTree.<ColumnData, ColumnData>transformAndFilter(updateTree, reconciler::removeShadowed);
                        }
                        else
                        {
                            Object[] retained = BTree.transformAndFilter(existingTree, reconciler::retain);
                            if (existingTree != retained)
                            {
                                onAllocatedOnHeap(BTree.sizeOnHeapOf(retained) - BTree.sizeOnHeapOf(existingTree));
                                existingTree = retained;
                            }
                        }
                    }
                    cells = BTree.update(existingTree, updateTree, existingComplex.column.cellComparator(), (UpdateFunction) reconciler);
                }
                return new ComplexColumnData(existingComplex.column, cells, maxComplexDeletion);
            }
        }

        @Override
        public void onAllocatedOnHeap(long heapSize)
        {
            postReconcile.onAllocatedOnHeap(heapSize);
        }

        @Override
        public ColumnData insert(ColumnData insert)
        {
            return postReconcile.insert(insert);
        }

        /**
         * Checks if the specified value  should be deleted or not.
         *
         * @param existing the existing value to check
         * @return {@code null} if the value should be removed from the BTree or the existing value if it should not.
         */
        public ColumnData retain(ColumnData existing)
        {
            return removeShadowed(existing, postReconcile);
        }

        private ColumnData removeShadowed(ColumnData existing)
        {
            return removeShadowed(existing, ColumnData.noOp);
        }

        /**
         * Checks if the specified value  should be deleted or not.
         *
         * @param existing the existing value to check
         * @return {@code null} if the value should be removed from the BTree or the existing value if it should not.
         */
        private ColumnData removeShadowed(ColumnData existing, PostReconciliationFunction recordDeletion)
        {
            if (!(existing instanceof ComplexColumnData))
            {
                if (activeDeletion.deletes((Cell<?>) existing))
                {
                    recordDeletion.delete(existing);
                    return null;
                }
            }
            else
            {
                ComplexColumnData existingComplex = (ComplexColumnData) existing;
                if (activeDeletion.supersedes(existingComplex.complexDeletion()))
                {
                    Object[] cells = BTree.transformAndFilter(existingComplex.tree(), (ColumnData cd) -> removeShadowed(cd, recordDeletion));
                    return BTree.isEmpty(cells) ? null : new ComplexColumnData(existingComplex.column, cells, DeletionTime.LIVE);
                }
            }

            return existing;
        }

        public void close()
        {
            activeDeletion = null;
            postReconcile = null;

            TinyThreadLocalPool.TinyPool<Reconciler> tmp = pool;
            pool = null;
            tmp.offer(this);

        }
    }

    protected final ColumnMetadata column;
    protected ColumnData(ColumnMetadata column)
    {
        this.column = column;
    }

    /**
     * The column this is data for.
     *
     * @return the column this is a data for.
     */
    public final ColumnMetadata column() { return column; }

    /**
     * The size of the data hold by this {@code ColumnData}.
     *
     * @return the size used by the data of this {@code ColumnData}.
     */
    public abstract int dataSize();

    public abstract long unsharedHeapSizeExcludingData();

    public abstract long unsharedHeapSize();

    /**
     * Validate the column data.
     *
     * @throws MarshalException if the data is not valid.
     */
    public abstract void validate();

    /**
     * Validates the deletions (ttl and local deletion time) if any.
     *
     * @return true if it has any invalid deletions, false otherwise
     */
    public abstract boolean hasInvalidDeletions();

    /**
     * Adds the data to the provided digest.
     *
     * @param digest the {@link Digest} to add the data to.
     */
    public abstract void digest(Digest digest);

    public static void digest(Digest digest, ColumnData cd)
    {
        cd.digest(digest);
    }

    public abstract ColumnData clone(Cloner cloner);

    /**
     * Returns a copy of the data where all timestamps for live data have replaced by {@code newTimestamp} and
     * all deletion timestamp by {@code newTimestamp - 1}.
     *
     * This exists for the Paxos path, see {@link PartitionUpdate#updateAllTimestamp} for additional details.
     */
    public abstract ColumnData updateAllTimestamp(long newTimestamp);

    public abstract ColumnData markCounterLocalToBeCleared();

    public abstract ColumnData purge(DeletionPurger purger, long nowInSec);
    public abstract ColumnData purgeDataOlderThan(long timestamp);

    public abstract long maxTimestamp();
}
