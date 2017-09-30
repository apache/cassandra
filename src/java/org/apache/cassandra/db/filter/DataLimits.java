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
package org.apache.cassandra.db.filter;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.aggregation.GroupMaker;
import org.apache.cassandra.db.aggregation.GroupingState;
import org.apache.cassandra.db.aggregation.AggregationSpecification;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.transform.BasePartitions;
import org.apache.cassandra.db.transform.BaseRows;
import org.apache.cassandra.db.transform.StoppingTransformation;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Object in charge of tracking if we have fetch enough data for a given query.
 *
 * The reason this is not just a simple integer is that Thrift and CQL3 count
 * stuffs in different ways. This is what abstract those differences.
 */
public abstract class DataLimits
{
    public static final Serializer serializer = new Serializer();

    public static final int NO_LIMIT = Integer.MAX_VALUE;

    public static final DataLimits NONE = new CQLLimits(NO_LIMIT)
    {
        @Override
        public boolean hasEnoughLiveData(CachedPartition cached, int nowInSec, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness)
        {
            return false;
        }

        @Override
        public UnfilteredPartitionIterator filter(UnfilteredPartitionIterator iter,
                                                  int nowInSec,
                                                  boolean countPartitionsWithOnlyStaticData)
        {
            return iter;
        }

        @Override
        public UnfilteredRowIterator filter(UnfilteredRowIterator iter,
                                            int nowInSec,
                                            boolean countPartitionsWithOnlyStaticData)
        {
            return iter;
        }
    };

    // We currently deal with distinct queries by querying full partitions but limiting the result at 1 row per
    // partition (see SelectStatement.makeFilter). So an "unbounded" distinct is still actually doing some filtering.
    public static final DataLimits DISTINCT_NONE = new CQLLimits(NO_LIMIT, 1, true);

    public enum Kind { CQL_LIMIT, CQL_PAGING_LIMIT, THRIFT_LIMIT, SUPER_COLUMN_COUNTING_LIMIT, CQL_GROUP_BY_LIMIT, CQL_GROUP_BY_PAGING_LIMIT }

    public static DataLimits cqlLimits(int cqlRowLimit)
    {
        return cqlRowLimit == NO_LIMIT ? NONE : new CQLLimits(cqlRowLimit);
    }

    public static DataLimits cqlLimits(int cqlRowLimit, int perPartitionLimit)
    {
        return cqlRowLimit == NO_LIMIT && perPartitionLimit == NO_LIMIT
             ? NONE
             : new CQLLimits(cqlRowLimit, perPartitionLimit);
    }

    private static DataLimits cqlLimits(int cqlRowLimit, int perPartitionLimit, boolean isDistinct)
    {
        return cqlRowLimit == NO_LIMIT && perPartitionLimit == NO_LIMIT && !isDistinct
             ? NONE
             : new CQLLimits(cqlRowLimit, perPartitionLimit, isDistinct);
    }

    public static DataLimits groupByLimits(int groupLimit,
                                           int groupPerPartitionLimit,
                                           int rowLimit,
                                           AggregationSpecification groupBySpec)
    {
        return new CQLGroupByLimits(groupLimit, groupPerPartitionLimit, rowLimit, groupBySpec);
    }

    public static DataLimits distinctLimits(int cqlRowLimit)
    {
        return CQLLimits.distinct(cqlRowLimit);
    }

    public static DataLimits thriftLimits(int partitionLimit, int cellPerPartitionLimit)
    {
        return new ThriftLimits(partitionLimit, cellPerPartitionLimit);
    }

    public static DataLimits superColumnCountingLimits(int partitionLimit, int cellPerPartitionLimit)
    {
        return new SuperColumnCountingLimits(partitionLimit, cellPerPartitionLimit);
    }

    public abstract Kind kind();

    public abstract boolean isUnlimited();
    public abstract boolean isDistinct();

    public boolean isGroupByLimit()
    {
        return false;
    }

    public boolean isExhausted(Counter counter)
    {
        return counter.counted() < count();
    }

    public abstract DataLimits forPaging(int pageSize);
    public abstract DataLimits forPaging(int pageSize, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining);

    public abstract DataLimits forShortReadRetry(int toFetch);

    /**
     * Creates a <code>DataLimits</code> instance to be used for paginating internally GROUP BY queries.
     *
     * @param state the <code>GroupMaker</code> state
     * @return a <code>DataLimits</code> instance to be used for paginating internally GROUP BY queries
     */
    public DataLimits forGroupByInternalPaging(GroupingState state)
    {
        throw new UnsupportedOperationException();
    }

    public abstract boolean hasEnoughLiveData(CachedPartition cached,
                                              int nowInSec,
                                              boolean countPartitionsWithOnlyStaticData,
                                              boolean enforceStrictLiveness);

    /**
     * Returns a new {@code Counter} for this limits.
     *
     * @param nowInSec the current time in second (to decide what is expired or not).
     * @param assumeLiveData if true, the counter will assume that every row passed is live and won't
     * thus check for liveness, otherwise it will. This should be {@code true} when used on a
     * {@code RowIterator} (since it only returns live rows), false otherwise.
     * @param countPartitionsWithOnlyStaticData if {@code true} the partitions with only static data should be counted
     * as 1 valid row.
     * @param enforceStrictLiveness whether the row should be purged if there is no PK liveness info,
     *                              normally retrieved from {@link CFMetaData#enforceStrictLiveness()}
     * @return a new {@code Counter} for this limits.
     */
    public abstract Counter newCounter(int nowInSec,
                                       boolean assumeLiveData,
                                       boolean countPartitionsWithOnlyStaticData,
                                       boolean enforceStrictLiveness);

    /**
     * The max number of results this limits enforces.
     * <p>
     * Note that the actual definition of "results" depends a bit: for CQL, it's always rows, but for
     * thrift, it means cells.
     *
     * @return the maximum number of results this limits enforces.
     */
    public abstract int count();

    public abstract int perPartitionCount();

    /**
     * Returns equivalent limits but where any internal state kept to track where we are of paging and/or grouping is
     * discarded.
     */
    public abstract DataLimits withoutState();

    public UnfilteredPartitionIterator filter(UnfilteredPartitionIterator iter,
                                              int nowInSec,
                                              boolean countPartitionsWithOnlyStaticData)
    {
        return this.newCounter(nowInSec,
                               false,
                               countPartitionsWithOnlyStaticData,
                               iter.metadata().enforceStrictLiveness())
                   .applyTo(iter);
    }

    public UnfilteredRowIterator filter(UnfilteredRowIterator iter,
                                        int nowInSec,
                                        boolean countPartitionsWithOnlyStaticData)
    {
        return this.newCounter(nowInSec,
                               false,
                               countPartitionsWithOnlyStaticData,
                               iter.metadata().enforceStrictLiveness())
                   .applyTo(iter);
    }

    public PartitionIterator filter(PartitionIterator iter, int nowInSec, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness)
    {
        return this.newCounter(nowInSec, true, countPartitionsWithOnlyStaticData, enforceStrictLiveness).applyTo(iter);
    }

    /**
     * Estimate the number of results (the definition of "results" will be rows for CQL queries
     * and partitions for thrift ones) that a full scan of the provided cfs would yield.
     */
    public abstract float estimateTotalResults(ColumnFamilyStore cfs);

    public static abstract class Counter extends StoppingTransformation<BaseRowIterator<?>>
    {
        protected final int nowInSec;
        protected final boolean assumeLiveData;
        private final boolean enforceStrictLiveness;

        // false means we do not propagate our stop signals onto the iterator, we only count
        private boolean enforceLimits = true;

        protected Counter(int nowInSec, boolean assumeLiveData, boolean enforceStrictLiveness)
        {
            this.nowInSec = nowInSec;
            this.assumeLiveData = assumeLiveData;
            this.enforceStrictLiveness = enforceStrictLiveness;
        }

        public Counter onlyCount()
        {
            this.enforceLimits = false;
            return this;
        }

        public PartitionIterator applyTo(PartitionIterator partitions)
        {
            return Transformation.apply(partitions, this);
        }

        public UnfilteredPartitionIterator applyTo(UnfilteredPartitionIterator partitions)
        {
            return Transformation.apply(partitions, this);
        }

        public UnfilteredRowIterator applyTo(UnfilteredRowIterator partition)
        {
            return (UnfilteredRowIterator) applyToPartition(partition);
        }

        public RowIterator applyTo(RowIterator partition)
        {
            return (RowIterator) applyToPartition(partition);
        }

        /**
         * The number of results counted.
         * <p>
         * Note that the definition of "results" should be the same that for {@link #count}.
         *
         * @return the number of results counted.
         */
        public abstract int counted();

        public abstract int countedInCurrentPartition();

        /**
         * The number of rows counted.
         *
         * @return the number of rows counted.
         */
        public abstract int rowCounted();

        /**
         * The number of rows counted in the current partition.
         *
         * @return the number of rows counted in the current partition.
         */
        public abstract int rowCountedInCurrentPartition();

        public abstract boolean isDone();
        public abstract boolean isDoneForPartition();

        protected boolean isLive(Row row)
        {
            return assumeLiveData || row.hasLiveData(nowInSec, enforceStrictLiveness);
        }

        @Override
        protected BaseRowIterator<?> applyToPartition(BaseRowIterator<?> partition)
        {
            return partition instanceof UnfilteredRowIterator ? Transformation.apply((UnfilteredRowIterator) partition, this)
                                                              : Transformation.apply((RowIterator) partition, this);
        }

        // called before we process a given partition
        protected abstract void applyToPartition(DecoratedKey partitionKey, Row staticRow);

        @Override
        protected void attachTo(BasePartitions partitions)
        {
            if (enforceLimits)
                super.attachTo(partitions);
            if (isDone())
                stop();
        }

        @Override
        protected void attachTo(BaseRows rows)
        {
            if (enforceLimits)
                super.attachTo(rows);
            applyToPartition(rows.partitionKey(), rows.staticRow());
            if (isDoneForPartition())
                stopInPartition();
        }

        @Override
        public void onClose()
        {
            super.onClose();
        }
    }

    /**
     * Limits used by CQL; this counts rows.
     */
    private static class CQLLimits extends DataLimits
    {
        protected final int rowLimit;
        protected final int perPartitionLimit;

        // Whether the query is a distinct query or not.
        protected final boolean isDistinct;

        private CQLLimits(int rowLimit)
        {
            this(rowLimit, NO_LIMIT);
        }

        private CQLLimits(int rowLimit, int perPartitionLimit)
        {
            this(rowLimit, perPartitionLimit, false);
        }

        private CQLLimits(int rowLimit, int perPartitionLimit, boolean isDistinct)
        {
            this.rowLimit = rowLimit;
            this.perPartitionLimit = perPartitionLimit;
            this.isDistinct = isDistinct;
        }

        private static CQLLimits distinct(int rowLimit)
        {
            return new CQLLimits(rowLimit, 1, true);
        }

        public Kind kind()
        {
            return Kind.CQL_LIMIT;
        }

        public boolean isUnlimited()
        {
            return rowLimit == NO_LIMIT && perPartitionLimit == NO_LIMIT;
        }

        public boolean isDistinct()
        {
            return isDistinct;
        }

        public DataLimits forPaging(int pageSize)
        {
            return new CQLLimits(pageSize, perPartitionLimit, isDistinct);
        }

        public DataLimits forPaging(int pageSize, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining)
        {
            return new CQLPagingLimits(pageSize, perPartitionLimit, isDistinct, lastReturnedKey, lastReturnedKeyRemaining);
        }

        public DataLimits forShortReadRetry(int toFetch)
        {
            return new CQLLimits(toFetch, perPartitionLimit, isDistinct);
        }

        public boolean hasEnoughLiveData(CachedPartition cached, int nowInSec, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness)
        {
            // We want the number of row that are currently live. Getting that precise number forces
            // us to iterate the cached partition in general, but we can avoid that if:
            //   - The number of rows with at least one non-expiring cell is greater than what we ask,
            //     in which case we know we have enough live.
            //   - The number of rows is less than requested, in which case we  know we won't have enough.
            if (cached.rowsWithNonExpiringCells() >= rowLimit)
                return true;

            if (cached.rowCount() < rowLimit)
                return false;

            // Otherwise, we need to re-count

            DataLimits.Counter counter = newCounter(nowInSec, false, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
            try (UnfilteredRowIterator cacheIter = cached.unfilteredIterator(ColumnFilter.selection(cached.columns()), Slices.ALL, false);
                 UnfilteredRowIterator iter = counter.applyTo(cacheIter))
            {
                // Consume the iterator until we've counted enough
                while (iter.hasNext())
                    iter.next();
                return counter.isDone();
            }
        }

        public Counter newCounter(int nowInSec,
                                  boolean assumeLiveData,
                                  boolean countPartitionsWithOnlyStaticData,
                                  boolean enforceStrictLiveness)
        {
            return new CQLCounter(nowInSec, assumeLiveData, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
        }

        public int count()
        {
            return rowLimit;
        }

        public int perPartitionCount()
        {
            return perPartitionLimit;
        }

        public DataLimits withoutState()
        {
            return this;
        }

        public float estimateTotalResults(ColumnFamilyStore cfs)
        {
            // TODO: we should start storing stats on the number of rows (instead of the number of cells, which
            // is what getMeanColumns returns)
            float rowsPerPartition = ((float) cfs.getMeanColumns()) / cfs.metadata.partitionColumns().regulars.size();
            return rowsPerPartition * (cfs.estimateKeys());
        }

        protected class CQLCounter extends Counter
        {
            protected int rowCounted;
            protected int rowInCurrentPartition;
            protected final boolean countPartitionsWithOnlyStaticData;

            protected boolean hasLiveStaticRow;

            public CQLCounter(int nowInSec,
                              boolean assumeLiveData,
                              boolean countPartitionsWithOnlyStaticData,
                              boolean enforceStrictLiveness)
            {
                super(nowInSec, assumeLiveData, enforceStrictLiveness);
                this.countPartitionsWithOnlyStaticData = countPartitionsWithOnlyStaticData;
            }

            @Override
            public void applyToPartition(DecoratedKey partitionKey, Row staticRow)
            {
                rowInCurrentPartition = 0;
                hasLiveStaticRow = !staticRow.isEmpty() && isLive(staticRow);
            }

            @Override
            public Row applyToRow(Row row)
            {
                if (isLive(row))
                    incrementRowCount();
                return row;
            }

            @Override
            public void onPartitionClose()
            {
                // Normally, we don't count static rows as from a CQL point of view, it will be merge with other
                // rows in the partition. However, if we only have the static row, it will be returned as one row
                // so count it.
                if (countPartitionsWithOnlyStaticData && hasLiveStaticRow && rowInCurrentPartition == 0)
                    incrementRowCount();
                super.onPartitionClose();
            }

            protected void incrementRowCount()
            {
                if (++rowCounted >= rowLimit)
                    stop();
                if (++rowInCurrentPartition >= perPartitionLimit)
                    stopInPartition();
            }

            public int counted()
            {
                return rowCounted;
            }

            public int countedInCurrentPartition()
            {
                return rowInCurrentPartition;
            }

            public int rowCounted()
            {
                return rowCounted;
            }

            public int rowCountedInCurrentPartition()
            {
                return rowInCurrentPartition;
            }

            public boolean isDone()
            {
                return rowCounted >= rowLimit;
            }

            public boolean isDoneForPartition()
            {
                return isDone() || rowInCurrentPartition >= perPartitionLimit;
            }
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();

            if (rowLimit != NO_LIMIT)
            {
                sb.append("LIMIT ").append(rowLimit);
                if (perPartitionLimit != NO_LIMIT)
                    sb.append(' ');
            }

            if (perPartitionLimit != NO_LIMIT)
                sb.append("PER PARTITION LIMIT ").append(perPartitionLimit);

            return sb.toString();
        }
    }

    private static class CQLPagingLimits extends CQLLimits
    {
        private final ByteBuffer lastReturnedKey;
        private final int lastReturnedKeyRemaining;

        public CQLPagingLimits(int rowLimit, int perPartitionLimit, boolean isDistinct, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining)
        {
            super(rowLimit, perPartitionLimit, isDistinct);
            this.lastReturnedKey = lastReturnedKey;
            this.lastReturnedKeyRemaining = lastReturnedKeyRemaining;
        }

        @Override
        public Kind kind()
        {
            return Kind.CQL_PAGING_LIMIT;
        }

        @Override
        public DataLimits forPaging(int pageSize)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public DataLimits forPaging(int pageSize, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public DataLimits withoutState()
        {
            return new CQLLimits(rowLimit, perPartitionLimit, isDistinct);
        }

        @Override
        public Counter newCounter(int nowInSec, boolean assumeLiveData, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness)
        {
            return new PagingAwareCounter(nowInSec, assumeLiveData, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
        }

        private class PagingAwareCounter extends CQLCounter
        {
            private PagingAwareCounter(int nowInSec,
                                       boolean assumeLiveData,
                                       boolean countPartitionsWithOnlyStaticData,
                                       boolean enforceStrictLiveness)
            {
                super(nowInSec, assumeLiveData, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
            }

            @Override
            public void applyToPartition(DecoratedKey partitionKey, Row staticRow)
            {
                if (partitionKey.getKey().equals(lastReturnedKey))
                {
                    rowInCurrentPartition = perPartitionLimit - lastReturnedKeyRemaining;
                    // lastReturnedKey is the last key for which we're returned rows in the first page.
                    // So, since we know we have returned rows, we know we have accounted for the static row
                    // if any already, so force hasLiveStaticRow to false so we make sure to not count it
                    // once more.
                    hasLiveStaticRow = false;
                }
                else
                {
                    super.applyToPartition(partitionKey, staticRow);
                }
            }
        }
    }

    /**
     * <code>CQLLimits</code> used for GROUP BY queries or queries with aggregates.
     * <p>Internally, GROUP BY queries are always paginated by number of rows to avoid OOMExceptions. By consequence,
     * the limits keep track of the number of rows as well as the number of groups.</p>
     * <p>A group can only be counted if the next group or the end of the data is reached.</p>
     */
    private static class CQLGroupByLimits extends CQLLimits
    {
        /**
         * The <code>GroupMaker</code> state
         */
        protected final GroupingState state;

        /**
         * The GROUP BY specification
         */
        protected final AggregationSpecification groupBySpec;

        /**
         * The limit on the number of groups
         */
        protected final int groupLimit;

        /**
         * The limit on the number of groups per partition
         */
        protected final int groupPerPartitionLimit;

        public CQLGroupByLimits(int groupLimit,
                                int groupPerPartitionLimit,
                                int rowLimit,
                                AggregationSpecification groupBySpec)
        {
            this(groupLimit, groupPerPartitionLimit, rowLimit, groupBySpec, GroupingState.EMPTY_STATE);
        }

        private CQLGroupByLimits(int groupLimit,
                                 int groupPerPartitionLimit,
                                 int rowLimit,
                                 AggregationSpecification groupBySpec,
                                 GroupingState state)
        {
            super(rowLimit, NO_LIMIT, false);
            this.groupLimit = groupLimit;
            this.groupPerPartitionLimit = groupPerPartitionLimit;
            this.groupBySpec = groupBySpec;
            this.state = state;
        }

        @Override
        public Kind kind()
        {
            return Kind.CQL_GROUP_BY_LIMIT;
        }

        @Override
        public boolean isGroupByLimit()
        {
            return true;
        }

        public boolean isUnlimited()
        {
            return groupLimit == NO_LIMIT && groupPerPartitionLimit == NO_LIMIT && rowLimit == NO_LIMIT;
        }

        public DataLimits forShortReadRetry(int toFetch)
        {
            return new CQLLimits(toFetch);
        }

        @Override
        public float estimateTotalResults(ColumnFamilyStore cfs)
        {
            // For the moment, we return the estimated number of rows as we have no good way of estimating 
            // the number of groups that will be returned. Hopefully, we should be able to fix
            // that problem at some point.
            return super.estimateTotalResults(cfs);
        }

        @Override
        public DataLimits forPaging(int pageSize)
        {
            return new CQLGroupByLimits(pageSize,
                                        groupPerPartitionLimit,
                                        rowLimit,
                                        groupBySpec,
                                        state);
        }

        @Override
        public DataLimits forPaging(int pageSize, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining)
        {
            return new CQLGroupByPagingLimits(pageSize,
                                              groupPerPartitionLimit,
                                              rowLimit,
                                              groupBySpec,
                                              state,
                                              lastReturnedKey,
                                              lastReturnedKeyRemaining);
        }

        @Override
        public DataLimits forGroupByInternalPaging(GroupingState state)
        {
            return new CQLGroupByLimits(rowLimit,
                                        groupPerPartitionLimit,
                                        rowLimit,
                                        groupBySpec,
                                        state);
        }

        @Override
        public Counter newCounter(int nowInSec,
                                  boolean assumeLiveData,
                                  boolean countPartitionsWithOnlyStaticData,
                                  boolean enforceStrictLiveness)
        {
            return new GroupByAwareCounter(nowInSec, assumeLiveData, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
        }

        @Override
        public int count()
        {
            return groupLimit;
        }

        @Override
        public int perPartitionCount()
        {
            return groupPerPartitionLimit;
        }

        @Override
        public DataLimits withoutState()
        {
            return state == GroupingState.EMPTY_STATE
                 ? this
                 : new CQLGroupByLimits(groupLimit, groupPerPartitionLimit, rowLimit, groupBySpec);
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();

            if (groupLimit != NO_LIMIT)
            {
                sb.append("GROUP LIMIT ").append(groupLimit);
                if (groupPerPartitionLimit != NO_LIMIT || rowLimit != NO_LIMIT)
                    sb.append(' ');
            }

            if (groupPerPartitionLimit != NO_LIMIT)
            {
                sb.append("GROUP PER PARTITION LIMIT ").append(groupPerPartitionLimit);
                if (rowLimit != NO_LIMIT)
                    sb.append(' ');
            }

            if (rowLimit != NO_LIMIT)
            {
                sb.append("LIMIT ").append(rowLimit);
            }

            return sb.toString();
        }

        @Override
        public boolean isExhausted(Counter counter)
        {
            return ((GroupByAwareCounter) counter).rowCounted < rowLimit
                    && counter.counted() < groupLimit;
        }

        protected class GroupByAwareCounter extends Counter
        {
            private final GroupMaker groupMaker;

            protected final boolean countPartitionsWithOnlyStaticData;

            /**
             * The key of the partition being processed.
             */
            protected DecoratedKey currentPartitionKey;

            /**
             * The number of rows counted so far.
             */
            protected int rowCounted;

            /**
             * The number of rows counted so far in the current partition.
             */
            protected int rowCountedInCurrentPartition;

            /**
             * The number of groups counted so far. A group is counted only once it is complete
             * (e.g the next one has been reached).
             */
            protected int groupCounted;

            /**
             * The number of groups in the current partition.
             */
            protected int groupInCurrentPartition;

            protected boolean hasGroupStarted;

            protected boolean hasLiveStaticRow;

            protected boolean hasReturnedRowsFromCurrentPartition;

            private GroupByAwareCounter(int nowInSec,
                                        boolean assumeLiveData,
                                        boolean countPartitionsWithOnlyStaticData,
                                        boolean enforceStrictLiveness)
            {
                super(nowInSec, assumeLiveData, enforceStrictLiveness);
                this.groupMaker = groupBySpec.newGroupMaker(state);
                this.countPartitionsWithOnlyStaticData = countPartitionsWithOnlyStaticData;

                // If the end of the partition was reached at the same time than the row limit, the last group might
                // not have been counted yet. Due to that we need to guess, based on the state, if the previous group
                // is still open.
                hasGroupStarted = state.hasClustering();
            }

            @Override
            public void applyToPartition(DecoratedKey partitionKey, Row staticRow)
            {
                if (partitionKey.getKey().equals(state.partitionKey()))
                {
                    // The only case were we could have state.partitionKey() equals to the partition key
                    // is if some of the partition rows have been returned in the previous page but the
                    // partition was not exhausted (as the state partition key has not been updated yet).
                    // Since we know we have returned rows, we know we have accounted for
                    // the static row if any already, so force hasLiveStaticRow to false so we make sure to not count it
                    // once more.
                    hasLiveStaticRow = false;
                    hasReturnedRowsFromCurrentPartition = true;
                    hasGroupStarted = true;
                }
                else
                {
                    // We need to increment our count of groups if we have reached a new one and unless we had no new
                    // content added since we closed our last group (that is, if hasGroupStarted). Note that we may get
                    // here with hasGroupStarted == false in the following cases:
                    // * the partition limit was reached for the previous partition
                    // * the previous partition was containing only one static row
                    // * the rows of the last group of the previous partition were all marked as deleted
                    if (hasGroupStarted && groupMaker.isNewGroup(partitionKey, Clustering.STATIC_CLUSTERING))
                    {
                        incrementGroupCount();
                        // If we detect, before starting the new partition, that we are done, we need to increase
                        // the per partition group count of the previous partition as the next page will start from
                        // there.
                        if (isDone())
                            incrementGroupInCurrentPartitionCount();
                        hasGroupStarted = false;
                    }
                    hasReturnedRowsFromCurrentPartition = false;
                    hasLiveStaticRow = !staticRow.isEmpty() && isLive(staticRow);
                }
                currentPartitionKey = partitionKey;
                // If we are done we need to preserve the groupInCurrentPartition and rowCountedInCurrentPartition
                // because the pager need to retrieve the count associated to the last value it has returned.
                if (!isDone())
                {
                    groupInCurrentPartition = 0;
                    rowCountedInCurrentPartition = 0;
                }
            }

            @Override
            protected Row applyToStatic(Row row)
            {
                // It's possible that we're "done" if the partition we just started bumped the number of groups (in
                // applyToPartition() above), in which case Transformation will still call this method. In that case, we
                // want to ignore the static row, it should (and will) be returned with the next page/group if needs be.
                if (isDone())
                {
                    hasLiveStaticRow = false; // The row has not been returned
                    return Rows.EMPTY_STATIC_ROW;
                }
                return row;
            }

            @Override
            public Row applyToRow(Row row)
            {
                // We want to check if the row belongs to a new group even if it has been deleted. The goal being
                // to minimize the chances of having to go through the same data twice if we detect on the next
                // non deleted row that we have reached the limit.
                if (groupMaker.isNewGroup(currentPartitionKey, row.clustering()))
                {
                    if (hasGroupStarted)
                    {
                        incrementGroupCount();
                        incrementGroupInCurrentPartitionCount();
                    }
                    hasGroupStarted = false;
                }

                // That row may have made us increment the group count, which may mean we're done for this partition, in
                // which case we shouldn't count this row (it won't be returned).
                if (isDoneForPartition())
                {
                    hasGroupStarted = false;
                    return null;
                }

                if (isLive(row))
                {
                    hasGroupStarted = true;
                    incrementRowCount();
                    hasReturnedRowsFromCurrentPartition = true;
                }

                return row;
            }

            @Override
            public int counted()
            {
                return groupCounted;
            }

            @Override
            public int countedInCurrentPartition()
            {
                return groupInCurrentPartition;
            }

            @Override
            public int rowCounted()
            {
                return rowCounted;
            }

            @Override
            public int rowCountedInCurrentPartition()
            {
                return rowCountedInCurrentPartition;
            }

            protected void incrementRowCount()
            {
                rowCountedInCurrentPartition++;
                if (++rowCounted >= rowLimit)
                    stop();
            }

            private void incrementGroupCount()
            {
                groupCounted++;
                if (groupCounted >= groupLimit)
                    stop();
            }

            private void incrementGroupInCurrentPartitionCount()
            {
                groupInCurrentPartition++;
                if (groupInCurrentPartition >= groupPerPartitionLimit)
                    stopInPartition();
            }

            @Override
            public boolean isDoneForPartition()
            {
                return isDone() || groupInCurrentPartition >= groupPerPartitionLimit;
            }

            @Override
            public boolean isDone()
            {
                return groupCounted >= groupLimit;
            }

            @Override
            public void onPartitionClose()
            {
                // Normally, we don't count static rows as from a CQL point of view, it will be merge with other
                // rows in the partition. However, if we only have the static row, it will be returned as one group
                // so count it.
                if (countPartitionsWithOnlyStaticData && hasLiveStaticRow && !hasReturnedRowsFromCurrentPartition)
                {
                    incrementRowCount();
                    incrementGroupCount();
                    incrementGroupInCurrentPartitionCount();
                    hasGroupStarted = false;
                }
                super.onPartitionClose();
            }

            @Override
            public void onClose()
            {
                // Groups are only counted when the end of the group is reached.
                // The end of a group is detected by 2 ways:
                // 1) a new group is reached
                // 2) the end of the data is reached
                // We know that the end of the data is reached if the group limit has not been reached
                // and the number of rows counted is smaller than the internal page size.
                if (hasGroupStarted && groupCounted < groupLimit && rowCounted < rowLimit)
                {
                    incrementGroupCount();
                    incrementGroupInCurrentPartitionCount();
                }

                super.onClose();
            }
        }
    }

    private static class CQLGroupByPagingLimits extends CQLGroupByLimits
    {
        private final ByteBuffer lastReturnedKey;

        private final int lastReturnedKeyRemaining;

        public CQLGroupByPagingLimits(int groupLimit,
                                      int groupPerPartitionLimit,
                                      int rowLimit,
                                      AggregationSpecification groupBySpec,
                                      GroupingState state,
                                      ByteBuffer lastReturnedKey,
                                      int lastReturnedKeyRemaining)
        {
            super(groupLimit,
                  groupPerPartitionLimit,
                  rowLimit,
                  groupBySpec,
                  state);

            this.lastReturnedKey = lastReturnedKey;
            this.lastReturnedKeyRemaining = lastReturnedKeyRemaining;
        }

        @Override
        public Kind kind()
        {
            return Kind.CQL_GROUP_BY_PAGING_LIMIT;
        }

        @Override
        public DataLimits forPaging(int pageSize)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public DataLimits forPaging(int pageSize, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public DataLimits forGroupByInternalPaging(GroupingState state)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Counter newCounter(int nowInSec, boolean assumeLiveData, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness)
        {
            assert state == GroupingState.EMPTY_STATE || lastReturnedKey.equals(state.partitionKey());
            return new PagingGroupByAwareCounter(nowInSec, assumeLiveData, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
        }

        @Override
        public DataLimits withoutState()
        {
            return new CQLGroupByLimits(groupLimit, groupPerPartitionLimit, rowLimit, groupBySpec);
        }

        private class PagingGroupByAwareCounter extends GroupByAwareCounter
        {
            private PagingGroupByAwareCounter(int nowInSec, boolean assumeLiveData, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness)
            {
                super(nowInSec, assumeLiveData, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
            }

            @Override
            public void applyToPartition(DecoratedKey partitionKey, Row staticRow)
            {
                if (partitionKey.getKey().equals(lastReturnedKey))
                {
                    currentPartitionKey = partitionKey;
                    groupInCurrentPartition = groupPerPartitionLimit - lastReturnedKeyRemaining;
                    hasReturnedRowsFromCurrentPartition = true;
                    hasLiveStaticRow = false;
                    hasGroupStarted = state.hasClustering();
                }
                else
                {
                    super.applyToPartition(partitionKey, staticRow);
                }
            }
        }
    }

    /**
     * Limits used by thrift; this count partition and cells.
     */
    private static class ThriftLimits extends DataLimits
    {
        protected final int partitionLimit;
        protected final int cellPerPartitionLimit;

        private ThriftLimits(int partitionLimit, int cellPerPartitionLimit)
        {
            this.partitionLimit = partitionLimit;
            this.cellPerPartitionLimit = cellPerPartitionLimit;
        }

        public Kind kind()
        {
            return Kind.THRIFT_LIMIT;
        }

        public boolean isUnlimited()
        {
            return partitionLimit == NO_LIMIT && cellPerPartitionLimit == NO_LIMIT;
        }

        public boolean isDistinct()
        {
            return false;
        }

        public DataLimits forPaging(int pageSize)
        {
            // We don't support paging on thrift in general but do use paging under the hood for get_count. For
            // that case, we only care about limiting cellPerPartitionLimit (since it's paging over a single
            // partition). We do check that the partition limit is 1 however to make sure this is not misused
            // (as this wouldn't work properly for range queries).
            assert partitionLimit == 1;
            return new ThriftLimits(partitionLimit, pageSize);
        }

        public DataLimits forPaging(int pageSize, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining)
        {
            throw new UnsupportedOperationException();
        }

        public DataLimits forShortReadRetry(int toFetch)
        {
            // Short read retries are always done for a single partition at a time, so it's ok to ignore the
            // partition limit for those
            return new ThriftLimits(1, toFetch);
        }

        public boolean hasEnoughLiveData(CachedPartition cached, int nowInSec, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness)
        {
            // We want the number of cells that are currently live. Getting that precise number forces
            // us to iterate the cached partition in general, but we can avoid that if:
            //   - The number of non-expiring live cells is greater than the number of cells asked (we then
            //     know we have enough live cells).
            //   - The number of cells cached is less than requested, in which case we know we won't have enough.
            if (cached.nonExpiringLiveCells() >= cellPerPartitionLimit)
                return true;

            if (cached.nonTombstoneCellCount() < cellPerPartitionLimit)
                return false;

            // Otherwise, we need to re-count
            DataLimits.Counter counter = newCounter(nowInSec, false, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
            try (UnfilteredRowIterator cacheIter = cached.unfilteredIterator(ColumnFilter.selection(cached.columns()), Slices.ALL, false);
                 UnfilteredRowIterator iter = counter.applyTo(cacheIter))
            {
                // Consume the iterator until we've counted enough
                while (iter.hasNext())
                    iter.next();
                return counter.isDone();
            }
        }

        public Counter newCounter(int nowInSec, boolean assumeLiveData, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness)
        {
            return new ThriftCounter(nowInSec, assumeLiveData, enforceStrictLiveness);
        }

        public int count()
        {
            return partitionLimit * cellPerPartitionLimit;
        }

        public int perPartitionCount()
        {
            return cellPerPartitionLimit;
        }

        public DataLimits withoutState()
        {
            return this;
        }

        public float estimateTotalResults(ColumnFamilyStore cfs)
        {
            // remember that getMeansColumns returns a number of cells: we should clean nomenclature
            float cellsPerPartition = ((float) cfs.getMeanColumns()) / cfs.metadata.partitionColumns().regulars.size();
            return cellsPerPartition * cfs.estimateKeys();
        }

        protected class ThriftCounter extends Counter
        {
            protected int partitionsCounted;
            protected int cellsCounted;
            protected int cellsInCurrentPartition;

            public ThriftCounter(int nowInSec, boolean assumeLiveData, boolean enforceStrictLiveness)
            {
                super(nowInSec, assumeLiveData, enforceStrictLiveness);
            }

            @Override
            public void applyToPartition(DecoratedKey partitionKey, Row staticRow)
            {
                cellsInCurrentPartition = 0;
                if (!staticRow.isEmpty())
                    applyToRow(staticRow);
            }

            @Override
            public Row applyToRow(Row row)
            {
                for (Cell cell : row.cells())
                {
                    if (assumeLiveData || cell.isLive(nowInSec))
                    {
                        ++cellsCounted;
                        if (++cellsInCurrentPartition >= cellPerPartitionLimit)
                            stopInPartition();
                    }
                }
                return row;
            }

            @Override
            public void onPartitionClose()
            {
                if (++partitionsCounted >= partitionLimit)
                    stop();
                super.onPartitionClose();
            }

            public int counted()
            {
                return cellsCounted;
            }

            public int countedInCurrentPartition()
            {
                return cellsInCurrentPartition;
            }

            public int rowCounted()
            {
                throw new UnsupportedOperationException();
            }

            public int rowCountedInCurrentPartition()
            {
                throw new UnsupportedOperationException();
            }

            public boolean isDone()
            {
                return partitionsCounted >= partitionLimit;
            }

            public boolean isDoneForPartition()
            {
                return isDone() || cellsInCurrentPartition >= cellPerPartitionLimit;
            }
        }

        @Override
        public String toString()
        {
            // This is not valid CQL, but that's ok since it's not used for CQL queries.
            return String.format("THRIFT LIMIT (partitions=%d, cells_per_partition=%d)", partitionLimit, cellPerPartitionLimit);
        }
    }

    /**
     * Limits used for thrift get_count when we only want to count super columns.
     */
    private static class SuperColumnCountingLimits extends ThriftLimits
    {
        private SuperColumnCountingLimits(int partitionLimit, int cellPerPartitionLimit)
        {
            super(partitionLimit, cellPerPartitionLimit);
        }

        public Kind kind()
        {
            return Kind.SUPER_COLUMN_COUNTING_LIMIT;
        }

        public DataLimits forPaging(int pageSize)
        {
            // We don't support paging on thrift in general but do use paging under the hood for get_count. For
            // that case, we only care about limiting cellPerPartitionLimit (since it's paging over a single
            // partition). We do check that the partition limit is 1 however to make sure this is not misused
            // (as this wouldn't work properly for range queries).
            assert partitionLimit == 1;
            return new SuperColumnCountingLimits(partitionLimit, pageSize);
        }

        public DataLimits forShortReadRetry(int toFetch)
        {
            // Short read retries are always done for a single partition at a time, so it's ok to ignore the
            // partition limit for those
            return new SuperColumnCountingLimits(1, toFetch);
        }

        @Override
        public Counter newCounter(int nowInSec, boolean assumeLiveData, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness)
        {
            return new SuperColumnCountingCounter(nowInSec, assumeLiveData, enforceStrictLiveness);
        }

        protected class SuperColumnCountingCounter extends ThriftCounter
        {
            private final boolean enforceStrictLiveness;

            public SuperColumnCountingCounter(int nowInSec, boolean assumeLiveData, boolean enforceStrictLiveness)
            {
                super(nowInSec, assumeLiveData, enforceStrictLiveness);
                this.enforceStrictLiveness = enforceStrictLiveness;
            }

            @Override
            public Row applyToRow(Row row)
            {
                // In the internal format, a row == a super column, so that's what we want to count.
                if (isLive(row))
                {
                    ++cellsCounted;
                    if (++cellsInCurrentPartition >= cellPerPartitionLimit)
                        stopInPartition();
                }
                return row;
            }
        }
    }

    public static class Serializer
    {
        public void serialize(DataLimits limits, DataOutputPlus out, int version, ClusteringComparator comparator) throws IOException
        {
            out.writeByte(limits.kind().ordinal());
            switch (limits.kind())
            {
                case CQL_LIMIT:
                case CQL_PAGING_LIMIT:
                    CQLLimits cqlLimits = (CQLLimits)limits;
                    out.writeUnsignedVInt(cqlLimits.rowLimit);
                    out.writeUnsignedVInt(cqlLimits.perPartitionLimit);
                    out.writeBoolean(cqlLimits.isDistinct);
                    if (limits.kind() == Kind.CQL_PAGING_LIMIT)
                    {
                        CQLPagingLimits pagingLimits = (CQLPagingLimits)cqlLimits;
                        ByteBufferUtil.writeWithVIntLength(pagingLimits.lastReturnedKey, out);
                        out.writeUnsignedVInt(pagingLimits.lastReturnedKeyRemaining);
                    }
                    break;
                case CQL_GROUP_BY_LIMIT:
                case CQL_GROUP_BY_PAGING_LIMIT:
                    CQLGroupByLimits groupByLimits = (CQLGroupByLimits) limits;
                    out.writeUnsignedVInt(groupByLimits.groupLimit);
                    out.writeUnsignedVInt(groupByLimits.groupPerPartitionLimit);
                    out.writeUnsignedVInt(groupByLimits.rowLimit);

                    AggregationSpecification groupBySpec = groupByLimits.groupBySpec;
                    AggregationSpecification.serializer.serialize(groupBySpec, out, version);

                    GroupingState.serializer.serialize(groupByLimits.state, out, version, comparator);

                    if (limits.kind() == Kind.CQL_GROUP_BY_PAGING_LIMIT)
                    {
                        CQLGroupByPagingLimits pagingLimits = (CQLGroupByPagingLimits) groupByLimits;
                        ByteBufferUtil.writeWithVIntLength(pagingLimits.lastReturnedKey, out);
                        out.writeUnsignedVInt(pagingLimits.lastReturnedKeyRemaining);
                     }
                     break;
                case THRIFT_LIMIT:
                case SUPER_COLUMN_COUNTING_LIMIT:
                    ThriftLimits thriftLimits = (ThriftLimits)limits;
                    out.writeUnsignedVInt(thriftLimits.partitionLimit);
                    out.writeUnsignedVInt(thriftLimits.cellPerPartitionLimit);
                    break;
            }
        }

        public DataLimits deserialize(DataInputPlus in, int version, ClusteringComparator comparator) throws IOException
        {
            Kind kind = Kind.values()[in.readUnsignedByte()];
            switch (kind)
            {
                case CQL_LIMIT:
                case CQL_PAGING_LIMIT:
                {
                    int rowLimit = (int) in.readUnsignedVInt();
                    int perPartitionLimit = (int) in.readUnsignedVInt();
                    boolean isDistinct = in.readBoolean();
                    if (kind == Kind.CQL_LIMIT)
                        return cqlLimits(rowLimit, perPartitionLimit, isDistinct);
                    ByteBuffer lastKey = ByteBufferUtil.readWithVIntLength(in);
                    int lastRemaining = (int) in.readUnsignedVInt();
                    return new CQLPagingLimits(rowLimit, perPartitionLimit, isDistinct, lastKey, lastRemaining);
                }
                case CQL_GROUP_BY_LIMIT:
                case CQL_GROUP_BY_PAGING_LIMIT:
                {
                    int groupLimit = (int) in.readUnsignedVInt();
                    int groupPerPartitionLimit = (int) in.readUnsignedVInt();
                    int rowLimit = (int) in.readUnsignedVInt();

                    AggregationSpecification groupBySpec = AggregationSpecification.serializer.deserialize(in, version, comparator);

                    GroupingState state = GroupingState.serializer.deserialize(in, version, comparator);

                    if (kind == Kind.CQL_GROUP_BY_LIMIT)
                        return new CQLGroupByLimits(groupLimit,
                                                    groupPerPartitionLimit,
                                                    rowLimit,
                                                    groupBySpec,
                                                    state);

                    ByteBuffer lastKey = ByteBufferUtil.readWithVIntLength(in);
                    int lastRemaining = (int) in.readUnsignedVInt();
                    return new CQLGroupByPagingLimits(groupLimit,
                                                      groupPerPartitionLimit,
                                                      rowLimit,
                                                      groupBySpec,
                                                      state,
                                                      lastKey,
                                                      lastRemaining);
                }
                case THRIFT_LIMIT:
                case SUPER_COLUMN_COUNTING_LIMIT:
                    int partitionLimit = (int) in.readUnsignedVInt();
                    int cellPerPartitionLimit = (int) in.readUnsignedVInt();
                    return kind == Kind.THRIFT_LIMIT
                            ? new ThriftLimits(partitionLimit, cellPerPartitionLimit)
                            : new SuperColumnCountingLimits(partitionLimit, cellPerPartitionLimit);
            }
            throw new AssertionError();
        }

        public long serializedSize(DataLimits limits, int version, ClusteringComparator comparator)
        {
            long size = TypeSizes.sizeof((byte) limits.kind().ordinal());
            switch (limits.kind())
            {
                case CQL_LIMIT:
                case CQL_PAGING_LIMIT:
                    CQLLimits cqlLimits = (CQLLimits) limits;
                    size += TypeSizes.sizeofUnsignedVInt(cqlLimits.rowLimit);
                    size += TypeSizes.sizeofUnsignedVInt(cqlLimits.perPartitionLimit);
                    size += TypeSizes.sizeof(cqlLimits.isDistinct);
                    if (limits.kind() == Kind.CQL_PAGING_LIMIT)
                    {
                        CQLPagingLimits pagingLimits = (CQLPagingLimits) cqlLimits;
                        size += ByteBufferUtil.serializedSizeWithVIntLength(pagingLimits.lastReturnedKey);
                        size += TypeSizes.sizeofUnsignedVInt(pagingLimits.lastReturnedKeyRemaining);
                    }
                    break;
                case CQL_GROUP_BY_LIMIT:
                case CQL_GROUP_BY_PAGING_LIMIT:
                    CQLGroupByLimits groupByLimits = (CQLGroupByLimits) limits;
                    size += TypeSizes.sizeofUnsignedVInt(groupByLimits.groupLimit);
                    size += TypeSizes.sizeofUnsignedVInt(groupByLimits.groupPerPartitionLimit);
                    size += TypeSizes.sizeofUnsignedVInt(groupByLimits.rowLimit);

                    AggregationSpecification groupBySpec = groupByLimits.groupBySpec;
                    size += AggregationSpecification.serializer.serializedSize(groupBySpec, version);

                    size += GroupingState.serializer.serializedSize(groupByLimits.state, version, comparator);

                    if (limits.kind() == Kind.CQL_GROUP_BY_PAGING_LIMIT)
                    {
                        CQLGroupByPagingLimits pagingLimits = (CQLGroupByPagingLimits) groupByLimits;
                        size += ByteBufferUtil.serializedSizeWithVIntLength(pagingLimits.lastReturnedKey);
                        size += TypeSizes.sizeofUnsignedVInt(pagingLimits.lastReturnedKeyRemaining);
                    }
                    break;
                case THRIFT_LIMIT:
                case SUPER_COLUMN_COUNTING_LIMIT:
                    ThriftLimits thriftLimits = (ThriftLimits) limits;
                    size += TypeSizes.sizeofUnsignedVInt(thriftLimits.partitionLimit);
                    size += TypeSizes.sizeofUnsignedVInt(thriftLimits.cellPerPartitionLimit);
                    break;
                default:
                    throw new AssertionError();
            }
            return size;
        }
    }
}
