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
import java.util.StringJoiner;

import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.aggregation.AggregationSpecification;
import org.apache.cassandra.db.aggregation.GroupMaker;
import org.apache.cassandra.db.aggregation.GroupingState;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.transform.BasePartitions;
import org.apache.cassandra.db.transform.BaseRows;
import org.apache.cassandra.db.transform.StoppingTransformation;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Object in charge of tracking if we have fetch enough data for a given query.
 *
 * This is more complicated than a single count because we support PER PARTITION
 * limits, but also due to GROUP BY and paging.
 */
public abstract class DataLimits
{
    public static final Serializer serializer = new Serializer();

    public static final int NO_LIMIT = Integer.MAX_VALUE;

    public static final DataLimits NONE = new CQLLimits(NO_LIMIT, NO_LIMIT, NO_LIMIT, false)
    {
        @Override
        public boolean hasEnoughLiveData(CachedPartition cached, long nowInSec, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness)
        {
            return false;
        }

        @Override
        public UnfilteredPartitionIterator filter(UnfilteredPartitionIterator iter,
                                                  long nowInSec,
                                                  boolean countPartitionsWithOnlyStaticData)
        {
            return iter;
        }

        @Override
        public UnfilteredRowIterator filter(UnfilteredRowIterator iter,
                                            long nowInSec,
                                            boolean countPartitionsWithOnlyStaticData)
        {
            return iter;
        }

        @Override
        public PartitionIterator filter(PartitionIterator iter, long nowInSec, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness)
        {
            return iter;
        }
    };

    // We currently deal with distinct queries by querying full partitions but limiting the result at 1 row per
    // partition (see SelectStatement.makeFilter). So an "unbounded" distinct is still actually doing some filtering.
    public static final DataLimits DISTINCT_NONE = new CQLLimits(NO_LIMIT, NO_LIMIT, 1, true);

    public enum Kind
    {
        CQL_LIMIT,
        CQL_PAGING_LIMIT,
        @Deprecated THRIFT_LIMIT, //Deprecated and unused in 4.0, stop publishing in 5.0, reclaim in 6.0
        @Deprecated SUPER_COLUMN_COUNTING_LIMIT, //Deprecated and unused in 4.0, stop publishing in 5.0, reclaim in 6.0
        CQL_GROUP_BY_LIMIT,
        CQL_GROUP_BY_PAGING_LIMIT,
    }

    public static DataLimits cqlLimits(int cqlRowLimit)
    {
        return cqlRowLimit == NO_LIMIT ? NONE : new CQLLimits(cqlRowLimit, NO_LIMIT, NO_LIMIT, false);
    }

    public static DataLimits cqlLimits(int cqlRowLimit, int perPartitionLimit)
    {
        return cqlRowLimit == NO_LIMIT && perPartitionLimit == NO_LIMIT
             ? NONE
             : new CQLLimits(cqlRowLimit, NO_LIMIT, perPartitionLimit, false);
    }

    private static DataLimits cqlLimits(int cqlRowLimit, int perPartitionLimit, boolean isDistinct)
    {
        return cqlRowLimit == NO_LIMIT && perPartitionLimit == NO_LIMIT && !isDistinct
             ? NONE
             : new CQLLimits(cqlRowLimit, NO_LIMIT, perPartitionLimit, isDistinct);
    }

    public static DataLimits groupByLimits(int groupLimit,
                                           int groupPerPartitionLimit,
                                           PageSize subPageSize,
                                           AggregationSpecification groupBySpec)
    {
        return new CQLGroupByLimits(groupLimit, NO_LIMIT, groupPerPartitionLimit, subPageSize.rows, subPageSize.bytes, groupBySpec, GroupingState.EMPTY_STATE);
    }

    public static DataLimits distinctLimits(int cqlRowLimit)
    {
        return CQLLimits.distinct(cqlRowLimit);
    }

    public abstract Kind kind();

    public abstract boolean isUnlimited();
    public abstract boolean isDistinct();

    public boolean isGroupByLimit()
    {
        return false;
    }

    /**
     * Returns true if the count limit is not reached.
     * <p/>
     * Note: currently this method's only usage is for paging, where it is checked after processing a page as a quick
     * signal that the data for the query is complete - if the count limit is not reached at the end of the page, this
     * must be because there is no more data to return - that is - the input is exhausted.
     */
    public boolean isExhausted(Counter counter)
    {
        return counter.counted() < count() && counter.bytesCounted() < bytes();
    }

    public abstract DataLimits forPaging(PageSize pageSize);
    public abstract DataLimits forPaging(PageSize pageSize, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining);

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
                                              long nowInSec,
                                              boolean countPartitionsWithOnlyStaticData,
                                              boolean enforceStrictLiveness);

    /**
     * Returns a new {@code Counter} for this limits instance.
     *
     * @param nowInSec the current time in second (to decide what is expired or not).
     * @param assumeLiveData if true, the counter will assume that every row passed is live and won't
     * thus check for liveness, otherwise it will. This should be {@code true} when used on a
     * {@code RowIterator} (since it only returns live rows), false otherwise.
     * @param countPartitionsWithOnlyStaticData if {@code true} the partitions with only static data should be counted
     * as 1 valid row.
     * @param enforceStrictLiveness whether the row should be purged if there is no PK liveness info,
     * normally retrieved from {@link org.apache.cassandra.schema.TableMetadata#enforceStrictLiveness()}
     * @return a new {@code Counter} for this limits instance.
     */
    public abstract Counter newCounter(long nowInSec,
                                       boolean assumeLiveData,
                                       boolean countPartitionsWithOnlyStaticData,
                                       boolean enforceStrictLiveness);

    /**
     * The max number of bytes this limits enforces.
     * <p>
     * Note that if this value is set, fewer rows might be returned if the size of the current rows exceeds the bytes limit.
     *
     * @return the maximum number of bytes this limits enforces.
     */
    public abstract int bytes();

    /**
     * The max number of results this limits enforces.
     * <p>
     * Note that the actual definition of "results" depends a bit: for "normal" queries it's a number of rows,
     * but for GROUP BY queries it's a number of groups.
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
                                              long nowInSec,
                                              boolean countPartitionsWithOnlyStaticData)
    {
        return this.newCounter(nowInSec,
                               false,
                               countPartitionsWithOnlyStaticData,
                               iter.metadata().enforceStrictLiveness())
                   .applyTo(iter);
    }

    public UnfilteredRowIterator filter(UnfilteredRowIterator iter,
                                        long nowInSec,
                                        boolean countPartitionsWithOnlyStaticData)
    {
        return this.newCounter(nowInSec,
                               false,
                               countPartitionsWithOnlyStaticData,
                               iter.metadata().enforceStrictLiveness())
                   .applyTo(iter);
    }

    public PartitionIterator filter(PartitionIterator iter, long nowInSec, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness)
    {
        return this.newCounter(nowInSec, true, countPartitionsWithOnlyStaticData, enforceStrictLiveness).applyTo(iter);
    }

    /**
     * Estimate the number of results that a full scan of the provided cfs would yield.
     */
    public abstract float estimateTotalResults(ColumnFamilyStore cfs);

    public static abstract class Counter extends StoppingTransformation<BaseRowIterator<?>>
    {
        protected final long nowInSec;
        protected final boolean assumeLiveData;
        private final boolean enforceStrictLiveness;

        // false means we do not propagate our stop signals onto the iterator, we only count
        protected boolean enforceLimits = true;

        protected Counter(long nowInSec, boolean assumeLiveData, boolean enforceStrictLiveness)
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
         * The number of bytes for the counted rows.
         *
         * @return the number of bytes counted.
         */
        public abstract int bytesCounted();

        /**
         * The number of rows counted.
         *
         * @return the number of rows counted.
         */
        public abstract int rowsCounted();

        /**
         * The number of rows counted in the current partition.
         *
         * @return the number of rows counted in the current partition.
         */
        public abstract int rowsCountedInCurrentPartition();

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

        public GroupingState recreateGroupingStateFor(ByteBuffer lastPartitionKey, Clustering<?> lastClustering)
        {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Limits used by CQL; this counts rows.
     */
    private static class CQLLimits extends DataLimits
    {
        protected final int bytesLimit;
        protected final int rowLimit;
        protected final int perPartitionLimit;

        // Whether the query is a distinct query or not.
        protected final boolean isDistinct;

        private CQLLimits(int rowLimit, int bytesLimit, int perPartitionLimit, boolean isDistinct)
        {
            this.rowLimit = rowLimit;
            this.bytesLimit = bytesLimit;
            this.perPartitionLimit = perPartitionLimit;
            this.isDistinct = isDistinct;
        }

        private static CQLLimits distinct(int rowLimit)
        {
            return new CQLLimits(rowLimit, NO_LIMIT, 1, true);
        }

        @Override
        public Kind kind()
        {
            return Kind.CQL_LIMIT;
        }

        @Override
        public boolean isUnlimited()
        {
            return rowLimit == NO_LIMIT && perPartitionLimit == NO_LIMIT && bytesLimit == NO_LIMIT;
        }

        @Override
        public boolean isDistinct()
        {
            return isDistinct;
        }

        @Override
        public DataLimits forPaging(PageSize pageSize)
        {
            return new CQLLimits(Math.min(rowLimit, pageSize.rows),
                                 Math.min(bytesLimit, pageSize.bytes),
                                 perPartitionLimit,
                                 isDistinct
            );
        }

        @Override
        public DataLimits forPaging(PageSize pageSize, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining)
        {
            return new CQLPagingLimits(Math.min(rowLimit, pageSize.rows),
                                       Math.min(bytesLimit, pageSize.bytes),
                                       perPartitionLimit,
                                       isDistinct,
                                       lastReturnedKey,
                                       lastReturnedKeyRemaining);
        }

        @Override
        public DataLimits forShortReadRetry(int toFetch)
        {
            return new CQLLimits(toFetch, bytesLimit, perPartitionLimit, isDistinct);
        }

        @Override
        public boolean hasEnoughLiveData(CachedPartition cached, long nowInSec, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness)
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

        @Override
        public Counter newCounter(long nowInSec,
                                  boolean assumeLiveData,
                                  boolean countPartitionsWithOnlyStaticData,
                                  boolean enforceStrictLiveness)
        {
            return new CQLCounter(nowInSec, assumeLiveData, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
        }

        @Override
        public int bytes()
        {
            return bytesLimit;
        }

        @Override
        public int count()
        {
            return rowLimit;
        }

        @Override
        public int perPartitionCount()
        {
            return perPartitionLimit;
        }

        @Override
        public DataLimits withoutState()
        {
            return this;
        }

        @Override
        public float estimateTotalResults(ColumnFamilyStore cfs)
        {
            // TODO: we should start storing stats on the number of rows (instead of the number of cells, which
            // is what getMeanColumns returns)
            float rowsPerPartition = ((float) cfs.getMeanEstimatedCellPerPartitionCount()) / cfs.metadata().regularColumns().size();
            return rowsPerPartition * (cfs.estimateKeys());
        }

        protected class CQLCounter extends Counter
        {
            protected int rowsCounted;
            protected int bytesCounted;
            protected int rowsInCurrentPartition;
            protected final boolean countPartitionsWithOnlyStaticData;

            protected int staticRowBytes;
            protected int partitionKeyBytes;

            protected boolean hasLiveStaticRow;

            public CQLCounter(long nowInSec,
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
                rowsInCurrentPartition = 0;
                hasLiveStaticRow = !staticRow.isEmpty() && isLive(staticRow);
                staticRowBytes = hasLiveStaticRow ? staticRow.dataSize() : 0;
                partitionKeyBytes = partitionKey.getKeyLength();
            }

            @Override
            public Row applyToRow(Row row)
            {
                if (isLive(row))
                    incrementRowCount(row.dataSize());
                return row;
            }

            @Override
            public void onPartitionClose()
            {
                // Normally, we don't count static rows as from a CQL point of view, it will be merged with other
                // rows in the partition. However, if we only have the static row, it will be returned as one row
                // so count it.
                if (countPartitionsWithOnlyStaticData && hasLiveStaticRow && rowsInCurrentPartition == 0)
                    incrementRowCount(staticRowBytes);
                super.onPartitionClose();
            }

            /**
             * Increments the number of counted rows and adds the given number of bytes to the total counted size
             */
            protected void incrementRowCount(int rowSizeInBytes)
            {
                bytesCounted += rowSizeInBytes + partitionKeyBytes;
                rowsCounted++;
                rowsInCurrentPartition++;
                if (bytesCounted >= bytesLimit || rowsCounted >= rowLimit)
                    stop();
                if (rowsInCurrentPartition >= perPartitionLimit)
                    stopInPartition();
            }

            @Override
            public int counted()
            {
                return rowsCounted;
            }

            @Override
            public int countedInCurrentPartition()
            {
                return rowsInCurrentPartition;
            }

            @Override
            public int bytesCounted()
            {
                return bytesCounted;
            }

            @Override
            public int rowsCounted()
            {
                return rowsCounted;
            }

            @Override
            public int rowsCountedInCurrentPartition()
            {
                return rowsInCurrentPartition;
            }

            @Override
            public boolean isDone()
            {
                return rowsCounted >= rowLimit || bytesCounted >= bytesLimit;
            }

            @Override
            public boolean isDoneForPartition()
            {
                return isDone() || rowsInCurrentPartition >= perPartitionLimit;
            }

            @Override
            public String toString()
            {
                return String.format("%s(bytes=%s/%s, rows=%s/%s, partition-rows=%s/%s)", this.getClass().getName(),
                                     bytesCounted(), bytesLimit, rowsCounted(), rowLimit, rowsCountedInCurrentPartition(), perPartitionLimit);
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

        public CQLPagingLimits(int rowLimit, int bytesLimit, int perPartitionLimit, boolean isDistinct, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining)
        {
            super(rowLimit, bytesLimit, perPartitionLimit, isDistinct);
            this.lastReturnedKey = lastReturnedKey;
            this.lastReturnedKeyRemaining = lastReturnedKeyRemaining;
        }

        @Override
        public Kind kind()
        {
            return Kind.CQL_PAGING_LIMIT;
        }

        @Override
        public DataLimits forPaging(PageSize pageSize)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public DataLimits forPaging(PageSize pageSize, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public DataLimits withoutState()
        {
            return new CQLLimits(rowLimit, bytesLimit, perPartitionLimit, isDistinct);
        }

        @Override
        public Counter newCounter(long nowInSec, boolean assumeLiveData, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness)
        {
            return new PagingAwareCounter(nowInSec, assumeLiveData, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
        }

        private class PagingAwareCounter extends CQLCounter
        {
            private PagingAwareCounter(long nowInSec,
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
                    rowsInCurrentPartition = perPartitionLimit - lastReturnedKeyRemaining;
                    // lastReturnedKey is the last key for which we're returned rows in the first page.
                    // So, since we know we have returned rows, we know we have accounted for the static row
                    // if any already, so force hasLiveStaticRow to false so we make sure to not count it
                    // once more.
                    hasLiveStaticRow = false;
                    staticRowBytes = 0;
                    partitionKeyBytes = partitionKey.getKeyLength();
                }
                else
                {
                    super.applyToPartition(partitionKey, staticRow);
                }
            }
        }

        @Override
        public String toString()
        {
            return new StringJoiner(", ", CQLPagingLimits.class.getSimpleName() + "[", "]")
                   .add("super=" + super.toString())
                   .add("lastReturnedKey=" + (lastReturnedKey != null ? ByteBufferUtil.bytesToHex(lastReturnedKey) : null))
                   .add("lastReturnedKeyRemaining=" + lastReturnedKeyRemaining)
                   .toString();
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

        protected final int groupBytesLimit;

        /**
         * The limit on the number of groups per partition
         */
        protected final int groupPerPartitionLimit;

        private CQLGroupByLimits(int groupLimit,
                                 int groupBytesLimit,
                                 int groupPerPartitionLimit,
                                 int rowLimit,
                                 int bytesLimit,
                                 AggregationSpecification groupBySpec,
                                 GroupingState state)
        {
            super(rowLimit, bytesLimit, NO_LIMIT, false);
            this.groupLimit = groupLimit;
            this.groupBytesLimit = groupBytesLimit;
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

        @Override
        public boolean isUnlimited()
        {
            return groupLimit == NO_LIMIT && groupBytesLimit == NO_LIMIT && groupPerPartitionLimit == NO_LIMIT && rowLimit == NO_LIMIT && bytesLimit == NO_LIMIT;
        }

        @Override
        public DataLimits forShortReadRetry(int toFetch)
        {
            return new CQLLimits(toFetch, NO_LIMIT, NO_LIMIT, false);
        }

        @Override
        public float estimateTotalResults(ColumnFamilyStore cfs) // TODO remove
        {
            // For the moment, we return the estimated number of rows as we have no good way of estimating 
            // the number of groups that will be returned. Hopefully, we should be able to fix
            // that problem at some point.
            return super.estimateTotalResults(cfs);
        }

        @Override
        public DataLimits forPaging(PageSize pageSize)
        {
            return new CQLGroupByLimits(Math.min(groupLimit, pageSize.rows),
                                        Math.min(groupBytesLimit, pageSize.bytes),
                                        groupPerPartitionLimit,
                                        rowLimit,
                                        bytesLimit,
                                        groupBySpec,
                                        state);
        }

        @Override
        public DataLimits forPaging(PageSize pageSize, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining)
        {
            return new CQLGroupByPagingLimits(Math.min(groupLimit, pageSize.rows),
                                              Math.min(groupBytesLimit, pageSize.bytes),
                                              groupPerPartitionLimit,
                                              rowLimit,
                                              bytesLimit,
                                              groupBySpec,
                                              state,
                                              lastReturnedKey,
                                              lastReturnedKeyRemaining);
        }

        @Override
        public DataLimits forGroupByInternalPaging(GroupingState state)
        {
            return new CQLGroupByLimits(groupLimit,
                                        groupBytesLimit,
                                        groupPerPartitionLimit,
                                        rowLimit,
                                        bytesLimit,
                                        groupBySpec,
                                        state);
        }

        @Override
        public Counter newCounter(long nowInSec,
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
        public int bytes()
        {
            return groupBytesLimit;
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
                 : new CQLGroupByLimits(groupLimit, groupBytesLimit, groupPerPartitionLimit, rowLimit, bytesLimit, groupBySpec, GroupingState.EMPTY_STATE);
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
            return ((GroupByAwareCounter) counter).rowsCounted < rowLimit && ((GroupByAwareCounter) counter).bytesCounted < bytesLimit
                    && counter.counted() < groupLimit && counter.bytesCounted() < groupBytesLimit;
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
             * The number of bytes counted so far.
             */
            protected int bytesCounted;
            /**
             * The number of rows counted so far.
             */
            protected int rowsCounted;

            protected int maxRowSize;

            /**
             * The number of rows counted so far in the current partition.
             */
            protected int rowsCountedInCurrentPartition;

            /**
             * The number of groups counted so far. A group is counted only once it is complete
             * (e.g the next one has been reached).
             */
            protected int groupCounted;

            protected int groupBytesCounted;

            /**
             * The number of groups in the current partition.
             */
            protected int groupInCurrentPartition;

            protected boolean hasUnfinishedGroup;

            protected boolean hasLiveStaticRow;

            protected int staticRowBytes;

            protected boolean hasReturnedRowsFromCurrentPartition;

            private GroupByAwareCounter(long nowInSec,
                                        boolean assumeLiveData,
                                        boolean countPartitionsWithOnlyStaticData,
                                        boolean enforceStrictLiveness)
            {
                super(nowInSec, assumeLiveData, enforceStrictLiveness);
                this.groupMaker = groupBySpec.newGroupMaker(state);
                this.countPartitionsWithOnlyStaticData = countPartitionsWithOnlyStaticData;

                // If the end of the partition was reached at the same time that the row limit, the last group might
                // not have been counted yet. Due to that we need to guess, based on the state, if the previous group
                // is still open.
                hasUnfinishedGroup = state.hasClustering();
                maxRowSize = state.bytes();
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
                    staticRowBytes = 0;
                    hasReturnedRowsFromCurrentPartition = true;
                    hasUnfinishedGroup = true;
                }
                else
                {
                    // We need to increment our count of groups if we have reached a new one and unless we had no new
                    // content added since we closed our last group (that is, if hasUnfinishedGroup). Note that we may get
                    // here with hasUnfinishedGroup == false in the following cases:
                    // * the partition limit was reached for the previous partition
                    // * the previous partition was containing only one static row
                    // * the rows of the last group of the previous partition were all marked as deleted
                    if (hasUnfinishedGroup && groupMaker.isNewGroup(partitionKey, Clustering.STATIC_CLUSTERING))
                    {
                        incrementGroupCount();
                        // If we detect, before starting the new partition, that we are done, we need to increase
                        // the per partition group count of the previous partition as the next page will start from
                        // there.
                        if (isDone())
                            incrementGroupInCurrentPartitionCount();
                        hasUnfinishedGroup = false;
                    }
                    hasReturnedRowsFromCurrentPartition = false;
                    hasLiveStaticRow = !staticRow.isEmpty() && isLive(staticRow);
                    staticRowBytes = hasLiveStaticRow ? staticRow.dataSize() : 0;
                }
                currentPartitionKey = partitionKey;
                // If we are done we need to preserve the groupInCurrentPartition and rowsCountedInCurrentPartition
                // because the pager need to retrieve the count associated to the last value it has returned.
                if (!isDone())
                {
                    groupInCurrentPartition = 0;
                    rowsCountedInCurrentPartition = 0;
                }
            }

            @Override
            protected Row applyToStatic(Row row)
            {
                // It's possible that we're "done" if the partition we just started bumped the number of groups (in
                // applyToPartition() above), in which case Transformation will still call this method. In that case, we
                // want to ignore the static row, it should (and will) be returned with the next page/group if needs be.
                if (enforceLimits && isDone())
                {
                    hasLiveStaticRow = false; // The row has not been returned
                    staticRowBytes = 0;
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
                    if (hasUnfinishedGroup)
                    {
                        incrementGroupCount();
                        incrementGroupInCurrentPartitionCount();
                    }
                    hasUnfinishedGroup = false;
                }

                // That row may have made us increment the group count, which may mean we're done for this partition, in
                // which case we shouldn't count this row (it won't be returned).
                if (enforceLimits && isDoneForPartition())
                {
                    hasUnfinishedGroup = false;
                    return null;
                }

                if (isLive(row))
                {
                    hasUnfinishedGroup = true;
                    incrementRowCount(row.dataSize());
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
            public int bytesCounted()
            {
                return groupBytesCounted;
            }

            @Override
            public int rowsCounted()
            {
                return rowsCounted;
            }

            @Override
            public int rowsCountedInCurrentPartition()
            {
                return rowsCountedInCurrentPartition;
            }

            /**
             * Increments the number of counted rows and adds the given number of bytes to the total counted size
             */
            protected void incrementRowCount(int rowSizeInBytes)
            {
                rowsCountedInCurrentPartition++;
                rowsCounted++;
                int rowSizeWithPartitionKey = rowSizeInBytes + currentPartitionKey.getKeyLength();
                bytesCounted += rowSizeWithPartitionKey;
                maxRowSize = Math.max(maxRowSize, rowSizeWithPartitionKey);
                if (rowsCounted >= rowLimit || bytesCounted >= bytesLimit)
                    stop();
            }

            private void incrementGroupCount()
            {
                groupCounted++;
                groupBytesCounted += maxRowSize;
                maxRowSize = 0;
                if (groupCounted >= groupLimit || groupBytesCounted >= groupBytesLimit)
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
                return groupCounted >= groupLimit || groupBytesCounted >= groupBytesLimit;
            }

            @Override
            public void onPartitionClose()
            {
                // Normally, we don't count static rows as from a CQL point of view, it will be merged with other
                // rows in the partition. However, if we only have the static row, it will be returned as one group
                // so count it.
                if (countPartitionsWithOnlyStaticData && hasLiveStaticRow && !hasReturnedRowsFromCurrentPartition)
                {
                    incrementRowCount(staticRowBytes);
                    incrementGroupCount();
                    incrementGroupInCurrentPartitionCount();
                    hasUnfinishedGroup = false;
                }
                super.onPartitionClose();
            }

            @Override
            public GroupingState recreateGroupingStateFor(ByteBuffer lastPartitionKey, Clustering<?> lastClustering)
            {
                return lastPartitionKey == null && lastClustering == null && maxRowSize == 0
                       ? GroupingState.EMPTY_STATE
                       : new GroupingState(lastPartitionKey, lastClustering, maxRowSize);
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
                if (hasUnfinishedGroup && groupCounted < groupLimit && groupBytesCounted < groupBytesLimit && bytesCounted < bytesLimit && rowsCounted < rowLimit)
                {
                    incrementGroupCount();
                    incrementGroupInCurrentPartitionCount();
                }

                super.onClose();
            }

            @Override
            public String toString()
            {
                return String.format("%s(bytes=%s/%s, rows=%s/%s, partition-rows=%s/%s, groups=%s/%s, group-bytes=%s/%s, partition-groups=%s/%s)", this.getClass().getName(),
                                     bytesCounted, bytesLimit,
                                     rowsCounted, rowLimit,
                                     rowsCountedInCurrentPartition, perPartitionLimit,
                                     groupCounted, groupLimit,
                                     groupBytesCounted, groupBytesLimit,
                                     groupInCurrentPartition, groupPerPartitionLimit);
            }
        }
    }

    private static class CQLGroupByPagingLimits extends CQLGroupByLimits
    {
        private final ByteBuffer lastReturnedKey;

        private final int lastReturnedKeyRemaining;

        public CQLGroupByPagingLimits(int groupLimit,
                                      int groupBytesLimit,
                                      int groupPerPartitionLimit,
                                      int rowLimit,
                                      int bytesLimit,
                                      AggregationSpecification groupBySpec,
                                      GroupingState state,
                                      ByteBuffer lastReturnedKey,
                                      int lastReturnedKeyRemaining)
        {
            super(groupLimit,
                  groupBytesLimit,
                  groupPerPartitionLimit,
                  rowLimit,
                  bytesLimit,
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
        public DataLimits forPaging(PageSize pageSize)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public DataLimits forPaging(PageSize pageSize, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public DataLimits forGroupByInternalPaging(GroupingState state)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Counter newCounter(long nowInSec, boolean assumeLiveData, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness)
        {
            assert state == GroupingState.EMPTY_STATE || lastReturnedKey.equals(state.partitionKey());
            return new PagingGroupByAwareCounter(nowInSec, assumeLiveData, countPartitionsWithOnlyStaticData, enforceStrictLiveness);
        }

        @Override
        public DataLimits withoutState()
        {
            return new CQLGroupByLimits(groupLimit, groupBytesLimit, groupPerPartitionLimit, rowLimit, bytesLimit, groupBySpec, GroupingState.EMPTY_STATE);
        }

        private class PagingGroupByAwareCounter extends GroupByAwareCounter
        {
            private PagingGroupByAwareCounter(long nowInSec, boolean assumeLiveData, boolean countPartitionsWithOnlyStaticData, boolean enforceStrictLiveness)
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
                    staticRowBytes = 0;
                    hasUnfinishedGroup = state.hasClustering();
                }
                else
                {
                    super.applyToPartition(partitionKey, staticRow);
                }
            }
        }

        @Override
        public String toString()
        {
            return new StringJoiner(", ", CQLGroupByPagingLimits.class.getSimpleName() + "[", "]")
                   .add("super=" + super.toString())
                   .add("lastReturnedKey=" + (lastReturnedKey != null ? ByteBufferUtil.bytesToHex(lastReturnedKey) : null))
                   .add("lastReturnedKeyRemaining=" + lastReturnedKeyRemaining)
                   .toString();
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
                    out.writeUnsignedVInt32(cqlLimits.rowLimit);
                    if (version >= MessagingService.VERSION_50)
                        out.writeUnsignedVInt32(cqlLimits.bytesLimit);
                    out.writeUnsignedVInt32(cqlLimits.perPartitionLimit);
                    out.writeBoolean(cqlLimits.isDistinct);
                    if (limits.kind() == Kind.CQL_PAGING_LIMIT)
                    {
                        CQLPagingLimits pagingLimits = (CQLPagingLimits)cqlLimits;
                        ByteBufferUtil.writeWithVIntLength(pagingLimits.lastReturnedKey, out);
                        out.writeUnsignedVInt32(pagingLimits.lastReturnedKeyRemaining);
                    }
                    break;
                case CQL_GROUP_BY_LIMIT:
                case CQL_GROUP_BY_PAGING_LIMIT:
                    CQLGroupByLimits groupByLimits = (CQLGroupByLimits) limits;
                    out.writeUnsignedVInt32(groupByLimits.groupLimit);
                    if (version >= MessagingService.VERSION_50)
                        out.writeUnsignedVInt32(groupByLimits.groupBytesLimit);
                    out.writeUnsignedVInt32(groupByLimits.groupPerPartitionLimit);
                    out.writeUnsignedVInt32(groupByLimits.rowLimit);
                    if (version >= MessagingService.VERSION_50)
                        out.writeUnsignedVInt32(groupByLimits.bytesLimit);

                    AggregationSpecification groupBySpec = groupByLimits.groupBySpec;
                    AggregationSpecification.serializer.serialize(groupBySpec, out, version);

                    GroupingState.serializer.serialize(groupByLimits.state, out, version, comparator);

                    if (limits.kind() == Kind.CQL_GROUP_BY_PAGING_LIMIT)
                    {
                        CQLGroupByPagingLimits pagingLimits = (CQLGroupByPagingLimits) groupByLimits;
                        ByteBufferUtil.writeWithVIntLength(pagingLimits.lastReturnedKey, out);
                        out.writeUnsignedVInt32(pagingLimits.lastReturnedKeyRemaining);
                    }
                    break;
            }
        }

        public DataLimits deserialize(DataInputPlus in, int version, TableMetadata metadata) throws IOException
        {
            Kind kind = Kind.values()[in.readUnsignedByte()];
            switch (kind)
            {
                case CQL_LIMIT:
                case CQL_PAGING_LIMIT:
                {
                    int rowLimit = in.readUnsignedVInt32();
                    int bytesLimit = version >= MessagingService.VERSION_50 ? (int) in.readUnsignedVInt() : NO_LIMIT;
                    int perPartitionLimit = in.readUnsignedVInt32();
                    boolean isDistinct = in.readBoolean();
                    if (kind == Kind.CQL_LIMIT)
                        return new CQLLimits(rowLimit, bytesLimit, perPartitionLimit, isDistinct);
                    ByteBuffer lastKey = ByteBufferUtil.readWithVIntLength(in);
                    int lastRemaining = in.readUnsignedVInt32();
                    return new CQLPagingLimits(rowLimit, bytesLimit, perPartitionLimit, isDistinct, lastKey, lastRemaining);
                }
                case CQL_GROUP_BY_LIMIT:
                case CQL_GROUP_BY_PAGING_LIMIT:
                {
                    int groupLimit = in.readUnsignedVInt32();
                    int groupBytesLimit = version >= MessagingService.VERSION_50 ? (int) in.readUnsignedVInt() : NO_LIMIT;
                    int groupPerPartitionLimit = in.readUnsignedVInt32();
                    int rowLimit = in.readUnsignedVInt32();
                    int bytesLimit = version >= MessagingService.VERSION_50 ? (int) in.readUnsignedVInt() : NO_LIMIT;

                    AggregationSpecification groupBySpec = AggregationSpecification.serializer.deserialize(in, version, metadata);

                    GroupingState state = GroupingState.serializer.deserialize(in, version, metadata.comparator);

                    if (kind == Kind.CQL_GROUP_BY_LIMIT)
                        return new CQLGroupByLimits(groupLimit,
                                                    groupBytesLimit,
                                                    groupPerPartitionLimit,
                                                    rowLimit,
                                                    bytesLimit, groupBySpec,
                                                    state);

                    ByteBuffer lastKey = ByteBufferUtil.readWithVIntLength(in);
                    int lastRemaining = in.readUnsignedVInt32();
                    return new CQLGroupByPagingLimits(groupLimit,
                                                      groupBytesLimit,
                                                      groupPerPartitionLimit,
                                                      rowLimit,
                                                      bytesLimit, groupBySpec,
                                                      state,
                                                      lastKey,
                                                      lastRemaining);
                }
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
                    if (version >= MessagingService.VERSION_50)
                        size += TypeSizes.sizeofUnsignedVInt(cqlLimits.bytesLimit);
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
                    if (version >= MessagingService.VERSION_50)
                        size += TypeSizes.sizeofUnsignedVInt(groupByLimits.groupBytesLimit);
                    size += TypeSizes.sizeofUnsignedVInt(groupByLimits.groupPerPartitionLimit);
                    size += TypeSizes.sizeofUnsignedVInt(groupByLimits.rowLimit);
                    if (version >= MessagingService.VERSION_50)
                        size += TypeSizes.sizeofUnsignedVInt(groupByLimits.bytesLimit);

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
                default:
                    throw new AssertionError();
            }
            return size;
        }
    }
}
