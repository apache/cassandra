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

import org.apache.cassandra.db.*;
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
        public boolean hasEnoughLiveData(CachedPartition cached, int nowInSec)
        {
            return false;
        }

        @Override
        public UnfilteredPartitionIterator filter(UnfilteredPartitionIterator iter, int nowInSec)
        {
            return iter;
        }

        @Override
        public UnfilteredRowIterator filter(UnfilteredRowIterator iter, int nowInSec)
        {
            return iter;
        }
    };

    // We currently deal with distinct queries by querying full partitions but limiting the result at 1 row per
    // partition (see SelectStatement.makeFilter). So an "unbounded" distinct is still actually doing some filtering.
    public static final DataLimits DISTINCT_NONE = new CQLLimits(NO_LIMIT, 1, true);

    public enum Kind { CQL_LIMIT, CQL_PAGING_LIMIT, THRIFT_LIMIT, SUPER_COLUMN_COUNTING_LIMIT }

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

    public abstract DataLimits forPaging(int pageSize);
    public abstract DataLimits forPaging(int pageSize, ByteBuffer lastReturnedKey, int lastReturnedKeyRemaining);

    public abstract DataLimits forShortReadRetry(int toFetch);

    public abstract boolean hasEnoughLiveData(CachedPartition cached, int nowInSec);

    /**
     * Returns a new {@code Counter} for this limits.
     *
     * @param nowInSec the current time in second (to decide what is expired or not).
     * @param assumeLiveData if true, the counter will assume that every row passed is live and won't
     * thus check for liveness, otherwise it will. This should be {@code true} when used on a
     * {@code RowIterator} (since it only returns live rows), false otherwise.
     * @return a new {@code Counter} for this limits.
     */
    public abstract Counter newCounter(int nowInSec, boolean assumeLiveData);

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

    public UnfilteredPartitionIterator filter(UnfilteredPartitionIterator iter, int nowInSec)
    {
        return this.newCounter(nowInSec, false).applyTo(iter);
    }

    public UnfilteredRowIterator filter(UnfilteredRowIterator iter, int nowInSec)
    {
        return this.newCounter(nowInSec, false).applyTo(iter);
    }

    public PartitionIterator filter(PartitionIterator iter, int nowInSec)
    {
        return this.newCounter(nowInSec, true).applyTo(iter);
    }

    /**
     * Estimate the number of results (the definition of "results" will be rows for CQL queries
     * and partitions for thrift ones) that a full scan of the provided cfs would yield.
     */
    public abstract float estimateTotalResults(ColumnFamilyStore cfs);

    public static abstract class Counter extends StoppingTransformation<BaseRowIterator<?>>
    {
        // false means we do not propagate our stop signals onto the iterator, we only count
        private boolean enforceLimits = true;

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

        public abstract boolean isDone();
        public abstract boolean isDoneForPartition();

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
            // When we do a short read retry, we're only ever querying the single partition on which we have a short read. So
            // we use toFetch as the row limit and use no perPartitionLimit (it would be equivalent in practice to use toFetch
            // for both argument or just for perPartitionLimit with no limit on rowLimit).
            return new CQLLimits(toFetch, NO_LIMIT, isDistinct);
        }

        public boolean hasEnoughLiveData(CachedPartition cached, int nowInSec)
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

            DataLimits.Counter counter = newCounter(nowInSec, false);
            try (UnfilteredRowIterator cacheIter = cached.unfilteredIterator(ColumnFilter.selection(cached.columns()), Slices.ALL, false);
                 UnfilteredRowIterator iter = counter.applyTo(cacheIter))
            {
                // Consume the iterator until we've counted enough
                while (iter.hasNext())
                    iter.next();
                return counter.isDone();
            }
        }

        public Counter newCounter(int nowInSec, boolean assumeLiveData)
        {
            return new CQLCounter(nowInSec, assumeLiveData);
        }

        public int count()
        {
            return rowLimit;
        }

        public int perPartitionCount()
        {
            return perPartitionLimit;
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
            protected final int nowInSec;
            protected final boolean assumeLiveData;

            protected int rowCounted;
            protected int rowInCurrentPartition;

            protected boolean hasLiveStaticRow;

            public CQLCounter(int nowInSec, boolean assumeLiveData)
            {
                this.nowInSec = nowInSec;
                this.assumeLiveData = assumeLiveData;
            }

            @Override
            public void applyToPartition(DecoratedKey partitionKey, Row staticRow)
            {
                rowInCurrentPartition = 0;
                if (!staticRow.isEmpty() && (assumeLiveData || staticRow.hasLiveData(nowInSec)))
                    hasLiveStaticRow = true;
            }

            @Override
            public Row applyToRow(Row row)
            {
                if (assumeLiveData || row.hasLiveData(nowInSec))
                    incrementRowCount();
                return row;
            }

            @Override
            public void onPartitionClose()
            {
                // Normally, we don't count static rows as from a CQL point of view, it will be merge with other
                // rows in the partition. However, if we only have the static row, it will be returned as one row
                // so count it.
                if (hasLiveStaticRow && rowInCurrentPartition == 0)
                    incrementRowCount();
                super.onPartitionClose();
            }

            private void incrementRowCount()
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
        public Counter newCounter(int nowInSec, boolean assumeLiveData)
        {
            return new PagingAwareCounter(nowInSec, assumeLiveData);
        }

        private class PagingAwareCounter extends CQLCounter
        {
            private PagingAwareCounter(int nowInSec, boolean assumeLiveData)
            {
                super(nowInSec, assumeLiveData);
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

        public boolean hasEnoughLiveData(CachedPartition cached, int nowInSec)
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
            DataLimits.Counter counter = newCounter(nowInSec, false);
            try (UnfilteredRowIterator cacheIter = cached.unfilteredIterator(ColumnFilter.selection(cached.columns()), Slices.ALL, false);
                 UnfilteredRowIterator iter = counter.applyTo(cacheIter))
            {
                // Consume the iterator until we've counted enough
                while (iter.hasNext())
                    iter.next();
                return counter.isDone();
            }
        }

        public Counter newCounter(int nowInSec, boolean assumeLiveData)
        {
            return new ThriftCounter(nowInSec, assumeLiveData);
        }

        public int count()
        {
            return partitionLimit * cellPerPartitionLimit;
        }

        public int perPartitionCount()
        {
            return cellPerPartitionLimit;
        }

        public float estimateTotalResults(ColumnFamilyStore cfs)
        {
            // remember that getMeansColumns returns a number of cells: we should clean nomenclature
            float cellsPerPartition = ((float) cfs.getMeanColumns()) / cfs.metadata.partitionColumns().regulars.size();
            return cellsPerPartition * cfs.estimateKeys();
        }

        protected class ThriftCounter extends Counter
        {
            protected final int nowInSec;
            protected final boolean assumeLiveData;

            protected int partitionsCounted;
            protected int cellsCounted;
            protected int cellsInCurrentPartition;

            public ThriftCounter(int nowInSec, boolean assumeLiveData)
            {
                this.nowInSec = nowInSec;
                this.assumeLiveData = assumeLiveData;
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

        public Counter newCounter(int nowInSec, boolean assumeLiveData)
        {
            return new SuperColumnCountingCounter(nowInSec, assumeLiveData);
        }

        protected class SuperColumnCountingCounter extends ThriftCounter
        {
            public SuperColumnCountingCounter(int nowInSec, boolean assumeLiveData)
            {
                super(nowInSec, assumeLiveData);
            }

            @Override
            public Row applyToRow(Row row)
            {
                // In the internal format, a row == a super column, so that's what we want to count.
                if (assumeLiveData || row.hasLiveData(nowInSec))
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
        public void serialize(DataLimits limits, DataOutputPlus out, int version) throws IOException
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
                case THRIFT_LIMIT:
                case SUPER_COLUMN_COUNTING_LIMIT:
                    ThriftLimits thriftLimits = (ThriftLimits)limits;
                    out.writeUnsignedVInt(thriftLimits.partitionLimit);
                    out.writeUnsignedVInt(thriftLimits.cellPerPartitionLimit);
                    break;
            }
        }

        public DataLimits deserialize(DataInputPlus in, int version) throws IOException
        {
            Kind kind = Kind.values()[in.readUnsignedByte()];
            switch (kind)
            {
                case CQL_LIMIT:
                case CQL_PAGING_LIMIT:
                    int rowLimit = (int)in.readUnsignedVInt();
                    int perPartitionLimit = (int)in.readUnsignedVInt();
                    boolean isDistinct = in.readBoolean();
                    if (kind == Kind.CQL_LIMIT)
                        return cqlLimits(rowLimit, perPartitionLimit, isDistinct);

                    ByteBuffer lastKey = ByteBufferUtil.readWithVIntLength(in);
                    int lastRemaining = (int)in.readUnsignedVInt();
                    return new CQLPagingLimits(rowLimit, perPartitionLimit, isDistinct, lastKey, lastRemaining);
                case THRIFT_LIMIT:
                case SUPER_COLUMN_COUNTING_LIMIT:
                    int partitionLimit = (int)in.readUnsignedVInt();
                    int cellPerPartitionLimit = (int)in.readUnsignedVInt();
                    return kind == Kind.THRIFT_LIMIT
                         ? new ThriftLimits(partitionLimit, cellPerPartitionLimit)
                         : new SuperColumnCountingLimits(partitionLimit, cellPerPartitionLimit);
            }
            throw new AssertionError();
        }

        public long serializedSize(DataLimits limits, int version)
        {
            long size = TypeSizes.sizeof((byte)limits.kind().ordinal());
            switch (limits.kind())
            {
                case CQL_LIMIT:
                case CQL_PAGING_LIMIT:
                    CQLLimits cqlLimits = (CQLLimits)limits;
                    size += TypeSizes.sizeofUnsignedVInt(cqlLimits.rowLimit);
                    size += TypeSizes.sizeofUnsignedVInt(cqlLimits.perPartitionLimit);
                    size += TypeSizes.sizeof(cqlLimits.isDistinct);
                    if (limits.kind() == Kind.CQL_PAGING_LIMIT)
                    {
                        CQLPagingLimits pagingLimits = (CQLPagingLimits)cqlLimits;
                        size += ByteBufferUtil.serializedSizeWithVIntLength(pagingLimits.lastReturnedKey);
                        size += TypeSizes.sizeofUnsignedVInt(pagingLimits.lastReturnedKeyRemaining);
                    }
                    break;
                case THRIFT_LIMIT:
                case SUPER_COLUMN_COUNTING_LIMIT:
                    ThriftLimits thriftLimits = (ThriftLimits)limits;
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
