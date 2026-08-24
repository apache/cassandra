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

import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.plan.Plan;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.utils.MonotonicClock;

import static java.lang.Math.max;

/**
 * Tracks state relevant to the execution of a single query, including metrics and timeout monitoring.
 */
@NotThreadSafe // this should only be manipulated by the single thread running the query it belongs to
public class QueryContext
{
    public static final boolean DISABLE_TIMEOUT = Boolean.getBoolean("cassandra.sai.test.disable.timeout");

    /** The thread ID that the query is running on, used to verify single-threaded access. */
    private final long owningThreadId = Thread.currentThread().getId();

    /** The query start time, in nanoseconds. Used to measure the query execution time. */
    private final long queryStartTimeNanos;

    /** How long the coordinator waits for SAI queries, in nanoseconds */
    private final long executionQuotaNano;

    /**
     * Whether the query has timed out, checked at {@link #checkpoint()}.
     */
    private boolean queryTimedOut = false;

    /** Number of sstables visited by the query. */
    private long sstablesHit = 0;

    /** Number of index segments having results for the query. */
    private long segmentsHit = 0;

    /**
     * Number of partition/row keys fetched from the indexes and that will be used to fetch rows from the base table.
     * They will be either partition keys in AA, or row keys in the later row-aware disk formats.
     */
    private long keysFetched = 0;

    /** Number of live partitions fetched from the storage engine, before post-filtering. */
    private long partitionsFetched = 0;

    /** Number of live partitions returned to the coordinator, after post-filtering. */
    private long partitionsReturned = 0;

    /** Number of deleted partitions that have been fetched. */
    private long partitionTombstonesFetched = 0;

    /** Number of live rows fetched from the storage engine, before post-filtering. */
    private long rowsFetched = 0;

    /** Number of live rows returned to the coordinator, after post-filtering. */
    private long rowsReturned = 0;

    /** Number of deleted individual rows or ranges of rows that have been fetched. */
    private long rowTombstonesFetched = 0;

    /** Number of cells fetched from the storage engine, regardless of liveness, before post-filtering. */
    private long cellsFetched = 0;

    /** Number of cells returned to the coordinator, regardless of liveness, after post-filtering. */
    private long cellsReturned = 0;

    /** Number of trie (literal or key) segments visited by the query. */
    private long trieSegmentsHit = 0;

    /** Number of times the query has jumped to the position of a row ID within a trie (literal or key) posting list. */
    private long triePostingsSkips = 0;

    /** Number of times the query has advanced into a trie (literal or key) posting list. */
    private long triePostingsDecodes = 0;

    /** Number of BKD (numeric) segments visited by the query. */
    private long bkdSegmentsHit = 0;

    /** Number of BKD (numeric) merged posting lists visited by the query. */
    private long bkdPostingListsHit = 0;

    /** Number of times the query has jumped to the position of a row ID within a BKD (numeric) posting list. */
    private long bkdPostingsSkips = 0;

    /** Number of times the query has advanced into a BKD (numeric) posting list. */
    private long bkdPostingsDecodes = 0;

    /** Cumulative time spent searching ANN graph, in nanoseconds. */
    private long annGraphSearchLatency = 0;

    /** The worst approximate score observed in ANN. */
    private float annRerankFloor = 0.0f; // only called from single-threaded setup code

    /** Metrics about the query plan. */
    private PlanInfo queryPlanInfo;

    @VisibleForTesting
    public QueryContext()
    {
        this(DatabaseDescriptor.getRangeRpcTimeout(TimeUnit.MILLISECONDS));
    }

    public QueryContext(long executionQuotaMs)
    {
        this.executionQuotaNano = TimeUnit.MILLISECONDS.toNanos(executionQuotaMs);
        this.queryStartTimeNanos = MonotonicClock.approxTime.now();
    }

    public long totalQueryTimeNs()
    {
        checkThreadOwnership();
        return MonotonicClock.approxTime.now() - queryStartTimeNanos;
    }

    public void addSstablesHit(long val)
    {
        checkThreadOwnership();
        sstablesHit += val;
    }

    public void addSegmentsHit(long val)
    {
        checkThreadOwnership();
        segmentsHit += val;
    }

    public void addKeysFetched(long val)
    {
        checkThreadOwnership();
        keysFetched += val;
    }

    public void addPartitionsFetched(long val)
    {
        checkThreadOwnership();
        partitionsFetched += val;
    }

    public void addPartitionsReturned(long val)
    {
        checkThreadOwnership();
        partitionsReturned += val;
    }

    public void addPartitionTombstonesFetched(long val)
    {
        checkThreadOwnership();
        partitionTombstonesFetched += val;
    }

    public void addRowsFetched(long val)
    {
        checkThreadOwnership();
        rowsFetched += val;
    }

    public void addRowsReturned(long val)
    {
        checkThreadOwnership();
        rowsReturned += val;
    }

    public void addRowTombstonesFetched(long val)
    {
        checkThreadOwnership();
        rowTombstonesFetched += val;
    }

    public void addCellsFetched(long val)
    {
        checkThreadOwnership();
        cellsFetched += val;
    }

    public void addCellsReturned(long val)
    {
        checkThreadOwnership();
        cellsReturned += val;
    }

    public void addTrieSegmentsHit(long val)
    {
        checkThreadOwnership();
        trieSegmentsHit += val;
    }

    public void addTriePostingsSkips(long val)
    {
        checkThreadOwnership();
        triePostingsSkips += val;
    }

    public void addTriePostingsDecodes(long val)
    {
        checkThreadOwnership();
        triePostingsDecodes += val;
    }

    public void addBkdSegmentsHit(long val)
    {
        checkThreadOwnership();
        bkdSegmentsHit += val;
    }

    public void addBkdPostingListsHit(long val)
    {
        checkThreadOwnership();
        bkdPostingListsHit += val;
    }

    public void addBkdPostingsSkips(long val)
    {
        checkThreadOwnership();
        bkdPostingsSkips += val;
    }

    public void addBkdPostingsDecodes(long val)
    {
        checkThreadOwnership();
        bkdPostingsDecodes += val;
    }

    public void addAnnGraphSearchLatency(long val)
    {
        checkThreadOwnership();
        annGraphSearchLatency += val;
    }

    /**
     * Checks if the query has exceeded its execution quota and aborts it if it has timed out.
     *
     * @throws AbortedOperationException if the query has timed out
     */
    public void checkpoint()
    {
        checkThreadOwnership();

        if (totalQueryTimeNs() >= executionQuotaNano && !DISABLE_TIMEOUT)
        {
            queryTimedOut = true;
            throw new AbortedOperationException();
        }
    }

    public float getAnnRerankFloor()
    {
        checkThreadOwnership();
        return annRerankFloor;
    }

    public void updateAnnRerankFloor(float observedFloor)
    {
        checkThreadOwnership();

        if (observedFloor < Float.POSITIVE_INFINITY)
            annRerankFloor = max(annRerankFloor, observedFloor);
    }

    public void recordQueryPlan(Plan.RowsIteration originalPlan, Plan.RowsIteration optimizedPlan)
    {
        if (CassandraRelevantProperties.SAI_QUERY_PLAN_METRICS_ENABLED.getBoolean())
            this.queryPlanInfo = new PlanInfo(originalPlan, optimizedPlan);
    }

    /**
     * @return a {@link Snapshot} representing an immutable version of this query context.
     */
    public Snapshot snapshot()
    {
        checkThreadOwnership();
        return new Snapshot(this);
    }

    /**
     * Verifies that the current thread is the owning thread of this QueryContext.
     * This is used to enforce single-threaded access to the QueryContext.
     *
     * @throws AssertionError if assertions are enabled and the current thread is not the owning thread
     */
    private void checkThreadOwnership()
    {
        assert Thread.currentThread().getId() == owningThreadId
                : String.format("QueryContext accessed from wrong thread. Expected thread ID: %d, Actual thread: %s (ID: %d)",
                owningThreadId, Thread.currentThread().getName(), Thread.currentThread().getId());
    }

    /**
     * A snapshot of all relevant metrics in a {@link QueryContext} at a specific point in time.
     * This class memoizes the values of those metrics so that they can be reused by multiple metrics instances,
     * without calculating the same values once and again.
     * Also, this class should be more lightweight than the full {@link QueryContext}, in case of needing to retain it
     * for long-ish periods of time, as in the case of the slow query logger, which tracks the metrics of the slowest
     * queries over a fixed period of time.
     */
    public static class Snapshot
    {
        public final long totalQueryTimeNs;
        public final long sstablesHit;
        public final long segmentsHit;
        public final long keysFetched;
        public final long partitionsFetched;
        public final long partitionsReturned;
        public final long partitionTombstonesFetched;
        public final long rowsFetched;
        public final long rowsReturned;
        public final long rowTombstonesFetched;
        public final long cellsFetched;
        public final long cellsReturned;
        public final long trieSegmentsHit;
        public final long triePostingsSkips;
        public final long triePostingsDecodes;
        public final long bkdSegmentsHit;
        public final long bkdPostingListsHit;
        public final long bkdPostingsSkips;
        public final long bkdPostingsDecodes;
        public final boolean queryTimedOut;
        public final long annGraphSearchLatency;

        @Nullable
        public final PlanInfo queryPlanInfo;

        /**
         * Creates a snapshot of all the metrics in the given {@link QueryContext}.
         *
         * @param context the query context to snapshot
         */
        private Snapshot(QueryContext context)
        {
            totalQueryTimeNs = context.totalQueryTimeNs();
            sstablesHit = context.sstablesHit;
            segmentsHit = context.segmentsHit;
            keysFetched = context.keysFetched;
            partitionsFetched = context.partitionsFetched;
            partitionsReturned = context.partitionsReturned;
            partitionTombstonesFetched = context.partitionTombstonesFetched;
            rowsFetched = context.rowsFetched;
            rowsReturned = context.rowsReturned;
            rowTombstonesFetched = context.rowTombstonesFetched;
            cellsFetched = context.cellsFetched;
            cellsReturned = context.cellsReturned;
            trieSegmentsHit = context.trieSegmentsHit;
            triePostingsSkips = context.triePostingsSkips;
            triePostingsDecodes = context.triePostingsDecodes;
            bkdSegmentsHit = context.bkdSegmentsHit;
            bkdPostingListsHit = context.bkdPostingListsHit;
            bkdPostingsSkips = context.bkdPostingsSkips;
            bkdPostingsDecodes = context.bkdPostingsDecodes;
            queryTimedOut = context.queryTimedOut;
            annGraphSearchLatency = context.annGraphSearchLatency;
            queryPlanInfo = context.queryPlanInfo;
        }
    }

    /**
     * Captures relevant information about a query plan, both original and optimized.
     */
    public static class PlanInfo
    {
        public final boolean searchExecutedBeforeOrder;
        public final boolean filterExecutedAfterOrderedScan;

        public final long costEstimated;
        public final long rowsToReturnEstimated;
        public final long rowsToFetchEstimated;
        public final long keysToIterateEstimated;
        public final int logSelectivityEstimated;

        public final int indexReferencesInQuery;
        public final int indexReferencesInPlan;

        public PlanInfo(@Nonnull Plan.RowsIteration originalPlan, @Nonnull Plan.RowsIteration optimizedPlan)
        {
            this.costEstimated = Math.round(optimizedPlan.fullCost());
            this.rowsToReturnEstimated = Math.round(optimizedPlan.expectedRows());
            this.rowsToFetchEstimated = Math.round(optimizedPlan.estimatedRowsToFetch());
            this.keysToIterateEstimated = Math.round(optimizedPlan.estimatedKeysToIterate());
            this.logSelectivityEstimated = Math.min(20, (int) Math.floor(-Math.log10(optimizedPlan.selectivity())));
            this.indexReferencesInQuery = originalPlan.referencedIndexCount();
            this.indexReferencesInPlan = optimizedPlan.referencedIndexCount();
            this.searchExecutedBeforeOrder = optimizedPlan.isSearchThenOrderHybrid();
            this.filterExecutedAfterOrderedScan = optimizedPlan.isOrderedScanThenFilterHybrid();
        }
    }
}
