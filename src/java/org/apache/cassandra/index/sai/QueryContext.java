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

import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import io.github.jbellis.jvector.util.Bits;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

/**
 * Tracks state relevant to the execution of a single query, including metrics and timeout monitoring.
 */
@NotThreadSafe
public class QueryContext
{
    private static final boolean DISABLE_TIMEOUT = Boolean.getBoolean("cassandra.sai.test.disable.timeout");

    protected final long queryStartTimeNanos;

    public final long executionQuotaNano;

    private final LongAdder sstablesHit = new LongAdder();
    private final LongAdder segmentsHit = new LongAdder();
    private final LongAdder partitionsRead = new LongAdder();
    private final LongAdder rowsFiltered = new LongAdder();
    private final LongAdder rowsMatched = new LongAdder();
    private final LongAdder trieSegmentsHit = new LongAdder();

    private final LongAdder bkdPostingListsHit = new LongAdder();
    private final LongAdder bkdSegmentsHit = new LongAdder();

    private final LongAdder bkdPostingsSkips = new LongAdder();
    private final LongAdder bkdPostingsDecodes = new LongAdder();

    private final LongAdder triePostingsSkips = new LongAdder();
    private final LongAdder triePostingsDecodes = new LongAdder();

    private final LongAdder queryTimeouts = new LongAdder();

    private final LongAdder annNodesVisited = new LongAdder();

    private final LongAdder shadowedKeysLoopCount = new LongAdder();
    private final NavigableSet<PrimaryKey> shadowedPrimaryKeys = new ConcurrentSkipListSet<>();

    // Total count of rows in all sstables and memtables.
    private Long totalAvailableRows = null;

    // Determines the order of using indexes for filtering and sorting.
    // Null means the query execution order hasn't been decided yet.
    private FilterSortOrder filterSortOrder = null;

    // Estimates the probability of a row picked by the index to be accepted by the post filter.
    private float postFilterSelectivityEstimate = 1.0f;

    // Last used soft limit for vector search.
    private int softLimit = -1;

    @VisibleForTesting
    public QueryContext()
    {
        this(DatabaseDescriptor.getRangeRpcTimeout(TimeUnit.MILLISECONDS));
    }

    public QueryContext(long executionQuotaMs)
    {
        this.executionQuotaNano = TimeUnit.MILLISECONDS.toNanos(executionQuotaMs);
        this.queryStartTimeNanos = System.nanoTime();
    }

    public long totalQueryTimeNs()
    {
        return System.nanoTime() - queryStartTimeNanos;
    }

    // setters
    public void addSstablesHit(long val)
    {
        sstablesHit.add(val);
    }
    public void addSegmentsHit(long val) {
        segmentsHit.add(val);
    }
    public void addPartitionsRead(long val)
    {
        partitionsRead.add(val);
    }
    public void addRowsFiltered(long val)
    {
        rowsFiltered.add(val);
    }
    public void addRowsMatched(long val)
    {
        rowsMatched.add(val);
    }
    public void resetRowsMatched()
    {
        rowsMatched.reset();
    }
    public void addTrieSegmentsHit(long val)
    {
        trieSegmentsHit.add(val);
    }
    public void addBkdPostingListsHit(long val)
    {
        bkdPostingListsHit.add(val);
    }
    public void addBkdSegmentsHit(long val)
    {
        bkdSegmentsHit.add(val);
    }
    public void addBkdPostingsSkips(long val)
    {
        bkdPostingsSkips.add(val);
    }
    public void addBkdPostingsDecodes(long val)
    {
        bkdPostingsDecodes.add(val);
    }
    public void addTriePostingsSkips(long val)
    {
        triePostingsSkips.add(val);
    }
    public void addTriePostingsDecodes(long val)
    {
        triePostingsDecodes.add(val);
    }
    public void addQueryTimeouts(long val)
    {
        queryTimeouts.add(val);
    }
    public void addAnnNodesVisited(long val)
    {
        annNodesVisited.add(val);
    }

    public void addShadowedKeysLoopCount(long val)
    {
        shadowedKeysLoopCount.add(val);
    }

    public void setTotalAvailableRows(long totalAvailableRows)
    {
        this.totalAvailableRows = totalAvailableRows;
    }

    public void setPostFilterSelectivityEstimate(float postFilterSelectivityEstimate)
    {
        this.postFilterSelectivityEstimate = postFilterSelectivityEstimate;
    }

    public void setFilterSortOrder(FilterSortOrder filterSortOrder)
    {
        this.filterSortOrder = filterSortOrder;
    }

    public void setSoftLimit(int softLimit)
    {
        this.softLimit = softLimit;
    }

    // getters

    public long sstablesHit()
    {
        return sstablesHit.longValue();
    }
    public long segmentsHit() {
        return segmentsHit.longValue();
    }
    public long partitionsRead()
    {
        return partitionsRead.longValue();
    }
    public long rowsFiltered()
    {
        return rowsFiltered.longValue();
    }
    public long rowsMatched()
    {
        return rowsMatched.longValue();
    }
    public long trieSegmentsHit()
    {
        return trieSegmentsHit.longValue();
    }
    public long bkdPostingListsHit()
    {
        return bkdPostingListsHit.longValue();
    }
    public long bkdSegmentsHit()
    {
        return bkdSegmentsHit.longValue();
    }
    public long bkdPostingsSkips()
    {
        return bkdPostingsSkips.longValue();
    }
    public long bkdPostingsDecodes()
    {
        return bkdPostingsDecodes.longValue();
    }
    public long triePostingsSkips()
    {
        return triePostingsSkips.longValue();
    }
    public long triePostingsDecodes()
    {
        return triePostingsDecodes.longValue();
    }
    public long queryTimeouts()
    {
        return queryTimeouts.longValue();
    }
    public long annNodesVisited()
    {
        return annNodesVisited.longValue();
    }

    public Long totalAvailableRows()
    {
        return totalAvailableRows;
    }

    public FilterSortOrder filterSortOrder()
    {
        return filterSortOrder;
    }

    public Float postFilterSelectivityEstimate()
    {
        return postFilterSelectivityEstimate;
    }

    public int softLimit()
    {
        return softLimit;
    }

    public void checkpoint()
    {
        if (totalQueryTimeNs() >= executionQuotaNano && !DISABLE_TIMEOUT)
        {
            addQueryTimeouts(1);
            throw new AbortedOperationException();
        }
    }

    public long shadowedKeysLoopCount()
    {
        return shadowedKeysLoopCount.longValue();
    }

    public void recordShadowedPrimaryKey(PrimaryKey primaryKey)
    {
        boolean isNewKey = shadowedPrimaryKeys.add(primaryKey);
        assert isNewKey : "Duplicate shadowed primary key added. Key should have been filtered out earlier in query. " + primaryKey;
    }

    // Returns true if the row ID will be included or false if the row ID will be shadowed
    public boolean shouldInclude(long sstableRowId, PrimaryKeyMap primaryKeyMap)
    {
        return !shadowedPrimaryKeys.contains(primaryKeyMap.primaryKeyFromRowId(sstableRowId));
    }

    public boolean shouldInclude(PrimaryKey pk)
    {
        return !shadowedPrimaryKeys.contains(pk);
    }

    /**
     * @return shadowed primary keys, in ascending order
     */
    public NavigableSet<PrimaryKey> getShadowedPrimaryKeys()
    {
        return shadowedPrimaryKeys;
    }

    public Bits bitsetForShadowedPrimaryKeys(CassandraOnHeapGraph<PrimaryKey> graph)
    {
        if (getShadowedPrimaryKeys().isEmpty())
            return Bits.ALL;

        return new IgnoredKeysBits(graph, getShadowedPrimaryKeys());
    }

    private static class IgnoredKeysBits implements Bits
    {
        private final CassandraOnHeapGraph<PrimaryKey> graph;
        private final NavigableSet<PrimaryKey> ignored;

        public IgnoredKeysBits(CassandraOnHeapGraph<PrimaryKey> graph, NavigableSet<PrimaryKey> ignored)
        {
            this.graph = graph;
            this.ignored = ignored;
        }

        @Override
        public boolean get(int ordinal)
        {
            var keys = graph.keysFromOrdinal(ordinal);
            return keys.stream().anyMatch(k -> !ignored.contains(k));
        }

        @Override
        public int length()
        {
            return graph.size();
        }
    }

    /**
     * Determines the order of filtering and sorting operations.
     * Currently used only by vector search.
     */
    public enum FilterSortOrder
    {
        /** First get the matching keys from the non-vector indexes, then use vector index to sort them */
        FILTER_THEN_SORT,

        /** First get the candidates in ANN order from the vector index, then fetch the rows and post-filter them */
        SORT_THEN_FILTER
    }
}
