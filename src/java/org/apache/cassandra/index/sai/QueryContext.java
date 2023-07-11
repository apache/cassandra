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
import java.util.Collections;
import java.util.HashSet;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.hnsw.CassandraOnDiskHnsw;
import org.apache.cassandra.index.sai.disk.hnsw.CassandraOnHeapHnsw;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.lucene.util.Bits;

/**
 * Tracks state relevant to the execution of a single query, including metrics and timeout monitoring.
 *
 * Fields here are non-volatile, as they are accessed from a single thread.
 */
@NotThreadSafe
public class QueryContext
{
    private static final boolean DISABLE_TIMEOUT = Boolean.getBoolean("cassandra.sai.test.disable.timeout");

    private final long queryStartTimeNanos;

    public final long executionQuotaNano;

    public long sstablesHit = 0;
    public long segmentsHit = 0;
    public long partitionsRead = 0;
    public long rowsFiltered = 0;

    public long trieSegmentsHit = 0;

    public long bkdPostingListsHit = 0;
    public long bkdSegmentsHit = 0;

    public long bkdPostingsSkips = 0;
    public long bkdPostingsDecodes = 0;

    public long triePostingsSkips = 0;
    public long triePostingsDecodes = 0;

    public long tokenSkippingCacheHits = 0;
    public long tokenSkippingLookups = 0;

    public long queryTimeouts = 0;

    public int hnswVectorsAccessed;
    public int hnswVectorCacheHits;

    private TreeSet<PrimaryKey> shadowedPrimaryKeys; // allocate when needed

    @VisibleForTesting
    public QueryContext()
    {
        this(DatabaseDescriptor.getRangeRpcTimeout(TimeUnit.MILLISECONDS));
    }

    public QueryContext(long executionQuotaMs)
    {
        this.executionQuotaNano = TimeUnit.MILLISECONDS.toNanos(executionQuotaMs);
        queryStartTimeNanos = System.nanoTime();
    }

    public long totalQueryTimeNs()
    {
        return System.nanoTime() - queryStartTimeNanos;
    }

    public void incSstablesHit()
    {
        sstablesHit++;
    }

    public void checkpoint()
    {
        if (totalQueryTimeNs() >= executionQuotaNano && !DISABLE_TIMEOUT)
        {
            queryTimeouts++;
            throw new AbortedOperationException();
        }
    }

    public void recordShadowedPrimaryKey(PrimaryKey primaryKey)
    {
        if (shadowedPrimaryKeys == null)
            shadowedPrimaryKeys = new TreeSet<>();
        shadowedPrimaryKeys.add(primaryKey);
    }

    // Returns true if the row ID will be included or false if the row ID will be shadowed
    public boolean shouldInclude(long sstableRowId, PrimaryKeyMap primaryKeyMap)
    {
        return shadowedPrimaryKeys == null || !shadowedPrimaryKeys.contains(primaryKeyMap.primaryKeyFromRowId(sstableRowId));
    }

    public boolean containsShadowedPrimaryKey(PrimaryKey primaryKey)
    {
        return shadowedPrimaryKeys != null && shadowedPrimaryKeys.contains(primaryKey);
    }

    /**
     * @return shadowed primary keys, in ascending order
     */
    public NavigableSet<PrimaryKey> getShadowedPrimaryKeys()
    {
        if (shadowedPrimaryKeys == null)
            return Collections.emptyNavigableSet();
        return shadowedPrimaryKeys;
    }

    public Bits bitsetForShadowedPrimaryKeys(CassandraOnHeapHnsw<PrimaryKey> graph)
    {
        if (shadowedPrimaryKeys == null)
            return null;

        return new IgnoredKeysBits(graph, shadowedPrimaryKeys);
    }

    public Bits bitsetForShadowedPrimaryKeys(SegmentMetadata metadata, PrimaryKeyMap primaryKeyMap, CassandraOnDiskHnsw graph) throws IOException
    {
        Set<Integer> ignoredOrdinals = null;
        try (var ordinalsView = graph.getOrdinalsView())
        {
            for (PrimaryKey primaryKey : getShadowedPrimaryKeys())
            {
                // not in current segment
                if (primaryKey.compareTo(metadata.minKey) < 0 || primaryKey.compareTo(metadata.maxKey) > 0)
                    continue;

                long sstableRowId = primaryKeyMap.rowIdFromPrimaryKey(primaryKey);
                if (sstableRowId == Long.MAX_VALUE) // not found
                    continue;

                int segmentRowId = metadata.toSegmentRowId(sstableRowId);
                // not in segment yet
                if (segmentRowId < 0)
                    continue;
                // end of segment
                if (segmentRowId > metadata.maxSSTableRowId)
                    break;

                int ordinal = ordinalsView.getOrdinalForRowId(segmentRowId);
                if (ordinal >= 0)
                {
                    if (ignoredOrdinals == null)
                        ignoredOrdinals = new HashSet<>();
                    ignoredOrdinals.add(ordinal);
                }
            }
        }

        if (ignoredOrdinals == null)
            return null;

        return new IgnoringBits(graph, ignoredOrdinals, metadata);
    }

    private static class IgnoringBits implements Bits
    {
        private final CassandraOnDiskHnsw graph;
        private final Set<Integer> ignoredOrdinals;
        private final int length;

        public IgnoringBits(CassandraOnDiskHnsw graph, Set<Integer> ignoredOrdinals, SegmentMetadata metadata)
        {
            this.graph = graph;
            this.ignoredOrdinals = ignoredOrdinals;
            this.length = 1 + metadata.toSegmentRowId(metadata.maxSSTableRowId);
        }

        @Override
        public boolean get(int index)
        {
            return !ignoredOrdinals.contains(index);
        }

        @Override
        public int length()
        {
            return length;
        }
    }

    private static class IgnoredKeysBits implements Bits
    {
        private final CassandraOnHeapHnsw<PrimaryKey> graph;
        private final NavigableSet<PrimaryKey> ignored;

        public IgnoredKeysBits(CassandraOnHeapHnsw<PrimaryKey> graph, NavigableSet<PrimaryKey> ignored)
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
}
