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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * Tracks state relevant to the execution of a single query, including metrics and timeout monitoring.
 *
 * Fields here are non-volatile, as they are accessed from a single thread.
 */
@NotThreadSafe
public class QueryContext
{
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

    private final Map<SSTableReader, SSTableQueryContext> sstableQueryContexts = new HashMap<>();

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

    public SSTableQueryContext getSSTableQueryContext(SSTableReader reader)
    {
        return sstableQueryContexts.computeIfAbsent(reader, k -> new SSTableQueryContext(this));
    }

    public void checkpoint()
    {
        if (totalQueryTimeNs() >= executionQuotaNano)
        {
            queryTimeouts++;
            throw new AbortedOperationException();
        }
    }
}
