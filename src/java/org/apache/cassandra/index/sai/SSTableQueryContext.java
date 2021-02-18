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

import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;

/**
 * Tracks SSTable-specific state relevant to the execution of a single query.
 *
 * Fields here are non-volatile, as they are accessed from a single thread.
 */
@NotThreadSafe
public class SSTableQueryContext
{
    public final QueryContext queryContext;

    // During intersection queries, multiple column indexes touch the same exact tokens as we skip
    // between range iterators. Caching the values of these global SSTable-specific lookups allows us to avoid
    // large chunks of duplicated work.
    public long prevTokenValue = Long.MIN_VALUE;
    public long prevSSTableRowId = -1;

    public long prevSkipToTokenValue = Long.MIN_VALUE;
    public long prevSkipToSSTableRowId = -1;

    public SSTableQueryContext(QueryContext queryContext)
    {
        this.queryContext = queryContext;
    }

    @VisibleForTesting
    public static SSTableQueryContext forTest()
    {
        return new SSTableQueryContext(new QueryContext());
    }

    public void markTokenSkippingLookup()
    {
        queryContext.tokenSkippingLookups++;
    }

    public void markTokenSkippingCacheHit()
    {
        queryContext.tokenSkippingCacheHits++;
    }
}
