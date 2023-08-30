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
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.exceptions.QueryCancelledException;
import org.apache.cassandra.utils.Clock;

import static org.apache.cassandra.config.CassandraRelevantProperties.SAI_TEST_DISABLE_TIMEOUT;

/**
 * Tracks state relevant to the execution of a single query, including metrics and timeout monitoring.
 *
 * Fields here are non-volatile, as they are accessed from a single thread.
 */
@NotThreadSafe
public class QueryContext
{
    private static final boolean DISABLE_TIMEOUT = SAI_TEST_DISABLE_TIMEOUT.getBoolean();

    private final ReadCommand readCommand;
    private final long queryStartTimeNanos;

    public final long executionQuotaNano;

    public long sstablesHit = 0;
    public long segmentsHit = 0;
    public long partitionsRead = 0;
    public long rowsFiltered = 0;

    public long trieSegmentsHit = 0;
    public long triePostingsSkips = 0;
    public long triePostingsDecodes = 0;

    public long balancedTreePostingListsHit = 0;
    public long balancedTreeSegmentsHit = 0;
    public long balancedTreePostingsSkips = 0;
    public long balancedTreePostingsDecodes = 0;

    public boolean queryTimedOut = false;

    private VectorQueryContext vectorContext;

    public QueryContext(ReadCommand readCommand, long executionQuotaMs)
    {
        this.readCommand = readCommand;
        executionQuotaNano = TimeUnit.MILLISECONDS.toNanos(executionQuotaMs);
        queryStartTimeNanos = Clock.Global.nanoTime();
    }

    public long totalQueryTimeNs()
    {
        return Clock.Global.nanoTime() - queryStartTimeNanos;
    }

    public void checkpoint()
    {
        if (totalQueryTimeNs() >= executionQuotaNano && !DISABLE_TIMEOUT)
        {
            queryTimedOut = true;
            throw new QueryCancelledException(readCommand);
        }
    }

    public VectorQueryContext vectorContext()
    {
        if (vectorContext == null)
            vectorContext = new VectorQueryContext(readCommand);
        return vectorContext;
    }
}
