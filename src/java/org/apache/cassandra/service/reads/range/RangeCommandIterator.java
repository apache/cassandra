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

package org.apache.cassandra.service.reads.range;

import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.metrics.ClientRangeRequestMetrics;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.CloseableIterator;

@VisibleForTesting
public abstract class RangeCommandIterator extends AbstractIterator<RowIterator> implements PartitionIterator
{
    private static final Logger logger = LoggerFactory.getLogger(RangeCommandIterator.class);

    @VisibleForTesting
    public static final ClientRangeRequestMetrics rangeMetrics = new ClientRangeRequestMetrics("RangeSlice");

    protected final CloseableIterator<ReplicaPlan.ForRangeRead> replicaPlans;
    private final int totalRangeCount;
    protected final PartitionRangeReadCommand command;
    protected final boolean enforceStrictLiveness;

    private final long startTime;
    protected final long queryStartNanoTime;
    protected DataLimits.Counter counter;
    private PartitionIterator sentQueryIterator;

    private final int maxConcurrencyFactor;
    protected int concurrencyFactor;
    // The two following "metric" are maintained to improve the concurrencyFactor
    // when it was not good enough initially.
    private int liveReturned;
    protected int rangesQueried;
    protected int batchesRequested = 0;

    @SuppressWarnings("resource")
    public static RangeCommandIterator create(CloseableIterator<ReplicaPlan.ForRangeRead> replicaPlans,
                                              PartitionRangeReadCommand command,
                                              int concurrencyFactor,
                                              int maxConcurrencyFactor,
                                              int totalRangeCount,
                                              long queryStartNanoTime)
    {
        return supportsEndpointGrouping(command) ? new EndpointGroupingRangeCommandIterator(replicaPlans,
                                                                                            command,
                                                                                            concurrencyFactor,
                                                                                            maxConcurrencyFactor,
                                                                                            totalRangeCount,
                                                                                            queryStartNanoTime)
                                                 : new NonGroupingRangeCommandIterator(replicaPlans,
                                                                                       command,
                                                                                       concurrencyFactor,
                                                                                       maxConcurrencyFactor,
                                                                                       totalRangeCount,
                                                                                       queryStartNanoTime);
    }

    RangeCommandIterator(CloseableIterator<ReplicaPlan.ForRangeRead> replicaPlans,
                         PartitionRangeReadCommand command,
                         int concurrencyFactor,
                         int maxConcurrencyFactor,
                         int totalRangeCount,
                         long queryStartNanoTime)
    {
        this.replicaPlans = replicaPlans;
        this.command = command;
        this.concurrencyFactor = concurrencyFactor;
        this.maxConcurrencyFactor = maxConcurrencyFactor;
        this.totalRangeCount = totalRangeCount;
        this.queryStartNanoTime = queryStartNanoTime;

        startTime = System.nanoTime();
        enforceStrictLiveness = command.metadata().enforceStrictLiveness();
    }

    @Override
    protected RowIterator computeNext()
    {
        try
        {
            while (sentQueryIterator == null || !sentQueryIterator.hasNext())
            {
                // If we don't have more range to handle, we're done
                if (!replicaPlans.hasNext())
                    return endOfData();

                // else, sends the next batch of concurrent queries (after having close the previous iterator)
                if (sentQueryIterator != null)
                {
                    liveReturned += counter.counted();
                    sentQueryIterator.close();

                    // It's not the first batch of queries and we're not done, so we we can use what has been
                    // returned so far to improve our rows-per-range estimate and update the concurrency accordingly
                    updateConcurrencyFactor();
                }
                sentQueryIterator = sendNextRequests();
            }

            return sentQueryIterator.next();
        }
        catch (UnavailableException e)
        {
            rangeMetrics.unavailables.mark();
            throw e;
        }
        catch (ReadTimeoutException e)
        {
            rangeMetrics.timeouts.mark();
            throw e;
        }
        catch (ReadFailureException e)
        {
            rangeMetrics.failures.mark();
            throw e;
        }
    }

    private void updateConcurrencyFactor()
    {
        liveReturned += counter.counted();

        concurrencyFactor = computeConcurrencyFactor(totalRangeCount, rangesQueried, maxConcurrencyFactor, command.limits().count(), liveReturned);
    }

    private static boolean supportsEndpointGrouping(ReadCommand command)
    {
        // With endpoint grouping, ranges executed on each endpoint are different, digest is unlikely to match.
        if (command.isDigestQuery())
            return false;

        // Endpoint grouping is currently only supported by SAI
        Index.QueryPlan queryPlan = command.indexQueryPlan();
        return queryPlan != null && queryPlan.supportsMultiRangeReadCommand();
    }

    @VisibleForTesting
    static int computeConcurrencyFactor(int totalRangeCount, int rangesQueried, int maxConcurrencyFactor, int limit, int liveReturned)
    {
        maxConcurrencyFactor = Math.max(1, Math.min(maxConcurrencyFactor, totalRangeCount - rangesQueried));
        if (liveReturned == 0)
        {
            // we haven't actually gotten any results, so query up to the limit if not results so far
            Tracing.trace("Didn't get any response rows; new concurrent requests: {}", maxConcurrencyFactor);
            return maxConcurrencyFactor;
        }

        // Otherwise, compute how many rows per range we got on average and pick a concurrency factor
        // that should allow us to fetch all remaining rows with the next batch of (concurrent) queries.
        int remainingRows = limit - liveReturned;
        float rowsPerRange = (float) liveReturned / (float) rangesQueried;
        int concurrencyFactor = Math.max(1, Math.min(maxConcurrencyFactor, Math.round(remainingRows / rowsPerRange)));
        logger.trace("Didn't get enough response rows; actual rows per range: {}; remaining rows: {}, new concurrent requests: {}",
                     rowsPerRange, remainingRows, concurrencyFactor);
        return concurrencyFactor;
    }

    protected abstract PartitionIterator sendNextRequests();

    @Override
    public void close()
    {
        try
        {
            if (sentQueryIterator != null)
                sentQueryIterator.close();

            replicaPlans.close();
        }
        finally
        {
            rangeMetrics.roundTrips.update(batchesRequested);
            long latency = System.nanoTime() - startTime;
            rangeMetrics.addNano(latency);
            Keyspace.openAndGetStore(command.metadata()).metric.coordinatorScanLatency.update(latency, TimeUnit.NANOSECONDS);
        }
    }

    @VisibleForTesting
    int rangesQueried()
    {
        return rangesQueried;
    }

    @VisibleForTesting
    int batchesRequested()
    {
        return batchesRequested;
    }

    @VisibleForTesting
    int maxConcurrencyFactor()
    {
        return maxConcurrencyFactor;
    }

    @VisibleForTesting
    int concurrencyFactor()
    {
        return concurrencyFactor;
    }
}
