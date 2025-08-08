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

package org.apache.cassandra.service.accord.repair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;

import accord.local.durability.DurabilityService.SyncRemote;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.metrics.LatencyMetrics;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.AccordTopology;
import org.apache.cassandra.service.accord.IAccordService;
import org.apache.cassandra.service.accord.RequestBookkeeping;
import org.apache.cassandra.service.accord.TimeOnlyRequestBookkeeping.LatencyRequestBookkeeping;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

import static accord.local.durability.DurabilityService.SyncLocal.NoLocal;
import static accord.local.durability.DurabilityService.SyncRemote.All;
import static accord.local.durability.DurabilityService.SyncRemote.Quorum;
import static accord.primitives.Timestamp.mergeMax;
import static accord.primitives.Timestamp.minForEpoch;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.config.DatabaseDescriptor.getAccordRepairTimeoutNanos;

/*
 * Accord repair consists of creating a barrier transaction for all the ranges which ensure that all Accord transactions
 * before the Epoch and point in time at which the repair started have their side effects visible to Paxos and regular quorum reads.
 */
public class AccordRepair
{
    private final SharedContext ctx;
    private final ColumnFamilyStore cfs;
    private final TimeUUID repairId;

    private final Ranges ranges;

    private final SyncRemote syncRemote;

    private final Epoch minEpoch = ClusterMetadata.current().epoch;

    private volatile Throwable shouldAbort = null;
    private volatile Thread waiting;

    public AccordRepair(SharedContext ctx, ColumnFamilyStore cfs, TimeUUID repairId, String keyspace, Collection<Range<Token>> ranges, boolean requireAllEndpoints)
    {
        this.ctx = ctx;
        this.cfs = cfs;
        this.repairId = repairId;
        this.syncRemote = requireAllEndpoints ? All : Quorum;
        this.ranges = AccordTopology.toAccordRanges(keyspace, ranges);
    }

    public Epoch minEpoch()
    {
        return minEpoch;
    }

    public static class AccordRepairResult
    {
        public final Ranges repairedRanges;
        public final long maxHlc;

        public AccordRepairResult(Ranges ranges, long maxHlc)
        {
            this.repairedRanges = ranges;
            this.maxHlc = maxHlc;
        }
    }

    public AccordRepairResult repair() throws Throwable
    {
        List<accord.primitives.Range> repairedRanges = new ArrayList<>();
        long maxHLC = Long.MIN_VALUE;
        for (accord.primitives.Range range : ranges)
        {
            Pair<List<accord.primitives.Range>, Long> rangesAndMaxHLC = repairRange((TokenRange) range);
            repairedRanges.addAll(rangesAndMaxHLC.left);
            maxHLC = Math.max(maxHLC, rangesAndMaxHLC.right);
        }
        return new AccordRepairResult(Ranges.of(repairedRanges.toArray(new accord.primitives.Range[0])), maxHLC);
    }

    public Future<AccordRepairResult> repair(Executor executor)
    {
        AsyncPromise<AccordRepairResult> future = new AsyncPromise<>();
        executor.execute(() -> {
            try
            {
                future.trySuccess(repair());
            }
            catch (Throwable e)
            {
                future.tryFailure(e);
            }
        });
        return future;
    }

    protected void abort(@Nullable Throwable reason)
    {
        shouldAbort = reason == null ? new RuntimeException("Abort") : reason;
        Thread thread = waiting;
        if (thread != null)
            thread.interrupt();
    }

    private Pair<List<accord.primitives.Range>, Long> repairRange(TokenRange range) throws Throwable
    {
        List<accord.primitives.Range> repairedRanges = new ArrayList<>();
        if (shouldAbort != null)
            throw shouldAbort;

        LatencyMetrics latency = null;
        {
            TableMetadata metadata = Schema.instance.getTableMetadata(range.table());
            if (metadata != null)
            {
                ColumnFamilyStore cfs = Keyspace.openAndGetStore(metadata);
                if (cfs != null)
                    latency = cfs.metric.accordRepair;
            }
        }
        long start = ctx.clock().nanoTime();
        try
        {
            IAccordService service = AccordService.instance();
            Ranges ranges = AccordService.intersecting(Ranges.of(range));
            waiting = Thread.currentThread();
            RequestBookkeeping bookkeeping = new LatencyRequestBookkeeping(latency);
            long timeoutNanos = getAccordRepairTimeoutNanos();
            long maxHlc = AccordService.getBlocking(service.maxConflict(ranges).flatMap(conflict -> {
                Timestamp conflictMax = mergeMax(conflict, minForEpoch(this.minEpoch.getEpoch()));
                return service.sync("[repairId #" + repairId + ']', conflictMax, Ranges.of(range), null, NoLocal, syncRemote, timeoutNanos, NANOSECONDS).map(ignored -> conflictMax.hlc());
            }), ranges, bookkeeping, start, start + timeoutNanos);
            waiting = null;

            if (shouldAbort != null)
                throw shouldAbort;

            for (accord.primitives.Range r : ranges)
                repairedRanges.add(r);

            return Pair.create(repairedRanges, maxHlc);
        }
        catch (Throwable t)
        {
            cfs.metric.accordRepairUnexpectedFailures.mark();
            if (shouldAbort != null)
            {
                shouldAbort.addSuppressed(t);
                throw shouldAbort;
            }
            throw t;
        }
        finally
        {
            long end = ctx.clock().nanoTime();
            cfs.metric.accordRepair.addNano(end - start);
        }
    }
}
