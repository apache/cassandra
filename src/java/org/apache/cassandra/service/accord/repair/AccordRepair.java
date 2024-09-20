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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.BarrierType;
import accord.api.RoutingKey;
import accord.primitives.Ranges;
import accord.primitives.Seekables;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.AccordSplitter;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.AccordTopology;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.config.CassandraRelevantProperties.ACCORD_REPAIR_RANGE_STEP_UPDATE_INTERVAL;

/*
 * Accord repair consists of creating a barrier transaction for all the ranges which ensure that all Accord transactions
 * before the Epoch and point in time at which the repair started have their side effects visible to Paxos and regular quorum reads.
 */
public class AccordRepair
{
    private static final Logger logger = LoggerFactory.getLogger(AccordRepair.class);

    public static final BigInteger TWO = BigInteger.valueOf(2);

    private final SharedContext ctx;
    private final ColumnFamilyStore cfs;

    private final Ranges ranges;

    private final AccordSplitter splitter;
    private final boolean requireAllEndpoints;
    private final List<InetAddressAndPort> endpoints;

    private BigInteger rangeStep;

    private final Epoch minEpoch = ClusterMetadata.current().epoch;

    private volatile Throwable shouldAbort = null;

    public AccordRepair(SharedContext ctx, ColumnFamilyStore cfs, IPartitioner partitioner, String keyspace, Collection<Range<Token>> ranges, boolean requireAllEndpoints, List<InetAddressAndPort> endpoints)
    {
        this.ctx = ctx;
        this.cfs = cfs;
        this.requireAllEndpoints = requireAllEndpoints;
        this.endpoints = endpoints;
        this.ranges = AccordTopology.toAccordRanges(keyspace, ranges);
        this.splitter = partitioner.accordSplitter().apply(this.ranges);
    }

    public Epoch minEpoch()
    {
        return minEpoch;
    }

    public Ranges repair() throws Throwable
    {
        List<accord.primitives.Range> repairedRanges = new ArrayList<>();
        for (accord.primitives.Range range : ranges)
            repairedRanges.addAll(repairRange((TokenRange)range));
        return Ranges.of(repairedRanges.toArray(new accord.primitives.Range[0]));
    }

    public Future<Ranges> repair(Executor executor)
    {
        AsyncPromise<Ranges> future = new AsyncPromise<>();
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
    }

    private List<accord.primitives.Range> repairRange(TokenRange range) throws Throwable
    {
        List<accord.primitives.Range> repairedRanges = new ArrayList<>();
        int rangeStepUpdateInterval = ACCORD_REPAIR_RANGE_STEP_UPDATE_INTERVAL.getInt();
        RoutingKey remainingStart = range.start();
        // TODO (expected): repair ranges should have a configurable lower limit of split size so already small repairs aren't broken up into excessively tiny ones
        BigInteger rangeSize = splitter.sizeOf(range);
        if (rangeStep == null)
        {
            BigInteger divide = splitter.divide(rangeSize, 10000);
            rangeStep = divide.equals(BigInteger.ZERO) ? rangeSize : BigInteger.ONE.max(divide);
        }

        BigInteger offset = BigInteger.ZERO;

        TokenRange lastRepaired = null;
        int iteration = 0;
        while (true)
        {
            if (shouldAbort != null)
                throw shouldAbort;
            iteration++;
            if (iteration % rangeStepUpdateInterval == 0)
                rangeStep = rangeStep.multiply(TWO);

            BigInteger remaining = rangeSize.subtract(offset);
            BigInteger length = remaining.min(rangeStep);

            long start = ctx.clock().nanoTime();
            boolean dependencyOverflow = false;
            try
            {
                // Splitter is approximate so it can't work right up to the end
                TokenRange toRepair;
                if (splitter.compare(offset, rangeSize) >= 0)
                {
                    if (remainingStart.equals(range.end()))
                    {
                        logger.info("Completed barriers for {} in {} iterations", range, iteration - 1);
                        return repairedRanges;
                    }

                    // Final repair is whatever remains
                    toRepair = range.newRange(remainingStart, range.end());
                }
                else
                {
                    toRepair = splitter.subRange(range, offset, splitter.add(offset, length));
                    checkState(iteration > 1 || toRepair.start().equals(range.start()));
                }
                checkState(!toRepair.equals(lastRepaired), "Shouldn't repair the same range twice");
                checkState(lastRepaired == null || toRepair.start().equals(lastRepaired.end()), "Next range should directly follow previous range");
                lastRepaired = toRepair;

                Ranges barrieredRanges;
                if (requireAllEndpoints)
                    barrieredRanges = (Ranges)AccordService.instance().repairWithRetries(Seekables.of(toRepair), minEpoch.getEpoch(), BarrierType.global_sync, false, endpoints);
                else
                    barrieredRanges = (Ranges)AccordService.instance().barrierWithRetries(Seekables.of(toRepair), minEpoch.getEpoch(), BarrierType.global_sync, false);
                for (accord.primitives.Range barrieredRange : barrieredRanges)
                    repairedRanges.add(barrieredRange);

                remainingStart = toRepair.end();
            }
            catch (RuntimeException e)
            {
                cfs.metric.accordRepairUnexpectedFailures.mark();
                throw e;
            }
            catch (Throwable t)
            {
                cfs.metric.accordRepairUnexpectedFailures.mark();
                throw new RuntimeException(t);
            }
            finally
            {
                long end = ctx.clock().nanoTime();
                cfs.metric.accordRepair.addNano(end - start);
            }

            // TODO when dependency limits are added to Accord need to test repair overflow
            if (dependencyOverflow)
            {
                offset = offset.subtract(rangeStep);
                if (rangeStep.equals(BigInteger.ONE))
                    throw new IllegalStateException("Unable to repair without overflowing with range step of 1");
                rangeStep = BigInteger.ONE.max(rangeStep.divide(TWO));
                continue;
            }

            offset = offset.add(length);
        }
    }
}
