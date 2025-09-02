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
package org.apache.cassandra.service.reads.tracked;

import java.util.ArrayList;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.service.reads.ReadCoordinator;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.replication.ExpiredStatePurger;
import org.apache.cassandra.replication.Log2OffsetsMap;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.ShortMutationId;
import org.apache.cassandra.service.reads.SpeculativeRetryPolicy;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.jctools.maps.NonBlockingHashMap;

import org.apache.cassandra.transport.Dispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/**
 * Since the read reconciliations don't use 2 way callbacks, maps of active reads and reconciliations
 * are maintained and expired here.
 * <p>
 * Borrowed heavily from RequestCallbacks
 */
public class TrackedLocalReads implements ExpiredStatePurger.Expireable
{
    private static final Logger logger = LoggerFactory.getLogger(TrackedLocalReads.class);

    private final NonBlockingHashMap<TrackedRead.Id, Coordinator> coordinators = new NonBlockingHashMap<>();

    public TrackedLocalReads()
    {
        ExpiredStatePurger.instance.register(this);
    }

    public AsyncPromise<TrackedDataResponse> beginRead(
        TrackedRead.Id readId,
        ClusterMetadata metadata,
        ReadCommand command,
        ConsistencyLevel consistencyLevel,
        int[] summaryNodes,
        Dispatcher.RequestTime requestTime,
        Consumer<PartialTrackedRead> partialReadConsumer)
    {
        Keyspace keyspace = Keyspace.open(command.metadata().keyspace);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(command.metadata().id);
        SpeculativeRetryPolicy retry = cfs.metadata().params.speculativeRetry;
        ReplicaPlan.AbstractForRead<?, ?> replicaPlan;

        if (command instanceof SinglePartitionReadCommand)
        {
            replicaPlan = ReplicaPlans.forRead(metadata,
                                               keyspace,
                                               command.metadata().id,
                                               ((SinglePartitionReadCommand) command).partitionKey().getToken(),
                                               command.indexQueryPlan(),
                                               consistencyLevel,
                                               retry,
                                               ReadCoordinator.DEFAULT);
        }
        else
        {
            // TODO: confirm range we're reading doesn't span multiple replica sets
            replicaPlan = ReplicaPlans.forRangeRead(keyspace,
                                                    command.metadata().id,
                                                    command.indexQueryPlan(),
                                                    consistencyLevel,
                                                    command.dataRange().keyRange(),
                                                    1);
        }
        // TODO: confirm all summaryNodes are present in the replica plan
        AsyncPromise<TrackedDataResponse> promise = new AsyncPromise<>();
        beginReadInternal(readId, command, replicaPlan, summaryNodes, requestTime, partialReadConsumer, promise);
        return promise;
    }

    // TODO (expected): skip local summaries and reconcile when summaryNodes is empty (e.g. for CL.ONE)
    private void beginReadInternal(
        TrackedRead.Id readId,
        ReadCommand command,
        ReplicaPlan.AbstractForRead<?, ?> replicaPlan,
        int[] summaryNodes,
        Dispatcher.RequestTime requestTime,
        Consumer<PartialTrackedRead> partialReadConsumer,
        AsyncPromise<TrackedDataResponse> promise)
    {
        PartialTrackedRead read = null;
        MutationSummary secondarySummary;

        MutationSummary initialSummary = command.createMutationSummary(false);
        ReadExecutionController controller = command.executionController(false);

        try
        {
            read = command.beginTrackedRead(controller);
            // Create another summary once initial data has been read fully. We do this to catch
            // any mutations that may have arrived during initial read execution.
            secondarySummary = command.createMutationSummary(true);
            processDelta(read, initialSummary, secondarySummary);
        }
        catch (Exception e)
        {
            controller.close();
            logger.trace("Aborting read {}", readId);
            if (read != null) read.close();
            throw e;
        }

        Coordinator coordinator =
                new Coordinator(readId, promise, command.columnFilter(), read, replicaPlan.consistencyLevel(), requestTime);
        coordinators.put(readId, coordinator);

        // TODO (expected): reconsider the approach to tracked mutation metrics
        ReadRepairMetrics.trackedReconcile.mark();

        // TODO (consider): is it possible to exit right here if this is the last missing summary, and we can tell that
        //                  no node needs anything from anyone?

        // pass the final summary to the reconcile service
        ReadReconciliations.instance.acceptLocalSummary(readId, secondarySummary, summaryNodes);
    }

    @VisibleForTesting
    public static void processDelta(PartialTrackedRead read, MutationSummary initialSummary, MutationSummary secondarySummary)
    {
        // Compute any mutations that we could've missed during initial read execution.
        ArrayList<ShortMutationId> delta = new ArrayList<>();
        MutationSummary.difference(secondarySummary, initialSummary, delta);
        delta.forEach(read::augment);
    }

    public void acknowledgeReconcile(TrackedRead.Id readId, Log2OffsetsMap<?> augmentingOffsets)
    {
        Coordinator coordinator = coordinators.remove(readId);
        if (coordinator != null)
            coordinator.acknowledgeReconcile(augmentingOffsets);
    }

    @Override
    public int expire(long nanoTime)
    {
        int n = 0;
        for (Map.Entry<TrackedRead.Id, Coordinator> entry : coordinators.entrySet())
        {
            TrackedRead.Id id = entry.getKey();
            Coordinator coordinator = entry.getValue();
            if (coordinator.isPurgeable(nanoTime) && coordinators.remove(id, coordinator))
            {
                coordinator.abort();
                n++;
            }
        }
        return n;
    }

    private static class Coordinator
    {
        private final TrackedRead.Id readId;
        private final AsyncPromise<TrackedDataResponse> promise;
        private final ColumnFilter selection;
        private final PartialTrackedRead read;
        private final ConsistencyLevel consistencyLevel;
        private final Dispatcher.RequestTime requestTime;

        Coordinator(
                TrackedRead.Id readId,
                AsyncPromise<TrackedDataResponse> promise,
                ColumnFilter selection,
                PartialTrackedRead read,
                ConsistencyLevel consistencyLevel,
                Dispatcher.RequestTime requestTime)
        {
            this.readId = readId;
            this.promise = promise;
            this.selection = selection;
            this.read = Preconditions.checkNotNull(read);
            this.consistencyLevel = consistencyLevel;
            this.requestTime = requestTime;
        }

        boolean isPurgeable(long nanoTime)
        {
            long deadline = requestTime.computeDeadline(read.command().verb().expiresAfterNanos());
            return nanoTime - deadline > 0;
        }

        void abort()
        {
            read.close();
        }

        void acknowledgeReconcile(Log2OffsetsMap<?> augmentingOffsets)
        {
            logger.trace("Reconciliation completed for {}, missing {}", readId, augmentingOffsets);

            Stage.READ.submit(() -> { 

                try
                {
                    read.augment(augmentingOffsets);
                    complete();
                } catch (Throwable t) {
                    logger.error("Exception thrown during read completion", t);
                    promise.tryFailure(t);
                    throw t;
                }
            });
        }

        private void complete()
        {
            try (PartialTrackedRead.CompletedRead completedRead = read.complete())
            {
                TrackedDataResponse response = completedRead.response();
                Future<TrackedDataResponse> followUp = completedRead.followupRead(response, consistencyLevel, requestTime);

                if (followUp != null)
                {
                    ReadCommand command = read.command();
                    followUp.addCallback((newResponse, error) -> {
                        if (error != null)
                        {
                            promise.tryFailure(error);
                            return;
                        }
                        promise.trySuccess(newResponse);
                    });
                }
                else
                {
                    promise.trySuccess(response);
                }
            }
            catch (Exception e)
            {
                promise.tryFailure(e);
                throw e;
            }
            finally
            {
                read.close();
            }
        }
    }
}
