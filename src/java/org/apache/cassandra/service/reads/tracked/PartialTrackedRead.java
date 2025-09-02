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

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.transform.RTBoundValidator;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.replication.Log2OffsetsMap;
import org.apache.cassandra.replication.MutationJournal;
import org.apache.cassandra.replication.ShortMutationId;
import org.apache.cassandra.transport.Dispatcher;

public abstract class PartialTrackedRead
{
    private static final Logger logger = LoggerFactory.getLogger(PartialTrackedRead.class);

    final ReadExecutionController executionController;
    final ColumnFamilyStore cfs;
    final long startTimeNanos;

    public PartialTrackedRead(ReadExecutionController executionController, ColumnFamilyStore cfs, long startTimeNanos)
    {
        this.executionController = executionController;
        this.cfs = cfs;
        this.startTimeNanos = startTimeNanos;
    }

    public ReadExecutionController executionController()
    {
        return executionController;
    }

    public ColumnFamilyStore cfs()
    {
        return cfs;
    }

    public long startTimeNanos()
    {
        return startTimeNanos;
    }

    abstract ReadCommand command();

    public abstract Index.Searcher searcher();

    protected interface Augmentable
    {
        void augment(PartitionUpdate update);
    }

    protected static abstract class State
    {
        protected static final State CLOSED = new State()
        {
            @Override
            String name()
            {
                return "closed";
            }

            @Override
            boolean isClosed()
            {
                return true;
            }
        };

        abstract String name();

        Initialized asInitialized()
        {
            throw new IllegalStateException("State is " + name() + ", not " + Initialized.NAME);
        }

        boolean isPrepared()
        {
            return false;
        }

        Prepared asPrepared()
        {
            throw new IllegalStateException("State is " + name() + ", not " + Prepared.NAME);
        }

        Completed asCompleted()
        {
            throw new IllegalStateException("State is " + name() + ", not " + Completed.NAME);
        }

        Augmentable asAugmentable()
        {
            if (isPrepared()) return asPrepared();
            throw new IllegalStateException("State is " + name() + ", not augmentable");
        }

        boolean isClosed()
        {
            return false;
        }

        void close()
        {
        }
    }

    // TODO (expected): this is a redundant state, never exposed
    protected final class Initialized extends State
    {
        static final String NAME = "initialized";

        @Override
        String name()
        {
            return NAME;
        }

        @Override
        Initialized asInitialized()
        {
            return this;
        }

        Prepared prepare(UnfilteredPartitionIterator initialData)
        {
            return prepareInternal(initialData);
        }
    }

    protected abstract Prepared prepareInternal(UnfilteredPartitionIterator initialData);

    protected static abstract class Prepared extends State implements Augmentable
    {
        private static final String NAME = "prepared";

        @Override
        String name()
        {
            return NAME;
        }

        @Override
        boolean isPrepared()
        {
            return true;
        }

        @Override
        Prepared asPrepared()
        {
            return this;
        }

        abstract Completed complete();
    }

    protected static abstract class Completed extends State
    {
        private static final String NAME = "completed";

        @Override
        String name()
        {
            return NAME;
        }

        protected abstract CompletedRead getResult();
    }

    protected abstract class AbstractCompleted extends Completed
    {
        protected abstract UnfilteredPartitionIterator iterator();
        protected abstract CompletedRead createResult(UnfilteredPartitionIterator iterator);

        @Override
        protected CompletedRead getResult()
        {
            UnfilteredPartitionIterator result = command().completeTrackedRead(iterator(), PartialTrackedRead.this);
            // validate that the sequence of RT markers is correct: open is followed by close, deletion times for both
            // ends equal, and there are no dangling RT bound in any partition.
            result = RTBoundValidator.validate(result, RTBoundValidator.Stage.PROCESSED, true);
            return createResult(result);
        }
    }

    protected State state = new Initialized();

    protected synchronized State state()
    {
        return state;
    }

    /**
     * Implementors need to call this before returning this from createInProgressRead
     * TODO (expected): this is a redundant transition from a redundant state (INITIALIZED)
     */
    synchronized void prepare(UnfilteredPartitionIterator initialData)
    {
        logger.trace("Preparing read {}", this);
        state = state.asInitialized().prepare(initialData);
    }

    void augment(PartitionUpdate update)
    {
        state.asAugmentable().augment(update);
    }

    public synchronized void augment(Mutation mutation)
    {
        PartitionUpdate update = mutation.getPartitionUpdate(command().metadata());
        if (update != null)
            augment(update);
    }

    void augment(Log2OffsetsMap<?> augmentingOffsets)
    {
        augmentingOffsets.forEach(this::augment);
    }

    /**
     * If a mutation to augment isn't present in {@link MutationJournal}, it's either a newly-activated transfer, or a
     * serious bug. In the case it's a transfer, we still want to signal this to the client to retry against another
     * node. This could be optimized to augment with updates from the newly-transferred SSTables.
     */
    void augment(ShortMutationId mutationId)
    {
        Mutation mutation = MutationJournal.instance.read(mutationId);
        if (mutation == null)
        {
            logger.error("Could not augment read with mutation not present in journal {}", mutationId);
            throw new RuntimeException(String.format("Missing mutation %s", mutationId));
        }
        if (!command().selectsKey(mutation.key()))
        {
            logger.trace("Skipping mutation {} - {} not in read range", mutationId, mutation.key());
            return;
        }
        augment(mutation);
    }

    public synchronized CompletedRead complete()
    {
        Preconditions.checkState(state.isPrepared());
        Completed completed = state.asPrepared().complete();
        state = completed;
        return completed.getResult();
    }

    public synchronized void close()
    {
        if (state.isClosed())
            return;

        logger.trace("Closing read {}", this);
        state.close();
        executionController.close();
        state = State.CLOSED;
    }

    public interface CompletedRead extends AutoCloseable
    {
        TrackedDataResponse response(); // must be called from the read stage
        Future<TrackedDataResponse> followupRead(TrackedDataResponse initialResponse, ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime);

        @Override
        void close();

        static TrackedDataResponse createResponse(UnfilteredPartitionIterator partition, ReadCommand command)
        {
            PartitionIterator iterator = UnfilteredPartitionIterators.filter(partition, command.nowInSec());
            DataLimits.Counter counter = command.limits().newCounter(command.nowInSec(),
                                                                     false,
                                                                     command.selectsFullPartition(),
                                                                     command.metadata().enforceStrictLiveness()).onlyCount();
            return TrackedDataResponse.create(counter.applyTo(iterator),
                                              command.columnFilter());
        }

        static CompletedRead simple(UnfilteredPartitionIterator partition, ReadCommand command, long nowInSec)
        {
            return new CompletedRead()
            {
                @Override
                public TrackedDataResponse response()
                {
                    return createResponse(partition, command);
                }

                @Override
                public Future<TrackedDataResponse> followupRead(TrackedDataResponse initialRead, ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime)
                {
                    return null;
                }

                @Override
                public void close()
                {
                    partition.close();
                }
            };
        }
    }

    /**
     * Sets consistency level and expiration info to be used for follow up reads. Needs to be called before making the
     * read available for receiving augmenting mutations
     */
    void setFollowUpReadContext(ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime) {}

    void complete(AsyncPromise<TrackedDataResponse> promise, ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime)
    {
        complete(promise, this, consistencyLevel, requestTime);
    }

    static void complete(AsyncPromise<TrackedDataResponse> promise, PartialTrackedRead read, ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime)
    {
        try (CompletedRead completedRead = read.complete())
        {
            TrackedDataResponse response = completedRead.response();
            Future<TrackedDataResponse> followUp = completedRead.followupRead(response, consistencyLevel, requestTime);

            if (followUp != null)
            {
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
