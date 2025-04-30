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
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.transform.RTBoundValidator;


public abstract class AbstractPartialTrackedRead implements PartialTrackedRead
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractPartialTrackedRead.class);

    protected interface Augmentable
    {
        State augment(PartitionUpdate update);
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

        boolean isInitialized()
        {
            return false;
        }

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

        boolean isCompleted()
        {
            return false;
        }

        Completed asCompleted()
        {
            throw new IllegalStateException("State is " + name() + ", not " + Completed.NAME);
        }

        boolean isAugmentable()
        {
            return isPrepared() || isInitialized();
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
        boolean isInitialized()
        {
            return true;
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

    protected abstract class Prepared extends State implements Augmentable
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

    protected abstract class Completed extends State
    {
        private static final String NAME = "completed";

        @Override
        String name()
        {
            return NAME;
        }

        protected abstract UnfilteredPartitionIterator iterator();
        protected abstract CompletedRead createResult(UnfilteredPartitionIterator iterator);

        protected CompletedRead getResult()
        {
            UnfilteredPartitionIterator result = command().completeTrackedRead(iterator(), AbstractPartialTrackedRead.this);
            // validate that the sequence of RT markers is correct: open is followed by close, deletion times for both
            // ends equal, and there are no dangling RT bound in any partition.
            result = RTBoundValidator.validate(result, RTBoundValidator.Stage.PROCESSED, true);
            return createResult(result);
        }
    }

    final ReadExecutionController executionController;
    final ColumnFamilyStore cfs;
    final long startTimeNanos;
    private State state = new Initialized();

    public AbstractPartialTrackedRead(ReadExecutionController executionController, ColumnFamilyStore cfs, long startTimeNanos)
    {
        this.executionController = executionController;
        this.cfs = cfs;
        this.startTimeNanos = startTimeNanos;
    }

    @Override
    public ReadExecutionController executionController()
    {
        return executionController;
    }

    @Override
    public ColumnFamilyStore cfs()
    {
        return cfs;
    }

    @Override
    public long startTimeNanos()
    {
        return startTimeNanos;
    }

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

    @Override
    public synchronized void augment(Mutation mutation)
    {
        PartitionUpdate update = mutation.getPartitionUpdate(command().metadata());
        if (update != null)
            state = state.asAugmentable().augment(update);
    }

    @Override
    public synchronized CompletedRead complete()
    {
        Preconditions.checkState(state.isPrepared());
        Completed completed = state.asPrepared().complete();
        state = completed;
        return completed.getResult();
    }

    @Override
    public synchronized void close()
    {
        if (state.isClosed())
            return;

        logger.trace("Closing read {}", this);
        state.close();
        executionController.close();
        state = State.CLOSED;
    }
}
