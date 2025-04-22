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
import java.util.List;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.index.Index;

import static org.apache.cassandra.db.partitions.UnfilteredPartitionIterators.MergeListener.NOOP;

public abstract class AbstractPartialTrackedRead implements PartialTrackedRead
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractPartialTrackedRead.class);
    // TODO: remove synchronized?

    private enum State
    {
        INITIALIZED,
        PREPARED,
        READING,
        FINISHED
    }

    final ReadExecutionController executionController;
    final Index.Searcher searcher;
    final ColumnFamilyStore cfs;
    final long startTimeNanos;
    volatile State state = State.INITIALIZED;
    private Slices slices = null;

    // TODO: do we really need the execution controller and the op-order?
    public AbstractPartialTrackedRead(ReadExecutionController executionController, Index.Searcher searcher, ColumnFamilyStore cfs, long startTimeNanos)
    {
        this.executionController = executionController;
        this.searcher = searcher;
        this.cfs = cfs;
        this.startTimeNanos = startTimeNanos;
    }

    @Override
    public ReadExecutionController executionController()
    {
        return executionController;
    }

    @Override
    public Index.Searcher searcher()
    {
        return searcher;
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

    protected abstract ReadCommand command();

    abstract void freezeInitialData();

    abstract UnfilteredPartitionIterator initialData();

    abstract UnfilteredPartitionIterator augmentedData();

    abstract void augmentResponse(PartitionUpdate update);

    @Override
    public long nowInSec()
    {
        return command().nowInSec();
    }

    /**
     * Implementors need to call this before returning this from createInProgressRead
     */
    synchronized void prepare()
    {
        logger.trace("Preparing read {}", this);
        Preconditions.checkState(state == State.INITIALIZED);
        freezeInitialData();
        state = State.PREPARED;
    }

    @Override
    public void augment(Mutation mutation)
    {
        Preconditions.checkState(state == State.PREPARED);
        PartitionUpdate update = mutation.getPartitionUpdate(command().metadata());
        if (update != null)
            augmentResponse(update);
    }

    @Override
    public synchronized UnfilteredPartitionIterator read()
    {
        Preconditions.checkState(state == State.PREPARED);
        state = State.READING;

        UnfilteredPartitionIterator initial = initialData();
        UnfilteredPartitionIterator augmented = augmentedData();
        if (augmented == null)
            return initial;

        List<UnfilteredPartitionIterator> partitions = new ArrayList<>(2);
        partitions.add(initial);
        partitions.add(augmented);
        UnfilteredPartitionIterator result = command().completeTrackedRead(UnfilteredPartitionIterators.merge(partitions, NOOP), this);
        return result;
    }

    @Override
    public synchronized void close()
    {
        if (state == State.FINISHED)
            return;

        logger.trace("Closing read {}", this);
        executionController.close();
        state = State.FINISHED;
    }
}
