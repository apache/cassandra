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

import java.util.List;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.transform.RTBoundValidator;
import org.apache.cassandra.index.Index;

import static org.apache.cassandra.db.partitions.UnfilteredPartitionIterators.MergeListener.NOOP;

public abstract class AbstractPartialTrackedRead implements PartialTrackedRead
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractPartialTrackedRead.class);

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

    abstract void freezeInitialData();

    abstract UnfilteredPartitionIterator initialData();

    abstract UnfilteredPartitionIterator augmentedData();

    abstract void augmentResponse(PartitionUpdate update);

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

    private UnfilteredPartitionIterator complete(UnfilteredPartitionIterator iterator)
    {
        return command().completeTrackedRead(iterator, this);
    }

    abstract CompletedRead createResult(UnfilteredPartitionIterator iterator);

    @Override
    public synchronized CompletedRead complete()
    {
        Preconditions.checkState(state == State.PREPARED);
        state = State.READING;

        UnfilteredPartitionIterator initial = initialData();
        UnfilteredPartitionIterator augmented = augmentedData();

        UnfilteredPartitionIterator result = augmented != null ?
                                             UnfilteredPartitionIterators.merge(List.of(initial, augmented), NOOP) :
                                             initial;

        result = complete(result);
        // validate that the sequence of RT markers is correct: open is followed by close, deletion times for both
        // ends equal, and there are no dangling RT bound in any partition.
        result = RTBoundValidator.validate(result, RTBoundValidator.Stage.PROCESSED, true);
        return createResult(complete(result));
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
