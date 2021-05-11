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

package org.apache.cassandra.db.memtable;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * Memtable that uses a commit log for persistence. Provides methods of tracking the commit log positions covered by
 * it and safely switching between memtables.
 */
public abstract class AbstractMemtableWithCommitlog extends AbstractMemtable
{
    // The approximate lower bound by this memtable; must be <= commitLogLowerBound once our predecessor
    // has been finalised, and this is enforced in the ColumnFamilyStore.setCommitLogUpperBound
    private final CommitLogPosition approximateCommitLogLowerBound = CommitLog.instance.getCurrentPosition();
    // the precise lower bound of CommitLogPosition owned by this memtable; equal to its predecessor's commitLogUpperBound
    private final AtomicReference<CommitLogPosition> commitLogLowerBound;
    // the write barrier for directing writes to this memtable or the next during a switch
    private volatile OpOrder.Barrier writeBarrier;
    // the precise upper bound of CommitLogPosition owned by this memtable
    private volatile AtomicReference<CommitLogPosition> commitLogUpperBound;

    public AbstractMemtableWithCommitlog(TableMetadataRef metadataRef, AtomicReference<CommitLogPosition> commitLogLowerBound)
    {
        super(metadataRef);
        this.commitLogLowerBound = commitLogLowerBound;
    }

    public CommitLogPosition getApproximateCommitLogLowerBound()
    {
        return approximateCommitLogLowerBound;
    }

    public void switchOut(OpOrder.Barrier writeBarrier, AtomicReference<CommitLogPosition> commitLogUpperBound)
    {
        // This can prepare the memtable data for deletion; it will still be used while the flush is proceeding.
        // A setDiscarded call will follow.
        assert this.writeBarrier == null;
        this.commitLogUpperBound = commitLogUpperBound;
        this.writeBarrier = writeBarrier;
    }

    public void discard()
    {
        assert writeBarrier != null : "Memtable must be switched out before being discarded.";
    }

    // decide if this memtable should take the write, or if it should go to the next memtable
    @Override
    public boolean accepts(OpOrder.Group opGroup, CommitLogPosition commitLogPosition)
    {
        // if the barrier hasn't been set yet, then this memtable is still the newest and is taking ALL writes.
        OpOrder.Barrier barrier = this.writeBarrier;
        if (barrier == null)
            return true;
        // Note that if this races with taking the barrier the opGroup and commit log position we were given must
        // necessarily be before the barrier and any LastCommitLogPosition is set, thus this function will return true
        // and no update to commitLogUpperBound is necessary.

        // If the barrier has been set and issued, but is in the past, we are definitely destined for a future memtable.
        // Because we issue the barrier after taking LastCommitLogPosition and mutations take their position after
        // taking the opGroup, this condition also ensures the given commit log position is greater than the chosen
        // upper bound.
        if (!barrier.isAfter(opGroup))
            return false;
        // We are in the segment of time between the barrier is constructed (and the memtable is switched out)
        // and the barrier is issued.
        // if we aren't durable we are directed only by the barrier
        if (commitLogPosition == null)
            return true;
        while (true)
        {
            // If the CL boundary has been set, the mutation can be accepted depending on whether it falls before it.
            // However, if it has not been set, the old sstable must still accept writes but we must also ensure that
            // their positions are accounted for in the boundary (as there may be a delay between taking the log
            // position for the boundary and setting it where a mutation sneaks in).
            // Thus, if the boundary hasn't been finalised yet, we simply update it to the max of its current value and
            // ours; this permits us to coordinate a safe boundary, as the boundary choice is made atomically wrt our
            // max() maintenance, so an operation cannot sneak into the past.
            CommitLogPosition currentLast = commitLogUpperBound.get();
            if (currentLast instanceof LastCommitLogPosition)
                return currentLast.compareTo(commitLogPosition) >= 0;
            if (currentLast != null && currentLast.compareTo(commitLogPosition) >= 0)
                return true;
            if (commitLogUpperBound.compareAndSet(currentLast, commitLogPosition))
                return true;
        }
    }

    public CommitLogPosition getCommitLogLowerBound()
    {
        return commitLogLowerBound.get();
    }

    public LastCommitLogPosition getFinalCommitLogUpperBound()
    {
        assert commitLogUpperBound != null : "Commit log upper bound should be set before flushing";
        assert commitLogUpperBound.get() instanceof LastCommitLogPosition : "Commit log upper bound has not been sealed yet? " + commitLogUpperBound.get();
        return (LastCommitLogPosition) commitLogUpperBound.get();
    }

    public boolean mayContainDataBefore(CommitLogPosition position)
    {
        return approximateCommitLogLowerBound.compareTo(position) < 0;
    }
}
