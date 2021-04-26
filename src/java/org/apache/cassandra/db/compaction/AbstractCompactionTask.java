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
package org.apache.cassandra.db.compaction;

import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.writers.CompactionAwareWriter;
import org.apache.cassandra.io.FSDiskFullWriteError;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;

import static com.google.common.base.Throwables.propagate;


public abstract class AbstractCompactionTask extends WrappedRunnable
{
    protected final ColumnFamilyStore cfs;
    protected LifecycleTransaction transaction;
    protected boolean isUserDefined;
    protected OperationType compactionType;
    protected TableOperationObserver opObserver;
    protected CompactionObserver compObserver;

    /**
     * @param cfs
     * @param transaction the modifying managing the status of the sstables we're replacing
     */
    public AbstractCompactionTask(ColumnFamilyStore cfs, LifecycleTransaction transaction)
    {
        this.cfs = cfs;
        this.transaction = transaction;
        this.isUserDefined = false;
        this.compactionType = OperationType.COMPACTION;
        this.opObserver = TableOperationObserver.NOOP;
        this.compObserver = CompactionObserver.NO_OP;

        try
        {
            // enforce contract that caller should mark sstables compacting
            Set<SSTableReader> compacting = transaction.getCompacting();
            for (SSTableReader sstable : transaction.originals())
                assert compacting.contains(sstable) : sstable.getFilename() + " is not correctly marked compacting";

            validateSSTables(transaction.originals());
        }
        catch (Throwable err)
        {
            propagate(cleanup(err));
        }
    }

    /**
     * Confirm that we're not attempting to compact repaired/unrepaired/pending repair sstables together
     */
    private void validateSSTables(Set<SSTableReader> sstables)
    {
        // do not allow  to be compacted together
        if (!sstables.isEmpty())
        {
            Iterator<SSTableReader> iter = sstables.iterator();
            SSTableReader first = iter.next();
            boolean isRepaired = first.isRepaired();
            UUID pendingRepair = first.getPendingRepair();
            while (iter.hasNext())
            {
                SSTableReader next = iter.next();
                Preconditions.checkArgument(isRepaired == next.isRepaired(),
                                            "Cannot compact repaired and unrepaired sstables");

                if (pendingRepair == null)
                {
                    Preconditions.checkArgument(!next.isPendingRepair(),
                                                "Cannot compact pending repair and non-pending repair sstables");
                }
                else
                {
                    Preconditions.checkArgument(next.isPendingRepair(),
                                                "Cannot compact pending repair and non-pending repair sstables");
                    Preconditions.checkArgument(pendingRepair.equals(next.getPendingRepair()),
                                                "Cannot compact sstables from different pending repairs");
                }
            }
        }
    }

    /**
     * Executes the task after setting a new observer, normally the observer is the
     * compaction manager metrics.
     */
    public int execute(TableOperationObserver observer)
    {
        return setOpObserver(observer).execute();
    }

    /** Executes the task */
    public int execute()
    {
        try
        {
            return executeInternal();
        }
        catch(FSDiskFullWriteError e)
        {
            RuntimeException cause = new RuntimeException("Converted from FSDiskFullWriteError: " + e.getMessage());
            cause.setStackTrace(e.getStackTrace());
            throw new RuntimeException("Throwing new Runtime to bypass exception handler when disk is full", cause);
        }
        finally
        {
            Throwables.maybeFail(cleanup(null));
        }
    }

    private Throwable cleanup(Throwable err)
    {
        return Throwables.perform(err,
                                  () -> compObserver.setCompleted(transaction.opId()),
                                  () -> transaction.close());
    }

    public abstract CompactionAwareWriter getCompactionAwareWriter(ColumnFamilyStore cfs, Directories directories, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables);

    protected abstract int executeInternal();

    // TODO Eventually these three setters should be passed in to the constructor.

    public AbstractCompactionTask setUserDefined(boolean isUserDefined)
    {
        this.isUserDefined = isUserDefined;
        return this;
    }

    public AbstractCompactionTask setCompactionType(OperationType compactionType)
    {
        this.compactionType = compactionType;
        return this;
    }

    /**
     * Override the NO OP observer, this is normally overridden by the compaction metrics.
     */
    AbstractCompactionTask setOpObserver(TableOperationObserver opObserver)
    {
        this.opObserver = opObserver;
        return this;
    }

    public String toString()
    {
        return "CompactionTask(" + transaction + ")";
    }
}
