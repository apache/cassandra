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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

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
    protected final CompactionRealm realm;
    protected LifecycleTransaction transaction;
    protected boolean isUserDefined;
    protected OperationType compactionType;
    protected TableOperationObserver opObserver;
    protected final List<CompactionObserver> compObservers;

    /**
     * @param realm
     * @param transaction the modifying managing the status of the sstables we're replacing
     */
    protected AbstractCompactionTask(CompactionRealm realm, LifecycleTransaction transaction)
    {
        this.realm = realm;
        this.transaction = transaction;
        this.isUserDefined = false;
        this.compactionType = OperationType.COMPACTION;
        this.opObserver = TableOperationObserver.NOOP;
        this.compObservers = new ArrayList<>();

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
        Throwable t = null;
        try
        {
            return executeInternal();
        }
        catch (FSDiskFullWriteError e)
        {
            RuntimeException cause = new RuntimeException("Converted from FSDiskFullWriteError: " + e.getMessage());
            cause.setStackTrace(e.getStackTrace());
            t = cause;
            throw new RuntimeException("Throwing new Runtime to bypass exception handler when disk is full", cause);
        }
        catch (Throwable t1)
        {
            t = t1;
            throw t1;
        }
        finally
        {
            Throwables.maybeFail(cleanup(t));
        }
    }

    public Throwable cleanup(Throwable err)
    {
        final boolean isSuccess = err == null;
        for (CompactionObserver compObserver : compObservers)
            err = Throwables.perform(err, () -> compObserver.onCompleted(transaction.opId(), isSuccess));

        return Throwables.perform(err, () -> transaction.close());
    }

    public abstract CompactionAwareWriter getCompactionAwareWriter(CompactionRealm realm, Directories directories, LifecycleTransaction txn, Set<SSTableReader> nonExpiredSSTables);

    @VisibleForTesting
    public LifecycleTransaction getTransaction()
    {
        return transaction;
    }

    @VisibleForTesting
    public OperationType getCompactionType()
    {
        return compactionType;
    }

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
    public AbstractCompactionTask setOpObserver(TableOperationObserver opObserver)
    {
        this.opObserver = opObserver;
        return this;
    }

    public void addObserver(CompactionObserver compObserver)
    {
        compObservers.add(compObserver);
    }

    @VisibleForTesting
    public List<CompactionObserver> getCompObservers()
    {
        return compObservers;
    }

    @VisibleForTesting
    public LifecycleTransaction transaction()
    {
        return transaction;
    }

    public String toString()
    {
        return "CompactionTask(" + transaction + ")";
    }
}
