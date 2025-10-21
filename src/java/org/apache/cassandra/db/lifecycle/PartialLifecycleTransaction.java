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

package org.apache.cassandra.db.lifecycle;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;

/// Partial lifecycle transaction. This works together with a CompositeLifecycleTransaction to allow for multiple
/// tasks using a shared transaction to be committed or aborted together. This is used to parallelize compaction
/// operations over the same sources. See [CompositeLifecycleTransaction] for more details.
///
/// This class takes care of synchronizing various operations on the shared transaction, making sure that an abort
/// or commit signal is given exactly once (provided that this partial transaction is closed), and throwing an exception
/// when progress is made when the transaction was already aborted by another part.
public class PartialLifecycleTransaction implements ILifecycleTransaction
{
    final CompositeLifecycleTransaction composite;
    final ILifecycleTransaction mainTransaction;
    final AtomicBoolean committedOrAborted = new AtomicBoolean(false);
    final TimeUUID id;

    public PartialLifecycleTransaction(CompositeLifecycleTransaction composite)
    {
        this.composite = composite;
        this.mainTransaction = composite.mainTransaction;
        this.id = composite.register(this);
    }

    public void checkpoint()
    {
        // don't do anything, composite will checkpoint at end
    }

    private RuntimeException earlyOpenUnsupported()
    {
        throw new UnsupportedOperationException("PartialLifecycleTransaction does not support early opening of SSTables");
    }

    public void update(SSTableReader reader, boolean original)
    {
        throwIfCompositeAborted();
        if (original)
            throw earlyOpenUnsupported();

        synchronized (mainTransaction)
        {
            mainTransaction.update(reader, original);
        }
    }

    public void update(Collection<SSTableReader> readers, boolean original)
    {
        throwIfCompositeAborted();
        if (original)
            throw earlyOpenUnsupported();

        synchronized (mainTransaction)
        {
            mainTransaction.update(readers, original);
        }
    }

    public SSTableReader current(SSTableReader reader)
    {
        synchronized (mainTransaction)
        {
            return mainTransaction.current(reader);
        }
    }

    public void obsolete(SSTableReader reader)
    {
        earlyOpenUnsupported();
    }

    public void obsoleteOriginals()
    {
        composite.requestObsoleteOriginals();
    }

    public Set<SSTableReader> originals()
    {
        return mainTransaction.originals();
    }

    public boolean isObsolete(SSTableReader reader)
    {
        throw earlyOpenUnsupported();
    }

    private boolean markCommittedOrAborted()
    {
        return committedOrAborted.compareAndSet(false, true);
    }

    /// Commit the transaction part. Because this is a part of a composite transaction, the actual commit will be
    /// carried out only after all parts have committed.
    ///
    ///
    public Throwable commit(Throwable accumulate)
    {
        Throwables.maybeFail(accumulate); // we must be called with a null accumulate
        if (markCommittedOrAborted())
            composite.commitPart();
        else
            throw new IllegalStateException("Partial transaction already committed or aborted.");
        return null;
    }

    public Throwable abort(Throwable accumulate)
    {
        Throwables.maybeFail(accumulate); // we must be called with a null accumulate
        if (markCommittedOrAborted())
            composite.abortPart();
        else
            throw new IllegalStateException("Partial transaction already committed or aborted.");
        return null;
    }

    private void throwIfCompositeAborted()
    {
        if (composite.wasAborted())
            throw new AbortedException("Transaction aborted, likely by another partial operation.");
    }

    public void prepareToCommit()
    {
        if (committedOrAborted.get())
            throw new IllegalStateException("Partial transaction already committed or aborted.");

        throwIfCompositeAborted();
        // nothing else to do, the composite transaction will perform the preparation when all parts are done
    }

    public void close()
    {
        if (markCommittedOrAborted())   // close should abort if not committed
            composite.abortPart();
    }

    public void trackNew(SSTable table)
    {
        throwIfCompositeAborted();
        synchronized (mainTransaction)
        {
            mainTransaction.trackNew(table);
        }
    }

    public void untrackNew(SSTable table)
    {
        synchronized (mainTransaction)
        {
            mainTransaction.untrackNew(table);
        }
    }

    public OperationType opType()
    {
        return mainTransaction.opType();
    }

    public boolean isOffline()
    {
        return mainTransaction.isOffline();
    }

    @Override
    public TimeUUID opId()
    {
        return id;
    }

    @Override
    public String opIdString()
    {
        return String.format("%s (%d/%d)", id, id.sequence(), composite.partsCount());
    }

    @Override
    public void cancel(SSTableReader removedSSTable)
    {
        synchronized (mainTransaction)
        {
            mainTransaction.cancel(removedSSTable);
        }
    }

    @Override
    public String toString()
    {
        return opIdString();
    }

    public static class AbortedException extends RuntimeException
    {
        public AbortedException(String message)
        {
            super(message);
        }
    }
}
