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

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.TimeUUID;

/// Composite lifecycle transaction. This is a wrapper around a lifecycle transaction that allows for multiple partial
/// operations that comprise the whole transaction. This is used to parallelize compaction operations over individual
/// output shards where the compaction sources are shared among the operations; in this case we can only release the
/// shared sources once all operations are complete.
///
/// A composite transaction is initialized with a main transaction that will be used to commit the transaction. Each
/// part of the composite transaction must be registered with the transaction before it is used. The transaction must
/// be initialized by calling [#completeInitialization()] before any of the processing is allowed to proceed.
///
/// The transaction is considered complete when all parts have been committed or aborted. If any part is aborted, the
/// whole transaction is also aborted ([PartialLifecycleTransaction] will also throw an exception on other parts when
/// they access it if the composite is already aborted).
///
/// When all parts are committed, the full transaction is applied by performing a checkpoint, obsoletion of the
/// originals if any of the parts requested it, preparation and commit. This may somewhat violate the rules of
/// transactions as a part that has been committed may actually have no effect if another part is aborted later.
/// There are also restrictions on the operations that this model can accept, e.g. replacement of sources and partial
/// checkpointing are not supported (as they are parts of early open which we don't aim to support at this time),
/// and we consider that all parts will have the same opinion about the obsoletion of the originals.
public class CompositeLifecycleTransaction
{
    protected static final Logger logger = LoggerFactory.getLogger(CompositeLifecycleTransaction.class);

    final LifecycleTransaction mainTransaction;
    private final AtomicInteger partsToCommitOrAbort;
    private volatile boolean obsoleteOriginalsRequested;
    private volatile boolean wasAborted;
    private volatile boolean initializationComplete;
    private volatile int partsCount = 0;

    /// Create a composite transaction wrapper over the given transaction. After construction, the individual parts of
    /// the operation must be registered using [#register] and the composite sealed by calling [#completeInitialization].
    /// The composite will then track the state of the parts and commit after all of them have committed (respectively
    /// abort if one aborts but only after waiting for all the other tasks to complete, successfully or not).
    ///
    /// To make it easy to recognize the parts of a composite transaction, the given transaction should have an id with
    /// sequence number 0, and partial transactions should use the id that [#register] returns.
    public CompositeLifecycleTransaction(LifecycleTransaction mainTransaction)
    {
        this.mainTransaction = mainTransaction;
        this.partsToCommitOrAbort = new AtomicInteger(0);
        this.wasAborted = false;
        this.obsoleteOriginalsRequested = false;
    }

    /// Register one part of the composite transaction. Every part must register itself before the composite transaction
    /// is initialized and the parts are allowed to proceed.
    /// @param part the part to register
    public TimeUUID register(PartialLifecycleTransaction part)
    {
        int index = partsToCommitOrAbort.incrementAndGet();
        return mainTransaction.opId().withSequence(index);
    }

    /// Complete the initialization of the composite transaction. This must be called before any of the parts are
    /// executed.
    public void completeInitialization()
    {
        partsCount = partsToCommitOrAbort.get();
        initializationComplete = true;
        if (logger.isTraceEnabled())
            logger.trace("Composite transaction {} initialized with {} parts.", mainTransaction.opIdString(), partsCount);
    }

    /// Get the number of parts in the composite transaction. 0 if the transaction is not yet initialized.
    public int partsCount()
    {
        return partsCount;
    }

    /// Request that the original sstables are obsoleted when the transaction is committed. Note that this class has
    /// an expectation that all parts will have the same opinion about this, and one request will be sufficient to
    /// trigger obsoletion.
    public void requestObsoleteOriginals()
    {
        obsoleteOriginalsRequested = true;
    }

    /// Commit a part of the composite transaction. This will trigger the final commit of the whole transaction if it is
    /// the last part to complete. A part has to commit or abort exactly once.
    public void commitPart()
    {
        partCommittedOrAborted();
    }

    /// Signal an abort of one part of the transaction. If this is the last part to signal, the whole transaction will
    /// now abort. Otherwise the composite transaction will wait for the other parts to complete and will abort the
    /// composite when they all give their commit or abort signal. A part has to commit or abort exactly once.
    ///
    /// [PartialLifecycleTransaction] will attempt to abort other parts sooner by throwing an exception when any of its
    /// methods are called when the composite transaction is already aborted.
    public void abortPart()
    {
        wasAborted = true;
        partCommittedOrAborted();
    }

    boolean wasAborted()
    {
        return wasAborted;
    }

    private void partCommittedOrAborted()
    {
        if (!initializationComplete)
            throw new IllegalStateException("Composite transaction used before initialization is complete.");
        if (partsToCommitOrAbort.decrementAndGet() == 0)
        {
            if (wasAborted)
            {
                if (logger.isTraceEnabled())
                    logger.trace("Composite transaction {} with {} parts aborted.",
                                 mainTransaction.opIdString(),
                                 partsCount);

                mainTransaction.abort();
            }
            else
            {
                if (logger.isTraceEnabled())
                    logger.trace("Composite transaction {} with {} parts completed{}.",
                                 mainTransaction.opIdString(),
                                 partsCount,
                                 obsoleteOriginalsRequested ? " with obsoletion" : "");

                mainTransaction.checkpoint();
                if (obsoleteOriginalsRequested)
                    mainTransaction.obsoleteOriginals();
                mainTransaction.prepareToCommit();
                mainTransaction.commit();
            }
        }
    }
}
