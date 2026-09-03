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

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Clock;

/**
 * Promotes reconciled tracked sstables to repaired in place. No data is rewritten.
 */
class PromoteReconciledTask extends AbstractCompactionTask
{
    private static final Logger logger = LoggerFactory.getLogger(PromoteReconciledTask.class);

    private final String reason;
    private final Runnable onCompleted;

    static AbstractCompactionTask tryPromote(ColumnFamilyStore cfs,
                                             Set<SSTableReader> candidates,
                                             String reason,
                                             Runnable onCompleted)
    {
        if (candidates.isEmpty())
            return null;

        Set<SSTableReader> available = new HashSet<>(candidates);
        available.removeAll(cfs.getTracker().getCompacting());
        if (available.isEmpty())
        {
            logger.trace("Deferring promotion of {}.{} {}; all {} sstables are busy",
                         cfs.metadata.keyspace, cfs.metadata.name, reason, candidates.size());
            return null;
        }

        // is txn is only here to serve as a lock, to prevent other compactions from modifying these
        // sstables while their metadata is being mutated
        LifecycleTransaction txn = cfs.getTracker().tryModify(available, OperationType.COMPACTION);
        if (txn == null)
        {
            // if one or more of the sstables are already marked compacted, remove them and try again. Since we try to
            // promote all eligible sstables in a single task, this keeps compaction from preventing and progress in
            // promotion
            available.removeAll(cfs.getTracker().getCompacting());
            if (available.isEmpty())
                return null;
            txn = cfs.getTracker().tryModify(available, OperationType.COMPACTION);
            if (txn == null)
            {
                logger.trace("Deferring promotion of {}.{} {}; lost the race for its sstables",
                             cfs.metadata.keyspace, cfs.metadata.name, reason);
                return null;
            }
        }
        return new PromoteReconciledTask(cfs, txn, reason, onCompleted);
    }

    PromoteReconciledTask(ColumnFamilyStore cfs, LifecycleTransaction transaction, String reason, Runnable onCompleted)
    {
        super(cfs, transaction);
        this.reason = reason;
        this.onCompleted = onCompleted;
    }

    protected void runMayThrow() throws Exception
    {
        boolean completed = false;
        try
        {
            logger.info("Promoting {} to repaired; {} have reconciled", transaction.originals(), reason);
            // One metadata mutation sets repairedAt and clears the offsets, so a repaired sstable never still claims
            // journal provenance, and an unrepaired one never loses it.
            cfs.getCompactionStrategyManager().promoteReconciled(transaction.originals(),
                                                                Clock.Global.currentTimeMillis());
            completed = true;
        }
        finally
        {
            transaction.abort();
            if (completed && onCompleted != null)
                onCompleted.run();
        }
    }

    protected void executeInternal(ActiveCompactionsTracker activeCompactions)
    {
        run();
    }
}
