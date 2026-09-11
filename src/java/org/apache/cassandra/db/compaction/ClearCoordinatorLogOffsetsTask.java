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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;

/**
 * Clears the coordinator log offsets of tracked sstables whose keyspace has migrated off of mutation tracking.
 */
class ClearCoordinatorLogOffsetsTask extends AbstractCompactionTask
{
    private static final Logger logger = LoggerFactory.getLogger(ClearCoordinatorLogOffsetsTask.class);

    private final Runnable onCompleted;

    ClearCoordinatorLogOffsetsTask(ColumnFamilyStore cfs, LifecycleTransaction transaction, Runnable onCompleted)
    {
        super(cfs, transaction);
        this.onCompleted = onCompleted;
    }

    protected void runMayThrow() throws Exception
    {
        boolean completed = false;
        try
        {
            logger.info("Clearing coordinator log offsets from {}; {}.{} no longer uses mutation tracking",
                        transaction.originals(), cfs.metadata.keyspace, cfs.metadata.name);
            cfs.getCompactionStrategyManager().clearCoordinatorLogOffsets(transaction.originals());
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
