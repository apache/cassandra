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

import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.apache.cassandra.utils.Throwables.maybeFail;

/**
 * Special, restricted LifecycleTransaction for streaming, synchronizes access to the shared transaction
 * and adds a method to take ownership of a "child-transaction".
 *
 * Each incoming file is now its own normal LifecycleTransaction.
 */
public class StreamingLifecycleTransaction
{
    private final LifecycleTransaction sharedTxn;

    public StreamingLifecycleTransaction()
    {
        this.sharedTxn = LifecycleTransaction.offline(OperationType.STREAM);
    }

    public synchronized Throwable commit(Throwable accumulate)
    {
        return sharedTxn.commit(accumulate);
    }

    public synchronized void update(Collection<SSTableReader> readers)
    {
        sharedTxn.update(readers, false);
    }

    public synchronized void abort()
    {
        maybeFail(sharedTxn.abort(null));
    }

    public synchronized void finish()
    {
        sharedTxn.prepareToCommit();
        sharedTxn.commit();
    }

    public synchronized void takeOwnership(ILifecycleTransaction txn)
    {
        sharedTxn.takeOwnership(txn);
    }
}
