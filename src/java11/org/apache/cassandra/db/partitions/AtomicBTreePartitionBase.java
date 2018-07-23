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

package org.apache.cassandra.db.partitions;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Condition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.utils.concurrent.SimpleCondition;

/**
 * Java 11 version for the partition-locks in {@link AtomicBTreePartition}.
 */
public abstract class AtomicBTreePartitionBase extends AbstractBTreePartition
{
    private static final Logger logger = LoggerFactory.getLogger(AtomicBTreePartitionBase.class);

    protected AtomicBTreePartitionBase(DecoratedKey partitionKey)
    {
        super(partitionKey);
    }

    // Replacement for Unsafe.monitorEnter/monitorExit.
    private volatile Condition lock;
    private static final AtomicReferenceFieldUpdater<AtomicBTreePartitionBase, Condition> lockFieldUpdater = AtomicReferenceFieldUpdater.newUpdater(AtomicBTreePartitionBase.class, Condition.class, "lock");

    static
    {
        logger.info("Initializing Java 11 support for AtomicBTreePartition");

        if (Runtime.version().version().get(0) < 11)
            throw new RuntimeException("Java 11 required, but found " + Runtime.version());
    }

    protected final boolean acquireLock()
    {
        while (true)
        {
            Condition c = lockFieldUpdater.get(this);
            if (c == null)
            {
                // If there is no "lock" in place yet, try to set in ours.

                if (lockFieldUpdater.compareAndSet(this, null, new SimpleCondition()))
                    // Our lock's in place, go ahead.
                    return true;

                // Some other thread succeeded, spin and try again.
                Thread.onSpinWait();
                continue;
            }

            // A lock's already in place, wait for it.
            try
            {
                c.await();
            }
            catch (InterruptedException e)
            {
                // Continue updating the partition, but return the fact, that we do _not_ own the lock.
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }

    protected final void releaseLock()
    {
        // acquireLock() returned true - and only the thread "owning" the lock calls this method.

        // free the lock
        lockFieldUpdater.set(this, null);

        Condition c = lockFieldUpdater.get(this);
        // Tell waiters that we've finished
        c.signalAll();
    }
}
