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

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.LockSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.db.DecoratedKey;

/**
 * Java 11 version for the partition-locks in {@link AtomicBTreePartition}.
 */
public abstract class AtomicBTreePartitionBase extends AbstractBTreePartition
{
    private static final Logger logger = LoggerFactory.getLogger(AtomicBTreePartitionBase.class);
    private static final long ACQUIRE_LOCK_SPIN_SLEEP = Long.getLong(Config.PROPERTY_PREFIX + ".btreepartition.spin.sleep.nanos", 5000L);

    protected AtomicBTreePartitionBase(DecoratedKey partitionKey)
    {
        super(partitionKey);
    }

    // Replacement for Unsafe.monitorEnter/monitorExit. Uses the thread-ID to indicate a lock
    // using a CAS operation on the primitive instance field.
    private volatile long lock;
    private static final AtomicLongFieldUpdater<AtomicBTreePartitionBase> lockFieldUpdater = AtomicLongFieldUpdater.newUpdater(AtomicBTreePartitionBase.class, "lock");

    static
    {
        logger.info("Initializing Java 11 support for AtomicBTreePartition");

        if (Runtime.version().version().get(0) < 11)
            throw new RuntimeException("Java 11 required, but found " + Runtime.version());
    }

    protected final void acquireLock()
    {
        long t = Thread.currentThread().getId();

        while (true)
        {
            if (lockFieldUpdater.compareAndSet(this, 0L, t))
                return;

            // "sleep" a bit - operations performed while the lock's being held
            // may include "expensive" operations (secondary indexes for example).
            LockSupport.parkNanos(ACQUIRE_LOCK_SPIN_SLEEP);
        }
    }

    protected final void releaseLock()
    {
        long t = Thread.currentThread().getId();
        boolean r = lockFieldUpdater.compareAndSet(this, t, 0L);
        assert r;
    }
}
