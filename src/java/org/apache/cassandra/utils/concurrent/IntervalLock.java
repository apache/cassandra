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
package org.apache.cassandra.utils.concurrent;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.utils.TimeSource;

/**
 * This class extends ReentrantReadWriteLock to provide a write lock that can only be acquired at provided intervals.
 */
public class IntervalLock extends ReentrantReadWriteLock
{
    private final AtomicLong lastAcquire = new AtomicLong();
    private final TimeSource timeSource;

    public IntervalLock(TimeSource timeSource)
    {
        this.timeSource = timeSource;
    }

    /**
     * Try acquiring a write lock if the given interval is passed since the last call to this method.
     *
     * @param interval In millis.
     * @return True if acquired and locked, false otherwise.
     */
    public boolean tryIntervalLock(long interval)
    {
        long now = timeSource.currentTimeMillis();
        boolean acquired = (now - lastAcquire.get() >= interval) && writeLock().tryLock();
        if (acquired)
            lastAcquire.set(now);

        return acquired;
    }

    /**
     * Release the last acquired interval lock.
     */
    public void releaseIntervalLock()
    {
        writeLock().unlock();
    }

    @VisibleForTesting
    public long getLastIntervalAcquire()
    {
        return lastAcquire.get();
    }
}
