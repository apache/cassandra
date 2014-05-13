/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.utils.memory;

import org.apache.cassandra.utils.concurrent.WaitQueue;

/**
 * A thread that reclaims memory from a MemtablePool on demand.  The actual reclaiming work is delegated to the
 * cleaner Runnable, e.g., FlushLargestColumnFamily
 */
class MemtableCleanerThread<P extends MemtablePool> extends Thread
{
    /** The pool we're cleaning */
    final P pool;

    /** should ensure that at least some memory has been marked reclaiming after completion */
    final Runnable cleaner;

    /** signalled whenever needsCleaning() may return true */
    final WaitQueue wait = new WaitQueue();

    MemtableCleanerThread(P pool, Runnable cleaner)
    {
        super(pool.getClass().getSimpleName() + "Cleaner");
        this.pool = pool;
        this.cleaner = cleaner;
        setDaemon(true);
    }

    boolean needsCleaning()
    {
        return pool.onHeap.needsCleaning() || pool.offHeap.needsCleaning();
    }

    // should ONLY be called when we really think it already needs cleaning
    void trigger()
    {
        wait.signal();
    }

    @Override
    public void run()
    {
        while (true)
        {
            while (!needsCleaning())
            {
                final WaitQueue.Signal signal = wait.register();
                if (!needsCleaning())
                    signal.awaitUninterruptibly();
                else
                    signal.cancel();
            }

            cleaner.run();
        }
    }
}
