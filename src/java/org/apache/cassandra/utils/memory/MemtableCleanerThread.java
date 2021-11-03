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

import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.concurrent.Interruptible;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static org.apache.cassandra.concurrent.InfiniteLoopExecutor.SimulatorSafe.SAFE;
import static org.apache.cassandra.utils.concurrent.WaitQueue.newWaitQueue;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

/**
 * A thread that reclaims memory from a MemtablePool on demand.  The actual reclaiming work is delegated to the
 * cleaner Runnable, e.g., FlushLargestColumnFamily
 */
public class MemtableCleanerThread<P extends MemtablePool> implements Interruptible
{
    private static final Logger logger = LoggerFactory.getLogger(MemtableCleanerThread.class);

    private static class Clean<P extends MemtablePool> implements Interruptible.SimpleTask
    {
        /** This is incremented when a cleaner is invoked and decremented when a cleaner has completed */
        final AtomicInteger numPendingTasks = new AtomicInteger(0);

        /** The pool we're cleaning */
        final P pool;

        /** should ensure that at least some memory has been marked reclaiming after completion */
        final MemtableCleaner cleaner;

        /** signalled whenever needsCleaning() may return true */
        final WaitQueue wait = newWaitQueue();

        private Clean(P pool, MemtableCleaner cleaner)
        {
            this.pool = pool;
            this.cleaner = cleaner;
        }

        /** Return the number of pending tasks */
        public int numPendingTasks()
        {
            return numPendingTasks.get();
        }

        @Override
        public void run() throws InterruptedException
        {
            if (!pool.needsCleaning())
            {
                final WaitQueue.Signal signal = wait.register();
                if (!pool.needsCleaning())
                    signal.await();
                else
                    signal.cancel();
            }
            else
            {
                int numPendingTasks = this.numPendingTasks.incrementAndGet();

                if (logger.isTraceEnabled())
                    logger.trace("Invoking cleaner with {} tasks pending", numPendingTasks);

                cleaner.clean().addCallback(this::apply);
            }
        }

        private Boolean apply(Boolean res, Throwable err)
        {
            final int tasks = numPendingTasks.decrementAndGet();

            // if the cleaning job was scheduled (res == true) or had an error, trigger again after decrementing the tasks
            if ((res || err != null) && pool.needsCleaning())
                wait.signal();

            if (err != null)
                logger.error("Memtable cleaning tasks failed with an exception and {} pending tasks ", tasks, err);
            else if (logger.isTraceEnabled())
                logger.trace("Memtable cleaning task completed ({}), currently pending: {}", res, tasks);

            return res;
        }

        public String toString()
        {
            return pool.toString() + ' ' + cleaner.toString();
        }
    }

    private final Interruptible executor;
    private final Runnable trigger;
    private final Clean<P> clean;

    private MemtableCleanerThread(Clean<P> clean)
    {
        this.executor = executorFactory().infiniteLoop(clean.pool.getClass().getSimpleName() + "Cleaner", clean, SAFE);
        this.trigger = clean.wait::signal;
        this.clean = clean;
    }

    public MemtableCleanerThread(P pool, MemtableCleaner cleaner)
    {
        this(new Clean<>(pool, cleaner));
    }

    // should ONLY be called when we really think it already needs cleaning
    public void trigger()
    {
        trigger.run();
    }

    /** Return the number of pending tasks */
    @VisibleForTesting
    public int numPendingTasks()
    {
        return clean.numPendingTasks();
    }

    @Override
    public void interrupt()
    {
        executor.interrupt();
    }

    @Override
    public boolean isTerminated()
    {
        return executor.isTerminated();
    }

    @Override
    public void shutdown()
    {
        executor.shutdown();
    }

    @Override
    public Object shutdownNow()
    {
        return executor.shutdownNow();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit units) throws InterruptedException
    {
        return executor.awaitTermination(timeout, units);
    }
}
