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

package org.apache.cassandra.test.microbench;

import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 2, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2)
@Threads(4)
@State(Scope.Benchmark)
public class TimedMonitorBench
{
    @Param({"A", "B"})
    private String type;

    private Lock lock;

    @State(Scope.Thread)
    public static class ThreadState
    {
        Lock lock;

        @Setup(Level.Iteration)
        public void setup(TimedMonitorBench benchState) throws Throwable
        {
            if (benchState.type.equals("A")) lock = new A();
            else if (benchState.type.equals("B")) lock = new B();
            else throw new IllegalStateException();
        }
    }

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        if (type.equals("A")) lock = new A();
        else if (type.equals("B")) lock = new B();
        else throw new IllegalStateException();
    }

    interface Lock
    {
        boolean lock(long deadline);
        void maybeUnlock();
    }

    static class A implements Lock
    {
        private volatile Thread lockedBy;
        private volatile int waiting;

        private static final AtomicReferenceFieldUpdater<A, Thread> lockedByUpdater = AtomicReferenceFieldUpdater.newUpdater(A.class, Thread.class, "lockedBy");

        public boolean lock(long deadline)
        {
            try
            {
                Thread thread = Thread.currentThread();
                if (lockedByUpdater.compareAndSet(this, null, thread))
                    return true;

                synchronized (this)
                {
                    waiting++;

                    try
                    {
                        while (true)
                        {
                            if (lockedByUpdater.compareAndSet(this, null, thread))
                                return true;

                            while (lockedBy != null)
                            {
                                long now = System.nanoTime();
                                if (now >= deadline)
                                    return false;

                                wait(1 + ((deadline - now) - 1) / 1000000);
                            }
                        }
                    }
                    finally
                    {
                        waiting--;
                    }
                }
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        public void maybeUnlock()
        {
            // no visibility requirements, as if we hold the lock it was last updated by us
            if (lockedBy == null)
                return;

            Thread thread = Thread.currentThread();

            if (lockedBy == thread)
            {
                lockedBy = null;
                if (waiting > 0)
                {
                    synchronized (this)
                    {
                        notify();
                    }
                }
            }
        }
    }

    static class B implements Lock
    {
        private Thread lockedBy;

        public synchronized boolean lock(long deadline)
        {
            try
            {
                Thread thread = Thread.currentThread();
                while (lockedBy != null)
                {
                    long now = System.nanoTime();
                    if (now >= deadline)
                        return false;

                    wait(1 + ((deadline - now) - 1) / 1000000);
                }
                lockedBy = thread;
                return true;
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        public void maybeUnlock()
        {
            // no visibility requirements, as if we hold the lock it was last updated by us
            if (lockedBy == null)
                return;

            Thread thread = Thread.currentThread();

            if (lockedBy == thread)
            {
                synchronized (this)
                {
                    lockedBy = null;
                    notify();
                }
            }
        }
    }

    @Benchmark
    public void unshared(ThreadState state)
    {
        state.lock.lock(Long.MAX_VALUE);
        state.lock.maybeUnlock();
    }

    @Benchmark
    public void shared()
    {
        lock.lock(Long.MAX_VALUE);
        lock.maybeUnlock();
    }
}
