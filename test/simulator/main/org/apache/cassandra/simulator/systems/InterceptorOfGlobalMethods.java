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

package org.apache.cassandra.simulator.systems;

import java.util.ArrayDeque;
import java.util.concurrent.BlockingQueue;

import net.openhft.chronicle.core.util.WeakIdentityHashMap;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.Shared;
import org.apache.cassandra.utils.concurrent.Awaitable.SyncAwaitable;
import org.apache.cassandra.utils.concurrent.BlockingQueues;
import org.apache.cassandra.utils.concurrent.Condition;
import org.apache.cassandra.utils.concurrent.CountDownLatch;
import org.apache.cassandra.utils.concurrent.Semaphore;
import org.apache.cassandra.utils.concurrent.Semaphore.UnfairAsync;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static org.apache.cassandra.utils.Shared.Recursive.INTERFACES;
import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

@SuppressWarnings("unused")
@Shared(scope = SIMULATION, inner = INTERFACES)
public interface InterceptorOfGlobalMethods extends Closeable
{
    WaitQueue newWaitQueue();
    CountDownLatch newCountDownLatch(int count);
    Condition newOneTimeCondition();
    void waitUntil(long deadlineNanos) throws InterruptedException;
    boolean waitUntil(Object monitor, long deadlineNanos) throws InterruptedException;
    void wait(Object monitor) throws InterruptedException;
    void wait(Object monitor, long millis) throws InterruptedException;
    void wait(Object monitor, long millis, int nanos) throws InterruptedException;
    void preMonitorEnter(Object object, float chanceOfSwitch);
    void preMonitorExit(Object object);
    void notify(Object monitor);
    void notifyAll(Object monitor);
    void nemesis(float chance);

    public static class NotIntercepted implements InterceptorOfGlobalMethods
    {
        @Override
        public WaitQueue newWaitQueue()
        {
            return WaitQueue.newWaitQueue();
        }

        @Override
        public CountDownLatch newCountDownLatch(int count)
        {
            return CountDownLatch.newCountDownLatch(count);
        }

        @Override
        public Condition newOneTimeCondition()
        {
            return Condition.newOneTimeCondition();
        }

        @Override
        public void waitUntil(long deadlineNanos) throws InterruptedException
        {
            Clock.waitUntil(deadlineNanos);
        }

        @Override
        public boolean waitUntil(Object monitor, long deadlineNanos) throws InterruptedException
        {
            return SyncAwaitable.waitUntil(monitor, deadlineNanos);
        }

        @Override
        public void wait(Object monitor) throws InterruptedException
        {
            monitor.wait();
        }

        @Override
        public void wait(Object monitor, long millis) throws InterruptedException
        {
            monitor.wait(millis);
        }

        @Override
        public void wait(Object monitor, long millis, int nanos) throws InterruptedException
        {
            monitor.wait(millis, nanos);
        }

        @Override
        public void preMonitorEnter(Object object, float chanceOfSwitch)
        {
        }

        @Override
        public void preMonitorExit(Object object)
        {
        }

        @Override
        public void notify(Object monitor)
        {
            monitor.notify();
        }

        @Override
        public void notifyAll(Object monitor)
        {
            monitor.notifyAll();
        }

        @Override
        public void nemesis(float chance)
        {
        }

        @Override
        public void close()
        {
        }
    }

    @SuppressWarnings("unused")
    public static class Global
    {
        private static InterceptorOfGlobalMethods instance;
        private static final IdentityHashCode identityHashCode = new IdentityHashCode();

        public static WaitQueue newWaitQueue()
        {
            return instance.newWaitQueue();
        }

        public static CountDownLatch newCountDownLatch(int count)
        {
            return instance.newCountDownLatch(count);
        }

        public static Semaphore newSemaphore(int count)
        {
            return new UnfairAsync(count);
        }

        public static Semaphore newFairSemaphore(int count)
        {
            return new UnfairAsync(count);
        }

        public static Condition newOneTimeCondition()
        {
            return instance.newOneTimeCondition();
        }

        public static <T> BlockingQueue<T> newBlockingQueue()
        {
            return newBlockingQueue(Integer.MAX_VALUE);
        }

        public static <T> BlockingQueue<T> newBlockingQueue(int capacity)
        {
            return new BlockingQueues.Sync<>(capacity, new ArrayDeque<>());
        }

        public static boolean waitUntil(Object monitor, long deadlineNanos) throws InterruptedException
        {
            return instance.waitUntil(monitor, deadlineNanos);
        }

        public static void waitUntil(long deadlineNanos) throws InterruptedException
        {
            instance.waitUntil(deadlineNanos);
        }

        public static void wait(Object monitor) throws InterruptedException
        {
            instance.wait(monitor);
        }

        public static void wait(Object monitor, long millis) throws InterruptedException
        {
            instance.wait(monitor, millis);
        }

        // TODO (now): this should be registered with each InterceptibleThread on creation, rather than done statically here
        //             as for things intercepted by the javaagent we have to set this globally and so cannot self-reconcile
        //             or have multiple simulations running on the same JVM
        @SuppressWarnings("unused")
        public static Object preMonitorEnter(Object object, float chance)
        {
            instance.preMonitorEnter(object, chance);
            return object;
        }

        public static Object preMonitorExit(Object object)
        {
            instance.preMonitorExit(object);
            return object;
        }

        public static void notify(Object monitor)
        {
            instance.notify(monitor);
        }

        public static void notifyAll(Object monitor)
        {
            instance.notifyAll(monitor);
        }

        public static void nemesis(float chance)
        {
            instance.nemesis(chance);
        }

        public static int identityHashCode(Object object)
        {
            return identityHashCode.get(object);
        }

        public static void unsafeReset()
        {
            instance = new NotIntercepted();
        }

        public static void unsafeSet(InterceptorOfGlobalMethods methods, int seed, int constant)
        {
            instance = methods;
            identityHashCode.set(seed, constant);
        }
    }

    static class IdentityHashCode
    {
        private static final int LCG_MULTIPLIER = 22695477;
        private int constant = 1;
        private int nextId;
        private final WeakIdentityHashMap<Object, Integer> saved = new WeakIdentityHashMap<>();

        synchronized void set(int nextId, int constant)
        {
            this.nextId = nextId;
            this.constant = constant == 0 ? 1 : constant;
        }

        public synchronized int get(Object value)
        {
            Integer id = saved.get(value);
            if (id == null)
            {
                id = nextId;
                nextId = (id * LCG_MULTIPLIER) + constant;
                id ^= id >> 16;
                saved.put(value, id);
            }
            return id;
        }
    }
}
