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
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.function.IntSupplier;
import java.util.function.LongConsumer;
import java.util.function.ToIntFunction;

import net.openhft.chronicle.core.util.WeakIdentityHashMap;
import org.apache.cassandra.simulator.systems.InterceptedWait.CaptureSites;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.Shared;
import org.apache.cassandra.utils.concurrent.BlockingQueues;
import org.apache.cassandra.utils.concurrent.Condition;
import org.apache.cassandra.utils.concurrent.CountDownLatch;
import org.apache.cassandra.utils.concurrent.Semaphore;
import org.apache.cassandra.utils.concurrent.Semaphore.Standard;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import static org.apache.cassandra.utils.Shared.Recursive.INTERFACES;
import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

@SuppressWarnings("unused")
@Shared(scope = SIMULATION, inner = INTERFACES)
public interface InterceptorOfGlobalMethods extends InterceptorOfSystemMethods, Closeable
{
    WaitQueue newWaitQueue();
    CountDownLatch newCountDownLatch(int count);
    Condition newOneTimeCondition();

    /**
     * If this interceptor is debugging wait/wake/now sites, return one initialised with the current trace of the
     * provided thread; otherwise return null.
     */
    CaptureSites captureWaitSite(Thread thread);

    /**
     * Returns the current thread as an InterceptibleThread IF it has its InterceptConsequences interceptor set.
     * Otherwise, one of the following will happen:
     *   * if the InterceptorOfWaits permits it, null will be returned;
     *   * if it does not, the process will be failed.
     */
    InterceptibleThread ifIntercepted();

    void uncaughtException(Thread thread, Throwable throwable);

    @PerClassLoader
    public static class IfInterceptibleThread extends None implements InterceptorOfGlobalMethods
    {
        static LongConsumer threadLocalRandomCheck;

        @Override
        public WaitQueue newWaitQueue()
        {
            Thread thread = Thread.currentThread();
            if (thread instanceof InterceptibleThread)
                return ((InterceptibleThread) thread).interceptorOfGlobalMethods().newWaitQueue();

            return WaitQueue.newWaitQueue();
        }

        @Override
        public CountDownLatch newCountDownLatch(int count)
        {
            Thread thread = Thread.currentThread();
            if (thread instanceof InterceptibleThread)
                return ((InterceptibleThread) thread).interceptorOfGlobalMethods().newCountDownLatch(count);

            return CountDownLatch.newCountDownLatch(count);
        }

        @Override
        public Condition newOneTimeCondition()
        {
            Thread thread = Thread.currentThread();
            if (thread instanceof InterceptibleThread)
                return ((InterceptibleThread) thread).interceptorOfGlobalMethods().newOneTimeCondition();

            return Condition.newOneTimeCondition();
        }

        @Override
        public CaptureSites captureWaitSite(Thread thread)
        {
            if (thread instanceof InterceptibleThread)
                return ((InterceptibleThread) thread).interceptorOfGlobalMethods().captureWaitSite(thread);

            Thread currentThread = Thread.currentThread();
            if (currentThread instanceof InterceptibleThread)
                return ((InterceptibleThread) currentThread).interceptorOfGlobalMethods().captureWaitSite(thread);

            return null;
        }

        @Override
        public InterceptibleThread ifIntercepted()
        {
            Thread thread = Thread.currentThread();
            if (thread instanceof InterceptibleThread)
                return ((InterceptibleThread) thread).interceptorOfGlobalMethods().ifIntercepted();

            return null;
        }

        @Override
        public void waitUntil(long deadlineNanos) throws InterruptedException
        {
            Thread thread = Thread.currentThread();
            if (thread instanceof InterceptibleThread)
            {
                ((InterceptibleThread) thread).interceptorOfGlobalMethods().waitUntil(deadlineNanos);
            }
            else
            {
                super.waitUntil(deadlineNanos);
            }
        }

        @Override
        public boolean waitUntil(Object monitor, long deadlineNanos) throws InterruptedException
        {
            Thread thread = Thread.currentThread();
            if (thread instanceof InterceptibleThread)
                return ((InterceptibleThread) thread).interceptorOfGlobalMethods().waitUntil(monitor, deadlineNanos);

            return super.waitUntil(monitor, deadlineNanos);
        }

        @Override
        public void wait(Object monitor) throws InterruptedException
        {
            Thread thread = Thread.currentThread();
            if (thread instanceof InterceptibleThread)
            {
                ((InterceptibleThread) thread).interceptorOfGlobalMethods().wait(monitor);
            }
            else
            {
                monitor.wait();
            }
        }

        @Override
        public void wait(Object monitor, long millis) throws InterruptedException
        {
            Thread thread = Thread.currentThread();
            if (thread instanceof InterceptibleThread)
            {
                ((InterceptibleThread) thread).interceptorOfGlobalMethods().wait(monitor, millis);
            }
            else
            {
                monitor.wait(millis);
            }
        }

        @Override
        public void wait(Object monitor, long millis, int nanos) throws InterruptedException
        {
            Thread thread = Thread.currentThread();
            if (thread instanceof InterceptibleThread)
            {
                ((InterceptibleThread) thread).interceptorOfGlobalMethods().wait(monitor, millis, nanos);
            }
            else
            {
                monitor.wait(millis, nanos);
            }
        }

        @Override
        public void preMonitorEnter(Object object, float chanceOfSwitch)
        {
            Thread thread = Thread.currentThread();
            if (thread instanceof InterceptibleThread)
            {
                ((InterceptibleThread) thread).interceptorOfGlobalMethods().preMonitorEnter(object, chanceOfSwitch);
            }
        }

        @Override
        public void preMonitorExit(Object object)
        {
            Thread thread = Thread.currentThread();
            if (thread instanceof InterceptibleThread)
            {
                ((InterceptibleThread) thread).interceptorOfGlobalMethods().preMonitorExit(object);
            }
        }

        @Override
        public void notify(Object monitor)
        {
            Thread thread = Thread.currentThread();
            if (thread instanceof InterceptibleThread)
            {
                ((InterceptibleThread) thread).interceptorOfGlobalMethods().notify(monitor);
            }
            else
            {
                monitor.notify();
            }
        }

        @Override
        public void notifyAll(Object monitor)
        {
            Thread thread = Thread.currentThread();
            if (thread instanceof InterceptibleThread)
            {
                ((InterceptibleThread) thread).interceptorOfGlobalMethods().notifyAll(monitor);
            }
            else
            {
                monitor.notifyAll();
            }
        }

        @Override
        public void park()
        {
            InterceptibleThread.park();
        }

        @Override
        public void parkNanos(long nanos)
        {
            InterceptibleThread.parkNanos(nanos);
        }

        @Override
        public void parkUntil(long millis)
        {
            InterceptibleThread.parkUntil(millis);
        }

        @Override
        public void park(Object blocker)
        {
            InterceptibleThread.park(blocker);
        }

        @Override
        public void parkNanos(Object blocker, long nanos)
        {
            InterceptibleThread.parkNanos(blocker, nanos);
        }

        @Override
        public void parkUntil(Object blocker, long millis)
        {
            InterceptibleThread.parkUntil(blocker, millis);
        }

        @Override
        public void unpark(Thread thread)
        {
            InterceptibleThread.unpark(thread);
        }

        @Override
        public void nemesis(float chance)
        {
            Thread thread = Thread.currentThread();
            if (thread instanceof InterceptibleThread)
            {
                ((InterceptibleThread) thread).interceptorOfGlobalMethods().nemesis(chance);
            }
        }

        @Override
        public long randomSeed()
        {
            Thread thread = Thread.currentThread();
            if (thread instanceof InterceptibleThread)
            {
                return ((InterceptibleThread) thread).interceptorOfGlobalMethods().randomSeed();
            }
            else
            {   // TODO: throw an exception? May result in non-determinism
                return super.randomSeed();
            }
        }

        @Override
        public UUID randomUUID()
        {
            Thread thread = Thread.currentThread();
            if (thread instanceof InterceptibleThread)
            {
                return ((InterceptibleThread) thread).interceptorOfGlobalMethods().randomUUID();
            }
            else
            {
                return super.randomUUID();
            }
        }

        @Override
        public void threadLocalRandomCheck(long seed)
        {
            if (threadLocalRandomCheck != null)
                threadLocalRandomCheck.accept(seed);
        }

        @Override
        public void uncaughtException(Thread thread, Throwable throwable)
        {
            if (thread instanceof InterceptibleThread)
                ((InterceptibleThread) thread).interceptorOfGlobalMethods().uncaughtException(thread, throwable);
        }

        @Override
        public long nanoTime()
        {
            return Clock.Global.nanoTime();
        }

        @Override
        public long currentTimeMillis()
        {
            return Clock.Global.currentTimeMillis();
        }

        public static void setThreadLocalRandomCheck(LongConsumer runnable)
        {
            threadLocalRandomCheck = runnable;
        }

        @Override
        public void close()
        {
        }
    }

    @SuppressWarnings("unused")
    public static class Global
    {
        private static InterceptorOfGlobalMethods methods;

        public static WaitQueue newWaitQueue()
        {
            return methods.newWaitQueue();
        }

        public static CountDownLatch newCountDownLatch(int count)
        {
            return methods.newCountDownLatch(count);
        }

        public static Semaphore newSemaphore(int count)
        {
            return new Standard(count, false);
        }

        public static Semaphore newFairSemaphore(int count)
        {
            return new Standard(count, true);
        }

        public static Condition newOneTimeCondition()
        {
            return methods.newOneTimeCondition();
        }

        public static <T> BlockingQueue<T> newBlockingQueue()
        {
            return newBlockingQueue(Integer.MAX_VALUE);
        }

        public static <T> BlockingQueue<T> newBlockingQueue(int capacity)
        {
            return new BlockingQueues.Sync<>(capacity, new ArrayDeque<>());
        }

        public static CaptureSites captureWaitSite(Thread thread)
        {
            return methods.captureWaitSite(thread);
        }

        public static InterceptibleThread ifIntercepted()
        {
            return methods.ifIntercepted();
        }

        public static void uncaughtException(Thread thread, Throwable throwable)
        {
            System.err.println(thread);
            throwable.printStackTrace(System.err);
            methods.uncaughtException(thread, throwable);
        }

        public static void unsafeReset()
        {
            Global.methods = new IfInterceptibleThread();
            InterceptorOfSystemMethods.Global.unsafeSet(methods);
        }

        public static void unsafeSet(InterceptorOfGlobalMethods methods, IntSupplier intSupplier)
        {
            unsafeSet(methods, new IdentityHashCode(intSupplier));
        }

        public static void unsafeSet(InterceptorOfGlobalMethods methods, ToIntFunction<Object> identityHashCode)
        {
            InterceptorOfSystemMethods.Global.unsafeSet(methods, identityHashCode);
            Global.methods = methods;
        }
    }

    static class IdentityHashCode implements ToIntFunction<Object>
    {
        static class LCGRandom implements IntSupplier
        {
            private static final int LCG_MULTIPLIER = 22695477;
            private final int constant;
            private int nextId;

            public LCGRandom(int constant)
            {
                this.constant = constant == 0 ? 1 : constant;
            }

            @Override
            public int getAsInt()
            {
                int id = nextId;
                nextId = (id * LCG_MULTIPLIER) + constant;
                id ^= id >> 16;
                return id;
            }
        }

        private final IntSupplier nextId;
        private final WeakIdentityHashMap<Object, Integer> saved = new WeakIdentityHashMap<>();

        public IdentityHashCode(IntSupplier nextId)
        {
            this.nextId = nextId;
        }

        public synchronized int applyAsInt(Object value)
        {
            Integer id = saved.get(value);
            if (id == null)
            {
                id = nextId.getAsInt();
                saved.put(value, id);
            }
            return id;
        }
    }

}
