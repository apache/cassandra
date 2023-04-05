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

import java.lang.reflect.Field;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.ToIntFunction;

import sun.misc.Unsafe;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * A superclass of InterceptorOfGlobalMethods exposing those methods we might want to use for byte-weaving classes
 * loaded by the system classloader (such as concurrency primitives). Today we byte weave Enum, Object, Random,
 * ThreadLocalRandom, ConcurrentHashMap (only for determinism) and all of java.util.concurrent.locks (for park/unpark).
 * See {@link org.apache.cassandra.simulator.asm.InterceptAgent} for more details.
 */
@SuppressWarnings("unused")
public interface InterceptorOfSystemMethods
{
    void waitUntil(long deadlineNanos) throws InterruptedException;
    void sleep(long period, TimeUnit units) throws InterruptedException;
    void sleepUninterriptibly(long period, TimeUnit units);
    boolean waitUntil(Object monitor, long deadlineNanos) throws InterruptedException;
    void wait(Object monitor) throws InterruptedException;
    void wait(Object monitor, long millis) throws InterruptedException;
    void wait(Object monitor, long millis, int nanos) throws InterruptedException;
    void preMonitorEnter(Object object, float chanceOfSwitch);
    void preMonitorExit(Object object);
    void notify(Object monitor);
    void notifyAll(Object monitor);
    void nemesis(float chance);

    void park();
    void parkNanos(long nanos);
    void parkUntil(long millis);
    void park(Object blocker);
    void parkNanos(Object blocker, long nanos);
    void parkUntil(Object blocker, long millis);
    void unpark(Thread thread);

    long randomSeed();
    UUID randomUUID();

    void threadLocalRandomCheck(long seed);

    long nanoTime();
    long currentTimeMillis();

    @SuppressWarnings("unused")
    public static class Global
    {
        private static InterceptorOfSystemMethods methods = new None();
        private static ToIntFunction<Object> identityHashCode;

        public static void waitUntil(long deadlineNanos) throws InterruptedException
        {
            methods.waitUntil(deadlineNanos);
        }

        public static void sleep(long millis) throws InterruptedException
        {
            sleep(MILLISECONDS, millis);
        }

        // slipped param order to replace instance method call without other ASM modification
        public static void sleep(TimeUnit units, long period) throws InterruptedException
        {
            methods.sleep(period, units);
        }

        // to match Guava Uninterruptibles
        public static void sleepUninterruptibly(long period, TimeUnit units)
        {
            methods.sleepUninterriptibly(period, units);
        }

        public static boolean waitUntil(Object monitor, long deadlineNanos) throws InterruptedException
        {
            return methods.waitUntil(monitor, deadlineNanos);
        }

        public static void wait(Object monitor) throws InterruptedException
        {
            methods.wait(monitor);
        }

        public static void wait(Object monitor, long millis) throws InterruptedException
        {
            methods.wait(monitor, millis);
        }

        @SuppressWarnings("unused")
        public static Object preMonitorEnter(Object object, float chance)
        {
            methods.preMonitorEnter(object, chance);
            return object;
        }

        public static Object preMonitorExit(Object object)
        {
            methods.preMonitorExit(object);
            return object;
        }

        public static void notify(Object monitor)
        {
            methods.notify(monitor);
        }

        public static void notifyAll(Object monitor)
        {
            methods.notifyAll(monitor);
        }

        public static void park()
        {
            methods.park();
        }

        public static void parkNanos(long nanos)
        {
            methods.parkNanos(nanos);
        }

        public static void parkUntil(long millis)
        {
            methods.parkUntil(millis);
        }

        public static void park(Object blocker)
        {
            methods.park(blocker);
        }

        public static void parkNanos(Object blocker, long nanos)
        {
            methods.parkNanos(blocker, nanos);
        }

        public static void parkUntil(Object blocker, long millis)
        {
            methods.parkUntil(blocker, millis);
        }

        public static void unpark(Thread thread)
        {
            methods.unpark(thread);
        }
        
        public static void nemesis(float chance)
        {
            methods.nemesis(chance);
        }

        public static int advanceProbe(int probe)
        {
            return probe + 1;
        }

        public static long randomSeed()
        {
            return methods.randomSeed();
        }

        public static UUID randomUUID()
        {
            return methods.randomUUID();
        }

        public static int threadLocalRandomCheck(int seed)
        {
            methods.threadLocalRandomCheck(seed);
            return seed;
        }

        public static long threadLocalRandomCheck(long seed)
        {
            methods.threadLocalRandomCheck(seed);
            return seed;
        }

        public static long nanoTime()
        {
            return methods.nanoTime();
        }

        public static long currentTimeMillis()
        {
            return methods.currentTimeMillis();
        }

        public static int identityHashCode(Object object)
        {
            return identityHashCode.applyAsInt(object);
        }

        public static Unsafe getUnsafe()
        {
            try
            {
                Field field = Unsafe.class.getDeclaredField("theUnsafe");
                field.setAccessible(true);
                return (Unsafe) field.get(null);
            }
            catch (Exception e)
            {
                throw new AssertionError(e);
            }
        }

        public static void unsafeSet(InterceptorOfSystemMethods methods)
        {
            Global.methods = methods;
        }

        public static void unsafeSet(InterceptorOfSystemMethods methods, ToIntFunction<Object> identityHashCode)
        {
            Global.methods = methods;
            Global.identityHashCode = identityHashCode;
        }
    }

    public static class None implements InterceptorOfSystemMethods
    {
        @Override
        public void waitUntil(long deadlineNanos) throws InterruptedException
        {
            long waitNanos = System.nanoTime() - deadlineNanos;
            if (waitNanos > 0)
                TimeUnit.NANOSECONDS.sleep(waitNanos);
        }

        @Override
        public void sleep(long period, TimeUnit units) throws InterruptedException
        {
            waitUntil(System.nanoTime() + units.toNanos(period));
        }

        @Override
        public void sleepUninterriptibly(long period, TimeUnit units)
        {
            long until = System.nanoTime() + units.toNanos(period);
            boolean isInterrupted = false;
            while (true)
            {
                try
                {
                    waitUntil(until);
                    break;
                }
                catch (InterruptedException e)
                {
                    isInterrupted = true;
                }
            }

            if (isInterrupted)
                Thread.currentThread().interrupt();
        }

        @Override
        public boolean waitUntil(Object monitor, long deadlineNanos) throws InterruptedException
        {
            long wait = deadlineNanos - System.nanoTime();
            if (wait <= 0)
                return false;

            monitor.wait((wait + 999999) / 1000000);
            return true;
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
        public void park()
        {
            LockSupport.park();
        }

        @Override
        public void parkNanos(long nanos)
        {
            LockSupport.parkNanos(nanos);
        }

        @Override
        public void parkUntil(long millis)
        {
            LockSupport.parkUntil(millis);
        }

        @Override
        public void park(Object blocker)
        {
            LockSupport.park(blocker);
        }

        @Override
        public void parkNanos(Object blocker, long nanos)
        {
            LockSupport.parkNanos(blocker, nanos);
        }

        @Override
        public void parkUntil(Object blocker, long millis)
        {
            LockSupport.parkUntil(blocker, millis);
        }

        @Override
        public void unpark(Thread thread)
        {
            LockSupport.unpark(thread);
        }

        private static final long SEED_MULTIPLIER = 2862933555777941757L;
        private static final long SEED_CONSTANT = 0x121d34a;
        private long nextSeed = 0x10523dfe2L;
        @Override
        public synchronized long randomSeed()
        {
            long next = nextSeed;
            nextSeed *= SEED_MULTIPLIER;
            nextSeed += SEED_CONSTANT;
            return next;
        }

        @Override
        public UUID randomUUID()
        {
            return UUID.randomUUID();
        }

        @Override
        public long nanoTime()
        {
            return System.nanoTime();
        }

        @Override
        public long currentTimeMillis()
        {
            return System.currentTimeMillis();
        }

        @Override
        public void threadLocalRandomCheck(long seed)
        {
        }
    }

}
