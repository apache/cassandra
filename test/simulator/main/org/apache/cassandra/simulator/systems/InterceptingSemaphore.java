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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.utils.concurrent.Semaphore;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.simulator.systems.InterceptorOfGlobalMethods.Global.ifIntercepted;

public class InterceptingSemaphore extends Semaphore.Standard
{
    final Queue<SemaphoreSignal> interceptible = new ConcurrentLinkedQueue<>();
    final AtomicInteger permits;
    final boolean fair;

    private static class SemaphoreSignal extends InterceptingAwaitable.InterceptingSignal<Void>
    {
        private final int permits;

        private SemaphoreSignal(int permits)
        {
            this.permits = permits;
        }
    }

    public InterceptingSemaphore(int permits, boolean fair)
    {
        super(permits);
        this.permits = new AtomicInteger(permits);
        this.fair = fair;
    }

    @Override
    public int permits()
    {
        if (ifIntercepted() == null)
            return super.permits();

        return permits.get();
    }

    @Override
    public int drain()
    {
        if (ifIntercepted() == null)
            return super.drain();

        for (int i = 0; i < 10; i++)
        {
            int current = permits.get();
            if (permits.compareAndSet(current, 0))
                return current;
        }

        throw new IllegalStateException("Too much contention");
    }

    @Override
    public void release(int release)
    {
        if (ifIntercepted() == null)
        {
            super.release(release);
            return;
        }

        int remaining = permits.addAndGet(release);
        while (!interceptible.isEmpty() && remaining > 0)
        {
            SemaphoreSignal signal = interceptible.peek();
            if (signal.permits >= remaining)
                interceptible.poll().signal();
            else if (fair)
                // Do not break enqueue order if using fair scheduler
                break;
        }
    }

    @Override
    public boolean tryAcquire(int acquire)
    {
        if (ifIntercepted() == null)
            return super.tryAcquire(acquire);

        for (int i = 0; i < 10; i++)
        {
            int current = permits.get();
            if (current >= acquire)
            {
                if (permits.compareAndSet(current, current - acquire))
                    return true;
            }
            else
            {
                return false;
            }
        }
        throw new IllegalStateException("Too much contention");
    }

    @Override
    public boolean tryAcquire(int acquire, long time, TimeUnit unit) throws InterruptedException
    {
        if (ifIntercepted() == null)
            return super.tryAcquire(acquire, time, unit);

        while (true)
        {
            int current = permits.get();
            if (current >= acquire && permits.compareAndSet(current, current - acquire))
                return true;

            SemaphoreSignal signal = new SemaphoreSignal(acquire);
            interceptible.add(signal);
            boolean res = signal.await(time, unit);
            interceptible.remove(signal);
            if (!res)
                return false;
        }
    }

    @Override
    public boolean tryAcquireUntil(int acquire, long deadline) throws InterruptedException
    {
        if (ifIntercepted() == null)
            return super.tryAcquireUntil(acquire, deadline);

        while (true)
        {
            int current = permits.get();
            if (current >= acquire && permits.compareAndSet(current, current - acquire))
                    return true;

            SemaphoreSignal signal = new SemaphoreSignal(acquire);
            interceptible.add(signal);
            boolean res = signal.awaitUntil(deadline);
            interceptible.remove(signal);
            if (!res)
                return false;
        }
    }

    @Override
    public void acquire(int acquire) throws InterruptedException
    {
        if (ifIntercepted() == null)
        {
            super.acquire(acquire);
            return;
        }

        while (true)
        {
            if (tryAcquire(acquire))
                return;

            SemaphoreSignal signal = new SemaphoreSignal(acquire);
            interceptible.add(signal);
            signal.await();
            interceptible.remove(signal);
        }
    }

    @Override
    public void acquireThrowUncheckedOnInterrupt(int acquire) throws UncheckedInterruptedException
    {
        try
        {
            acquire(acquire);
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
    }
}
