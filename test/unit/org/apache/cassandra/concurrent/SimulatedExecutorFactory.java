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

package org.apache.cassandra.concurrent;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

import accord.utils.Gens;
import accord.utils.RandomSource;
import org.apache.cassandra.utils.Clock;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class SimulatedExecutorFactory implements ExecutorFactory, Clock
{
    private static class Item implements Comparable<Item>
    {
        private final long runAtNanos;
        private final long seq;
        private final org.apache.cassandra.utils.concurrent.RunnableFuture<?>  action;

        private Item(long runAtNanos, long seq, org.apache.cassandra.utils.concurrent.RunnableFuture<?>  action)
        {
            if (runAtNanos < 0)
                throw new IllegalArgumentException("Time went backwards!  Given " + runAtNanos);
            this.runAtNanos = runAtNanos;
            this.seq = seq;
            this.action = action;
        }

        @Override
        public int compareTo(Item o)
        {
            int rc = Long.compare(runAtNanos, o.runAtNanos);
            if (rc != 0)
                return rc;
            return Long.compare(seq, o.seq);
        }

        @Override
        public String toString()
        {
            return "Item{" +
                   "runAtNanos=" + runAtNanos +
                   ", seq=" + seq +
                   '}';
        }
    }

    private final RandomSource rs;
    private final long startTimeNanos;
    private final PriorityQueue<Item> queue = new PriorityQueue<>();
    private long seq = 0;
    private long nowNanos;
    private int repeatedTasks = 0;

    public SimulatedExecutorFactory(RandomSource rs, long startTimeNanos)
    {
        this.rs = rs;
        this.startTimeNanos = startTimeNanos;
    }

    public boolean processOne()
    {
        // if we count the repeated tasks, then processAll will never complete
        if (queue.size() == repeatedTasks)
            return false;
        Item item = queue.poll();
        if (item == null)
            return false;
        nowNanos = Math.max(nowNanos + 1, item.runAtNanos);
        item.action.run();
        return true;
    }

    @Override
    public long nanoTime()
    {
        return nowNanos++;
    }

    @Override
    public long currentTimeMillis()
    {
        return TimeUnit.NANOSECONDS.toMillis(startTimeNanos + nanoTime());
    }


    @Override
    public ExecutorBuilder<? extends SequentialExecutorPlus> configureSequential(String name)
    {
        return new SimulatedExecutorBuilder<>().configureSequential(name);
    }

    @Override
    public ExecutorBuilder<? extends ExecutorPlus> configurePooled(String name, int threads)
    {
        return new SimulatedExecutorBuilder<>().configurePooled(name, threads);
    }

    @Override
    public ExecutorBuilderFactory<ExecutorPlus, SequentialExecutorPlus> withJmx(String jmxPath)
    {
        return this;
    }

    @Override
    public LocalAwareSubFactory localAware()
    {
        return new SimulatedExecutorBuilder<>();
    }

    @Override
    public ScheduledExecutorPlus scheduled(boolean executeOnShutdown, String name, int priority, SimulatorSemantics simulatorSemantics)
    {
        return new ForwardingScheduledExecutorPlus(new UnorderedScheduledExecutorService());
    }

    @Override
    public Thread startThread(String name, Runnable runnable, InfiniteLoopExecutor.Daemon daemon)
    {
        throw new UnsupportedOperationException("Thread can't be simualted");
    }

    @Override
    public Interruptible infiniteLoop(String name, Interruptible.Task task, InfiniteLoopExecutor.SimulatorSafe simulatorSafe, InfiniteLoopExecutor.Daemon daemon, InfiniteLoopExecutor.Interrupts interrupts)
    {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public ThreadGroup newThreadGroup(String name)
    {
        throw new UnsupportedOperationException("Thread can't be simualted");
    }

    private class SimulatedExecutorBuilder<E extends ExecutorService> implements ExecutorBuilder<E>, LocalAwareSubFactory, LocalAwareSubFactoryWithJMX
    {
        private int threads = -1;

        @Override
        public ExecutorBuilder<E> withKeepAlive(long keepAlive, TimeUnit keepAliveUnits)
        {
            return this;
        }

        @Override
        public ExecutorBuilder<E> withKeepAlive()
        {
            return this;
        }

        @Override
        public ExecutorBuilder<E> withThreadPriority(int threadPriority)
        {
            return this;
        }

        @Override
        public ExecutorBuilder<E> withThreadGroup(ThreadGroup threadGroup)
        {
            return this;
        }

        @Override
        public ExecutorBuilder<E> withDefaultThreadGroup()
        {
            return this;
        }

        @Override
        public ExecutorBuilder<E> withQueueLimit(int queueLimit)
        {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public ExecutorBuilder<E> withRejectedExecutionHandler(RejectedExecutionHandler rejectedExecutionHandler)
        {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public ExecutorBuilder<E> withUncaughtExceptionHandler(Thread.UncaughtExceptionHandler uncaughtExceptionHandler)
        {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public ExecutorBuilder<? extends LocalAwareSequentialExecutorPlus> configureSequential(String name)
        {
            threads = 1;
            return (ExecutorBuilder<? extends LocalAwareSequentialExecutorPlus>) this;
        }

        @Override
        public ExecutorBuilder<? extends LocalAwareExecutorPlus> configurePooled(String name, int threads)
        {
            this.threads = threads;
            return (ExecutorBuilder<? extends LocalAwareExecutorPlus>) this;
        }

        @Override
        public LocalAwareSubFactoryWithJMX withJmx(String jmxPath)
        {
            return this;
        }

        @Override
        public LocalAwareSubFactoryWithJMX withJmxInternal()
        {
            return this;
        }

        @Override
        public LocalAwareExecutorPlus shared(String name, int threads, ExecutorPlus.MaximumPoolSizeListener onSetMaxSize)
        {
            return new ForwardingLocalAwareExecutorPlus(build0());
        }

        @Override
        public E build()
        {
            return (E) build0();
        }

        private ForwardingExecutorPlus build0()
        {
            return new ForwardingExecutorPlus(threads == 1 ?
                                              new OrderedExecutorService() :
                                              new UnorderedExecutorService());
        }
    }

    private class UnorderedExecutorService extends AbstractExecutorService
    {
        private final LongSupplier jitterNanos;
        private boolean shutdown = false;

        UnorderedExecutorService()
        {
            long maxSmall = TimeUnit.MICROSECONDS.toNanos(50);
            long max = TimeUnit.MILLISECONDS.toNanos(5);
            LongSupplier small = () -> rs.nextLong(0, maxSmall);
            LongSupplier large = () -> rs.nextLong(maxSmall, max);
            this.jitterNanos = Gens.bools().runs(rs.nextInt(1, 11) / 100.0D, rs.nextInt(3, 15)).mapToLong(b -> b ? large.getAsLong() : small.getAsLong()).asLongSupplier(rs);
        }

        @Override
        protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value)
        {
            return new FutureTask<>(Executors.callable(runnable, value));
        }

        @Override
        protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable)
        {
            return new FutureTask<>(callable);
        }

        protected org.apache.cassandra.utils.concurrent.RunnableFuture<?> taskFor(Runnable command)
        {
            if (command instanceof org.apache.cassandra.utils.concurrent.RunnableFuture<?>)
                return (org.apache.cassandra.utils.concurrent.RunnableFuture<?>) command;
            return new FutureTask<>(Executors.callable(command));
        }

        @Override
        public void shutdown()
        {
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow()
        {
            return Collections.emptyList();
        }

        @Override
        public boolean isShutdown()
        {
            return shutdown;
        }

        @Override
        public boolean isTerminated()
        {
            return shutdown;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
        {
            return shutdown;
        }

        @Override
        public void execute(Runnable command)
        {
            checkNotShutdown();
            queue.add(new Item(nowWithJitter(), SimulatedExecutorFactory.this.seq++, taskFor(command)));
        }

        protected void checkNotShutdown()
        {
            if (isShutdown())
                throw new RejectedExecutionException("Shutdown");
        }

        protected long nowWithJitter()
        {
            return nanoTime() + jitterNanos.getAsLong();
        }
    }

    private class OrderedExecutorService extends UnorderedExecutorService
    {
        private final Queue<Item> pending = new LinkedList<>();

        @Override
        public void execute(Runnable command)
        {
            checkNotShutdown();
            boolean wasEmpty = pending.isEmpty();
            Item task = new Item(nowWithJitter(), SimulatedExecutorFactory.this.seq++, taskFor(command));
            pending.add(task);
            if (wasEmpty)
                runNextTask();
        }

        private void runNextTask()
        {
            Item next = pending.peek();
            if (next == null)
                return;

            next.action.addCallback((s, f) -> afterExecution());
            queue.add(next);
        }

        private void afterExecution()
        {
            pending.poll();
            runNextTask();
        }
    }

    private class UnorderedScheduledExecutorService extends UnorderedExecutorService implements ScheduledExecutorService
    {
        private class ScheduledFuture<T> extends FutureTask<T> implements java.util.concurrent.ScheduledFuture<T>
        {
            private final long sequenceNumber;
            private final long periodNanos;
            private long nextExecuteAtNanos;

            ScheduledFuture(long sequenceNumber, long initialDelay, long value, TimeUnit unit, Callable<? extends T> call)
            {
                super(call);
                this.sequenceNumber = sequenceNumber;
                periodNanos = unit.toNanos(value);
                nextExecuteAtNanos = triggerTimeNanos(initialDelay, unit);
            }

            private long triggerTimeNanos(long delay, TimeUnit unit)
            {
                long delayNanos = unit.toNanos(delay < 0 ? 0 : delay);
                return nanoTime() + delayNanos;
            }

            @Override
            public long getDelay(TimeUnit unit)
            {
                return unit.convert(nextExecuteAtNanos - nanoTime(), TimeUnit.NANOSECONDS);
            }

            @Override
            public int compareTo(Delayed other)
            {
                if (other == this) // compare zero if same object
                    return 0;
                if (other instanceof ScheduledFuture)
                {
                    ScheduledFuture<?> x = (ScheduledFuture<?>) other;
                    long diff = nextExecuteAtNanos - x.nextExecuteAtNanos;
                    if (diff < 0)
                        return -1;
                    else if (diff > 0)
                        return 1;
                    else if (sequenceNumber < x.sequenceNumber)
                        return -1;
                    else
                        return 1;
                }
                long diff = getDelay(NANOSECONDS) - other.getDelay(NANOSECONDS);
                return (diff < 0) ? -1 : (diff > 0) ? 1 : 0;
            }

            @Override
            public void run()
            {
                boolean periodic = periodNanos != 0;
                if (!periodic)
                {
                    super.run();
                }
                else
                {
                    if (isCancelled())
                        return;
                    // run without setting the result
                    try
                    {
                        call();
                        long nowNanos = nanoTime();
                        if (periodNanos > 0)
                        {
                            // scheduleAtFixedRate
                            nextExecuteAtNanos += periodNanos;
                        }
                        else
                        {
                            // scheduleWithFixedDelay
                            nextExecuteAtNanos = nowNanos + (-periodNanos);
                        }
                        long delayNanos = nextExecuteAtNanos - nowNanos;
                        if (delayNanos < 0)
                            delayNanos = 0;
                        schedule(this, delayNanos, NANOSECONDS);
                    }
                    catch (Throwable t)
                    {
                        tryFailure(t);
                    }
                }
            }
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit)
        {
            return schedule(Executors.callable(command), delay, unit);
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit)
        {
            checkNotShutdown();
            ScheduledFuture<V> task = new ScheduledFuture<>(seq++, delay, 0, NANOSECONDS, callable);
            queue.add(new Item(nowWithJitter() + unit.toNanos(delay), task.sequenceNumber, task));
            return task;
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit)
        {
            checkNotShutdown();
            ScheduledFuture<?> task = new ScheduledFuture<>(seq++, initialDelay, period, unit, Executors.callable(command));
            repeatedTasks++;
            task.addCallback((s, f) -> repeatedTasks--);
            queue.add(new Item(nowWithJitter() + unit.toNanos(initialDelay), task.sequenceNumber, task));
            return task;
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit)
        {
            checkNotShutdown();
            ScheduledFuture<?> task = new ScheduledFuture<>(seq++, initialDelay, -delay, unit, Executors.callable(command));
            repeatedTasks++;
            task.addCallback((s, f) -> repeatedTasks--);
            queue.add(new Item(nowWithJitter() + unit.toNanos(initialDelay), task.sequenceNumber, task));
            return task;
        }
    }
}
