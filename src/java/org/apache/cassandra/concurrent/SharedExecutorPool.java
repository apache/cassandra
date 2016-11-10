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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.cassandra.concurrent.SEPWorker.Work;

/**
 * A pool of worker threads that are shared between all Executors created with it. Each executor is treated as a distinct
 * unit, with its own concurrency and task queue limits, but the threads that service the tasks on each executor are
 * free to hop between executors at will.
 *
 * To keep producers from incurring unnecessary delays, once an executor is "spun up" (i.e. is processing tasks at a steady
 * rate), adding tasks to the executor often involves only placing the task on the work queue and updating the
 * task permits (which imposes our max queue length constraints). Only when it cannot be guaranteed the task will be serviced
 * promptly, and the maximum concurrency has not been reached, does the producer have to schedule a thread itself to perform 
 * the work ('promptly' in this context means we already have a worker spinning for work, as described next).
 *
 * Otherwise the worker threads schedule themselves: when they are assigned a task, they will attempt to spawn
 * a partner worker to service any other work outstanding on the queue (if any); once they have finished the task they
 * will either take another (if any remaining) and repeat this, or they will attempt to assign themselves to another executor
 * that does have tasks remaining. If both fail, it will enter a non-busy-spinning phase, where it will sleep for a short
 * random interval (based upon the number of threads in this mode, so that the total amount of non-sleeping time remains
 * approximately fixed regardless of the number of spinning threads), and upon waking will again try to assign itself to
 * an executor with outstanding tasks to perform. As a result of always scheduling a partner before committing to performing
 * any work, with a steady state of task arrival we should generally have either one spinning worker ready to promptly respond 
 * to incoming work, or all possible workers actively committed to tasks.
 * 
 * In order to prevent this executor pool acting like a noisy neighbour to other processes on the system, workers also deschedule
 * themselves when it is detected that there are too many for the current rate of operation arrival. This is decided as a function 
 * of the total time spent spinning by all workers in an interval; as more workers spin, workers are descheduled more rapidly.
 */
public class SharedExecutorPool
{

    public static final SharedExecutorPool SHARED = new SharedExecutorPool("SharedPool");

    // the name assigned to workers in the pool, and the id suffix
    final String poolName;
    final AtomicLong workerId = new AtomicLong();

    // the collection of executors serviced by this pool; periodically ordered by traffic volume
    final List<SEPExecutor> executors = new CopyOnWriteArrayList<>();

    // the number of workers currently in a spinning state
    final AtomicInteger spinningCount = new AtomicInteger();
    // see SEPWorker.maybeStop() - used to self coordinate stopping of threads
    final AtomicLong stopCheck = new AtomicLong();
    // the collection of threads that are (most likely) in a spinning state - new workers are scheduled from here first
    // TODO: consider using a queue partially-ordered by scheduled wake-up time
    // (a full-fledged correctly ordered SkipList is overkill)
    final ConcurrentSkipListMap<Long, SEPWorker> spinning = new ConcurrentSkipListMap<>();
    // the collection of threads that have been asked to stop/deschedule - new workers are scheduled from here last
    final ConcurrentSkipListMap<Long, SEPWorker> descheduled = new ConcurrentSkipListMap<>();

    public SharedExecutorPool(String poolName)
    {
        this.poolName = poolName;
    }

    void schedule(Work work)
    {
        // we try to hand-off our work to the spinning queue before the descheduled queue, even though we expect it to be empty
        // all we're doing here is hoping to find a worker without work to do, but it doesn't matter too much what we find;
        // we atomically set the task so even if this were a collection of all workers it would be safe, and if they are both
        // empty we schedule a new thread
        Map.Entry<Long, SEPWorker> e;
        while (null != (e = spinning.pollFirstEntry()) || null != (e = descheduled.pollFirstEntry()))
            if (e.getValue().assign(work, false))
                return;

        if (!work.isStop())
            new SEPWorker(workerId.incrementAndGet(), work, this);
    }

    void maybeStartSpinningWorker()
    {
        // in general the workers manage spinningCount directly; however if it is zero, we increment it atomically
        // ourselves to avoid starting a worker unless we have to
        int current = spinningCount.get();
        if (current == 0 && spinningCount.compareAndSet(0, 1))
            schedule(Work.SPINNING);
    }

    public LocalAwareExecutorService newExecutor(int maxConcurrency, int maxQueuedTasks, String jmxPath, String name)
    {
        SEPExecutor executor = new SEPExecutor(this, maxConcurrency, maxQueuedTasks, jmxPath, name);
        executors.add(executor);
        return executor;
    }
}
