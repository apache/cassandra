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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.JVMStabilityInspector;

final class SEPWorker extends AtomicReference<SEPWorker.Work> implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(SEPWorker.class);

    final Long workerId;
    final Thread thread;
    final SharedExecutorPool pool;

    // prevStopCheck stores the value of pool.stopCheck after we last incremented it; if it hasn't changed,
    // we know nobody else was spinning in the interval, so we increment our soleSpinnerSpinTime accordingly,
    // and otherwise we set it to zero; this is then used to terminate the final spinning thread, as the coordinated
    // strategy can only work when there are multiple threads spinning (as more sleep time must elapse than real time)
    long prevStopCheck = 0;
    long soleSpinnerSpinTime = 0;

    SEPWorker(Long workerId, Work initialState, SharedExecutorPool pool)
    {
        this.pool = pool;
        this.workerId = workerId;
        thread = new Thread(this, pool.poolName + "-Worker-" + workerId);
        thread.setDaemon(true);
        set(initialState);
        thread.start();
    }

    public void run()
    {
        /**
         * we maintain two important invariants:
         * 1)   after exiting spinning phase, we ensure at least one more task on _each_ queue will be processed
         *      promptly after we begin, assuming any are outstanding on any pools. this is to permit producers to
         *      avoid signalling if there are _any_ spinning threads. we achieve this by simply calling maybeSchedule()
         *      on each queue if on decrementing the spin counter we hit zero.
         * 2)   before processing a task on a given queue, we attempt to assign another worker to the _same queue only_;
         *      this allows a producer to skip signalling work if the task queue is currently non-empty, and in conjunction
         *      with invariant (1) ensures that if any thread was spinning when a task was added to any executor, that
         *      task will be processed immediately if work permits are available
         */

        SEPExecutor assigned = null;
        Runnable task = null;
        try
        {
            while (true)
            {
                if (isSpinning() && !selfAssign())
                {
                    doWaitSpin();
                    continue;
                }

                // if stop was signalled, go to sleep (don't try self-assign; being put to sleep is rare, so let's obey it
                // whenever we receive it - though we don't apply this constraint to producers, who may reschedule us before
                // we go to sleep)
                if (stop())
                    while (isStopped())
                        LockSupport.park();

                // we can be assigned any state from STOPPED, so loop if we don't actually have any tasks assigned
                assigned = get().assigned;
                if (assigned == null)
                    continue;
                task = assigned.tasks.poll();

                // if we do have tasks assigned, nobody will change our state so we can simply set it to WORKING
                // (which is also a state that will never be interrupted externally)
                set(Work.WORKING);
                boolean shutdown;
                while (true)
                {
                    // before we process any task, we maybe schedule a new worker _to our executor only_; this
                    // ensures that even once all spinning threads have found work, if more work is left to be serviced
                    // and permits are available, it will be dealt with immediately.
                    assigned.maybeSchedule();

                    // we know there is work waiting, as we have a work permit, so poll() will always succeed
                    task.run();
                    task = null;

                    // if we're shutting down, or we fail to take a permit, we don't perform any more work
                    if ((shutdown = assigned.shuttingDown) || !assigned.takeTaskPermit())
                        break;
                    task = assigned.tasks.poll();
                }

                // return our work permit, and maybe signal shutdown
                assigned.returnWorkPermit();
                if (shutdown && assigned.getActiveCount() == 0)
                    assigned.shutdown.signalAll();
                assigned = null;

                // try to immediately reassign ourselves some work; if we fail, start spinning
                if (!selfAssign())
                    startSpinning();
            }
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            while (true)
            {
                if (get().assigned != null)
                {
                    assigned = get().assigned;
                    set(Work.WORKING);
                }
                if (assign(Work.STOPPED, true))
                    break;
            }
            if (assigned != null)
                assigned.returnWorkPermit();
            if (task != null)
                logger.error("Failed to execute task, unexpected exception killed worker: {}", t);
            else
                logger.error("Unexpected exception killed worker: {}", t);
        }
    }

    // try to assign this worker the provided work
    // valid states to assign are SPINNING, STOP_SIGNALLED, (ASSIGNED);
    // restores invariants of the various states (e.g. spinningCount, descheduled collection and thread park status)
    boolean assign(Work work, boolean self)
    {
        Work state = get();
        while (state.canAssign(self))
        {
            if (!compareAndSet(state, work))
            {
                state = get();
                continue;
            }
            // if we were spinning, exit the state (decrement the count); this is valid even if we are already spinning,
            // as the assigning thread will have incremented the spinningCount
            if (state.isSpinning())
                stopSpinning();

            // if we're being descheduled, place ourselves in the descheduled collection
            if (work.isStop())
                pool.descheduled.put(workerId, this);

            // if we're currently stopped, and the new state is not a stop signal
            // (which we can immediately convert to stopped), unpark the worker
            if (state.isStopped() && (!work.isStop() || !stop()))
                LockSupport.unpark(thread);
            return true;
        }
        return false;
    }

    // try to assign ourselves an executor with work available
    private boolean selfAssign()
    {
        // if we aren't permitted to assign in this state, fail
        if (!get().canAssign(true))
            return false;
        for (SEPExecutor exec : pool.executors)
        {
            if (exec.takeWorkPermit(true))
            {
                Work work = new Work(exec);
                // we successfully started work on this executor, so we must either assign it to ourselves or ...
                if (assign(work, true))
                    return true;
                // ... if we fail, schedule it to another worker
                pool.schedule(work);
                // and return success as we must have already been assigned a task
                assert get().assigned != null;
                return true;
            }
        }
        return false;
    }

    // we can only call this when our state is WORKING, and no other thread may change our state in this case;
    // so in this case only we do not need to CAS. We increment the spinningCount and add ourselves to the spinning
    // collection at the same time
    private void startSpinning()
    {
        assert get() == Work.WORKING;
        pool.spinningCount.incrementAndGet();
        set(Work.SPINNING);
    }

    // exit the spinning state; if there are no remaining spinners, we immediately try and schedule work for all executors
    // so that any producer is safe to not spin up a worker when they see a spinning thread (invariant (1) above)
    private void stopSpinning()
    {
        if (pool.spinningCount.decrementAndGet() == 0)
            for (SEPExecutor executor : pool.executors)
                executor.maybeSchedule();
        prevStopCheck = soleSpinnerSpinTime = 0;
    }

    // perform a sleep-spin, incrementing pool.stopCheck accordingly
    private void doWaitSpin()
    {
        // pick a random sleep interval based on the number of threads spinning, so that
        // we should always have a thread about to wake up, but most threads are sleeping
        long sleep = 10000L * pool.spinningCount.get();
        sleep = Math.min(1000000, sleep);
        sleep *= Math.random();
        sleep = Math.max(10000, sleep);

        long start = System.nanoTime();

        // place ourselves in the spinning collection; if we clash with another thread just exit
        Long target = start + sleep;
        if (pool.spinning.putIfAbsent(target, this) != null)
            return;
        LockSupport.parkNanos(sleep);

        // remove ourselves (if haven't been already) - we should be at or near the front, so should be cheap-ish
        pool.spinning.remove(target, this);

        // finish timing and grab spinningTime (before we finish timing so it is under rather than overestimated)
        long end = System.nanoTime();
        long spin = end - start;
        long stopCheck = pool.stopCheck.addAndGet(spin);
        maybeStop(stopCheck, end);
        if (prevStopCheck + spin == stopCheck)
            soleSpinnerSpinTime += spin;
        else
            soleSpinnerSpinTime = 0;
        prevStopCheck = stopCheck;
    }

    private static final long stopCheckInterval = TimeUnit.MILLISECONDS.toNanos(10L);

    // stops a worker if elapsed real time is less than elapsed spin time, as this implies the equivalent of
    // at least one worker achieved nothing in the interval. we achieve this by maintaining a stopCheck which
    // is initialised to a negative offset from realtime; as we spin we add to this value, and if we ever exceed
    // realtime we have spun too much and deschedule; if we get too far behind realtime, we reset to our initial offset
    private void maybeStop(long stopCheck, long now)
    {
        long delta = now - stopCheck;
        if (delta <= 0)
        {
            // if stopCheck has caught up with present, we've been spinning too much, so if we can atomically
            // set it to the past again, we should stop a worker
            if (pool.stopCheck.compareAndSet(stopCheck, now - stopCheckInterval))
            {
                // try and stop ourselves;
                // if we've already been assigned work stop another worker
                if (!assign(Work.STOP_SIGNALLED, true))
                    pool.schedule(Work.STOP_SIGNALLED);
            }
        }
        else if (soleSpinnerSpinTime > stopCheckInterval && pool.spinningCount.get() == 1)
        {
            // permit self-stopping
            assign(Work.STOP_SIGNALLED, true);
        }
        else
        {
            // if stop check has gotten too far behind present, update it so new spins can affect it
            while (delta > stopCheckInterval * 2 && !pool.stopCheck.compareAndSet(stopCheck, now - stopCheckInterval))
            {
                stopCheck = pool.stopCheck.get();
                delta = now - stopCheck;
            }
        }
    }

    private boolean isSpinning()
    {
        return get().isSpinning();
    }

    private boolean stop()
    {
        return get().isStop() && compareAndSet(Work.STOP_SIGNALLED, Work.STOPPED);
    }

    private boolean isStopped()
    {
        return get().isStopped();
    }

    /**
     * Represents, and communicates changes to, a worker's work state - there are three non-actively-working
     * states (STOP_SIGNALLED, STOPPED, AND SPINNING) and two working states: WORKING, and (ASSIGNED), the last
     * being represented by a non-static instance with its "assigned" executor set.
     *
     * STOPPED:         indicates the worker is descheduled, and whilst accepts work in this state (causing it to
     *                  be rescheduled) it will generally not be considered for work until all other worker threads are busy.
     *                  In this state we should be present in the pool.descheduled collection, and should be parked
     * -> (ASSIGNED)|SPINNING
     * STOP_SIGNALLED:  the worker has been asked to deschedule itself, but has not yet done so; only entered from a SPINNING
     *                  state, and generally communicated to itself, but maybe set from any worker. this state may be preempted
     *                  and replaced with (ASSIGNED) or SPINNING
     *                  In this state we should be present in the pool.descheduled collection
     * -> (ASSIGNED)|STOPPED|SPINNING
     * SPINNING:        indicates the worker has no work to perform, so is performing a friendly wait-based-spinning
     *                  until it either is (ASSIGNED) some work (by itself or another thread), or sent STOP_SIGNALLED
     *                  In this state we _may_ be in the pool.spinning collection (but only if we are in the middle of a sleep)
     * -> (ASSIGNED)|STOP_SIGNALLED|SPINNING
     * (ASSIGNED):      asks the worker to perform some work against the specified executor, and preassigns a task permit
     *                  from that executor so that in this state there is always work to perform.
     *                  In general a worker assigns itself this state, but sometimes it may assign another worker the state
     *                  either if there is work outstanding and no-spinning threads, or there is a race to self-assign
     * -> WORKING
     * WORKING:         indicates the worker is actively processing an executor's task queue; in this state it accepts
     *                  no state changes/communications, except from itself; it usually exits this mode into SPINNING,
     *                  but if work is immediately available on another executor it self-triggers (ASSIGNED)
     * -> SPINNING|(ASSIGNED)
     */

    static final class Work
    {
        static final Work STOP_SIGNALLED = new Work();
        static final Work STOPPED = new Work();
        static final Work SPINNING = new Work();
        static final Work WORKING = new Work();

        final SEPExecutor assigned;

        Work(SEPExecutor executor)
        {
            this.assigned = executor;
        }

        private Work()
        {
            this.assigned = null;
        }

        boolean canAssign(boolean self)
        {
            // we can assign work if there isn't new work already assigned and either
            // 1) we are assigning to ourselves
            // 2) the worker we are assigning to is not already in the middle of WORKING
            return assigned == null && (self || !isWorking());
        }

        boolean isSpinning()
        {
            return this == Work.SPINNING;
        }

        boolean isWorking()
        {
            return this == Work.WORKING;
        }

        boolean isStop()
        {
            return this == Work.STOP_SIGNALLED;
        }

        boolean isStopped()
        {
            return this == Work.STOPPED;
        }

        boolean isAssigned()
        {
            return assigned != null;
        }
    }
}
