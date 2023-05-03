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

package org.apache.cassandra.simulator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.internal.DefaultPriorityQueue;
import io.netty.util.internal.PriorityQueue;
import org.apache.cassandra.simulator.OrderOn.OrderOnId;
import org.apache.cassandra.simulator.Ordered.Sequence;
import org.apache.cassandra.simulator.systems.SimulatedTime;
import org.apache.cassandra.simulator.utils.SafeCollections;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_SIMULATOR_DEBUG;
import static org.apache.cassandra.simulator.Action.Modifier.DAEMON;
import static org.apache.cassandra.simulator.Action.Modifier.STREAM;
import static org.apache.cassandra.simulator.Action.Phase.CONSEQUENCE;
import static org.apache.cassandra.simulator.Action.Phase.READY_TO_SCHEDULE;
import static org.apache.cassandra.simulator.Action.Phase.RUNNABLE;
import static org.apache.cassandra.simulator.Action.Phase.SCHEDULED;
import static org.apache.cassandra.simulator.Action.Phase.SEQUENCED_POST_SCHEDULED;
import static org.apache.cassandra.simulator.Action.Phase.SEQUENCED_PRE_SCHEDULED;
import static org.apache.cassandra.simulator.ActionSchedule.Mode.TIME_LIMITED;
import static org.apache.cassandra.simulator.ActionSchedule.Mode.UNLIMITED;
import static org.apache.cassandra.simulator.SimulatorUtils.failWithOOM;
import static org.apache.cassandra.simulator.SimulatorUtils.dumpStackTraces;

/**
 * TODO (feature): support total stalls on specific nodes
 *
 * This class coordinates the running of actions that have been planned by an ActionPlan, or are the consequences
 * of actions that have been executed by such a plan. This coordination includes enforcing all {@link OrderOn}
 * criteria, and running DAEMON (recurring scheduled) tasks.
 *
 * Note there is a distinct scheduling mechanism {@link org.apache.cassandra.simulator.Action.Modifier#WITHHOLD}
 * that is coordinated by an Action and its parent, that is used to prevent certain actions from running unless
 * all descendants have executed (with the aim of it ordinarily being invalidated before this happens), and this
 * is not imposed here because it would be more complicated to manage.
 */
public class ActionSchedule implements CloseableIterator<Object>, LongConsumer
{
    private static final Logger logger = LoggerFactory.getLogger(ActionList.class);

    public enum Mode { TIME_LIMITED, STREAM_LIMITED, TIME_AND_STREAM_LIMITED, FINITE, UNLIMITED }

    public static class Work
    {
        final Mode mode;
        final long runForNanos;
        final List<ActionList> actors;

        public Work(Mode mode, List<ActionList> actors)
        {
            this(mode, -1, actors);
            Preconditions.checkArgument(mode != TIME_LIMITED);
        }

        public Work(long runForNanos, List<ActionList> actors)
        {
            this(TIME_LIMITED, runForNanos, actors);
            Preconditions.checkArgument(runForNanos > 0);
        }

        public Work(Mode mode, long runForNanos, List<ActionList> actors)
        {
            this.mode = mode;
            this.runForNanos = runForNanos;
            this.actors = actors;
        }
    }

    public static class ReconcileItem
    {
        final long start, end;
        final Action performed;
        final ActionList result;

        public ReconcileItem(long start, long end, Action performed, ActionList result)
        {
            this.start = start;
            this.end = end;
            this.performed = performed;
            this.result = result;
        }

        public String toString()
        {
            return "run:" + performed.toReconcileString() + "; next:" + result.toReconcileString()
                   + "; between [" + start + ',' + end + ']';
        }
    }

    final SimulatedTime time;
    final FutureActionScheduler scheduler;
    final RunnableActionScheduler runnableScheduler;
    final LongSupplier schedulerJitter; // we will prioritise all actions scheduled to run within this period of the current oldest action
    long currentJitter, currentJitterUntil;

    // Action flow is:
    //    perform() -> [withheld]
    //              -> consequences
    //              -> [pendingDaemonWave | <invalidate daemon>]
    //              -> [sequences (if ordered and SEQUENCE_EAGERLY)]
    //              -> [scheduled]
    //              -> [sequences (if ordered and !SEQUENCE_EAGERLY)]
    //              -> runnable + [runnableByScheduledAt]
    final Map<OrderOn, Sequence> sequences = new HashMap<>();
    // queue of items that are not yet runnable sorted by deadline
    final PriorityQueue<Action> scheduled = new DefaultPriorityQueue<>(Action::compareByDeadline, 128);
    // queue of items that are runnable (i.e. within scheduler jitter of min deadline) sorted by their execution order (i.e. priority)
    final PriorityQueue<Action> runnable = new DefaultPriorityQueue<>(Action::compareByPriority, 128);
    // auxillary queue of items that are runnable so that we may track the time span covered by runnable items we are randomising execution of
    final PriorityQueue<Action> runnableByDeadline = new DefaultPriorityQueue<>(Action::compareByDeadline, 128);

    private Mode mode;

    // if running in TIME_LIMITED mode, stop ALL streams (finite or infinite) and daemon tasks once we pass this point
    private long runUntilNanos;

    // if running in STREAM_LIMITED mode, stop infinite streams once we have no more finite streams to process
    private int activeFiniteStreamCount;

    // If running in UNLIMITED mode, release daemons (recurring tasks) in waves,
    // so we can simplify checking if they're all that's running
    // TODO (cleanup): we can do better than this, probably most straightforwardly by ensuring daemon actions
    //                 have a consistent but unique id(), and managing the set of these.
    private int activeDaemonWaveCount;
    private int pendingDaemonWaveCountDown;
    private DefaultPriorityQueue<Action> pendingDaemonWave;

    private final Iterator<Work> moreWork;

    public ActionSchedule(SimulatedTime time, FutureActionScheduler futureScheduler, LongSupplier schedulerJitter, RunnableActionScheduler runnableScheduler, Work... moreWork)
    {
        this(time, futureScheduler, runnableScheduler, schedulerJitter, Arrays.asList(moreWork).iterator());
    }

    public ActionSchedule(SimulatedTime time, FutureActionScheduler futureScheduler, RunnableActionScheduler runnableScheduler, LongSupplier schedulerJitter, Iterator<Work> moreWork)
    {
        this.time = time;
        this.runnableScheduler = runnableScheduler;
        this.time.onDiscontinuity(this);
        this.scheduler = futureScheduler;
        this.schedulerJitter = schedulerJitter;
        this.moreWork = moreWork;
        moreWork();
    }

    void add(Action action)
    {
        Preconditions.checkState(action.phase() == CONSEQUENCE);
        action.schedule(time, scheduler);
        action.setupOrdering(this);
        if (action.is(STREAM) && !action.is(DAEMON))
            ++activeFiniteStreamCount;

        switch (mode)
        {
            default: throw new AssertionError();
            case TIME_AND_STREAM_LIMITED:
                if ((activeFiniteStreamCount == 0 || time.nanoTime() >= runUntilNanos) && action.is(DAEMON))
                {
                    action.cancel();
                    return;
                }
                break;
            case TIME_LIMITED:
                if (time.nanoTime() >= runUntilNanos && (action.is(DAEMON) || action.is(STREAM)))
                {
                    action.cancel();
                    return;
                }
                break;
            case STREAM_LIMITED:
                if (activeFiniteStreamCount == 0 && action.is(DAEMON))
                {
                    action.cancel();
                    return;
                }
                break;
            case UNLIMITED:
                if (action.is(STREAM)) throw new IllegalStateException();
                if (action.is(DAEMON))
                {
                    action.saveIn(pendingDaemonWave);
                    action.advanceTo(READY_TO_SCHEDULE);
                    return;
                }
                break;
            case FINITE:
                if (action.is(STREAM)) throw new IllegalStateException();
                break;
        }
        action.advanceTo(READY_TO_SCHEDULE);
        advance(action);
    }

    void advance(Action action)
    {
        switch (action.phase())
        {
            default:
                throw new AssertionError();

            case CONSEQUENCE:
                    // this should only happen if we invalidate an Ordered action that tries to
                    // enqueue one of the actions we are in the middle of scheduling for the first time
                    return;

            case READY_TO_SCHEDULE:
                if (action.ordered != null && action.ordered.waitPreScheduled())
                {
                    action.advanceTo(SEQUENCED_PRE_SCHEDULED);
                    return;
                }

            case SEQUENCED_PRE_SCHEDULED:
                if (action.deadline() > time.nanoTime())
                {
                    action.addTo(scheduled);
                    action.advanceTo(SCHEDULED);
                    return;
                }

            case SCHEDULED:
                if (action.ordered != null && action.ordered.waitPostScheduled())
                {
                    action.advanceTo(SEQUENCED_POST_SCHEDULED);
                    return;
                }

            case SEQUENCED_POST_SCHEDULED:
                action.addTo(runnable);
                action.saveIn(runnableByDeadline);
                action.advanceTo(RUNNABLE);
        }
    }

    void add(ActionList add)
    {
        if (add.isEmpty())
            return;

        add.forEach(this::add);
    }

    public boolean hasNext()
    {
        if (!runnable.isEmpty() || !scheduled.isEmpty())
            return true;

        while (moreWork())
        {
            if (!runnable.isEmpty() || !scheduled.isEmpty())
                return true;
        }

        if (!sequences.isEmpty())
        {
            // TODO (feature): detection of which action is blocking progress, and logging of its stack trace only
            Stream<Action> actions;
            if (Ordered.DEBUG)
            {
                logger.error("Simulation failed to make progress; blocked task graph:");
                actions = sequences.values()
                                   .stream()
                                   .flatMap(s -> Stream.concat(s.maybeRunning.stream(), s.next.stream()))
                                   .map(o -> o.ordered().action);
            }
            else
            {
                logger.error("Simulation failed to make progress. Run with -D{}=true to see the blocked task graph. Blocked tasks:", TEST_SIMULATOR_DEBUG.getKey());
                actions = sequences.values()
                                   .stream()
                                   .filter(s -> s.on instanceof OrderOnId)
                                   .map(s -> ((OrderOnId) s.on).id)
                                   .flatMap(s -> s instanceof ActionList ? ((ActionList) s).stream() : Stream.empty());
            }

            actions.filter(Action::isStarted)
                   .distinct()
                   .sorted(Comparator.comparingLong(a -> ((long) ((a.isStarted() ? 1 : 0) + (a.isFinished() ? 2 : 0)) << 32) | a.childCount()))
                   .forEach(a -> logger.error(a.describeCurrentState()));

            logger.error("Thread stack traces:");
            dumpStackTraces(logger);
            throw failWithOOM();
        }

        return false;
    }

    private boolean moreWork()
    {
        if (!moreWork.hasNext())
            return false;

        Work work = moreWork.next();
        this.runUntilNanos = work.runForNanos < 0 ? -1 : time.nanoTime() + work.runForNanos;
        Mode oldMode = mode;
        mode = work.mode;
        if (oldMode != work.mode)
        {
            if (work.mode == UNLIMITED)
            {
                this.pendingDaemonWave = new DefaultPriorityQueue<>(Action::compareByPriority, 128);
            }
            else if (oldMode == UNLIMITED)
            {
                while (!pendingDaemonWave.isEmpty())
                    advance(pendingDaemonWave.poll());
                pendingDaemonWave = null;
            }
        }
        work.actors.forEach(runnableScheduler::attachTo);
        work.actors.forEach(a -> a.forEach(Action::setConsequence));
        work.actors.forEach(this::add);
        return true;
    }

    public Object next()
    {
        long now = time.nanoTime();
        if (now >= currentJitterUntil)
        {
            currentJitter = schedulerJitter.getAsLong();
            currentJitterUntil = now + currentJitter + schedulerJitter.getAsLong();
        }
        if (!scheduled.isEmpty())
        {
            long scheduleUntil = Math.min((runnableByDeadline.isEmpty() ? now : runnableByDeadline.peek().deadline())
                                          + currentJitter, currentJitterUntil);

            while (!scheduled.isEmpty() && (runnable.isEmpty() || scheduled.peek().deadline() <= scheduleUntil))
                advance(scheduled.poll());
        }

        Action perform = runnable.poll();
        if (perform == null)
            throw new NoSuchElementException();

        if (!runnableByDeadline.remove(perform) && perform.deadline() > 0)
            throw new IllegalStateException();
        time.tick(perform.deadline());
        maybeScheduleDaemons(perform);

        ActionList consequences = perform.perform();
        add(consequences);
        if (perform.is(STREAM) && !perform.is(DAEMON))
            --activeFiniteStreamCount;

        long end = time.nanoTime();
        return new ReconcileItem(now, end, perform, consequences);
    }

    private void maybeScheduleDaemons(Action perform)
    {
        if (pendingDaemonWave != null)
        {
            if (perform.is(DAEMON) && --activeDaemonWaveCount == 0)
            {
                pendingDaemonWaveCountDown = Math.max(128, 16 * (scheduled.size() + pendingDaemonWave.size()));
            }
            else if (activeDaemonWaveCount == 0 && --pendingDaemonWaveCountDown <= 0)
            {
                activeDaemonWaveCount = pendingDaemonWave.size();
                while (!pendingDaemonWave.isEmpty())
                    advance(pendingDaemonWave.poll());
                if (activeDaemonWaveCount == 0) pendingDaemonWaveCountDown = Math.max(128, 16 * scheduled.size());
            }
        }
    }

    public void close()
    {
        if (sequences.isEmpty() && scheduled.isEmpty() && runnable.isEmpty()
            && (pendingDaemonWave == null || pendingDaemonWave.isEmpty()) && !moreWork.hasNext())
            return;

        List<Sequence> invalidateSequences = new ArrayList<>(this.sequences.values());
        List<Action> invalidateActions = new ArrayList<>(scheduled.size() + runnable.size() + (pendingDaemonWave == null ? 0 : pendingDaemonWave.size()));
        invalidateActions.addAll(scheduled);
        invalidateActions.addAll(runnable);
        if (pendingDaemonWave != null)
            invalidateActions.addAll(pendingDaemonWave);
        while (moreWork.hasNext())
            moreWork.next().actors.forEach(invalidateActions::addAll);

        Throwable fail = SafeCollections.safeForEach(invalidateSequences, Sequence::invalidatePending);
        fail = Throwables.merge(fail, SafeCollections.safeForEach(invalidateActions, Action::invalidate));
        scheduled.clear();
        runnable.clear();
        runnableByDeadline.clear();
        if (pendingDaemonWave != null)
            pendingDaemonWave.clear();
        sequences.clear();
        Throwables.maybeFail(fail);
    }

    @Override
    public void accept(long discontinuity)
    {
        if (runUntilNanos > 0)
            runUntilNanos += discontinuity;
    }

}
