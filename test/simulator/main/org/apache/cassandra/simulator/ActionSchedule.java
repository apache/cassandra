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

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.internal.DefaultPriorityQueue;
import io.netty.util.internal.PriorityQueue;
import org.apache.cassandra.simulator.systems.SimulatedTime;

import static java.util.Collections.newSetFromMap;
import static java.util.Spliterators.spliteratorUnknownSize;
import static org.apache.cassandra.simulator.Action.Modifier.DAEMON;
import static org.apache.cassandra.simulator.SimulatorUtils.failWithOOM;
import static org.apache.cassandra.simulator.SimulatorUtils.dumpStackTraces;

/**
 * This class coordinates the running of actions that have been planned by an ActionPlan, or are the consequences
 * of actions that have been executed by such a plan. This coordination includes enforcing all {@link OrderOn}
 * criteria, and running DAEMON (recurring scheduled) tasks.
 *
 * Note there is a distinct scheduling mechanism {@link org.apache.cassandra.simulator.Action.Modifier#WITHHOLD}
 * that is coordinated by an Action and its parent, that is used to prevent certain actions from running unless
 * all descendants have executed (with the aim of it ordinarily being invalidated before this happens), and this
 * is not imposed here because it would be more complicated to manage.
 */
public class ActionSchedule implements Iterator<Object>
{
    private static final Logger logger = LoggerFactory.getLogger(ActionSequence.class);

    private static final boolean DEBUG = ActionSchedule.class.desiredAssertionStatus();

    /**
     * A sequence is used to model STRICT execution order imposed on certain actions that are not able
     * to reliably complete if their actions are re-ordered, and to implement thread executor order,
     * both for sequential executors and for ensuring executors with a given concurrency level do not
     * exceed that concurrency level.
     */
    static class Sequence
    {
        final OrderOn on;
        final int concurrency;
        /** The tasks we are currently permitting to run (but may not be running due to membership of other sequences) */
        final Collection<Ordered> maybeRunning;
        /** The tasks we have pending */
        final OrderedQueue<Ordered> next = new OrderedQueue<>();

        Sequence(OrderOn on)
        {
            this.on = on;
            this.concurrency = on.concurrency();
            this.maybeRunning = !DEBUG ? new CountingCollection<>()
                                       : concurrency == 1
                                         ? new ArrayList<>(1)
                                         : new LinkedHashSet<>();
        }

        <O extends Ordered> void addInternal(O add, Function<O, List<Sequence>> memberOf)
        {
            memberOf.apply(add).add(this);
            if (maybeRunning.size() < concurrency)
            {
                maybeRunning.add(add);
            }
            else
            {
                next.add(add);
                add.predecessors.add(this); // we don't submit, as we may yet be added to other sequences that prohibit our execution
            }
        }

        void add(OrderedAction add)
        {
            add(add, o -> o.memberOf);
        }

        void addStrict(StrictlyOrderedAction add)
        {
            add(add, o -> o.strictMemberOf);
        }

        <O extends OrderedAction> void add(O add, Function<O, List<Sequence>> memberOf)
        {
            addInternal(add, memberOf);
        }

        void add(List<OrderedAction> add)
        {
            add(add, o -> o.memberOf);
        }

        void addStrict(List<StrictlyOrderedAction> add)
        {
            add(add, o -> o.strictMemberOf);
        }

        <O extends OrderedAction> void add(List<O> add, Function<O, List<Sequence>> memberOf)
        {
            for (int i = 0, size = add.size() ; i < size ; ++i)
                addInternal(add.get(i), memberOf);
        }

        /**
         * Mark a task complete, and maybe schedule another from {@link #next}
         */
        void complete(OrderedAction completed, ActionSchedule schedule)
        {
            if (!maybeRunning.remove(completed))
                throw new IllegalStateException();

            if (next.isEmpty() && maybeRunning.isEmpty())
            {
                schedule.sequences.remove(on);
            }
            else
            {
                Ordered next = this.next.poll();
                if (next != null)
                {
                    if (!next.predecessors.remove(this))
                        throw new IllegalStateException();
                    maybeRunning.add(next);
                    next.maybeAddToSchedule();
                }
            }
        }

        public String toString()
        {
            return on.toString();
        }
    }

    abstract static class Ordered
    {
        final ActionSchedule schedule;
        /** Those sequences that contain tasks that must complete before we can execute */
        final Collection<Sequence> predecessors = !DEBUG ? new CountingCollection<>() : newSetFromMap(new IdentityHashMap<>());
        Ordered prev;
        Ordered next;

        protected Ordered(ActionSchedule schedule)
        {
            this.schedule = schedule;
        }

        void maybeAddToSchedule()
        {
            if (predecessors.isEmpty())
                addToSchedule();
        }

        abstract void addToSchedule();

        void remove()
        {
            prev.next = next;
            next.prev = prev;
        }
    }

    /**
     * A simple intrusive double-linked list for maintaining a list of tasks,
     * useful for invalidating queued ordered tasks
     */
    @SuppressWarnings("unchecked")
    static class OrderedQueue<O extends Ordered> extends Ordered
    {
        protected OrderedQueue()
        {
            super(null);
            prev = next = this;
        }

        void add(O add)
        {
            Ordered after = this;
            Ordered before = prev;
            add.next = after;
            add.prev = before;
            before.next = add;
            after.prev = add;
        }

        O poll()
        {
            if (isEmpty())
                return null;

            Ordered next = this.next;
            next.remove();
            return (O) next;
        }

        boolean isEmpty()
        {
            return next == this;
        }

        Stream<O> stream()
        {
            Iterator<O> iterator = new Iterator<O>()
            {
                Ordered next = OrderedQueue.this.next;

                @Override
                public boolean hasNext()
                {
                    return next != OrderedQueue.this;
                }

                @Override
                public O next()
                {
                    O result = (O)next;
                    next = next.next;
                    return result;
                }
            };

            return StreamSupport.stream(spliteratorUnknownSize(iterator, Spliterator.IMMUTABLE), false);
        }

        @Override
        void addToSchedule()
        {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Represents an action that may not run before certain other actions
     * have been executed, excluding child tasks that are not continuations
     * (i.e. required threads/tasks to terminate their execution, but not
     * any other child or transitive child actions)
     */
    static class OrderedAction extends Ordered implements ActionListener
    {
        /** The sequences we participate in, in a non-strict fashion */
        final List<Sequence> memberOf = new ArrayList<>(1);
        /** The underlying action waiting to execute */
        final Action action;
        /** State tracking to assert correct behaviour */
        boolean isStarted, isComplete;

        OrderedAction(Action action, ActionSchedule schedule)
        {
            super(schedule);
            this.action = action;
            action.register(this);
        }

        void addToSchedule()
        {
            assert !isStarted;
            assert !isComplete;

            assert memberOf.stream().allMatch(m -> m.maybeRunning.contains(this));
            schedule.addNow(action);
        }

        public String toString()
        {
            return action.toString();
        }

        public void before(Action performed, boolean performing)
        {
            assert performed == action;
            assert !isStarted;
            isStarted = true;
        }

        @Override
        public void after(Action performed)
        {
            assert isStarted;
            assert !isComplete;
            isComplete = true;
            memberOf.forEach(m -> m.complete(this, schedule));
        }

        @Override
        public void invalidated()
        {
            if (predecessors.isEmpty()) after(null);
            else remove();
        }
    }

    /**
     * Represents an action that may not run before all child actions
     * have been executed, transitively (i.e. child of child, ad infinitum).
     */
    static class StrictlyOrderedAction extends OrderedAction implements ActionListener
    {
        /** The sequences we participate in, in a strict fashion */
        final List<Sequence> strictMemberOf = new ArrayList<>(1);
        boolean isCompleteStrict;

        StrictlyOrderedAction(Action action, ActionSchedule schedule)
        {
            super(action, schedule);
        }

        void addToSchedule()
        {
            assert !isCompleteStrict;
            assert strictMemberOf.stream().allMatch(m -> m.maybeRunning.contains(this));
            super.addToSchedule();
        }

        public void transitivelyAfter(Action finished)
        {
            assert !isCompleteStrict;
            isCompleteStrict = true;
            strictMemberOf.forEach(m -> m.complete(this, schedule));
        }
    }

    final SimulatedTime time;
    final Map<OrderOn, Sequence> sequences = new IdentityHashMap<>();
    final PriorityQueue<Action> scheduled = new DefaultPriorityQueue<>(Action::compareTo, 1024);

    // release daemons in waves, so we can simplify checking if they're all that's running
    private int activeDaemonWaveCount;
    private int pendingDaemonWaveCountDown;
    private final List<Action> pendingDaemonWave = new ArrayList<>();

    public ActionSchedule(SimulatedTime time, ActionSchedulers scheduler, List<ActionSequence> actors)
    {
        actors.forEach(time::initialize);
        actors.forEach(scheduler::attachTo);
        actors.forEach(this::add);
        this.time = time;
    }

    void add(Action add)
    {
        if (add.orderOn().concurrency() == Integer.MAX_VALUE)
            addNow(add);
        else if (add.orderOn().isStrict())
            addToSequence(add, StrictlyOrderedAction::new, Sequence::addStrict).maybeAddToSchedule();
        else
            addToSequence(add, OrderedAction::new, Sequence::add).maybeAddToSchedule();
    }

    private <T extends OrderedAction> T addToSequence(Action add,
                                                      BiFunction<Action, ActionSchedule, T> factory,
                                                      BiConsumer<Sequence, T> adder)
    {
        T ordered = factory.apply(add, this);
        if (add.orderOn().isOrdered())
            adder.accept(sequences.computeIfAbsent(add.orderOn(), Sequence::new), ordered);
        return ordered;
    }

    void add(ActionSequence add)
    {
        if (add.anyMatch(a -> a.is(DAEMON)))
        {
            add.forEach(a -> { if (a.is(DAEMON)) pendingDaemonWave.add(a); });
            add = add.filter(a -> !a.is(DAEMON));
        }

        if (add.isEmpty())
            return;

        if (!add.orderOn.isOrdered())
        {
            for (int i = 0, size = add.size() ; i < size ; ++i)
                add(add.get(i));
        }
        else if (add.orderOn.isStrict())
            addToSequence(add, StrictlyOrderedAction::new, StrictlyOrderedAction::new, Sequence::addStrict, Sequence::addStrict);
        else
            addToSequence(add, OrderedAction::new, StrictlyOrderedAction::new, Sequence::addStrict, Sequence::add);
    }

    private <O extends OrderedAction, S extends O> void addToSequence(ActionSequence add,
                                                                      BiFunction<Action, ActionSchedule, O> factory,
                                                                      BiFunction<Action, ActionSchedule, S> strictFactory,
                                                                      BiConsumer<Sequence, S> strictAdder,
                                                                      BiConsumer<Sequence, List<O>> listAdder)
    {
        List<O> ordereds = new ArrayList<>(add.size());
        for (int i = 0, size = add.size() ; i < size ; ++i)
        {
            Action a = add.get(i);
            ordereds.add(a.orderOn().isOrdered() && a.orderOn().isStrict()
                    ? addToSequence(a, strictFactory, strictAdder)
                    : addToSequence(a, factory, Sequence::add));
        }

        if (add.orderOn.isOrdered())
            listAdder.accept(sequences.computeIfAbsent(add.orderOn, Sequence::new), ordereds);

        ordereds.forEach(OrderedAction::maybeAddToSchedule);
    }

    private void addNow(Action action)
    {
        if (action == null)
            return;

        action.schedule(scheduled);
    }

    public boolean hasNext()
    {
        if (!scheduled.isEmpty())
            return true;

        if (!sequences.isEmpty())
        {
            // TODO: detection of which action is blocking progress, and logging of its stack trace only
            if (DEBUG)
            {
                logger.error("Simulation failed to make progress; blocked task graph:");
                sequences.values()
                         .stream()
                         .flatMap(s -> Stream.concat(s.maybeRunning.stream(), s.next.stream()))
                         .filter(o -> o instanceof OrderedAction)
                         .map(o -> ((OrderedAction)o).action)
                         .filter(Action::isStarted)
                         .distinct()
                         .sorted(Comparator.comparingLong(a -> ((long) ((a.isStarted() ? 1 : 0) + (a.isFinished() ? 2 : 0)) << 32) | a.childCount()))
                         .forEach(a -> logger.error(a.describeCurrentState()));
            }
            else
            {
                logger.error("Simulation failed to make progress; blocked tasks:");
                sequences.values()
                         .stream()
                         .map(s -> s.on instanceof OrderOn.Sequential ? ((OrderOn.Sequential) s.on).id : null)
                         .filter(Objects::nonNull)
                         .flatMap(s -> s instanceof ActionList ? ((ActionList) s).stream() : Stream.empty())
                         .filter(Action::isStarted)
                         .distinct()
                         .sorted(Comparator.comparingLong(a -> ((long) ((a.isStarted() ? 1 : 0) + (a.isFinished() ? 2 : 0)) << 32) | a.childCount()))
                         .forEach(a -> logger.error(a.describeCurrentState()));
                logger.error("Run with assertions enabled to see the blocked task graph.");
            }
            logger.error("Thread stack traces:");
            dumpStackTraces(logger);
            failWithOOM();
        }

        return false;
    }

    public Object next()
    {
        time.tick();

        Action perform = scheduled.poll();
        if (perform == null)
            throw new NoSuchElementException();

        if (perform.is(DAEMON) && --activeDaemonWaveCount == 0)
        {
            // TODO: configurable ratio + random time to elapse
            pendingDaemonWaveCountDown = Math.max(128, 16 * (scheduled.size() + pendingDaemonWave.size()));
        }
        else if (activeDaemonWaveCount == 0 && --pendingDaemonWaveCountDown <= 0)
        {
            activeDaemonWaveCount = pendingDaemonWave.size();
            pendingDaemonWave.forEach(this::add);
            pendingDaemonWave.clear();
            if (activeDaemonWaveCount == 0) pendingDaemonWaveCountDown = Math.max(128, 16 * scheduled.size());
        }

        // TODO (now): separate thread to detect stalled progress
        // TODO (future): detection of monitor or other scheduling deadlock
        ActionSequence consequences = perform.perform();
        add(consequences);

        return Pair.of(perform, consequences);
    }

    static class CountingCollection<T> extends AbstractCollection<T>
    {
        int count;

        @Override
        public boolean add(T t)
        {
            ++count;
            return true;
        }

        @Override
        public boolean remove(Object o)
        {
            if (count == 0) throw new AssertionError();
            --count;
            return true;
        }

        @Override
        public Iterator<T> iterator()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int size()
        {
            return count;
        }
    }
}
