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
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.Function;

import com.google.common.base.Preconditions;

import org.apache.cassandra.simulator.utils.CountingCollection;
import org.apache.cassandra.simulator.utils.IntrusiveLinkedList;
import org.apache.cassandra.simulator.utils.IntrusiveLinkedListNode;

import static java.util.Collections.newSetFromMap;
import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_SIMULATOR_DEBUG;

/**
 * Represents an action that may not run before certain other actions
 * have been executed, excluding child tasks that are not continuations
 * (i.e. required threads/tasks to terminate their execution, but not
 * any other child or transitive child actions)
 */
class Ordered extends OrderedLink implements ActionListener
{
    static final boolean DEBUG = TEST_SIMULATOR_DEBUG.getBoolean();

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
        final IntrusiveLinkedList<OrderedLink> next = new IntrusiveLinkedList<>();

        Sequence(OrderOn on)
        {
            this.on = on;
            this.concurrency = on.concurrency();
            this.maybeRunning = concurrency == 1
                                ? new ArrayList<>(1)
                                : new LinkedHashSet<>();
        }

        <O extends Ordered> void add(O add, Function<O, List<Sequence>> memberOf)
        {
            memberOf.apply(add).add(this);
            if (maybeRunning.size() < concurrency)
            {
                maybeRunning.add(add);
            }
            else
            {
                if (add.isFree())
                {
                    next.add(add);
                }
                else
                {
                    Preconditions.checkState(add.additionalLink == null);
                    add.additionalLink = new AdditionalOrderedLink(add);
                    next.add(add.additionalLink);
                }

                add.predecessors.add(this); // we don't submit, as we may yet be added to other sequences that prohibit our execution
            }
        }

        /**
         * Mark a task complete, and maybe schedule another from {@link #next}
         */
        void complete(Ordered completed, ActionSchedule schedule)
        {
            if (!maybeRunning.remove(completed))
                throw new IllegalStateException();

            complete(schedule);
        }

        void invalidate(Ordered completed, ActionSchedule schedule)
        {
            if (maybeRunning.remove(completed))
                complete(schedule);
        }

        void invalidatePending()
        {
            if (next.isEmpty())
                return;

            List<Ordered> invalidate = new ArrayList<>();
            for (OrderedLink link = next.poll() ; link != null ; link = next.poll())
                invalidate.add(link.ordered());
            invalidate.forEach(Ordered::invalidate);
        }

        void complete(ActionSchedule schedule)
        {
            if (next.isEmpty() && maybeRunning.isEmpty())
            {
                schedule.sequences.remove(on);
            }
            else
            {
                OrderedLink nextLink = this.next.poll();
                if (nextLink != null)
                {
                    Ordered next = nextLink.ordered();
                    if (!next.predecessors.remove(this))
                        throw new IllegalStateException();
                    maybeRunning.add(next);
                    next.maybeAdvance();
                }
            }
        }

        public String toString()
        {
            return on.toString();
        }
    }

    /**
     * Represents an action that may not run before all child actions
     * have been executed, transitively (i.e. child of child, ad infinitum).
     */
    static class StrictlyOrdered extends Ordered implements ActionListener
    {
        /** The sequences we participate in, in a strict fashion */
        final List<Sequence> strictMemberOf = new ArrayList<>(1);
        boolean isCompleteStrict;

        StrictlyOrdered(Action action, ActionSchedule schedule)
        {
            super(action, schedule);
        }

        @Override
        public void transitivelyAfter(Action finished)
        {
            assert !isCompleteStrict;
            isCompleteStrict = true;
            strictMemberOf.forEach(m -> m.complete(this, schedule));
        }

        @Override
        void invalidate(boolean isCancellation)
        {
            super.invalidate(isCancellation);
            strictMemberOf.forEach(m -> m.invalidate(this, schedule));
        }

        @Override
        void joinNow(OrderOn orderOn)
        {
            schedule.sequences.computeIfAbsent(orderOn.unwrap(), Sequence::new)
                              .add(this, orderOn.isStrict() ? o -> o.strictMemberOf : o -> o.memberOf);
        }
    }

    final ActionSchedule schedule;
    /** Those sequences that contain tasks that must complete before we can execute */
    final Collection<Sequence> predecessors = !DEBUG ? new CountingCollection<>() : newSetFromMap(new IdentityHashMap<>());

    /** The sequences we participate in, in a non-strict fashion */
    final List<Sequence> memberOf = new ArrayList<>(1);
    /** The underlying action waiting to execute */
    final Action action;
    /** State tracking to assert correct behaviour */
    boolean isStarted, isComplete;
    List<OrderOn> joinPostScheduling;
    OrderedLink additionalLink;

    Ordered(Action action, ActionSchedule schedule)
    {
        this.schedule = schedule;
        this.action = action;
        action.register(this);
    }

    public String toString()
    {
        return action.toString();
    }

    public void before(Action performed, Before before)
    {
        switch (before)
        {
            default: throw new AssertionError();
            case INVALIDATE: // will be handled by invalidate()
                return;
            case DROP:
            case EXECUTE:
                assert performed == action;
                assert !isStarted;
                isStarted = true;
        }
    }

    void join(OrderOn orderOn)
    {
        if (!orderOn.isOrdered())
            return;

        if (orderOn.appliesBeforeScheduling()) joinNow(orderOn);
        else joinPostScheduling(orderOn);
    }

    void joinNow(OrderOn orderOn)
    {
        schedule.sequences.computeIfAbsent(orderOn.unwrap(), Sequence::new)
                          .add(this, o -> o.memberOf);
    }

    void joinPostScheduling(OrderOn orderOn)
    {
        if (joinPostScheduling == null)
        {
            joinPostScheduling = Collections.singletonList(orderOn);
        }
        else
        {
            if (joinPostScheduling.size() == 1)
            {
                List<OrderOn> tmp = new ArrayList<>(2);
                tmp.addAll(joinPostScheduling);
                joinPostScheduling = tmp;
            }
            joinPostScheduling.add(orderOn);
        }
    }

    boolean waitPreScheduled()
    {
        return !predecessors.isEmpty();
    }

    boolean waitPostScheduled()
    {
        Preconditions.checkState(predecessors.isEmpty());
        if (joinPostScheduling == null)
            return false;
        joinPostScheduling.forEach(this::joinNow);
        joinPostScheduling = null;
        return !predecessors.isEmpty();
    }

    void invalidate()
    {
        invalidate(false);
    }

    void invalidate(boolean isCancellation)
    {
        Preconditions.checkState(!isCancellation || !isStarted);
        isStarted = isComplete = true;
        action.deregister(this);
        remove();
        if (additionalLink != null)
        {
            additionalLink.remove();
            additionalLink = null;
        }
        memberOf.forEach(m -> m.invalidate(this, schedule));
    }

    void maybeAdvance()
    {
        if (predecessors.isEmpty())
            schedule.advance(action);
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
    Ordered ordered()
    {
        return this;
    }
}

abstract class OrderedLink extends IntrusiveLinkedListNode
{
    abstract Ordered ordered();
    public void remove() { super.remove(); }
    public boolean isFree() { return super.isFree(); }
}

class AdditionalOrderedLink extends OrderedLink
{
    final Ordered ordered;

    AdditionalOrderedLink(Ordered ordered) { this.ordered = ordered; }
    Ordered ordered() { return ordered; }
}
