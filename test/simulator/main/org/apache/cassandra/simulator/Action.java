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

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import io.netty.util.internal.DefaultPriorityQueue;
import io.netty.util.internal.PriorityQueue;
import io.netty.util.internal.PriorityQueueNode;

import static org.apache.cassandra.simulator.Action.Modifier.*;
import static org.apache.cassandra.simulator.Action.Modifiers.NONE;
import static org.apache.cassandra.simulator.Action.RegisteredType.CHILD;
import static org.apache.cassandra.simulator.Action.RegisteredType.LISTENER;

public abstract class Action implements PriorityQueueNode, Comparable<Action>
{
    private static final boolean DEBUG = Action.class.desiredAssertionStatus();

    public enum Modifier
    {
        // TODO: introduce scales of delay?
        // Delays are ordinarily imposed at random, but some operations should always be delayed (e.g. timeouts)
        ALWAYS_DELAY(false),

        // Never drop the action
        RELIABLE(true),

        // a general purpose mechanism for withholding actions until all other actions have had an opportunity to run
        // (intended primarily for TIMEOUT+NO_TIMEOUT, but may be used elsewhere)
        // this is a very similar feature to Ordered, but it was easier to model this way, as the predecessors are
        // all other child actions in the entire transitive closure of operations, with the premise that the action
        // will no longer be valid by the time it has an opportunity run
        WITHHOLD(false),

        // Mark operations as a THREAD_TIMEOUT, and parent operations as forbidding such timeouts (unless all else has failed)
        // TODO (now): use the time provided with submit to influence the time of the timeout?
        // TODO (now): invert NO_TIMEOUTS as we want default behaviour to be NO_TIMEOUTS?
        NO_TIMEOUTS(true, null, true), TIMEOUT(false, NO_TIMEOUTS),

        /**
         * All children of this action should be performed in strict order wrt the parent's consequences
         * i.e. this is the continuation version of {@link #STRICT_CHILD_ORDER}
         * this is a bit clunky, but not the end of the world
         */
        STRICT_CHILD_OF_PARENT_ORDER(false),

        /**
         * All children of this action should be performed in strict order, which means not only that
         * they must be performed in the provided order, but all of their consequences must finish before
         * the next sibling is permitted to run
         */
        STRICT_CHILD_ORDER(true, null, STRICT_CHILD_OF_PARENT_ORDER),

        /**
         * InfiniteLoopExecutors, when started, should be treated as detached from the action that happens to start them
         * so the child action is considered to be orphaned, and not registered or counted against its parent action
         */
        ORPHAN(false),

        /**
         * Recurring scheduled tasks, that the scheduler should discount when determining if has terminated
         */
        DAEMON(false),

        /**
         * Informational messages produced for logging only
         */
        INFO(false),

        /**
         * A wakeup action messages produced for logging only
         */
        WAKEUP(false);

        final boolean heritable;
        final Modifier withholdIfPresent;
        final Modifier inheritIfContinuation;

        private Modifiers asSet;

        Modifier(boolean heritable)
        {
            this(heritable, null);
        }

        Modifier(boolean heritable, Modifier withholdIfPresent)
        {
            this(heritable, withholdIfPresent, null);
        }

        Modifier(boolean heritable, Modifier withholdIfPresent, boolean inheritIfContinuation)
        {
            this.heritable = heritable;
            this.withholdIfPresent = withholdIfPresent;
            this.inheritIfContinuation = inheritIfContinuation ? this : null;
        }

        Modifier(boolean heritable, Modifier withholdIfPresent, Modifier inheritIfContinuation)
        {
            this.heritable = heritable;
            this.withholdIfPresent = withholdIfPresent;
            this.inheritIfContinuation = inheritIfContinuation;
        }

        Modifiers asSet()
        {
            if (asSet == null)
                asSet = Modifiers.of(this);
            return asSet;
        }
    }

    public static class Modifiers implements Serializable
    {
        public static final Modifiers NONE = of();
        public static final Modifiers INFO = of(Modifier.INFO);
        public static final Modifiers RELIABLE = of(Modifier.RELIABLE);
        public static final Modifiers STRICT = of(STRICT_CHILD_ORDER, Modifier.RELIABLE);
        public static final Modifiers TIMEOUT = of(Modifier.TIMEOUT, Modifier.ALWAYS_DELAY, Modifier.RELIABLE);
        public static final Modifiers NO_TIMEOUTS = of(Modifier.NO_TIMEOUTS);
        public static final Modifiers RELIABLE_NO_TIMEOUTS = of(Modifier.NO_TIMEOUTS, Modifier.RELIABLE);
        public static final Modifiers SCHEDULED = of(Modifier.RELIABLE, ALWAYS_DELAY);
        public static final Modifiers DAEMON = of(ORPHAN, Modifier.DAEMON, Modifier.RELIABLE, ALWAYS_DELAY);
        public static final Modifiers START_INFINITE_LOOP = of(ORPHAN, Modifier.RELIABLE);
        public static final Modifiers WAKE_UP_THREAD = of(Modifier.RELIABLE, WAKEUP);
        public static final Modifiers WAKE_UP_THREAD_LATER = of(Modifier.RELIABLE, WAKEUP, ALWAYS_DELAY);

        public static Modifiers of()
        {
            return new Modifiers(EnumSet.noneOf(Modifier.class));
        }

        public static Modifiers of(Modifier first, Modifier ... rest)
        {
            return new Modifiers(EnumSet.of(first, rest));
        }

        final EnumSet<Modifier> contents;
        Modifiers(EnumSet<Modifier> contents)
        {
            this.contents = contents;
        }

        public Modifiers with(Modifiers add)
        {
            if (add == NONE)
                return this;

            if (this == NONE)
                return add;

            if (contents.containsAll(add.contents))
                return this;

            return add(add.contents);
        }

        public Modifiers with(Modifier add)
        {
            if (this == NONE)
                return add.asSet();

            if (contents.contains(add))
                return this;

            return add(add.asSet().contents);
        }

        private Modifiers add(EnumSet<Modifier> add)
        {
            EnumSet<Modifier> merge = EnumSet.noneOf(Modifier.class);
            for (Modifier modifier : this.contents) add(modifier, merge, add);
            for (Modifier modifier : add) add(modifier, merge, this.contents);
            return new Modifiers(merge);
        }

        private static void add(Modifier modifier, EnumSet<Modifier> to, EnumSet<Modifier> mergingWith)
        {
            if (modifier.withholdIfPresent != null && mergingWith.contains(modifier.withholdIfPresent))
                to.add(WITHHOLD);
            to.add(modifier);
        }

        // for continuations to inherit the relevant modifiers from their immediate parent
        // (since we represent a continuation of the same execution)
        public Modifiers inheritIfContinuation(Modifiers inheritIfContinuation)
        {
            EnumSet<Modifier> merge = null;
            for (Modifier modifier : inheritIfContinuation.contents)
            {
                if (modifier.inheritIfContinuation != null)
                {
                    if (merge == null && !contents.contains(modifier.inheritIfContinuation)) merge = EnumSet.copyOf(contents);
                    if (merge != null) merge.add(modifier.inheritIfContinuation);
                }
            }

            if (merge == null)
                return this;

            if (!merge.contains(WITHHOLD))
            {
                for (Modifier modifier : merge)
                {
                    if (modifier.withholdIfPresent != null && merge.contains(modifier.withholdIfPresent))
                        merge.add(WITHHOLD);
                }
            }
            return new Modifiers(merge);
        }

        public Modifiers without(Modifier modifier)
        {
            if (!contents.contains(modifier))
                return this;

            EnumSet<Modifier> remove = EnumSet.noneOf(Modifier.class);
            remove.addAll(this.contents);
            remove.remove(modifier);
            return new Modifiers(remove);
        }

        public boolean is(Modifier modifier)
        {
            return contents.contains(modifier);
        }
    }

    enum RegisteredType { LISTENER, CHILD }

    // configuration/status
    private final Object description;
    private final OrderOn orderOn;
    private Modifiers self, transitive;
    private boolean isStarted, isFinished, isInvalidated;

    /** The listeners (and, if DEBUG, children) we have already registered */
    private final Map<Object, RegisteredType> registered = new IdentityHashMap<>(2);

    /** The list of listeners (for deterministic evaluation order) to notify on any event */
    final List<ActionListener> listeners = new ArrayList<>(2);

    /** The immediate parent, and furthest ancestor of this Action */
    protected Action parent, origin = this;

    /** The number of direct consequences of this action that have not <i>transitively</i> terminated */
    private int childCount;

    /**
     * Consequences marked WITHHOLD are kept in their parent (or parent thread's) {@code withheld} queue until all
     * other immediate children have <i>transitively</i> terminated their execution
     */
    private Queue<Action> withheld;

    // scheduler and scheduled state
    protected ActionScheduler scheduler;
    private PriorityQueue<?> scheduledIn;
    private double scheduledPriority;
    private int scheduledIndex = -1;

    public Action(Object description)
    {
        this(description, NONE, NONE);
    }
    public Action(Object description, Modifiers self)
    {
        this(description, self, NONE);
    }
    public Action(Object description, Modifiers self, Modifiers transitive)
    {
        this(description, OrderOn.NONE, self, transitive);
    }

    public Action(Object description, OrderOn orderOn, Modifiers self, Modifiers transitive)
    {
        this.description = description;
        if (orderOn == null || self == null || transitive == null)
            throw new IllegalArgumentException();
        assert transitive.contents.stream().allMatch(m -> m.heritable) : transitive.contents.toString();
        this.orderOn = orderOn;
        this.self = self;
        this.transitive = transitive;
    }

    public Object description()
    {
        return description;
    }
    public OrderOn orderOn() { return orderOn; }
    public Modifiers self() { return self; }
    public Modifiers transitive() { return transitive; }
    public boolean is(Modifier modifier)
    {
        return self.contents.contains(modifier);
    }
    public void inherit(Modifiers add)
    {
        if (add != NONE)
            add(add, add);
    }
    public void add(Modifiers self, Modifiers children)
    {
        this.self = this.self.with(self);
        this.transitive = this.transitive.with(children);
    }

    public boolean isStarted()
    {
        return isStarted;
    }
    public boolean isFinished()
    {
        return isFinished;
    }
    public boolean isInvalidated()
    {
        return isInvalidated;
    }

    public Action parent()
    {
        return parent;
    }
    public int childCount()
    {
        return childCount;
    }

    private boolean register(Object object, RegisteredType type)
    {
        RegisteredType prev = registered.putIfAbsent(object, type);
        if (prev != null && prev != type)
            throw new AssertionError();
        return prev == null;
    }

    public void register(ActionListener listener)
    {
        if (register(listener, LISTENER))
            listeners.add(listener);
    }

    /**
     * Register consequences of this action, i.e.:
     *  - attach a scheduler to them for prioritising when they are permitted to execute
     *  - pass them to any listeners as consequences
     *  - count them as children, and mark ourselves as parent, so that we may track transitive completion
     *  - withhold any actions that are so marked, to be {@link #restore}d once we have transitively completed
     *    all non-withheld actions.
     */
    protected ActionList register(ActionList consequences)
    {
        assert !isFinished();
        if (consequences.isEmpty())
            return consequences;

        scheduler.attachTo(consequences);
        listeners.forEach(l -> l.consequences(consequences));

        boolean withhold = false;
        int orphanCount = 0;
        for (int i = 0 ; i < consequences.size() ; ++i)
        {
            Action child = consequences.get(i);
            if (child.is(ORPHAN)) ++orphanCount;
            else
            {
                child.inherit(transitive);
                if (child.is(WITHHOLD))
                {
                    withhold = true;
                    if (withheld == null)
                        withheld = new ArrayDeque<>();
                    withheld.add(child);
                }

                assert child.parent == null;
                child.parent = this;
                child.origin = origin;

                if (DEBUG && !register(child, CHILD)) throw new AssertionError();
            }
        }

        int addChildCount = consequences.size() - orphanCount;
        childCount += addChildCount;

        if (!withhold)
            return consequences;

        return consequences.filter(child -> !child.is(WITHHOLD));
    }

    /**
     * Restore withheld (by ourselves or a parent) actions, when no other outstanding actions remain
     */
    public ActionList restore(ActionList consequences)
    {
        if (withheld != null && childCount == withheld.size())
        {
            consequences = consequences.andThen(ActionList.of(withheld));
            withheld = null;
            if (parent != null)
                consequences = parent.restore(consequences);
        }
        else if (childCount == 0 && parent != null)
            consequences = parent.restore(consequences);
        return consequences;
    }

    /**
     * Invoked once we finish executing ourselves. Typically this occurs immediately after invocation,
     * but for SimulatedAction it occurs only once the thread terminates its execution.
     *
     * In practice this is entirely determined by the {@code isFinished} parameter supplied to
     * {@link #performed(ActionList, boolean, boolean)}.
     */
    void finishedSelf()
    {
        assert isFinished();
        listeners.forEach(l -> l.after(this));
        if (childCount == 0)
            transitivelyFinished();
    }

    /**
     * Invoked once all of the consequences of this action, and of those actions (recursively) have completed.
     */
    void transitivelyFinished()
    {
        assert 0 == childCount && isFinished();
        if (DEBUG && registered.values().stream().anyMatch(t -> t == CHILD)) throw new AssertionError();
        listeners.forEach(l -> l.transitivelyAfter(this));
        if (parent != null)
        {
            if (DEBUG && CHILD != parent.registered.remove(this)) throw new AssertionError();
            if (--parent.childCount == 0 && parent.isFinished())
                parent.transitivelyFinished();
        }
    }

    /**
     * Invoke the action, and return its consequences, i.e. any follow up actions.
     */
    public final ActionSequence perform()
    {
        boolean drop = scheduler.drop(this);
        listeners.forEach(l -> l.before(this, !drop));

        ActionList next = perform(!drop);

        if (next.isEmpty()) return ActionSequence.empty();
        else if (is(STRICT_CHILD_ORDER)) return next.strictlySequential(this);
        else if (is(STRICT_CHILD_OF_PARENT_ORDER)) return next.strictlySequential(parent);
        else return next.unordered();
    }

    /**
     * Main implementation of {@link #perform()}, to be completed by extending classes
     *
     * MUST invoke performed() on its results before returning, to register children and record the action's state
     * @param perform whether to perform the embodied action, or to drop it (and produce any relevant actions)
     * @return the consequences
     */
    protected abstract ActionList perform(boolean perform);

    /**
     * To be invoked on the results of {@link #perform(boolean)} by its implementations.
     * We invite the implementation to invoke it so that it may control state either side of its invocation.
     *
     * {@link #register(ActionList)}'s the consequences, restores any old withheld actions,
     * and updates this Action's internal state.
     *
     * @return the provided actions, minus any withheld
     */
    protected ActionList performed(ActionList consequences, boolean isStart, boolean isFinish)
    {
        assert isStarted != isStart;
        assert !isFinished;

        consequences = register(consequences);
        assert consequences.stream().noneMatch(c -> c.is(WITHHOLD));

        if (isStart) isStarted = true;
        if (isFinish)
        {
            isFinished = true;
            if (withheld != null)
            {
                Queue<Action> withheld = this.withheld;
                this.withheld = null;
                withheld.forEach(Action::invalidate);
            }
            finishedSelf();
        }

        return restore(consequences);
    }

    /**
     * To be invoked when this action has become redundant.
     *  - Marks itself invalidated
     *  - Notifies its listeners (which may remove it from any ordered sequences in the ActionSchedule)
     *  - If present, clears itself directly from:
     *    - its parent's withholding space
     *    - the schedule's priority queue
     */
    public void invalidate()
    {
        assert !isStarted();
        isStarted = isFinished = isInvalidated = true;
        listeners.forEach(ActionListener::invalidated);
        if (parent.withheld != null && is(WITHHOLD))
            parent.withheld.remove(this);
        if (scheduledIn != null)
            scheduledIn.remove(this);
        finishedSelf();
    }

    void setScheduler(ActionScheduler scheduler)
    {
        if (this.scheduler != null)
            throw new IllegalStateException();
        this.scheduler = scheduler;
    }

    public void schedule(PriorityQueue<Action> into)
    {
        scheduledPriority = scheduler.priority(this);
        scheduledIn = into;
        into.add(this);
    }

    @Override
    public int priorityQueueIndex(DefaultPriorityQueue<?> queue)
    {
        return scheduledIndex;
    }

    @Override
    public void priorityQueueIndex(DefaultPriorityQueue<?> queue, int i)
    {
        this.scheduledIndex = i;
    }

    @Override
    public int compareTo(Action that)
    {
        return Double.compare(this.scheduledPriority, that.scheduledPriority);
    }

    public String toString()
    {
        return description() + (origin != this ? " for " + origin : "");
    }

    public String describeCurrentState()
    {
        return describeCurrentState(new StringBuilder(), "").toString();
    }

    private StringBuilder describeCurrentState(StringBuilder sb, String prefix)
    {
        if (!prefix.isEmpty())
        {
            sb.append(prefix);
        }
        if (!isStarted()) sb.append("NOT_STARTED ");
        else if (!isFinished()) sb.append("NOT_FINISHED ");
        if (childCount > 0)
        {
            sb.append('(');
            sb.append(childCount);
            sb.append(") ");
        }
        if (orderOn.isOrdered())
        {
            sb.append(orderOn);
            sb.append(": ");
        }
        sb.append(description());
        registered.entrySet().stream()
                  .filter(e -> e.getValue() == CHILD)
                  .map(e -> (Action) e.getKey())
                  .forEach(a -> {
                      sb.append('\n');
                      a.describeCurrentState(sb, prefix + "   |");
                  });

        return sb;
    }
}