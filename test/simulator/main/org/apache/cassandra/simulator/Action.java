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
import java.util.Deque;
import java.util.EnumSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import com.google.common.base.Preconditions;

import io.netty.util.internal.DefaultPriorityQueue;
import io.netty.util.internal.PriorityQueue;
import io.netty.util.internal.PriorityQueueNode;
import org.apache.cassandra.simulator.Ordered.StrictlyOrdered;
import org.apache.cassandra.simulator.systems.SimulatedTime;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.config.CassandraRelevantProperties.TEST_SIMULATOR_DEBUG;
import static org.apache.cassandra.simulator.Action.Modifier.*;
import static org.apache.cassandra.simulator.Action.Modifiers.NONE;
import static org.apache.cassandra.simulator.Action.Phase.CANCELLED;
import static org.apache.cassandra.simulator.Action.Phase.CONSEQUENCE;
import static org.apache.cassandra.simulator.Action.Phase.FINISHED;
import static org.apache.cassandra.simulator.Action.Phase.INVALIDATED;
import static org.apache.cassandra.simulator.Action.Phase.NASCENT;
import static org.apache.cassandra.simulator.Action.Phase.STARTED;
import static org.apache.cassandra.simulator.Action.Phase.WITHHELD;
import static org.apache.cassandra.simulator.Action.RegisteredType.CHILD;
import static org.apache.cassandra.simulator.Action.RegisteredType.LISTENER;
import static org.apache.cassandra.simulator.ActionListener.Before.DROP;
import static org.apache.cassandra.simulator.ActionListener.Before.INVALIDATE;
import static org.apache.cassandra.simulator.ActionListener.Before.EXECUTE;
import static org.apache.cassandra.simulator.utils.CompactLists.append;
import static org.apache.cassandra.simulator.utils.CompactLists.remove;
import static org.apache.cassandra.simulator.utils.CompactLists.safeForEach;

public abstract class Action implements PriorityQueueNode
{
    private static final boolean DEBUG = TEST_SIMULATOR_DEBUG.getBoolean();

    public enum Modifier
    {
        /**
         * Never drop an action. Primarily intended to transitively mark an action's descendants as
         * required to succeed (e.g. in the case of repair and other cluster actions that are brittle)
         */
        RELIABLE('r', true),

        /**
         * Mark an action to be discarded, which may result in some alternative action being undertaken.
         * This marker is only to ensure correctness, as the simulator will fail if an action is performed
         * that is marked DROP and RELIABLE.
         */
        DROP('f', false),

        // indicates some scheduler delay should be added to the execution time of this action
        // TODO (feature): support per node scheduler delay to simulate struggling nodes
        THREAD_SIGNAL('t', false),

        /**
         * a general purpose mechanism for withholding actions until all other actions have had an opportunity to run
         * (intended primarily for TIMEOUT+NO_TIMEOUTS, but may be used elsewhere)
         * this is a very similar feature to Ordered, but it was easier to model this way, as the predecessors are
         * all other child actions in the entire transitive closure of operations, with the premise that the action
         * will no longer be valid by the time it has an opportunity run
         */
        WITHHOLD((char)0, false),

        // Mark operations as a THREAD_TIMEOUT, and parent operations as forbidding such timeouts (unless all else has failed)
        NO_THREAD_TIMEOUTS('n', true, null, true), THREAD_TIMEOUT('t', false, NO_THREAD_TIMEOUTS),

        /**
         * All children of this action should be performed in strict order wrt the parent's consequences
         * i.e. this is the continuation version of {@link #STRICT_CHILD_ORDER}
         * this is a bit clunky, but not the end of the world
         */
        STRICT_CHILD_OF_PARENT_ORDER('c', false),

        /**
         * All children of this action should be performed in strict order, which means not only that
         * they must be performed in the provided order, but all of their consequences must finish before
         * the next sibling is permitted to run
         */
        STRICT_CHILD_ORDER('s', true, null, STRICT_CHILD_OF_PARENT_ORDER),

        /**
         * InfiniteLoopExecutors, when started, should be treated as detached from the action that happens to start them
         * so the child action is considered to be orphaned, and not registered or counted against its parent action
         */
        ORPHAN('o', false),

        /**
         * Must be combined with ORPHAN. Unlinks an Action from its direct parent, attaching it as a child of its
         * grandparent. This is used to support streams of streams
         */
        ORPHAN_TO_GRANDPARENT((char)0, false),

        /**
         * When we both deliver a message and timeout, the timeout may be scheduled for much later. We do not want to
         * apply restrictions on later operations starting because we are waiting for a timeout to fire in this case,
         * so we detach the timeout from its parent's accounting - but re-attach its children to the parent if
         * still alive. Must be coincident with ORPHAN.
         */
        PSEUDO_ORPHAN('p', false),

        /**
         * Recurring tasks, that the schedule may discount when determining if has terminated
         */
        STREAM((char)0, false),

        /**
         * Recurring scheduled tasks, that the schedule should discount when determining if has terminated
         */
        DAEMON((char)0, false),

        /**
         * Informational messages produced for logging only
         */
        INFO((char)0, false),

        /**
         * A thread wakeup action that is ordinarily filtered from logging.
         */
        WAKEUP('w', false),

        /**
         * Show this action in the chain of origin actions
         */
        DISPLAY_ORIGIN('d', false);

        final char displayId;
        final boolean heritable;
        final Modifier withholdIfPresent;
        final Modifier inheritIfContinuation;

        private Modifiers asSet;

        Modifier(char displayId, boolean heritable)
        {
            this(displayId, heritable, null);
        }

        Modifier(char displayId, boolean heritable, Modifier withholdIfPresent)
        {
            this(displayId, heritable, withholdIfPresent, null);
        }

        Modifier(char displayId, boolean heritable, Modifier withholdIfPresent, boolean inheritIfContinuation)
        {
            this.displayId = displayId;
            this.heritable = heritable;
            this.withholdIfPresent = withholdIfPresent;
            this.inheritIfContinuation = inheritIfContinuation ? this : null;
        }

        Modifier(char displayId, boolean heritable, Modifier withholdIfPresent, Modifier inheritIfContinuation)
        {
            this.displayId = displayId;
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
        public static final Modifiers INFO = Modifier.INFO.asSet();
        public static final Modifiers RELIABLE = Modifier.RELIABLE.asSet();
        public static final Modifiers DROP = Modifier.DROP.asSet();
        public static final Modifiers PSEUDO_ORPHAN = of(Modifier.PSEUDO_ORPHAN);
        public static final Modifiers STREAM = of(Modifier.STREAM);
        public static final Modifiers INFINITE_STREAM = of(Modifier.STREAM, DAEMON);
        public static final Modifiers STREAM_ITEM = of(Modifier.STREAM, ORPHAN, ORPHAN_TO_GRANDPARENT);
        public static final Modifiers INFINITE_STREAM_ITEM = of(Modifier.STREAM, DAEMON, ORPHAN, ORPHAN_TO_GRANDPARENT);

        public static final Modifiers START_TASK = of(THREAD_SIGNAL);
        public static final Modifiers START_THREAD = of(THREAD_SIGNAL);
        public static final Modifiers START_INFINITE_LOOP = of(ORPHAN, THREAD_SIGNAL);
        public static final Modifiers START_SCHEDULED_TASK = of(THREAD_SIGNAL);
        public static final Modifiers START_TIMEOUT_TASK = of(Modifier.THREAD_TIMEOUT, THREAD_SIGNAL);
        public static final Modifiers START_DAEMON_TASK = of(ORPHAN, Modifier.DAEMON, THREAD_SIGNAL);

        public static final Modifiers WAKE_UP_THREAD = of(THREAD_SIGNAL, WAKEUP);

        public static final Modifiers STRICT = of(STRICT_CHILD_ORDER);
        public static final Modifiers NO_TIMEOUTS = Modifier.NO_THREAD_TIMEOUTS.asSet();

        public static final Modifiers RELIABLE_NO_TIMEOUTS = of(Modifier.NO_THREAD_TIMEOUTS, Modifier.RELIABLE);
        public static final Modifiers DISPLAY_ORIGIN = of(Modifier.DISPLAY_ORIGIN);

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

    enum Phase
    {
        NASCENT,
        WITHHELD,
        CONSEQUENCE,
        READY_TO_SCHEDULE,
        SEQUENCED_PRE_SCHEDULED,
        SCHEDULED,
        SEQUENCED_POST_SCHEDULED,
        RUNNABLE,
        STARTED,
        FINISHED,
        CANCELLED,
        INVALIDATED
    }

    // configuration/status
    private final Object description;
    private OrderOns orderOn;
    private Modifiers self, transitive;
    private Phase phase = NASCENT;
    Ordered ordered;

    /** The listeners (and, if DEBUG, children) we have already registered */
    private final Map<Object, RegisteredType> registered = new IdentityHashMap<>(2);

    /** The list of listeners (for deterministic evaluation order) to notify on any event */
    private List<ActionListener> listeners;

    /** The immediate parent, and furthest ancestor of this Action */
    protected Action parent, origin = this, pseudoParent;

    /** The number of direct consequences of this action that have not <i>transitively</i> terminated */
    private int childCount;

    /**
     * Consequences marked WITHHOLD are kept in their parent (or parent thread's) {@code withheld} queue until all
     * other immediate children have <i>transitively</i> terminated their execution
     */
    private DefaultPriorityQueue<Action> withheld;

    // scheduler and scheduled state
    protected RunnableActionScheduler scheduler;

    private long deadline; // some actions have an associated wall clock time to execute and are first scheduled by this
    private double priority; // all actions eventually get prioritised for execution in some order "now"

    // used to track the index and priority queue we're being managed for execution by (either by scheduledAt or priority)
    private PriorityQueue<?> scheduledIn;
    private int scheduledIndex = -1;

    // used to track the scheduledAt of events we have moved to actively scheduling/prioritising
    private PriorityQueue<?> savedIn;
    private int savedIndex = -1;

    public Action(Object description, Modifiers self)
    {
        this(description, self, NONE);
    }
    public Action(Object description, Modifiers self, Modifiers transitive)
    {
        this(description, OrderOn.NONE, self, transitive);
    }

    public Action(Object description, OrderOns orderOn, Modifiers self, Modifiers transitive)
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
    public OrderOns orderOns() { return orderOn; }
    public Phase phase() { return phase; }
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
        return phase.compareTo(STARTED) >= 0;
    }
    public boolean isFinished()
    {
        return phase.compareTo(FINISHED) >= 0;
    }
    public boolean isCancelled()
    {
        return phase.compareTo(CANCELLED) >= 0;
    }
    public boolean isInvalidated()
    {
        return phase.compareTo(INVALIDATED) >= 0;
    }

    public Action parent()
    {
        return parent;
    }
    public int childCount()
    {
        return childCount;
    }

    /**
     * Main implementation of {@link #perform()}, that must be completed by an extending classes.
     *
     * Does not need to handle consequences, registration etc.
     *
     * @return the action consequences
     */
    protected abstract ActionList performSimple();

    /**
     * Invokes {@link #performSimple} before invoking {@link #performed}.
     *
     * May be overridden by an extending classes that does not finish immediately (e.g, SimulatedAction).
     *
     * MUST handle consequences, registration etc by invoking performed() on its results before returning,
     * to register children and record the action's state
     *
     * @return the action consequences
     */
    protected ActionList performAndRegister()
    {
        return performed(performSimple(), true, true);
    }

    /**
     * Invoke the action, and return its consequences, i.e. any follow up actions.
     */
    public final ActionList perform()
    {
        Preconditions.checkState(!(is(RELIABLE) && is(Modifier.DROP)));
        Throwable fail = safeForEach(listeners, ActionListener::before, this, is(Modifier.DROP) ? DROP : EXECUTE);
        if (fail != null)
        {
            invalidate(false);
            Throwables.maybeFail(fail);
        }

        if (DEBUG && parent != null && parent.registered.get(this) != CHILD) throw new AssertionError();

        ActionList next = performAndRegister();
        next.forEach(Action::setConsequence);

        if (is(STRICT_CHILD_ORDER)) next.setStrictlySequentialOn(this);
        else if (is(STRICT_CHILD_OF_PARENT_ORDER)) next.setStrictlySequentialOn(parent);

        return next;
    }

    /**
     * To be invoked on the results of {@link #performSimple()} by its implementations.
     * We invite the implementation to invoke it so that it may control state either side of its invocation.
     *
     * {@link #register(ActionList)}'s the consequences, restores any old withheld actions,
     * and updates this Action's internal state.
     *
     * @return the provided actions, minus any withheld
     */
    protected ActionList performed(ActionList consequences, boolean isStart, boolean isFinish)
    {
        assert isStarted() != isStart;
        assert !isFinished();

        consequences = register(consequences);
        assert !consequences.anyMatch(c -> c.is(WITHHOLD));

        if (isFinish) finishedSelf();
        else if (isStart) phase = STARTED;

        return restoreWithheld(consequences);
    }

    /**
     * Similar to cancel() but invoked under abnormal termination
     */
    public void invalidate()
    {
        invalidate(INVALIDATED);
    }

    /**
     * To be invoked when this action has become redundant.
     *  - Marks itself invalidated
     *  - Notifies its listeners (which may remove it from any ordered sequences in the ActionSchedule)
     *  - If present, clears itself directly from:
     *    - its parent's withholding space
     *    - the schedule's priority queue
     */
    public void cancel()
    {
        assert !isStarted();
        invalidate(CANCELLED);
    }

    private void invalidate(Phase advanceTo)
    {
        if (phase.compareTo(CANCELLED) >= 0)
            return;

        advanceTo(advanceTo);
        Throwable fail = safeForEach(listeners, ActionListener::before, this, INVALIDATE);
        fail = Throwables.merge(fail, safeInvalidate(phase == CANCELLED));
        invalidate(phase == CANCELLED);
        finishedSelf();
        Throwables.maybeFail(fail);
    }

    protected Throwable safeInvalidate(boolean isCancellation)
    {
        return null;
    }

    private void invalidate(boolean isCancellation)
    {
        if (parent != null && parent.withheld != null && is(WITHHOLD))
        {
            if (parent.withheld.remove(this))
                parent.cleanupWithheld();
        }
        if (scheduledIndex >= 0) scheduledIn.remove(this);
        if (savedIndex >= 0) savedIn.remove(this);
        if (ordered != null) ordered.invalidate(isCancellation);
    }

    /**
     * Register consequences of this action, i.e.:
     *  - attach a scheduler to them for prioritising when they are permitted to execute
     *  - pass them to any listeners as consequences
     *  - count them as children, and mark ourselves as parent, so that we may track transitive completion
     *  - withhold any actions that are so marked, to be {@link #restoreWithheld}d once we have transitively completed
     *    all non-withheld actions.
     */
    protected ActionList register(ActionList consequences)
    {
        assert !isFinished();
        if (consequences.isEmpty())
            return consequences;

        scheduler.attachTo(consequences);
        Throwable fail = safeForEach(listeners, ActionListener::consequences, consequences);
        if (fail != null)
        {
            invalidate(false);
            Throwables.merge(fail, consequences.safeForEach(Action::invalidate));
            Throwables.maybeFail(fail);
        }

        boolean isParentPseudoOrphan = is(PSEUDO_ORPHAN);
        boolean withheld = false;
        for (int i = 0 ; i < consequences.size() ; ++i)
        {
            Action child = consequences.get(i);
            if (child.is(ORPHAN))
            {
                if (parent != null && child.is(ORPHAN_TO_GRANDPARENT))
                {
                    ++parent.childCount;
                    parent.registerChild(child);
                }
                else if (child.is(PSEUDO_ORPHAN))
                {
                    child.inherit(transitive);
                    registerPseudoOrphan(child);
                    assert !child.is(WITHHOLD);
                }
            }
            else
            {
                Action parent;
                if (isParentPseudoOrphan && pseudoParent != null && pseudoParent.childCount > 0)
                    parent = pseudoParent;
                else
                    parent = this;

                child.inherit(parent.transitive);
                if (child.is(WITHHOLD))
                {
                    // this could be supported in principle by applying the ordering here, but it would be
                    // some work to ensure it doesn't lead to deadlocks so for now just assert we don't use it
                    Preconditions.checkState(!parent.is(STRICT_CHILD_ORDER) && !parent.is(STRICT_CHILD_OF_PARENT_ORDER));
                    withheld = true;
                    parent.addWithheld(child);
                }

                parent.registerChild(child);
                parent.childCount++;
            }
        }

        if (!withheld)
            return consequences;

        return consequences.filter(child -> !child.is(WITHHOLD));
    }

    // setup the child relationship, but do not update childCount
    private void registerChild(Action child)
    {
        assert child.parent == null;
        child.parent = this;
        registerChildOrigin(child);
        if (DEBUG && !register(child, CHILD)) throw new AssertionError();
    }

    private void registerPseudoOrphan(Action child)
    {
        assert child.parent == null;
        assert child.pseudoParent == null;
        child.pseudoParent = this;
        registerChildOrigin(child);
    }

    private void registerChildOrigin(Action child)
    {
        if (is(Modifier.DISPLAY_ORIGIN)) child.origin = this;
        else if (origin != this) child.origin = origin;
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
            listeners = append(listeners, listener);
    }

    private boolean deregister(Object object, RegisteredType type)
    {
        return registered.remove(object, type);
    }

    public void deregister(ActionListener listener)
    {
        if (deregister(listener, LISTENER))
            listeners = remove(listeners, listener);
    }

    private void addWithheld(Action action)
    {
        if (withheld == null)
            withheld = new DefaultPriorityQueue<>(Action::compareByPriority, 2);
        action.advanceTo(WITHHELD);
        action.saveIn(withheld);
    }

    /**
     * Restore withheld (by ourselves or a parent) actions, when no other outstanding actions remain
     */
    public ActionList restoreWithheld(ActionList consequences)
    {
        if (withheld != null && childCount == withheld.size())
        {
            Action next = withheld.poll();
            cleanupWithheld();
            consequences = consequences.andThen(next);
        }
        else if (childCount == 0 && parent != null)
        {
            Action cur = parent;
            while (cur.childCount == 0 && cur.parent != null)
                cur = cur.parent;
            consequences = cur.restoreWithheld(consequences);
        }
        return consequences;
    }

    private void cleanupWithheld()
    {
        Action cur = this;
        if (cur.withheld.isEmpty())
            cur.withheld = null;
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
        if (phase.compareTo(CANCELLED) < 0)
            advanceTo(FINISHED);

        scheduler = null;
        if (withheld != null)
        {
            Queue<Action> withheld = this.withheld;
            this.withheld = null;
            withheld.forEach(Action::cancel);
        }
        Throwable fail = safeForEach(listeners, ActionListener::after, this);
        if (childCount == 0)
            fail = Throwables.merge(fail, transitivelyFinished());

        if (fail != null)
        {
            invalidate(false);
            Throwables.maybeFail(fail);
        }
    }

    /**
     * Invoked once all of the consequences of this action, and of those actions (recursively) have completed.
     */
    Throwable transitivelyFinished()
    {
        return transitivelyFinished(this);
    }

    static Throwable transitivelyFinished(Action cur)
    {
        Throwable fail = null;
        while (true)
        {
            Action parent = cur.parent;
            assert 0 == cur.childCount && cur.isFinished();
            if (DEBUG && cur.registered.values().stream().anyMatch(t -> t == CHILD)) throw new AssertionError();
            fail = Throwables.merge(fail, safeForEach(cur.listeners, ActionListener::transitivelyAfter, cur));
            if (parent == null)
                break;
            if (DEBUG && CHILD != parent.registered.remove(cur)) throw new AssertionError();
            if (--parent.childCount == 0 && parent.isFinished()) cur = parent;
            else break;
        }
        return fail;
    }

    void orderOn(OrderOn orderOn)
    {
        this.orderOn = this.orderOn.with(orderOn);
    }

    void setupOrdering(ActionSchedule schedule)
    {
        if (orderOn.isOrdered())
        {
            ordered = orderOn.isStrict() ? new StrictlyOrdered(this, schedule) : new Ordered(this, schedule);
            for (int i = 0, maxi = orderOn.size(); i < maxi ; ++i)
                ordered.join(orderOn.get(i));
        }
    }

    void advanceTo(Phase phase)
    {
        Preconditions.checkArgument(phase.compareTo(this.phase) > 0);
        this.phase = phase;
    }

    void addTo(PriorityQueue<Action> byDeadline)
    {
        Preconditions.checkState(scheduledIndex < 0);
        scheduledIn = byDeadline;
        byDeadline.add(this);
    }

    void saveIn(PriorityQueue<Action> saveIn)
    {
        Preconditions.checkState(savedIndex < 0);
        savedIn = saveIn;
        saveIn.add(this);
    }

    void setScheduler(RunnableActionScheduler scheduler)
    {
        Preconditions.checkState(this.scheduler == null);
        Preconditions.checkState(this.phase == NASCENT);
        this.scheduler = scheduler;
    }

    void setConsequence()
    {
        advanceTo(CONSEQUENCE);
    }

    void schedule(SimulatedTime time, FutureActionScheduler future)
    {
        setPriority(time, scheduler.priority());
        if (is(THREAD_SIGNAL) || deadline == 0)
        {
            long newDeadline = deadline == 0 ? time.nanoTime() : deadline;
            newDeadline += future.schedulerDelayNanos();
            deadline = newDeadline;
            time.onTimeEvent("ResetDeadline", newDeadline);
        }
    }

    public void setDeadline(SimulatedTime time, long deadlineNanos)
    {
        Preconditions.checkState(deadline == 0);
        Preconditions.checkArgument(deadlineNanos >= deadline);
        deadline = deadlineNanos;
        time.onTimeEvent("SetDeadline", deadlineNanos);
    }

    public void setPriority(SimulatedTime time, double priority)
    {
        this.priority = priority;
        time.onTimeEvent("SetPriority", Double.doubleToLongBits(priority));
    }

    public long deadline()
    {
        if (deadline < 0) throw new AssertionError();
        return deadline;
    }

    public double priority()
    {
        return priority;
    }

    @Override
    public int priorityQueueIndex(DefaultPriorityQueue<?> queue)
    {
        if (queue == scheduledIn) return scheduledIndex;
        else if (queue == savedIn) return savedIndex;
        else return -1;
    }

    @Override
    public void priorityQueueIndex(DefaultPriorityQueue<?> queue, int i)
    {
        if (queue == scheduledIn) { scheduledIndex = i; if (i < 0) scheduledIn = null; }
        else if (queue == savedIn) { savedIndex = i; if (i < 0) savedIn = null; }
        else throw new IllegalStateException();
    }

    public int compareByDeadline(Action that)
    {
        return Long.compare(this.deadline, that.deadline);
    }

    public int compareByPriority(Action that)
    {
        return Double.compare(this.priority, that.priority);
    }

    private String describeModifiers()
    {
        StringBuilder builder = new StringBuilder("[");
        for (Modifier modifier : self.contents)
        {
            if (modifier.displayId == 0)
                continue;

            if (!transitive.is(modifier)) builder.append(modifier.displayId);
            else builder.append(Character.toUpperCase(modifier.displayId));
        }

        boolean hasTransitiveOnly = false;
        for (Modifier modifier : transitive.contents)
        {
            if (modifier.displayId == 0)
                continue;

            if (!self.is(modifier))
            {
                if (!hasTransitiveOnly)
                {
                    hasTransitiveOnly = true;
                    builder.append('(');
                }
                builder.append(modifier.displayId);
            }
        }

        if (builder.length() == 1)
            return "";

        if (hasTransitiveOnly)
            builder.append(')');
        builder.append(']');

        return builder.toString();
    }

    public String toString()
    {
        return describeModifiers() + description() + (origin != this ? " for " + origin : "");
    }

    public String toReconcileString()
    {
        return this + " at [" + deadline + ',' + priority + ']';
    }

    private static class StackElement
    {
        final Action action;
        final Deque<Action> children;

        private StackElement(Action action)
        {
            this.action = action;
            this.children = new ArrayDeque<>(action.childCount);
            for (Map.Entry<Object, RegisteredType> e : action.registered.entrySet())
            {
                if (e.getValue() == CHILD)
                    children.add((Action) e.getKey());
            }
        }
    }

    public String describeCurrentState()
    {
        StringBuilder sb = new StringBuilder();
        Deque<StackElement> stack = new ArrayDeque<>();
        appendCurrentState(sb);

        stack.push(new StackElement(this));
        while (!stack.isEmpty())
        {
            StackElement last = stack.peek();
            if (last.children.isEmpty())
            {
                stack.pop();
            }
            else
            {
                Action child = last.children.pop();
                sb.append('\n');
                appendPrefix(stack.size(), sb);
                child.appendCurrentState(sb);
                stack.push(new StackElement(child));
            }
        }
        return sb.toString();
    }

    private static void appendPrefix(int count, StringBuilder sb)
    {
        while (--count >= 0)
            sb.append("   |");
    }

    private void appendCurrentState(StringBuilder sb)
    {
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
    }

}