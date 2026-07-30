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

package org.apache.cassandra.service.accord.execution;

import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;

import accord.local.ExecutionContext;
import accord.messages.Accept;
import accord.messages.Commit;
import accord.messages.MessageType;
import accord.messages.Request;
import accord.primitives.Ballot;
import accord.primitives.SaveStatus;
import accord.primitives.TxnId;
import accord.utils.IntrusiveHeapNode;
import accord.utils.Invariants;
import accord.utils.TinyEnumSet;
import accord.utils.UnhandledEnum;
import accord.utils.async.Cancellable;

import org.apache.cassandra.concurrent.DebuggableTask;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.service.accord.debug.DebugExecution.DebugTask;
import org.apache.cassandra.service.accord.execution.ExclusiveExecutor.ExclusiveExecutorTask;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.WithResources;

import static accord.local.ExecutionContext.ExecutionSequence.BY_PRIORITY;
import static accord.local.ExecutionContext.ExecutionSequence.ATOMIC_CONSEQUENCE;
import static accord.primitives.Routable.Domain.Range;
import static accord.utils.Invariants.illegalState;
import static org.apache.cassandra.config.AccordConfig.QueuePriorityModel.ORIG_HLC_FIFO;
import static org.apache.cassandra.service.accord.debug.DebugExecution.DEBUG_EXECUTION;
import static org.apache.cassandra.service.accord.execution.Task.ExclusiveGroup.APPLY;
import static org.apache.cassandra.service.accord.execution.Task.ExclusiveGroup.COMMIT;
import static org.apache.cassandra.service.accord.execution.Task.ExclusiveGroup.STABLE;
import static org.apache.cassandra.service.accord.execution.Task.RunState.NOT_YET_RUN;
import static org.apache.cassandra.service.accord.execution.Task.RunState.RUNNING;
import static org.apache.cassandra.service.accord.execution.Task.RunState.RUN_FAILED;
import static org.apache.cassandra.service.accord.execution.Task.RunState.RUN_INCOMPLETE;
import static org.apache.cassandra.service.accord.execution.Task.State.CANCELLED_UNREGISTERED;
import static org.apache.cassandra.service.accord.execution.Task.State.EXECUTED;
import static org.apache.cassandra.service.accord.execution.Task.State.FAILED;
import static org.apache.cassandra.service.accord.execution.Task.State.LOADING_OPTIONAL;
import static org.apache.cassandra.service.accord.execution.Task.State.LOADING_REQUIRED;
import static org.apache.cassandra.service.accord.execution.Task.State.PREPARED;
import static org.apache.cassandra.service.accord.execution.Task.State.REGISTERED;
import static org.apache.cassandra.service.accord.execution.Task.State.RUNNING_OR_EXECUTED;
import static org.apache.cassandra.service.accord.execution.Task.State.RUNNING_WHILE_FAILED;
import static org.apache.cassandra.service.accord.execution.Task.State.SCANNING_RANGES;
import static org.apache.cassandra.service.accord.execution.Task.State.UNREGISTERED;
import static org.apache.cassandra.service.accord.execution.Task.State.WAITING_ON_KEY;
import static org.apache.cassandra.service.accord.execution.Task.State.WAITING_ON_TXN;
import static org.apache.cassandra.service.accord.execution.Task.State.WAITING_TO_RUN;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public abstract class Task extends IntrusiveHeapNode implements Cancellable, DebuggableTask
{
    private static final int WAITING_ON_KEY_BIT = 1 << 7;
    private static final int WAITING_TO_RUN_BIT = 1 << 8;
    private static final int INCOMPLETE_BIT = 1 << 11;

    enum State
    {
        UNREGISTERED(),
        CANCELLED_UNREGISTERED(UNREGISTERED),
        REGISTERED(UNREGISTERED),
        SCANNING_RANGES(REGISTERED),
        LOADING_REQUIRED(REGISTERED, SCANNING_RANGES),
        LOADING_OPTIONAL(REGISTERED, SCANNING_RANGES, LOADING_REQUIRED),
        WAITING_ON_TXN(WAITING_ON_KEY_BIT | WAITING_TO_RUN_BIT, REGISTERED, SCANNING_RANGES, LOADING_REQUIRED, LOADING_OPTIONAL),
        WAITING_ON_KEY(WAITING_TO_RUN_BIT | INCOMPLETE_BIT, REGISTERED, SCANNING_RANGES, LOADING_REQUIRED, LOADING_OPTIONAL, WAITING_ON_TXN),
        WAITING_TO_RUN(INCOMPLETE_BIT, UNREGISTERED, REGISTERED, SCANNING_RANGES, LOADING_REQUIRED, LOADING_OPTIONAL, WAITING_ON_TXN, WAITING_ON_KEY),
        PREPARING(WAITING_TO_RUN),
        PREPARED(WAITING_TO_RUN, PREPARING),
        INCOMPLETE(PREPARED),
        /**
         * We were failed while running - a cache entry we hold failed to load - and cannot be completed from outside,
         * as our run is in flight. We finish the iteration, then abandon and release everything as we complete. Only a
         * run that cannot be its task's last may enter this state, so it can never complete the callback.
         */
        RUNNING_WHILE_FAILED(PREPARED, INCOMPLETE),
        EXECUTED(PREPARED),
        FAILED(UNREGISTERED, REGISTERED, SCANNING_RANGES, LOADING_REQUIRED, LOADING_OPTIONAL, WAITING_ON_TXN, WAITING_ON_KEY, WAITING_TO_RUN, PREPARING, PREPARED, INCOMPLETE, RUNNING_WHILE_FAILED),
        CANCELLED(UNREGISTERED, REGISTERED, SCANNING_RANGES, LOADING_REQUIRED, LOADING_OPTIONAL, WAITING_ON_TXN, WAITING_ON_KEY, WAITING_TO_RUN, PREPARING),
        ;

        private final int permittedFrom;
        public static final int LOADING_OR_WAITING_REQUIRED = TinyEnumSet.encode(SCANNING_RANGES, LOADING_REQUIRED, WAITING_ON_TXN, WAITING_ON_KEY);
        public static final int WAITING = TinyEnumSet.encode(WAITING_ON_TXN, WAITING_ON_KEY, WAITING_TO_RUN);
        public static final int WAITING_OR_RUNNING = WAITING | TinyEnumSet.encode(PREPARING, PREPARED, RUNNING_WHILE_FAILED);
        /** the states in which a run is in flight, or has just finished, so may record its run state */
        public static final int RUNNING_OR_EXECUTED = TinyEnumSet.encode(PREPARED, RUNNING_WHILE_FAILED, EXECUTED);
        /** the states from which nothing further will run, so any position or reference we still hold is dead */
        public static final int TERMINAL_FAILURE = TinyEnumSet.encode(CANCELLED_UNREGISTERED, CANCELLED, FAILED);
        public static final int FAILURE = TERMINAL_FAILURE | TinyEnumSet.encode(RUNNING_WHILE_FAILED);
        /** the states in which we have begun executing, so precede anything that has not, whatever compare says */
        public static final int HAS_RUN = TinyEnumSet.encode(PREPARED, INCOMPLETE, RUNNING_WHILE_FAILED, EXECUTED);
        static final State[] VALUES = values();

        static
        {
            // hack to allow us to create loops in our enum transition declarations
            Invariants.require(INCOMPLETE_BIT == 1 << INCOMPLETE.ordinal());
            Invariants.require(WAITING_TO_RUN_BIT == 1 << WAITING_TO_RUN.ordinal());
            Invariants.require(WAITING_ON_KEY_BIT == 1 << WAITING_ON_KEY.ordinal());
            Invariants.require(VALUES.length <= 16);
        }

        State()
        {
            this.permittedFrom = 0;
        }

        State(State... permittedFroms)
        {
            this(0, permittedFroms);
        }

        State(int additional, State... permittedFroms)
        {
            int permittedFrom = additional;
            for (State state : permittedFroms)
                permittedFrom |= 1 << state.ordinal();
            this.permittedFrom = permittedFrom;
        }

        boolean isPermittedFrom(int prevOrdinal)
        {
            return (permittedFrom & (1 << prevOrdinal)) != 0;
        }

        boolean isDone()
        {
            return this.compareTo(EXECUTED) >= 0;
        }

        boolean hasStarted()
        {
            return this.compareTo(WAITING_TO_RUN) > 0;
        }

        static State forOrdinal(int ordinal)
        {
            return VALUES[ordinal];
        }
    }

    enum RunState
    {
        NOT_YET_RUN, RUNNING, RUN_INCOMPLETE, RUN_PERSISTING, RUN_SUCCESS, RUN_FAILED;

        private static final RunState[] VALUES = values();

        static RunState forOrdinal(int ordinal)
        {
            return VALUES[ordinal];
        }
    }

    enum GlobalGroup
    {
        COMMAND_STORE,
        LOAD,
        SAVE,
        OTHER,
        RANGE_LOAD,
        RANGE_SCAN,
    }

    enum ExclusiveGroup
    {
        APPLY,
        STABLE,
        COMMIT,
        ACCEPT,
        OTHER,
        RECOVER,
        PREACCEPT,
        RANGE,
    }

    public enum GroupKind
    {
        EXCLUSIVE(ExclusiveGroup.values().length, EXCLUSIVE_GROUP_SHIFT),
        GLOBAL(GlobalGroup.values().length, GLOBAL_GROUP_SHIFT),
        NONE(0, 0);

        final int count;
        final byte shift;

        GroupKind(int count, int shift)
        {
            this.count = count;
            this.shift = (byte) shift;
        }
    }

    enum ExecutorQueue
    {
        NONE(),
        LOADING(SCANNING_RANGES, LOADING_OPTIONAL, LOADING_REQUIRED),
        WAITING(WAITING_ON_TXN, WAITING_ON_KEY),
        RUNNABLE(WAITING_TO_RUN);

        private static final ExecutorQueue[] VALUES = values();

        final int permittedStates;

        ExecutorQueue(State ... states)
        {
            this.permittedStates = TinyEnumSet.encode(states);
        }

        public static ExecutorQueue forOrdinal(int ordinal)
        {
            return VALUES[ordinal];
        }
    }

    private static final int STATE_MASK = 0xf;
    static final int GROUP_MASK = 0x7;
    private static final int EXCLUSIVE_GROUP_SHIFT = 4;
    private static final int GLOBAL_GROUP_SHIFT = 7;

    private static final int NONSYNC_BIT = 1 << 10;

    // used for validation only, can be repurposed if needed; marked only when waiting on keys and txns
    private static final int VALIDATE_CACHE_QUEUED_BIT = 1 << 11;

    private static final int IS_CONTINUATION = 1 << 12;
    private static final int HAS_PRESETUP_BIT = 1 << 13;

    private static final int HAS_SETUP_SHIFT = 14;
    private static final int HAS_SETUP_MASK = 0x3 << HAS_SETUP_SHIFT;
    private static final int HAS_TRANCHE = 0x1 << HAS_SETUP_SHIFT;
    private static final int HAS_INHERITED_TRANCHE = 0x2 << HAS_SETUP_SHIFT;
    private static final int HAS_INHERITED_RANGE_SCAN = 0x3 << HAS_SETUP_SHIFT;

    private static final int EXECUTOR_QUEUE_SHIFT = 16;
    private static final int EXECUTOR_QUEUE_UNSHIFTED_MASK = 0x3;
    private static final int EXECUTOR_QUEUE_SHIFTED_MASK = EXECUTOR_QUEUE_UNSHIFTED_MASK << EXECUTOR_QUEUE_SHIFT;

    private static final int INCREMENTAL_SHIFT = 18;
    private static final int INCREMENTAL_MASK = 0x3 << INCREMENTAL_SHIFT;
    private static final int INCREMENTAL = 0x1 << INCREMENTAL_SHIFT;
    private static final int INCREMENTAL_STARTED = 0x2 << INCREMENTAL_SHIFT;
    private static final int INCREMENTAL_FINISHING = 0x3 << INCREMENTAL_SHIFT;

    private static final int SEQUENCED_SHIFT = 20;
    private static final int SEQUENCED_MASK = 0x3 << SEQUENCED_SHIFT;
    private static final int SEQUENCED_PRIORITY = 0x1 << SEQUENCED_SHIFT;
    private static final int SEQUENCED_ATOMIC = 0x2 << SEQUENCED_SHIFT;
    private static final int SEQUENCED_ATOMIC_AND_QUEUED = 0x3 << SEQUENCED_SHIFT;

    private static final int TRANCHE_SHIFT = 22;
    static final int MAX_TRANCHE = 0x3ff;

    static
    {
        Invariants.require(SEQUENCED_PRIORITY == BY_PRIORITY.ordinal() << SEQUENCED_SHIFT);
        Invariants.require(SEQUENCED_ATOMIC == ATOMIC_CONSEQUENCE.ordinal() << SEQUENCED_SHIFT);
        Invariants.require(ExecutionContext.ExecutionSequence.values().length <= 3);
    }

    // TODO (desired): quite heavy to pass-through tracing session state we mostly don't use
    public final WithResources resources;
    Task next;

    long position;
    int info;

    public final long createdAt;
    long runningAt;
    // TODO (desired): expose via executors vtable
    private volatile int runState;

    private static final AtomicIntegerFieldUpdater<Task> runStateUpdater = AtomicIntegerFieldUpdater.newUpdater(Task.class, "runState");

    Task(GlobalGroup group)
    {
        resources = DebugTask.maybeDebug(ExecutorLocals.propagate(), this);
        info = init(group, ExclusiveGroup.OTHER);
        createdAt = nanoTime();
    }

    Task(ExclusiveGroup group)
    {
        resources = DebugTask.maybeDebug(ExecutorLocals.propagate(), this);
        info = init(GlobalGroup.OTHER, group);
        createdAt = nanoTime();
    }

    protected Task(ExecutionContext context, AtomicLong lastCreatedAt)
    {
        resources = DebugTask.maybeDebug(ExecutorLocals.propagate(), this);
        createdAt = lastCreatedAt.accumulateAndGet(nanoTime(), (prev, next) -> next <= prev ? prev + 1 : next);
        ExclusiveGroup group = ExclusiveGroup.OTHER;
        TxnId txnId = context.primaryTxnId();
        if (txnId != null)
        {
            if (txnId.is(Range)) group = ExclusiveGroup.RANGE;
            else
            {
                switch (AccordExecutor.PRIORITY_MODEL)
                {
                    case HLC_FIFO:
                    case ORIG_HLC_FIFO:
                    {
                        // TODO (expected): port to ExecutionKind; also we aren't consistent about using Ballot
                        if (context instanceof Request)
                        {
                            MessageType type = ((Request) context).type();
                            if (type instanceof MessageType.StandardMessage)
                            {
                                switch ((MessageType.StandardMessage) type)
                                {
                                    case APPLY_REQ:
                                    {
                                        group = APPLY;
                                        break;
                                    }
                                    case READ_EPHEMERAL_REQ:
                                    case READ_REQ:
                                    case STABLE_THEN_READ_REQ:
                                    {
                                        group = STABLE;
                                        break;
                                    }
                                    case COMMIT_REQ:
                                    {
                                        Commit commit = (Commit) context;
                                        if (AccordExecutor.PRIORITY_MODEL == ORIG_HLC_FIFO && !commit.ballot.equals(Ballot.ZERO))
                                            txnId = null;
                                        if (commit.kind.saveStatus == SaveStatus.Stable) group = STABLE;
                                        else group = COMMIT;
                                        break;
                                    }
                                    case ACCEPT_REQ:
                                    {
                                        Accept accept = (Accept) context;
                                        if (AccordExecutor.PRIORITY_MODEL == ORIG_HLC_FIFO && !accept.ballot.equals(Ballot.ZERO))
                                            txnId = null;
                                        group = ExclusiveGroup.ACCEPT;
                                        break;
                                    }
                                    case GET_EPHEMERAL_READ_DEPS_REQ:
                                    case PRE_ACCEPT_REQ:
                                    {
                                        group = ExclusiveGroup.PREACCEPT;
                                        break;
                                    }
                                    default:
                                    {
                                        txnId = null;
                                    }
                                }
                            }
                        }
                        else
                        {
                            txnId = null;
                        }
                        break;
                    }
                    case FIFO:
                    {
                        txnId = null;
                        break;
                    }
                }
            }
        }

        this.info = init(GlobalGroup.OTHER, group);
        if (txnId != null)
            this.position = txnId.hlc();
    }

    public final Task unwrap()
    {
        if (this instanceof ExclusiveExecutorTask)
            return ((ExclusiveExecutorTask) this).queue.task;
        return this;
    }

    public DebuggableTask debuggable()
    {
        return this;
    }

    public final long creationTimeNanos()
    {
        return createdAt;
    }

    public final long startTimeNanos()
    {
        if (runState() == NOT_YET_RUN)
            return 0;
        return runningAt;
    }

    abstract void submitExclusiveMayThrow();
    /** Return true if COMPLETED successfully. false indicates more work is being done. Should not throw any exceptions related to reporting success. */
    abstract boolean runMayThrow();
    abstract void completeExclusiveMayThrow();
    abstract void tryCancelExclusive();
    abstract void reportFailureMayThrow(Throwable fail);

    abstract AccordExecutor executor();
    abstract void unqueueIfQueued();
    abstract boolean isNewWork();
    abstract String briefDescription();

    final void submitExclusiveNoExcept()
    {
        if (is(UNREGISTERED))
        {
            try { submitExclusiveMayThrow(); }
            catch (Throwable t)
            {
                tryFailAndCompleteUnexecutedExclusive(t, State.FAILED);
                unhandledException(t);
            }
        }
        else if (is(CANCELLED_UNREGISTERED))
        {
            releaseResourcesExclusiveNoExcept();
        }
        else throw illegalState("Invalid submission (%s): %s", state(), description());
    }

    /**
     * Prepare to run while holding the state cache lock.
     * If returns false, prepare failed and the task should be discarded with no further action.
     */
    final boolean prepareExclusiveNoExcept()
    {
        if (getClass() == ExclusiveExecutorTask.class)
        {
            return ((ExclusiveExecutorTask)this).prepareTask();
        }
        else
        {
            try
            {
                prepareExclusiveMayThrow();
                setStateExclusive(State.PREPARED);
                return true;
            }
            catch (Throwable t)
            {
                failAndCompleteExclusive(t, State.FAILED);
                return false;
            }
        }
    }

    void prepareExclusiveMayThrow()
    {
    }

    /**
     * Run the command; the state cache lock may or may not be held depending on the executor implementation
     */
    final void runNoExcept(TaskRunner self)
    {
        if (getClass() == ExclusiveExecutorTask.class)
        {
            ((ExclusiveExecutorTask)this).queue.runTask(self);
        }
        else
        {
            onRunning();
            self.setAccordActiveTask(this);
            try (Closeable close = resources.get())
            {
                if (runMayThrow())
                    onSuccess();
                else
                    Invariants.require(compareTo(RUNNING) > 0);
            }
            catch (Throwable t)
            {
                setRunState(RunState.RUN_FAILED);
                reportFailureNoExcept(t);
            }
            finally
            {
                if (DEBUG_EXECUTION) ((DebugTask) resources).onRunComplete();
                self.setAccordActiveTask(null);
            }
        }
    }

    final void rejectAtRuntime(Throwable reject)
    {
        setRunState(RunState.RUN_FAILED);
        reportFailureNoExcept(reject);
    }

    final void completeExclusiveNoExcept()
    {
        if (this instanceof ExclusiveExecutorTask)
        {
            completeExclusiveMayThrow();
        }
        else
        {
            try
            {
                // we submit before completing to ensure that consequences are setup correctly in ExclusiveExecutor before we poll the next task
                submitConsequencesExclusive(prepareConsequencesExclusive());
                if (DEBUG_EXECUTION) DebugTask.get(this).onComplete();
                completeExclusiveMayThrow();
            }
            catch (Throwable t)
            {
                releaseResourcesExclusiveNoExcept();
                unhandledException(t);
                if (compareTo(EXECUTED) < 0)
                    failExclusive(t, State.FAILED);
            }
            finally
            {
                if (compareTo(EXECUTED) >= 0) // this is to handle INCR tasks - it is too implicit though, need to improve
                    executor().completedTaskExclusive(this);
            }
        }
    }

    final void unhandledException(Throwable t)
    {
        try { executor().agent.onException(t); }
        catch (Throwable t2) { }
    }

    final void reportFailureNoExcept(Throwable fail)
    {
        try { reportFailureMayThrow(fail); }
        catch (Throwable t)
        {
            try { fail.addSuppressed(t); }
            catch (Throwable t2) { }
            unhandledException(fail);
        }
    }

    // propagate RunState to State
    // true if task was executed successfully
    final boolean completeState()
    {
        RunState runState = runState();
        boolean success;
        switch (runState)
        {
            default: throw UnhandledEnum.unknown(runState);
            case RUN_INCOMPLETE: throw UnhandledEnum.invalid(runState);
            case NOT_YET_RUN:
                Invariants.expect(state().isDone());
                Invariants.expect(next == null);
                success = false;
                break;

            case RUN_FAILED:
                if (compareTo(EXECUTED) < 0)
                    setStateExclusive(State.FAILED);
                success = false;
                break;

            case RUNNING:
            case RUN_PERSISTING:
            case RUN_SUCCESS:
                setStateExclusive(EXECUTED);
                success = true;
                break;
        }
        return success;
    }

    final void tryFailAndCompleteUnexecutedExclusive(Throwable fail, State newState)
    {
        try
        {
            if (is(UNREGISTERED))
            {
                releaseResourcesExclusiveNoExcept();
                failExclusive(fail, CANCELLED_UNREGISTERED);
            }
            else if (compareTo(REGISTERED) >= 0 && compareTo(WAITING_TO_RUN) <= 0)
            {
                failAndCompleteExclusive(fail, newState);
            }
        }
        catch (Throwable t)
        {
            try { t.addSuppressed(fail); }
            catch (Throwable t2) { /* unsafe to do anything */ }
            unhandledException(t);
        }
    }

    /**
     * Fail a task whose run is in flight, so cannot be completed from here: it will see that it is no longer PREPARED as
     * it completes, and abandon and release everything then. Only legal for a run that cannot be its task's last, as we
     * report the failure now and a final run would then complete the callback twice.
     */
    final void failWhileRunningExclusive(Throwable fail)
    {
        Invariants.require(is(PREPARED) || is(State.INCOMPLETE));
        setStateExclusive(RUNNING_WHILE_FAILED);
        reportFailureNoExcept(fail);
    }

    void releaseResourcesExclusiveNoExcept() {}

    final void failExclusive(Throwable fail, State newState)
    {
        unqueueIfQueued();
        setStateExclusive(newState);
        reportFailureNoExcept(fail);
    }

    final void failAndCompleteExclusive(Throwable fail, State newState)
    {
        failExclusive(fail, newState);
        completeExclusiveNoExcept();
    }

    final void unqueue(TaskQueue expected)
    {
        Invariants.require(queuedOrdinal() == expected.kind.ordinal());
        expected.unqueue(this);
        info &= ~EXECUTOR_QUEUE_SHIFTED_MASK;
    }

    final void unsetQueue(ExecutorQueue expected)
    {
        Invariants.require(expected.ordinal() == queuedOrdinal());
        info &= ~EXECUTOR_QUEUE_SHIFTED_MASK;
    }

    final void onRunning()
    {
        Invariants.require(is(PREPARED));
        Invariants.require(is(NOT_YET_RUN) || is(RUN_INCOMPLETE));
        runningAt = Math.max(createdAt, nanoTime());
        setRunState(RunState.RUNNING);
        if (DEBUG_EXECUTION) ((DebugTask) resources).onRunning();
    }

    final void onSuccess()
    {
        setRunState(RunState.RUN_SUCCESS);
    }

    /** whether we have failed or been cancelled, so will never run again and must not keep a queue position */
    boolean isTerminalFailure()
    {
        return isState(State.TERMINAL_FAILURE);
    }

    /**
     * whether we have already been failed, so must not be failed again: {@link State#TERMINAL_FAILURE}, or
     * {@code RUNNING_WHILE_FAILED} - which keeps its positions until its run completes, so is still notified
     */
    final boolean hasAlreadyFailed()
    {
        return isState(State.FAILURE);
    }

    boolean hasStartedRunning()
    {
        return isState(State.HAS_RUN);
    }

    final State state()
    {
        return State.forOrdinal(stateOrdinal());
    }

    final RunState runState()
    {
        return RunState.forOrdinal(runState);
    }

    final Enum<?> currentState()
    {
        State state = state();
        if (state == State.PREPARED || state == EXECUTED)
        {
            RunState runState = runState();
            if (runState == NOT_YET_RUN)
                return state;
            return runState;
        }
        return State.forOrdinal(stateOrdinal());
    }

    final int stateOrdinal()
    {
        return info & STATE_MASK;
    }

    final boolean is(State state)
    {
        return stateOrdinal() == state.ordinal();
    }

    final int compareTo(State state)
    {
        return stateOrdinal() - state.ordinal();
    }

    final int compareTo(RunState state)
    {
        return runState - state.ordinal();
    }

    final boolean is(RunState state)
    {
        return runState == state.ordinal();
    }

    final boolean isEither(RunState state1, RunState state2)
    {
        int runState = this.runState;
        return runState == state1.ordinal() || runState == state2.ordinal();
    }

    final boolean isState(int stateBitSet)
    {
        return TinyEnumSet.contains(stateBitSet, stateOrdinal());
    }

    final boolean is(GlobalGroup group)
    {
        return globalGroupOrdinal() == group.ordinal();
    }

    final boolean is(ExclusiveGroup group)
    {
        return exclusiveGroupOrdinal() == group.ordinal();
    }

    final void override(GlobalGroup group)
    {
        info = (info & ~(GROUP_MASK << GLOBAL_GROUP_SHIFT)) | (group.ordinal() << GLOBAL_GROUP_SHIFT);
    }

    final void setStateExclusive(State state)
    {
        Invariants.require(state.isPermittedFrom(stateOrdinal()), "%s forbidden from %s", state, this, Task::reportBadStateTransition);
        unsafeSetStateExclusive(state);
    }

    final void setRunState(RunState state)
    {
        Invariants.require(isState(RUNNING_OR_EXECUTED) || (state == RUN_FAILED && is(FAILED)));
        setRunState(state.ordinal());
    }

    final void setRunState(int newRunState)
    {
        runStateUpdater.lazySet(this, newRunState);
    }

    private static String reportBadStateTransition(Task task)
    {
        return task.state() + " for " + task.description();
    }

    final void unsafeSetStateExclusive(State state)
    {
        info = (info & ~STATE_MASK) | state.ordinal();
    }

    final int globalGroupOrdinal()
    {
        return (info >>> GLOBAL_GROUP_SHIFT) & GROUP_MASK;
    }

    final int exclusiveGroupOrdinal()
    {
        return (info >>> EXCLUSIVE_GROUP_SHIFT) & GROUP_MASK;
    }

    private boolean isCompatible(ExecutorQueue queue)
    {
        int self = stateOrdinal();
        return TinyEnumSet.contains(queue.permittedStates, self);
    }

    final boolean isSync()
    {
        return 0 == (info & NONSYNC_BIT);
    }

    final boolean isNonSync()
    {
        return !isSync();
    }

    final void setNonSyncExclusive()
    {
        info |= NONSYNC_BIT;
    }

    final boolean isIncremental()
    {
        return 0 != (info & INCREMENTAL_MASK);
    }

    final void setIncrementalExclusive()
    {
        info |= INCREMENTAL | NONSYNC_BIT;
    }

    final boolean hasIncrementalStarted()
    {
        return (info & INCREMENTAL_MASK) >= INCREMENTAL_STARTED;
    }

    final void setIncrementalStartedExclusive()
    {
        Invariants.require(isIncremental());
        if (!isIncrementalFinishing())
            info = (info & ~INCREMENTAL_MASK) | INCREMENTAL_STARTED;
    }

    final boolean isIncrementalFinishing()
    {
        return (info & INCREMENTAL_MASK) >= INCREMENTAL_FINISHING;
    }

    final void setIncrementalFinishingExclusive()
    {
        Invariants.require(isIncremental());
        info |= INCREMENTAL_FINISHING;
    }

    final void setSequencedExclusive(ExecutionContext.ExecutionSequence sequence)
    {
        Invariants.require(isUnsequenced());
        info |= sequence.ordinal() << SEQUENCED_SHIFT;
    }

    final boolean isUnsequenced()
    {
        return (info & SEQUENCED_MASK) == 0;
    }

    final boolean isSequencedByPriority()
    {
        return (info & SEQUENCED_MASK) == SEQUENCED_PRIORITY;
    }

    /** the sequence we were set up with, ignoring the fifo-claim upgrade that shares the same field */
    final boolean isSequencedBy(ExecutionContext.ExecutionSequence sequence)
    {
        int sequenced = info & SEQUENCED_MASK;
        if (sequenced == SEQUENCED_ATOMIC_AND_QUEUED)
            sequenced = SEQUENCED_ATOMIC;
        return sequenced == sequence.ordinal() << SEQUENCED_SHIFT;
    }

    final boolean isSequencedByPriorityAtomic()
    {
        return (info & SEQUENCED_MASK) >= SEQUENCED_ATOMIC;
    }

    final boolean isCacheQueuedFifo()
    {
        return (info & SEQUENCED_MASK) == SEQUENCED_ATOMIC_AND_QUEUED;
    }

    final boolean isCacheQueued()
    {
        return 0 != (info & VALIDATE_CACHE_QUEUED_BIT);
    }

    final boolean isQueued()
    {
        return 0 != (info & EXECUTOR_QUEUE_SHIFTED_MASK);
    }

    final int queuedOrdinal()
    {
        return (info >>> EXECUTOR_QUEUE_SHIFT) & EXECUTOR_QUEUE_UNSHIFTED_MASK;
    }

    final ExecutorQueue queued()
    {
        return ExecutorQueue.forOrdinal(queuedOrdinal());
    }

    final void setQueue(ExecutorQueue queue)
    {
        Invariants.require(isCompatible(queue));
        Invariants.require(!isQueued());
        info |= queue.ordinal() << EXECUTOR_QUEUE_SHIFT;
    }

    // supersedes priority, in whichever order they're called
    final void setCacheQueuedFifoExclusive()
    {
        // An incremental task over txnId holds its txnId locks across runs and is upgraded to atomic when it starts,
        // so this is reached by a task that was BY_PRIORITY until now: from that point it holds a fifo position on
        // every entry it claims and keeps it across its runs, putting it ahead of every sorted or bagged claim. An
        // incremental task that holds no lock between runs is not upgraded, and so never reaches this.
        Invariants.require(isSequencedByPriorityAtomic() || isIncremental());
        info |= SEQUENCED_ATOMIC_AND_QUEUED | VALIDATE_CACHE_QUEUED_BIT;
    }

    final void setCacheQueuedExclusive()
    {
        info |= VALIDATE_CACHE_QUEUED_BIT;
    }

    final int tranche()
    {
        Invariants.require((info & HAS_SETUP_MASK) != 0);
        return info >>> TRANCHE_SHIFT;
    }

    final void setTranche(int tranche)
    {
        Invariants.require(tranche <= MAX_TRANCHE);
        Invariants.require((info & HAS_SETUP_MASK) == 0);
        info = info | (tranche << TRANCHE_SHIFT) | HAS_TRANCHE;
    }

    final Task inherit(Task parent)
    {
        position = parent.position;
        setInheritedWithTranche(parent.tranche());
        return this;
    }

    static
    {
        /** see {@link #setInheritedWithTranche}, where we permit setting inherited flag after inherited range scan */
        Invariants.require((HAS_INHERITED_TRANCHE | HAS_INHERITED_RANGE_SCAN) == HAS_INHERITED_RANGE_SCAN);
    }

    final void setInheritedWithTranche(int tranche)
    {
        Invariants.require(tranche <= MAX_TRANCHE);
        Invariants.require(!hasInherited() || hasInheritedRangeScan()); // we allow setting inherit range scan in advance because it composes cleanly
        info = info | (tranche << TRANCHE_SHIFT) | HAS_INHERITED_TRANCHE;
    }

    final boolean isContinuation()
    {
        return 0 != (info & IS_CONTINUATION);
    }

    final void setIsContinuation()
    {
        info |= IS_CONTINUATION;
    }

    final boolean hasPreSetup()
    {
        return 0 != (info & HAS_PRESETUP_BIT);
    }

    final void setHasPreSetupExclusive()
    {
        Invariants.require(!hasPreSetup());
        info |= HAS_PRESETUP_BIT;
    }

    final boolean hasInherited()
    {
        return (info & HAS_SETUP_MASK) >= HAS_INHERITED_TRANCHE;
    }

    final void setInheritedRangeScan()
    {
        info |= HAS_INHERITED_RANGE_SCAN;
    }

    final boolean hasInheritedRangeScan()
    {
        return (info & HAS_SETUP_MASK) == HAS_INHERITED_RANGE_SCAN;
    }

    void addConsequence(Task task)
    {
        Invariants.require(!(this instanceof ExclusiveExecutorTask));
        Task prev = next;
        Invariants.require(prev == null || prev.is(UNREGISTERED));
        task.inherit(this);
        task.next = prev;
        next = task;
    }

    Task prepareConsequencesExclusive()
    {
        // keep only those still awaiting submission, and terminate the chain at whatever ends up last
        Invariants.require(!(this instanceof ExclusiveExecutorTask));
        Task head = next;
        if (head == null)
            return null;

        next = null;
        prepareConsequences(this, head);
        return reverse(head);
    }

    static void submitConsequencesExclusive(Task cur)
    {
        while (cur != null)
        {
            Task next = cur.next;
            cur.next = null;
            try { cur.submitExclusiveNoExcept(); }
            catch (Throwable t) { cur.unhandledException(t); }
            cur = next;
        }
    }

    static void prepareConsequences(Task parent, Task consqeuences)
    {
        if (parent.is(RUN_FAILED))
            cancelSafeTasksAndContinuations(parent instanceof SafeTask<?>, consqeuences);
    }

    static void cancelSafeTasksAndContinuations(boolean isSafeParent, Task cur)
    {
        while (cur != null)
        {
            if (Invariants.expect(cur.is(UNREGISTERED)) && ((isSafeParent && cur instanceof SafeTask<?>) || cur.isContinuation()))
            {
                cur.setStateExclusive(CANCELLED_UNREGISTERED);
                cur.reportFailureNoExcept(new CancellationException("Parent task failed"));
            }
            cur = cur.next;
        }
    }

    void prepareRunAndCompleteExclusive(TaskRunner self)
    {
        if (prepareExclusiveNoExcept())
        {
            try { runNoExcept(self); }
            finally { completeExclusiveNoExcept(); }
        }
    }

    static int init(GlobalGroup global, ExclusiveGroup exclusive)
    {
        return (global.ordinal() << GLOBAL_GROUP_SHIFT) | (exclusive.ordinal() << EXCLUSIVE_GROUP_SHIFT);
    }

    static Task reverse(Task unqueued)
    {
        Task prev = null;
        Task cur = unqueued;
        while (cur != null)
        {
            Task next = cur.next;
            cur.next = prev;
            prev = cur;
            cur = next;
        }
        return prev;
    }
}
