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
import static accord.local.ExecutionContext.ExecutionSequence.BY_PRIORITY_ATOMIC;
import static accord.primitives.Routable.Domain.Range;
import static org.apache.cassandra.config.AccordConfig.QueuePriorityModel.ORIG_HLC_FIFO;
import static org.apache.cassandra.service.accord.debug.DebugExecution.DEBUG_EXECUTION;
import static org.apache.cassandra.service.accord.execution.Task.ExclusiveGroup.APPLY;
import static org.apache.cassandra.service.accord.execution.Task.ExclusiveGroup.COMMIT;
import static org.apache.cassandra.service.accord.execution.Task.ExclusiveGroup.STABLE;
import static org.apache.cassandra.service.accord.execution.Task.RunState.NOT_YET_RUN;
import static org.apache.cassandra.service.accord.execution.Task.RunState.RUN_FAILED;
import static org.apache.cassandra.service.accord.execution.Task.RunState.RUN_INCOMPLETE;
import static org.apache.cassandra.service.accord.execution.Task.RunState.RUNNING;
import static org.apache.cassandra.service.accord.execution.Task.State.CANCELLED;
import static org.apache.cassandra.service.accord.execution.Task.State.CANCELLED_UNREGISTERED;
import static org.apache.cassandra.service.accord.execution.Task.State.EXECUTED;
import static org.apache.cassandra.service.accord.execution.Task.State.FAILED;
import static org.apache.cassandra.service.accord.execution.Task.State.LOADING_OPTIONAL;
import static org.apache.cassandra.service.accord.execution.Task.State.LOADING_REQUIRED;
import static org.apache.cassandra.service.accord.execution.Task.State.PREPARED;
import static org.apache.cassandra.service.accord.execution.Task.State.PREPARED_OR_EXECUTED;
import static org.apache.cassandra.service.accord.execution.Task.State.SCANNING_RANGES;
import static org.apache.cassandra.service.accord.execution.Task.State.UNREGISTERED;
import static org.apache.cassandra.service.accord.execution.Task.State.WAITING_ON_OPTIONAL;
import static org.apache.cassandra.service.accord.execution.Task.State.WAITING_ON_REQUIRED;
import static org.apache.cassandra.service.accord.execution.Task.State.WAITING_TO_RUN;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public abstract class Task extends IntrusiveHeapNode implements Cancellable, DebuggableTask
{
    private static final int WAITING_ON_OPTIONAL_BIT = 1 << 7;
    private static final int WAITING_TO_RUN_BIT = 1 << 8;
    private static final int INCOMPLETE_BIT = 1 << 10;

    enum State
    {
        UNREGISTERED(),
        CANCELLED_UNREGISTERED(UNREGISTERED),
        REGISTERED(UNREGISTERED),
        SCANNING_RANGES(REGISTERED),
        LOADING_REQUIRED(REGISTERED, SCANNING_RANGES),
        LOADING_OPTIONAL(REGISTERED, SCANNING_RANGES, LOADING_REQUIRED),
        WAITING_ON_REQUIRED(WAITING_ON_OPTIONAL_BIT | WAITING_TO_RUN_BIT, REGISTERED, SCANNING_RANGES, LOADING_REQUIRED, LOADING_OPTIONAL),
        WAITING_ON_OPTIONAL(WAITING_TO_RUN_BIT | INCOMPLETE_BIT, REGISTERED, SCANNING_RANGES, LOADING_REQUIRED, LOADING_OPTIONAL, WAITING_ON_REQUIRED),
        WAITING_TO_RUN(INCOMPLETE_BIT, UNREGISTERED, REGISTERED, SCANNING_RANGES, LOADING_REQUIRED, LOADING_OPTIONAL, WAITING_ON_REQUIRED, WAITING_ON_OPTIONAL),
        PREPARED(WAITING_TO_RUN),
        INCOMPLETE(PREPARED),
        EXECUTED(PREPARED),
        FAILED(UNREGISTERED, REGISTERED, SCANNING_RANGES, LOADING_REQUIRED, LOADING_OPTIONAL, WAITING_ON_REQUIRED, WAITING_ON_OPTIONAL, WAITING_TO_RUN, PREPARED, INCOMPLETE),
        CANCELLED(UNREGISTERED, REGISTERED, SCANNING_RANGES, LOADING_REQUIRED, LOADING_OPTIONAL, WAITING_ON_REQUIRED, WAITING_ON_OPTIONAL, WAITING_TO_RUN),
        ;

        private final int permittedFrom;
        public static final int WAITING = TinyEnumSet.encode(WAITING_ON_REQUIRED, WAITING_ON_OPTIONAL, WAITING_TO_RUN);
        public static final int WAITING_OR_PREPARED = WAITING | TinyEnumSet.encode(PREPARED);
        public static final int PREPARED_OR_EXECUTED = TinyEnumSet.encode(PREPARED, EXECUTED);
        static final State[] VALUES = values();

        static
        {
            // hack to allow us to create loops in our enum transition declarations
            Invariants.require(INCOMPLETE_BIT == 1 << INCOMPLETE.ordinal());
            Invariants.require(WAITING_TO_RUN_BIT == 1 << WAITING_TO_RUN.ordinal());
            Invariants.require(WAITING_ON_OPTIONAL_BIT == 1 << WAITING_ON_OPTIONAL.ordinal());
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
            return this.compareTo(PREPARED) >= 0;
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
        WAITING(WAITING_ON_OPTIONAL, WAITING_ON_REQUIRED),
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
    private static final int CACHE_QUEUED_BIT = 1 << 11;

    private static final int HAS_TRANCHE_BIT = 1 << 12;
    private static final int HAS_INHERITED_BIT = 1 << 13;
    private static final int HAS_INHERITED_RANGE_SCAN_BIT = 1 << 14;

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
        Invariants.require(SEQUENCED_ATOMIC == BY_PRIORITY_ATOMIC.ordinal() << SEQUENCED_SHIFT);
        Invariants.require(ExecutionContext.ExecutionSequence.values().length <= 3);
    }

    // TODO (desired): quite heavy to pass-through tracing session state we mostly don't use
    public final WithResources resources;
    Task next;
    Task consequences;

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
    abstract void maybeCompleteExclusiveMayThrow();
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
                tryFailAndCompleteExclusive(t, State.FAILED);
                onException(t);
            }
        }
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
        try
        {
            if (DEBUG_EXECUTION) DebugTask.get(this).onComplete();
            maybeCompleteExclusiveMayThrow();
        }
        catch (Throwable t)
        {
            onException(t);
            if (compareTo(EXECUTED) < 0)
                failExclusive(t, State.FAILED);
        }
        finally
        {
            if (compareTo(EXECUTED) >= 0)
            {
                try { submitConsequencesExclusive(is(EXECUTED)); }
                catch (Throwable t) { onException(t); }
                executor().completedTaskExclusive(this);
            }
        }
    }

    final void onException(Throwable t)
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
            onException(fail);
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
                Invariants.expect(consequences == null);
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

    final void tryFailAndCompleteExclusive(Throwable fail, State newState)
    {
        if (is(UNREGISTERED))
            failExclusive(fail, CANCELLED_UNREGISTERED);
        else if (compareTo(WAITING_TO_RUN) <= 0)
            failAndCompleteExclusive(fail, newState);
    }

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
        if (DEBUG_EXECUTION) ((DebugTask) resources).onRunComplete();
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
        Invariants.require(isState(PREPARED_OR_EXECUTED) || (state == RUN_FAILED && is(FAILED)));
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
        return 0 != (info & CACHE_QUEUED_BIT);
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
        Invariants.require(isSequencedByPriorityAtomic());
        info |= SEQUENCED_ATOMIC_AND_QUEUED | CACHE_QUEUED_BIT;
    }

    final void setCacheQueuedExclusive()
    {
        info |= CACHE_QUEUED_BIT;
    }

    final int tranche()
    {
        Invariants.require((info & HAS_TRANCHE_BIT) != 0);
        return info >>> TRANCHE_SHIFT;
    }

    final void setTranche(int tranche)
    {
        Invariants.require(tranche <= MAX_TRANCHE);
        info = info | (tranche << TRANCHE_SHIFT) | HAS_TRANCHE_BIT;
    }

    final Task inherit(Task parent)
    {
        Invariants.require(!hasInherited());
        position = parent.position;
        setInheritedWithTranche(parent.tranche());
        return this;
    }

    final void setInheritedWithTranche(int tranche)
    {
        Invariants.require(tranche <= MAX_TRANCHE);
        info = info | (tranche << TRANCHE_SHIFT) | HAS_TRANCHE_BIT | HAS_INHERITED_BIT;
    }

    final boolean hasInherited()
    {
        return (info & HAS_INHERITED_BIT) != 0;
    }

    final void setInheritedRangeScan()
    {
        info = info | HAS_INHERITED_RANGE_SCAN_BIT;
    }

    final boolean hasInheritedRangeScan()
    {
        return (info & HAS_INHERITED_RANGE_SCAN_BIT) != 0;
    }

    void addConsequence(Task task)
    {
        Task prev = consequences;
        Invariants.require(prev == null || prev.is(UNREGISTERED));
        task.next = prev;
        consequences = task;
    }

    final void submitConsequencesExclusive(boolean success)
    {
        if (consequences == null)
            return;

        Task cur = Task.reverse(consequences);
        consequences = null;

        while (cur != null)
        {
            Task next = cur.next;
            cur.next = null;
            if (cur.is(UNREGISTERED))
            {
                if (success || !(cur instanceof SafeTask<?> || this instanceof SafeTask<?>))
                {
                    cur.inherit(this);
                    cur.submitExclusiveNoExcept();
                }
                else
                {
                    cur.failExclusive(new CancellationException("Parent task failed"), CANCELLED);
                }
            }
            cur = next;
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
