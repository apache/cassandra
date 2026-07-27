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

import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

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
import accord.utils.async.Cancellable;

import org.apache.cassandra.concurrent.DebuggableTask;
import org.apache.cassandra.concurrent.ExecutorLocals;
import org.apache.cassandra.service.accord.debug.DebugExecution;
import org.apache.cassandra.utils.WithResources;

import static accord.local.ExecutionContext.ExecutionSequence.BY_PRIORITY;
import static accord.local.ExecutionContext.ExecutionSequence.BY_PRIORITY_ATOMIC;
import static accord.primitives.Routable.Domain.Range;
import static org.apache.cassandra.config.AccordConfig.QueuePriorityModel.ORIG_HLC_FIFO;
import static org.apache.cassandra.service.accord.debug.DebugExecution.DEBUG_EXECUTION;
import static org.apache.cassandra.service.accord.execution.Task.ExclusiveGroup.APPLY;
import static org.apache.cassandra.service.accord.execution.Task.ExclusiveGroup.COMMIT;
import static org.apache.cassandra.service.accord.execution.Task.ExclusiveGroup.STABLE;
import static org.apache.cassandra.service.accord.execution.Task.State.EXECUTED;
import static org.apache.cassandra.service.accord.execution.Task.State.RUNNING;
import static org.apache.cassandra.service.accord.execution.Task.State.RUNNING_OR_EXECUTED;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public abstract class Task extends IntrusiveHeapNode implements Cancellable
{
    private static final int WAITING_ON_OPTIONAL_BIT = 1 << 5;
    private static final int WAITING_TO_RUN_BIT = 1 << 6;
    private static final int INCOMPLETE_BIT = 1 << 9;

    enum State
    {
        UNINITIALIZED(),
        SCANNING_RANGES(UNINITIALIZED),
        LOADING_REQUIRED(UNINITIALIZED, SCANNING_RANGES),
        LOADING_OPTIONAL(UNINITIALIZED, SCANNING_RANGES, LOADING_REQUIRED),
        WAITING_ON_REQUIRED(WAITING_ON_OPTIONAL_BIT | WAITING_TO_RUN_BIT, UNINITIALIZED, SCANNING_RANGES, LOADING_REQUIRED, LOADING_OPTIONAL),
        WAITING_ON_OPTIONAL(WAITING_TO_RUN_BIT | INCOMPLETE_BIT, UNINITIALIZED, SCANNING_RANGES, LOADING_REQUIRED, LOADING_OPTIONAL, WAITING_ON_REQUIRED),
        WAITING_TO_RUN(INCOMPLETE_BIT, UNINITIALIZED, SCANNING_RANGES, LOADING_REQUIRED, LOADING_OPTIONAL, WAITING_ON_REQUIRED, WAITING_ON_OPTIONAL),
        RUNNING(WAITING_TO_RUN),
        EXECUTED(RUNNING),
        INCOMPLETE(RUNNING),
        FAILED_TO_LOAD(SCANNING_RANGES, LOADING_REQUIRED, LOADING_OPTIONAL),
        FAILED_OTHER(SCANNING_RANGES, LOADING_REQUIRED, LOADING_OPTIONAL, WAITING_ON_REQUIRED, WAITING_ON_OPTIONAL, WAITING_TO_RUN),
        CANCELLED(SCANNING_RANGES, LOADING_REQUIRED, LOADING_OPTIONAL, WAITING_ON_REQUIRED, WAITING_ON_OPTIONAL, WAITING_TO_RUN, RUNNING),
        ;

        private final int permittedFrom;
        public static final int WAITING = TinyEnumSet.encode(WAITING_ON_REQUIRED, WAITING_ON_OPTIONAL, WAITING_TO_RUN);
        public static final int WAITING_OR_RUNNING = WAITING | TinyEnumSet.encode(RUNNING);
        public static final int RUNNING_OR_EXECUTED = WAITING | TinyEnumSet.encode(RUNNING, EXECUTED);
        static final State[] VALUES = values();

        static
        {
            // hack to allow us to create loops in our enum transition declarations
            Invariants.require(INCOMPLETE_BIT == 1 << INCOMPLETE.ordinal());
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

        boolean isExecuted()
        {
            return this.compareTo(EXECUTED) >= 0;
        }

        boolean hasStarted()
        {
            return this.compareTo(RUNNING) >= 0;
        }

        static State forOrdinal(int ordinal)
        {
            return VALUES[ordinal];
        }
    }

    enum RunState
    {
        NONE, PERSISTING, SUCCESS, FAILED;

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

    private static final int STATE_MASK = 0xf;
    static final int GROUP_MASK = 0x7;
    private static final int EXCLUSIVE_GROUP_SHIFT = 4;
    private static final int GLOBAL_GROUP_SHIFT = 7;

    private static final int NONSYNC_BIT = 1 << 10;
    private static final int CACHE_QUEUED_BIT = 1 << 11;

    private static final int INCREMENTAL_MASK = 0x3 << 12;
    private static final int INCREMENTAL = 0x1 << 12;
    private static final int INCREMENTAL_STARTED = 0x2 << 12;
    private static final int INCREMENTAL_FINISHING = 0x3 << 12;

    private static final int SEQUENCED_SHIFT = 14;
    private static final int SEQUENCED_MASK = 0x3 << SEQUENCED_SHIFT;
    private static final int SEQUENCED_PRIORITY = 0x1 << SEQUENCED_SHIFT;
    private static final int SEQUENCED_ATOMIC = 0x2 << SEQUENCED_SHIFT;
    private static final int SEQUENCED_ATOMIC_AND_QUEUED = 0x3 << SEQUENCED_SHIFT;

    // spare two bits

    private static final int HAS_TRANCHE_BIT = 1 << 18;
    private static final int HAS_INHERITED_BIT = 1 << 19;
    private static final int HAS_INHERITED_RANGE_SCAN_BIT = 1 << 20;

    private static final int TRANCHE_SHIFT = 22;
    static final int MAX_TRANCHE = 0x3ff;

    static
    {
        Invariants.require(SEQUENCED_PRIORITY == BY_PRIORITY.ordinal() << SEQUENCED_SHIFT);
        Invariants.require(SEQUENCED_ATOMIC == BY_PRIORITY_ATOMIC.ordinal() << SEQUENCED_SHIFT);
        Invariants.require(ExecutionContext.ExecutionSequence.values().length <= 3);
    }

    public final WithResources resources;
    Task next;

    long position;
    int info;

    // TODO (expected): do we need this? we should be able to determine the queue from state() if needed for e.g. cancellation
    private TaskQueue queued;

    public final long createdAt;
    // TODO (expected): expose via executors vtable
    // TODO (expected): use just one long and some flag bits to indicate which point it represents, and report incrementally
    public long loadedAt, runningAt, completeAt;
    private byte runState;

    Task(GlobalGroup group)
    {
        resources = DebugExecution.DebugTask.maybeDebug(ExecutorLocals.propagate(), this);
        info = init(group, ExclusiveGroup.OTHER);
        createdAt = nanoTime();
    }

    Task(ExclusiveGroup group)
    {
        resources = DebugExecution.DebugTask.maybeDebug(ExecutorLocals.propagate(), this);
        info = init(GlobalGroup.OTHER, group);
        createdAt = nanoTime();
    }

    Task(GlobalGroup group, long position, int tranche)
    {
        this(group);
        this.position = position;
        setInheritedWithTranche(tranche);
    }

    Task(ExclusiveGroup group, long position, int tranche)
    {
        this(group);
        this.position = position;
        setInheritedWithTranche(tranche);
    }

    protected Task(ExecutionContext context, AtomicLong lastCreatedAt)
    {
        resources = DebugExecution.DebugTask.maybeDebug(ExecutorLocals.propagate(), this);
        createdAt = lastCreatedAt.accumulateAndGet(nanoTime(), (prev, next) -> next < prev ? prev + 1 : next);
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
        if (this instanceof ExclusiveExecutor.ExclusiveExecutorTask)
            return ((ExclusiveExecutor.ExclusiveExecutorTask) this).queue.task;
        return this;
    }

    static Task unwrap(Task task)
    {
        return task == null ? null : task.unwrap();
    }

    public DebuggableTask debuggable()
    {
        return null;
    }

    abstract String toDescription();

    abstract void submitExclusive();

    /**
     * Prepare to run while holding the state cache lock
     */
    void preRunExclusive()
    {
        setStateExclusive(RUNNING);
    }

    /**
     * Run the command; the state cache lock may or may not be held depending on the executor implementation
     */
    abstract void run();

    /**
     * Fail the command; the state cache lock may or may not be held depending on the executor implementation
     */
    abstract void reportFailure(Throwable fail);

    final void failExclusive(Throwable fail, State newState)
    {
        try
        {
            setStateExclusive(newState);
        }
        finally
        {
            reportFailure(fail);
        }
    }

    final void failExecution(Throwable fail)
    {
        Invariants.require(is(RUNNING));
        try
        {
            setRunState(RunState.FAILED);
        }
        finally
        {
            reportFailure(fail);
        }
    }

    abstract boolean isNewWork();

    /**
     * Cleanup the command while holding the state cache lock
     */
    void cleanupExclusive(AccordExecutor executor, boolean executed)
    {
        if (executed) setStateExclusive(EXECUTED);
        else Invariants.require(state().isExecuted());
        executor.unregisterExclusive(this);
        completeAt = nanoTime();
        if (runningAt != 0)
        {
            if (loadedAt == 0)
                loadedAt = runningAt;
            executor.elapsedWaitingToRun.increment(runningAt - loadedAt, runningAt);
            executor.elapsedPreparingToRun.increment(loadedAt - createdAt, runningAt);
            executor.elapsedRunning.increment(completeAt - runningAt, completeAt);
            executor.elapsed.increment(completeAt - createdAt, completeAt);
        }
        if (DEBUG_EXECUTION) DebugExecution.DebugTask.get(this).onCompleted(executor.debug);
    }

    void cancelExclusive()
    {
    }

    @Nullable
    final TaskQueue<?> queued()
    {
        return queued;
    }

    final void unqueueIfQueued()
    {
        if (queued != null)
        {
            queued.unqueue(this);
            queued = null;
        }
    }

    final void unqueue(TaskQueue expected)
    {
        Invariants.require(queued == expected, "%s != %s", queued, expected);
        queued.unqueue(this);
        queued = null;
    }

    final void unsetQueue(TaskQueue<?> expected)
    {
        Invariants.require(queued == expected, "%s != %s", queued, expected);
        queued = null;
    }

    final void setQueue(TaskQueue<?> queue)
    {
        Invariants.require(queued == null);
        Invariants.require(isCompatible(queue));
        queued = queue;
    }

    final void onRunning()
    {
        runningAt = nanoTime();
        if (DEBUG_EXECUTION) ((DebugExecution.DebugTask) resources).onRunning();
    }

    final void onRunComplete()
    {
        if (DEBUG_EXECUTION) ((DebugExecution.DebugTask) resources).onRunComplete();
    }

    final void onLoaded()
    {
        loadedAt = nanoTime();
    }

    final State state()
    {
        return State.forOrdinal(stateOrdinal());
    }

    final RunState runState()
    {
        return RunState.forOrdinal(runState);
    }

    final Enum<?> describeState()
    {
        State state = state();
        if (state == RUNNING || state == EXECUTED)
        {
            RunState runState = runState();
            if (runState == RunState.NONE)
                return state;
            return runState;
        }
        return State.forOrdinal(stateOrdinal());
    }

    private int stateOrdinal()
    {
        return info & STATE_MASK;
    }

    final boolean is(State state)
    {
        return stateOrdinal() == state.ordinal();
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

    final int compareTo(State state)
    {
        return stateOrdinal() - state.ordinal();
    }

    final void setStateExclusive(State state)
    {
        Invariants.require(state.isPermittedFrom(stateOrdinal()), "%s forbidden from %s", state, this, Task::reportBadStateTransition);
        unsafeSetStateExclusive(state);
    }

    final void setRunState(RunState state)
    {
        Invariants.require(isState(RUNNING_OR_EXECUTED));
        runState = (byte) state.ordinal();
    }

    private static String reportBadStateTransition(Task task)
    {
        return task.state() + " for " + task.toDescription();
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

    private boolean isCompatible(TaskQueue<?> queue)
    {
        int self = stateOrdinal();
        return TinyEnumSet.contains(queue.states, self);
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
