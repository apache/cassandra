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

import accord.utils.Invariants;
import accord.utils.UnhandledEnum;

import org.apache.cassandra.service.accord.execution.Task.ExecutorQueue;
import org.apache.cassandra.service.accord.execution.Task.GroupKind;

import static org.apache.cassandra.service.accord.execution.Task.ExecutorQueue.RUNNABLE;

/**
 * A {@link TaskQueue} sub-divided into up to eight per-group sub-queues (groups are phases for the exclusive
 * executor, or work classes globally) plus a policy for choosing which sub-queue to serve next. The policy balances
 * three competing concerns: ordering/priority (run the oldest / highest-priority work), fairness (do not let one
 * group monopolise the executor), and throughput (do not let an even split leave a busy group's backlog growing
 * without bound).
 *
 * <h2>Packed counters</h2>
 * Per-group state is held in {@code long}s, one 7-bit lane per group; bit 7 of each byte is a spare "guard" bit used
 * to run branch-free min/compare across all eight lanes at once (see {@code minCounters}). The lanes are:
 * <ul>
 *   <li>{@code positions[]} - the head task's position (HLC, or submission order) per group; drives FIFO/priority.</li>
 *   <li>{@code recent} - a windowed count of recently <i>served</i> tasks per group (service received).</li>
 *   <li>{@code arrivals} - a windowed, size-biased count of recent <i>arrivals</i> per group (demand offered).</li>
 *   <li>{@code current} - tasks currently in flight per group, used to enforce {@code queue_active_limits}.</li>
 *   <li>{@code hasWork}/{@code dirty} - guard-bit masks: which groups have queued work / need a position refresh.</li>
 * </ul>
 *
 * <h2>Windowing: a bounded memory of the past</h2>
 * {@code recent} and {@code arrivals} are not running totals. Whenever incrementing {@code recent} tips any lane to
 * its maximum (0x80), <em>both</em> {@code recent} and {@code arrivals} are halved (see {@code incrementRecents}),
 * and each lane also saturates at 0x7f. They are therefore an exponentially-decaying window keyed on service/time.
 * This is what stops a one-off burst of arrivals granting a group a permanent boost: the burst saturates the lane
 * briefly and then decays away as other work is served.
 *
 * <h2>The fair selection counter</h2>
 * The fair models pick the group with the smallest {@code effective} counter, where per lane
 * <pre>effective = min(0x7f, max(0, recent - arrivals) + bias)</pre>
 * <ul>
 *   <li>{@code max(0, recent - arrivals)} - a group whose arrivals have outpaced its recent service clamps to 0 and
 *       is therefore preferred; in steady state each group's service tracks its arrival rate, which bounds backlog.
 *       The clamp is symmetric, so a group cannot bank "credit" during a lull and later use it to monopolise service.</li>
 * </ul>
 *
 * <h2>Choosing a group ({@code minGroup} / {@code pollByBlend})</h2>
 * <ul>
 *   <li>{@code PRIORITY_ONLY} - always the lowest {@code positions} (oldest / highest priority), ignoring fairness.</li>
 *   <li>{@code PHASE_OVERRIDE} - strict phase order: the lowest-index group with work, always.</li>
 *   <li>{@code PHASE_FAIR} - pure fairness: the smallest {@code effective}; ties broken by index, within a group by
 *       position. It has no priority fallback, so it is deliberately completing-first under contention.</li>
 *   <li>{@code PRIORITY_FAIR} (default) - a deficit round-robin blend of two strategies, chosen per poll: <b>flow</b>
 *       (smallest {@code effective}, i.e. least fairly serviced) and <b>age</b> (oldest {@code positions}, i.e. FIFO).
 *       The flow weight ramps up with the flow imbalance ({@code max-min} of {@code effective}) over
 *       {@code queue_flow_imbalance_onset..+width}, trading against age zero-sum; when balanced it is pure age. The
 *       ramp is smooth (not a hard mode switch), so there is no boundary to oscillate around and no hysteresis is
 *       needed. Age also drains stale/standing backlogs for free: a backlog's items are the oldest work, so age
 *       clears them without a separate backlog-aware strategy (the {@code effective} flow counter tracks arrival
 *       <em>rate</em> and is deliberately blind to standing <em>stock</em>).</li>
 * </ul>
 *
 * <h2>Concurrency limits and eligibility</h2>
 * A group whose in-flight {@code current} count has reached its {@code queue_active_limits} limit is
 * {@code saturated} and excluded from selection ({@code disabled}), as is any group with no queued work.
 *
 * <h2>Note on {@code positions} freshness</h2>
 * {@code positions} is refreshed lazily from the sub-queue heads via the {@code dirty} mask; {@code dirty} must use
 * the same guard-bit layout as {@code hasWork} so the refresh loop in {@code minGroupByPriority} actually runs -
 * otherwise it silently degenerates to lowest-index-with-work and starves high-index / new-work groups.
 *
 * <p>NB it extends {@link TaskQueue} to keep the type hierarchy simple for method dispatch, and for efficiency for
 * anonymous ExclusiveExecutors which do not use multiple queues, while letting ExclusiveExecutor share a parent
 * class for both use cases.
 */
abstract class TaskQueueMulti<T extends Task> extends TaskQueue<T>
{
    private static final TaskQueue[] NO_QUEUES = new TaskQueue[0];
    private static final long[] NO_POSITIONS = new long[0];
    static final long COUNTER_OVERFLOWS = 0x8080808080808080L;
    static final long COUNTER_MASKS = 0x7f7f7f7f7f7f7f7fL;
    static final long COUNTER_LOWBITS = 0x0101010101010101L;

    final TaskQueue<T>[] queues;
    final long[] positions;
    final byte groupShift;
    final long limits;

    /**
     * sets overflow bits for a queue that has been stopped
     */
    long stopped;
    /**
     * sets overflow bits for each counter when it needs its position updated
     */
    long dirty;
    /**
     * sets overflow bits for each counter when there's associated work
     */
    long hasWork;
    /**
     * Stores recent dequeue counts for up to 8 sub queues.
     */
    long dispatches;
    // TODO (required): increment arrivals based on internal queue for ExclusiveExecutors
    //    also: experiment with decaying on arrival schedule rather than poll schedule, since this should respond to work growth more accurately
    /**
     * Stores recent enqueue counts for up to 8 sub queues.
     */
    long arrivals;
    /**
     * Stores currently-active counts for up to 8 sub queues. We can use this to impose limits on specific queues.
     */
    long active;
    /**
     * deficit-round-robin credits for the two PRIORITY_FAIR strategies (flow/age).
     */
    int creditFlow, creditAge;

    int waitingCount;

    TaskQueueMulti(ExecutorQueue kind, GroupKind groups, long limits)
    {
        super(kind);
        this.limits = limits;
        int queueCount = groups.count;
        Invariants.require(queueCount <= 8);
        queues = queueCount == 0 ? NO_QUEUES : new TaskQueue[queueCount];
        positions = queueCount > 0 && AccordExecutor.BALANCE_BY_POSITION ? new long[queueCount] : NO_POSITIONS;
        groupShift = groups.shift;
    }

    final int group(Task task)
    {
        if (groupShift == 0)
            return -1;

        return (task.info >>> groupShift) & Task.GROUP_MASK;
    }

    void stop(long groupOverflowBits)
    {
        stopped |= groupOverflowBits;
    }

    void restart(long groupOverflowBits)
    {
        stopped &= ~groupOverflowBits;
    }

    final TaskQueue<T> queue(Task task)
    {
        int group = group(task);
        if (group < 0)
            return this;

        return queue(group);
    }

    final TaskQueue<T> queue(int group)
    {
        TaskQueue<T> queue = queues[group];
        if (queue == null)
            queues[group] = queue = new TaskQueue<>(RUNNABLE);

        return queue;
    }

    private int pollGroup()
    {
        if (hasWork == 0)
            return -1;

        switch (AccordExecutor.BALANCING_MODEL)
        {
            default:
                throw new UnhandledEnum(AccordExecutor.PRIORITY_MODEL);
            case PRIORITY_ONLY:
                return pollGroupByPriority();
            case PHASE_ONLY:
                return pollGroupByIndex();
            case PHASE_FAIR:
                return pollGroupByPhaseFair();
            case BLENDED_PRIORITY_PHASE_FAIR:
                return pollGroupByBlended();
        }
    }

    private int pollGroupByPriority()
    {
        return pollGroupByPriority(unsaturatedWithWork());
    }

    private int pollGroupByPriority(long enabled)
    {
        long refresh = dirty & hasWork;
        while (refresh != 0)
        {
            int bitIndex = Long.numberOfTrailingZeros(refresh);
            int group = bitIndex / 8;
            positions[group] = queues[group].peekSingle().position;
            refresh ^= 1L << bitIndex;
        }
        dirty = 0;

        long minPosition = Long.MAX_VALUE;
        int minGroup = -1;
        long visit = enabled >>> 7;
        while (visit != 0)
        {
            int bitIndex = Long.numberOfTrailingZeros(visit);
            int group = bitIndex / 8;
            long position = positions[group];
            if (position < minPosition)
            {
                minGroup = group;
                minPosition = position;
            }
            visit ^= 1L << bitIndex;
        }

        return minGroup;
    }

    private int pollGroupByIndex()
    {
        long visit = (hasWork & unsaturated()) >>> 7;
        if (visit == 0)
            return -1;

        int bitIndex = Long.numberOfTrailingZeros(visit);
        return bitIndex / 8;
    }

    private int pollGroupByPhaseFair()
    {
        return minCounterIndex(recentFlowImbalances());
    }

    // PRIORITY_FAIR selection: a deficit round-robin blend of two strategies, chosen per poll:
    //   flow -> minCounterIndex(recent - arrivals + bias)  (least fairly serviced)
    //   age  -> minGroupByPriority()  (earliest-queued work)
    // wFlow = ramp(flow imbalance F = max-min of the selection counters); wAge = BLEND_TOTAL - wFlow. As flow gets
    // uneven, polls trade from age to flow zero-sum; when balanced it is pure age (FIFO). A ramp (not a hard
    // threshold) means there is no mode cliff to oscillate around, so no anti-oscillation penalty is needed. Age is
    // also what drains a stale/standing backlog -- its items are the oldest -- so no separate stock strategy is needed.
    private int pollGroupByBlended()
    {
        return pollGroupByBlended(saturatedOrWithoutWork());
    }

    private int pollGroupByBlended(long disabled)
    {
        long withoutWork = hasWork ^ COUNTER_OVERFLOWS;
        long counters = recentFlowImbalances();
        long minMax = minMaxCounterValue(counters, withoutWork);
        long min = minMax & 0x7f;
        long max = minMax >>> 8;
        int flowImbalance = (int) (max - min);

        int flowWeight = flowWeight(flowImbalance);
        int priorityWeight = AccordExecutor.BLEND_TOTAL - flowWeight;

        creditFlow += flowWeight;
        creditAge += priorityWeight;

        if (creditFlow >= creditAge)
        {
            creditFlow -= AccordExecutor.BLEND_TOTAL;
            if (disabled != withoutWork)
                min = minCounterValue(counters, disabled);

            return minCounterIndex(counters, min, disabled);
        }
        else
        {
            creditAge -= AccordExecutor.BLEND_TOTAL;
            return pollGroupByPriority(disabled ^ COUNTER_OVERFLOWS);
        }
    }

    private long saturated()
    {
        return ((active | COUNTER_OVERFLOWS) - limits) & COUNTER_OVERFLOWS;
    }

    private long unsaturated()
    {
        return saturated() ^ COUNTER_OVERFLOWS;
    }

    private long unsaturatedWithWork()
    {
        return hasWork & unsaturated() & ~stopped;
    }

    private long saturatedOrWithoutWork()
    {
        return (hasWork ^ COUNTER_OVERFLOWS) | saturated() | stopped;
    }

    // arrivals is a windowed measure of ARRIVAL (incremented on enqueue, size-biased; decays on recent's overflow).
    // Combined with recent (service) as effective = max(0, recent - arrivals): a queue whose arrivals outpace its
    // service clamps to 0 and is preferred, so service converges to arrival rate (bounding the busy queue's backlog).
    private long recentFlowImbalances()
    {
        return clampedSubtract(dispatches, arrivals);
    }

    static long minCounterValue(long counters, long disabled)
    {
        long mins = counters;
        mins |= overflowsToLowMasks(disabled);
        mins = minCounters(mins, mins >>> 8); // each slot is min of slots [i..i+1]
        mins = minCounters(mins, mins >>> 16); // each slot is min of slots [i..i+3]
        mins = minCounters(mins, mins >>> 32); // each slot is min of slots [i..i+7]
        return mins & 0x7f;
    }

    static long minMaxCounterValue(long counters, long disabled)
    {
        long mins = counters;
        long maxs = counters ^ COUNTER_MASKS;
        long overflowMasks = overflowsToLowMasks(disabled);
        mins |= overflowMasks;
        maxs |= overflowMasks;
        mins = minCounters(mins, mins >>> 8) & 0x007f007f007f007fL; // each slot is min of slots [i..i+1]
        maxs = (minCounters(maxs, maxs << 8) & 0x7f007f007f007f00L); // each slot is min of slots ~[i..i+1]
        long minmaxs = mins | maxs;
        minmaxs = minCounters(minmaxs, minmaxs >>> 16); // each slot is min of slots [i..i+3]
        minmaxs = minCounters(minmaxs, minmaxs >>> 32); // each slot is min of slots [i..i+7]
        return (minmaxs ^ 0x7f00) & 0x7f7f;
    }

    /**
     * If provided two counters (containing 8 7 bit counters each),
     * returns the minimum of each matching counter
     */
    private static long minCounters(long a, long b)
    {
        // set overflow bits where a <= b
        long selecta = setOverflowWhenLessEqual(a, b);
        return selectByOverflowBits(selecta, a, b);
    }

    static long setOverflowWhenLessEqual(long a, long b)
    {
        return ((b | COUNTER_OVERFLOWS) - a) & COUNTER_OVERFLOWS;
    }

    // select a if overflow bit is set; b if it is unset
    static long selectByOverflowBits(long selecta, long a, long b)
    {
        selecta = overflowsToLowMasks(selecta);
        a &= selecta;
        b &= ~selecta;
        return a | b;
    }

    static long overflowsToLowMasks(long v)
    {
        return v - (v >>> 7);
    }

    private static int flowWeight(int flowImbalance)
    {
        if (flowImbalance <= AccordExecutor.FLOW_ONSET) return 0;
        return Math.min(AccordExecutor.BLEND_TOTAL, ((flowImbalance - AccordExecutor.FLOW_ONSET) << AccordExecutor.BLEND_SHIFT) >>> AccordExecutor.FLOW_WIDTH_SHIFT);
    }

    // per-lane max(0, a - b), carry-free: zero both a and b in lanes where a <= b, then subtract
    private static long clampedSubtract(long a, long b)
    {
        long keep = ~overflowsToLowMasks(setOverflowWhenLessEqual(a, b));
        return (a & keep) - (b & keep);
    }

    private int minCounterIndex(long counters)
    {
        return minCounterIndex(counters, saturatedOrWithoutWork());
    }

    private int minCounterIndex(long counters, long disabled)
    {
        return minCounterIndex(counters, minCounterValue(counters, disabled), disabled);
    }

    private int minCounterIndex(long counters, long minCounterValue, long disabled)
    {
        long mins = minCounterValue * COUNTER_LOWBITS;
        long select = ((mins | COUNTER_OVERFLOWS) - counters) & COUNTER_OVERFLOWS;
        // now unset those overflow bits associated with disabled queues
        select &= ~disabled;
        if (select == 0)
            return -1;
        return (Long.numberOfTrailingZeros(select) - 7) / 8;
    }

    final T pollMulti()
    {
        int group = pollGroup();
        if (group < 0)
        {
            // group < 0 can mean EITHER we don't have any nested queues OR those queues are either empty or DISABLED
            T result = pollSingle();
            if (result != null)
                --waitingCount;
            return result;
        }

        --waitingCount;
        incrementActive(group);
        incrementDispatches(group);

        TaskQueue<T> queue = queues[group];
        T head = queue.pollSingle();
        // NOTE: must clear dirty when emptied, symmetrically with unqueue(): the fair selection paths
        // never consume the dirty bit, so a group drained during a fairness episode would otherwise
        // retain a stale dirty bit and NPE in minGroupByPriority.peekSingle() when balance is restored.
        if (queue.isEmptySingle())
        {
            unsetHasWork(group);
            unsetDirty(group);
        }
        else setDirty(group);
        return head;
    }

    final void enqueueMulti(T task, boolean incrementArrivals)
    {
        task.setQueue(kind);
        int group = group(task);
        if (group < 0)
        {
            enqueueSingle(task);
        }
        else
        {
            TaskQueue<T> queue = queue(group);
            int result = queue.enqueueSingle(task);
            if (incrementArrivals)
                incrementArrivals(group);
            if (result < 0) setHasWork(group);
            if (result != 0) setDirty(group);
        }
        ++waitingCount;
    }

    final void requeue(T task)
    {
        int group = group(task);
        if (group < 0) requeueSingle(task);
        else
        {
            TaskQueue<T> queue = queue(group);
            Invariants.require(queue != null && queue.isQueuedSingle(task));
            if (queue.requeueSingle(task))
                setDirty(group);
        }
    }

    final void unqueueMulti(T task)
    {
        int group = group(task);
        TaskQueue<T> queue = group < 0 ? this : queue(task);
        Invariants.require(queue.isQueuedSingle(task));
        unqueue(task, group, queue);
    }

    // if there is an active collection, we return false and do not remove ourselves from it
    boolean tryUnqueueWaiting(T task)
    {
        int group = group(task);
        TaskQueue<T> queue = group < 0 ? this : queue(task);
        if (!queue.isQueuedSingle(task))
            return false;

        unqueue(task, group, queue);
        return true;
    }

    private void unqueue(T task, int group, TaskQueue<T> queue)
    {
        task.unsetQueue(kind);
        boolean dirty = queue.unqueueSingle(task);
        --waitingCount;
        if (group >= 0)
        {
            if (queue.isEmptySingle())
            {
                unsetHasWork(group);
                unsetDirty(group);
            }
            else if (dirty) setDirty(group);
        }
    }

    final void incrementActive(int group)
    {
        active += lowBit(group);
    }

    final void decrementActive(int group)
    {
        active -= lowBit(group);
    }

    final void incrementDispatches(Task task)
    {
        int group = group(task);
        if (group >= 0)
            incrementDispatches(group);
    }

    final void incrementDispatches(int group)
    {
        dispatches += lowBit(group);
        if ((dispatches & COUNTER_OVERFLOWS) != 0)
        {
            dispatches = (dispatches >>> 1) & COUNTER_MASKS;
            arrivals = (arrivals >>> 1) & COUNTER_MASKS; // arrivals (arrival) decays on the service/time clock
        }
    }

    final void decrementDispatches(Task task)
    {
        int group = group(task);
        if (group >= 0)
            decrementDispatches(group);
    }

    final void decrementDispatches(int group)
    {
        long lowBit = lowBit(group);
        dispatches -= lowBit;
        dispatches += (dispatches >>> 7) & lowBit;
    }

    final void incrementArrivals(Task task)
    {
        int group = group(task);
        if (group >= 0)
            incrementArrivals(group);
    }

    final void incrementArrivals(int group)
    {
        int shift = group * 8;
        long overflowBit = 0x80L << shift;
        arrivals += 1L << shift;
        // if we overflow, unset the overflow bit and set all other bits for the counter
        long overflow = arrivals & overflowBit;
        arrivals ^= overflow;
        arrivals |= overflow - (overflow >>> 7);
    }

    final boolean hasWaitingToRunExcluding(long groupOverflowBits)
    {
        return (unsaturatedWithWork() & ~groupOverflowBits) != 0;
    }

    final void setHasWork(int group)
    {
        hasWork |= overflowBit(group);
    }

    final void unsetHasWork(int group)
    {
        hasWork &= ~overflowBit(group);
    }

    final void setDirty(int group)
    {
        dirty |= overflowBit(group);
    }

    final void unsetDirty(int group)
    {
        dirty &= ~overflowBit(group);
    }

    final boolean hasWaitingToRun()
    {
        return unsaturatedWithWork() != 0;
    }

    final boolean isWaiting(T task)
    {
        return queue(task).isQueuedSingle(task);
    }

    final int waitingCount()
    {
        return waitingCount;
    }

    static long lowBit(int group)
    {
        return 1L << (group * 8);
    }

    static long overflowBit(int group)
    {
        return 0x80L << (group * 8);
    }

    static long overflowBit(Enum<?> group)
    {
        return overflowBit(group.ordinal());
    }
}
