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
package org.apache.cassandra.db.context;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ClockAndCount;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.*;

/**
 * An implementation of a partitioned counter context.
 *
 * A context is primarily a list of tuples (counter id, clock, count) -- called
 * shards, with some shards flagged as global or local (with
 * special resolution rules in merge()).
 *
 * The data structure has two parts:
 *   a) a header containing the lists of global and local shard indexes in the body
 *   b) a list of shards -- (counter id, logical clock, count) tuples -- (the so-called 'body' below)
 *
 * The exact layout is:
 *            | header  |   body   |
 * context :  |--|------|----------|
 *             ^     ^
 *             |   list of indices in the body list (2*#elt bytes)
 *    #elt in rest of header (2 bytes)
 *
 * Non-negative indices refer to local shards. Global shard indices are encoded as [idx + Short.MIN_VALUE],
 * and are thus always negative.
 *
 * The body layout being:
 *
 * body:     |----|----|----|----|----|----|....
 *             ^    ^    ^     ^   ^    ^
 *             |    |  count_1 |   |   count_2
 *             |  clock_1      |  clock_2
 *       counterid_1         counterid_2
 *
 * The rules when merging two shard with the same counter id are:
 *   - global + global = keep the shard with the highest logical clock
 *   - global + local  = keep the global one
 *   - global + remote = keep the global one
 *   - local  + local  = sum counts (and logical clocks)
 *   - local  + remote = keep the local one
 *   - remote + remote = keep the shard with the highest logical clock
 *
 * For a detailed description of the meaning of a local and why the merging
 * rules work this way, see CASSANDRA-1938 - specifically the 1938_discussion
 * attachment (doesn't cover global shards, see CASSANDRA-4775 for that).
 */
public class CounterContext
{
    private static final int HEADER_SIZE_LENGTH = TypeSizes.sizeof(Short.MAX_VALUE);
    private static final int HEADER_ELT_LENGTH = TypeSizes.sizeof(Short.MAX_VALUE);
    private static final int CLOCK_LENGTH = TypeSizes.sizeof(Long.MAX_VALUE);
    private static final int COUNT_LENGTH = TypeSizes.sizeof(Long.MAX_VALUE);
    private static final int STEP_LENGTH = CounterId.LENGTH + CLOCK_LENGTH + COUNT_LENGTH;

    /*
     * A special hard-coded value we use for clock ids to differentiate between regular local shards
     * and 'fake' local shards used to emulate pre-3.0 CounterUpdateCell-s in UpdateParameters.
     *
     * Important for handling counter writes and reads during rolling 2.1/2.2 -> 3.0 upgrades.
     */
    static final CounterId UPDATE_CLOCK_ID = CounterId.fromInt(0);

    private static final Logger logger = LoggerFactory.getLogger(CounterContext.class);

    public static enum Relationship
    {
        EQUAL, GREATER_THAN, LESS_THAN, DISJOINT
    }

    // lazy-load singleton
    private static class LazyHolder
    {
        private static final CounterContext counterContext = new CounterContext();
    }

    public static CounterContext instance()
    {
        return LazyHolder.counterContext;
    }

    /**
     * Creates a counter context with a single local shard with clock id of UPDATE_CLOCK_ID.
     *
     * This is only used in a PartitionUpdate until the update has gone through
     * CounterMutation.apply(), at which point this special local shard will be replaced by a regular global one.
     * It should never hit commitlog / memtable / disk, but can hit network.
     *
     * We use this so that if an update statement has multiple increments of the same counter we properly
     * add them rather than keeping only one of them.
     *
     * NOTE: Before CASSANDRA-13691 we used a regular local shard without a hard-coded clock id value here.
     * It was problematic, because it was possible to return a false positive, and on read path encode an old counter
     * cell from 2.0 era with a regular local shard as a counter update, and to break the 2.1 coordinator.
     */
    public ByteBuffer createUpdate(long count)
    {
        ContextState state = ContextState.allocate(0, 1, 0);
        state.writeLocal(UPDATE_CLOCK_ID, 1L, count);
        return state.context;
    }

    /**
     * Checks if a context is an update (see createUpdate() for justification).
     */
    public boolean isUpdate(ByteBuffer context)
    {
        return ContextState.wrap(context).getCounterId().equals(UPDATE_CLOCK_ID);
    }

    /**
     * Returns the count associated with the update counter id, or 0 if no such shard is present.
     */
    public long getUpdateCount(ByteBuffer context)
    {
        return getClockAndCountOf(context, UPDATE_CLOCK_ID).count;
    }

    /**
     * Creates a counter context with a single global, 2.1+ shard (a result of increment).
     */
    public ByteBuffer createGlobal(CounterId id, long clock, long count)
    {
        ContextState state = ContextState.allocate(1, 0, 0);
        state.writeGlobal(id, clock, count);
        return state.context;
    }

    /**
     * Creates a counter context with a single local shard.
     * For use by tests of compatibility with pre-2.1 counters only.
     */
    public ByteBuffer createLocal(long count)
    {
        ContextState state = ContextState.allocate(0, 1, 0);
        state.writeLocal(CounterId.getLocalId(), 1L, count);
        return state.context;
    }

    /**
     * Creates a counter context with a single remote shard.
     * For use by tests of compatibility with pre-2.1 counters only.
     */
    public ByteBuffer createRemote(CounterId id, long clock, long count)
    {
        ContextState state = ContextState.allocate(0, 0, 1);
        state.writeRemote(id, clock, count);
        return state.context;
    }

    private static int headerLength(ByteBuffer context)
    {
        return HEADER_SIZE_LENGTH + Math.abs(context.getShort(context.position())) * HEADER_ELT_LENGTH;
    }

    private static int compareId(ByteBuffer bb1, int pos1, ByteBuffer bb2, int pos2)
    {
        return ByteBufferUtil.compareSubArrays(bb1, pos1, bb2, pos2, CounterId.LENGTH);
    }

    /**
     * Determine the count relationship between two contexts.
     *
     * EQUAL:        Equal set of nodes and every count is equal.
     * GREATER_THAN: Superset of nodes and every count is equal or greater than its corollary.
     * LESS_THAN:    Subset of nodes and every count is equal or less than its corollary.
     * DISJOINT:     Node sets are not equal and/or counts are not all greater or less than.
     *
     * Strategy: compare node logical clocks (like a version vector).
     *
     * @param left counter context.
     * @param right counter context.
     * @return the Relationship between the contexts.
     */
    public Relationship diff(ByteBuffer left, ByteBuffer right)
    {
        Relationship relationship = Relationship.EQUAL;
        ContextState leftState = ContextState.wrap(left);
        ContextState rightState = ContextState.wrap(right);

        while (leftState.hasRemaining() && rightState.hasRemaining())
        {
            // compare id bytes
            int compareId = leftState.compareIdTo(rightState);
            if (compareId == 0)
            {
                long leftClock  = leftState.getClock();
                long rightClock = rightState.getClock();
                long leftCount = leftState.getCount();
                long rightCount = rightState.getCount();

                // advance
                leftState.moveToNext();
                rightState.moveToNext();

                // process clock comparisons
                if (leftClock == rightClock)
                {
                    if (leftCount != rightCount)
                    {
                        // Inconsistent shard (see the corresponding code in merge()). We return DISJOINT in this
                        // case so that it will be treated as a difference, allowing read-repair to work.
                        return Relationship.DISJOINT;
                    }
                }
                else if ((leftClock >= 0 && rightClock > 0 && leftClock > rightClock)
                      || (leftClock < 0 && (rightClock > 0 || leftClock < rightClock)))
                {
                    if (relationship == Relationship.EQUAL)
                        relationship = Relationship.GREATER_THAN;
                    else if (relationship == Relationship.LESS_THAN)
                        return Relationship.DISJOINT;
                    // relationship == Relationship.GREATER_THAN
                }
                else
                {
                    if (relationship == Relationship.EQUAL)
                        relationship = Relationship.LESS_THAN;
                    else if (relationship == Relationship.GREATER_THAN)
                        return Relationship.DISJOINT;
                    // relationship == Relationship.LESS_THAN
                }
            }
            else if (compareId > 0)
            {
                // only advance the right context
                rightState.moveToNext();

                if (relationship == Relationship.EQUAL)
                    relationship = Relationship.LESS_THAN;
                else if (relationship == Relationship.GREATER_THAN)
                    return Relationship.DISJOINT;
                // relationship == Relationship.LESS_THAN
            }
            else // compareId < 0
            {
                // only advance the left context
                leftState.moveToNext();

                if (relationship == Relationship.EQUAL)
                    relationship = Relationship.GREATER_THAN;
                else if (relationship == Relationship.LESS_THAN)
                    return Relationship.DISJOINT;
                // relationship == Relationship.GREATER_THAN
            }
        }

        // check final lengths
        if (leftState.hasRemaining())
        {
            if (relationship == Relationship.EQUAL)
                return Relationship.GREATER_THAN;
            else if (relationship == Relationship.LESS_THAN)
                return Relationship.DISJOINT;
        }

        if (rightState.hasRemaining())
        {
            if (relationship == Relationship.EQUAL)
                return Relationship.LESS_THAN;
            else if (relationship == Relationship.GREATER_THAN)
                return Relationship.DISJOINT;
        }

        return relationship;
    }

    /**
     * Return a context w/ an aggregated count for each counter id.
     *
     * @param left counter context.
     * @param right counter context.
     */
    public ByteBuffer merge(ByteBuffer left, ByteBuffer right)
    {
        boolean leftIsSuperSet = true;
        boolean rightIsSuperSet = true;

        int globalCount = 0;
        int localCount = 0;
        int remoteCount = 0;

        ContextState leftState = ContextState.wrap(left);
        ContextState rightState = ContextState.wrap(right);

        while (leftState.hasRemaining() && rightState.hasRemaining())
        {
            int cmp = leftState.compareIdTo(rightState);
            if (cmp == 0)
            {
                Relationship rel = compare(leftState, rightState);
                if (rel == Relationship.GREATER_THAN)
                    rightIsSuperSet = false;
                else if (rel == Relationship.LESS_THAN)
                    leftIsSuperSet = false;
                else if (rel == Relationship.DISJOINT)
                    leftIsSuperSet = rightIsSuperSet = false;

                if (leftState.isGlobal() || rightState.isGlobal())
                    globalCount += 1;
                else if (leftState.isLocal() || rightState.isLocal())
                    localCount += 1;
                else
                    remoteCount += 1;

                leftState.moveToNext();
                rightState.moveToNext();
            }
            else if (cmp > 0)
            {
                leftIsSuperSet = false;

                if (rightState.isGlobal())
                    globalCount += 1;
                else if (rightState.isLocal())
                    localCount += 1;
                else
                    remoteCount += 1;

                rightState.moveToNext();
            }
            else // cmp < 0
            {
                rightIsSuperSet = false;

                if (leftState.isGlobal())
                    globalCount += 1;
                else if (leftState.isLocal())
                    localCount += 1;
                else
                    remoteCount += 1;

                leftState.moveToNext();
            }
        }

        if (leftState.hasRemaining())
            rightIsSuperSet = false;
        else if (rightState.hasRemaining())
            leftIsSuperSet = false;

        // if one of the contexts is a superset, return it early.
        if (leftIsSuperSet)
            return left;
        else if (rightIsSuperSet)
            return right;

        while (leftState.hasRemaining())
        {
            if (leftState.isGlobal())
                globalCount += 1;
            else if (leftState.isLocal())
                localCount += 1;
            else
                remoteCount += 1;

            leftState.moveToNext();
        }

        while (rightState.hasRemaining())
        {
            if (rightState.isGlobal())
                globalCount += 1;
            else if (rightState.isLocal())
                localCount += 1;
            else
                remoteCount += 1;

            rightState.moveToNext();
        }

        leftState.reset();
        rightState.reset();

        return merge(ContextState.allocate(globalCount, localCount, remoteCount), leftState, rightState);
    }

    private ByteBuffer merge(ContextState mergedState, ContextState leftState, ContextState rightState)
    {
        while (leftState.hasRemaining() && rightState.hasRemaining())
        {
            int cmp = leftState.compareIdTo(rightState);
            if (cmp == 0)
            {
                Relationship rel = compare(leftState, rightState);
                if (rel == Relationship.DISJOINT) // two local shards
                    mergedState.writeLocal(leftState.getCounterId(),
                                           leftState.getClock() + rightState.getClock(),
                                           leftState.getCount() + rightState.getCount());
                else if (rel == Relationship.GREATER_THAN)
                    leftState.copyTo(mergedState);
                else // EQUAL or LESS_THAN
                    rightState.copyTo(mergedState);

                rightState.moveToNext();
                leftState.moveToNext();
            }
            else if (cmp > 0)
            {
                rightState.copyTo(mergedState);
                rightState.moveToNext();
            }
            else // cmp < 0
            {
                leftState.copyTo(mergedState);
                leftState.moveToNext();
            }
        }

        while (leftState.hasRemaining())
        {
            leftState.copyTo(mergedState);
            leftState.moveToNext();
        }

        while (rightState.hasRemaining())
        {
            rightState.copyTo(mergedState);
            rightState.moveToNext();
        }

        return mergedState.context;
    }

    /*
     * Compares two shards, returns:
     * - GREATER_THAN if leftState overrides rightState
     * - LESS_THAN if rightState overrides leftState
     * - EQUAL for two equal, non-local, shards
     * - DISJOINT for any two local shards
     */
    private Relationship compare(ContextState leftState, ContextState rightState)
    {
        long leftClock = leftState.getClock();
        long leftCount = leftState.getCount();
        long rightClock = rightState.getClock();
        long rightCount = rightState.getCount();

        if (leftState.isGlobal() || rightState.isGlobal())
        {
            if (leftState.isGlobal() && rightState.isGlobal())
            {
                if (leftClock == rightClock)
                {
                    // Can happen if an sstable gets lost and disk failure policy is set to 'best effort'
                    if (leftCount != rightCount && CompactionManager.isCompactionManager.get())
                    {
                        logger.warn("invalid global counter shard detected; ({}, {}, {}) and ({}, {}, {}) differ only in "
                                    + "count; will pick highest to self-heal on compaction",
                                    leftState.getCounterId(), leftClock, leftCount,
                                    rightState.getCounterId(), rightClock, rightCount);
                    }

                    if (leftCount > rightCount)
                        return Relationship.GREATER_THAN;
                    else if (leftCount == rightCount)
                        return Relationship.EQUAL;
                    else
                        return Relationship.LESS_THAN;
                }
                else
                {
                    return leftClock > rightClock ? Relationship.GREATER_THAN : Relationship.LESS_THAN;
                }
            }
            else // only one is global - keep that one
            {
                return leftState.isGlobal() ? Relationship.GREATER_THAN : Relationship.LESS_THAN;
            }
        }

        if (leftState.isLocal() || rightState.isLocal())
        {
            // Local id and at least one is a local shard.
            if (leftState.isLocal() && rightState.isLocal())
                return Relationship.DISJOINT;
            else // only one is local - keep that one
                return leftState.isLocal() ? Relationship.GREATER_THAN : Relationship.LESS_THAN;
        }

        // both are remote shards
        if (leftClock == rightClock)
        {
            // We should never see non-local shards w/ same id+clock but different counts. However, if we do
            // we should "heal" the problem by being deterministic in our selection of shard - and
            // log the occurrence so that the operator will know something is wrong.
            if (leftCount != rightCount && CompactionManager.isCompactionManager.get())
            {
                logger.warn("invalid remote counter shard detected; ({}, {}, {}) and ({}, {}, {}) differ only in "
                            + "count; will pick highest to self-heal on compaction",
                            leftState.getCounterId(), leftClock, leftCount,
                            rightState.getCounterId(), rightClock, rightCount);
            }

            if (leftCount > rightCount)
                return Relationship.GREATER_THAN;
            else if (leftCount == rightCount)
                return Relationship.EQUAL;
            else
                return Relationship.LESS_THAN;
        }
        else
        {
            if ((leftClock >= 0 && rightClock > 0 && leftClock >= rightClock)
                    || (leftClock < 0 && (rightClock > 0 || leftClock < rightClock)))
                return Relationship.GREATER_THAN;
            else
                return Relationship.LESS_THAN;
        }
    }

    /**
     * Human-readable String from context.
     *
     * @param context counter context.
     * @return a human-readable String of the context.
     */
    public String toString(ByteBuffer context)
    {
        ContextState state = ContextState.wrap(context);
        StringBuilder sb = new StringBuilder();
        sb.append("[");

        while (state.hasRemaining())
        {
            if (state.getElementIndex() > 0)
                sb.append(",");
            sb.append("{");
            sb.append(state.getCounterId()).append(", ");
            sb.append(state.getClock()).append(", ");
            sb.append(state.getCount());
            sb.append("}");
            if (state.isGlobal())
                sb.append("$");
            else if (state.isLocal())
                sb.append("*");
            state.moveToNext();
        }

        sb.append("]");
        return sb.toString();
    }

    /**
     * Returns the aggregated count across all counter ids.
     *
     * @param context a counter context
     * @return the aggregated count represented by {@code context}
     */
    public long total(ByteBuffer context)
    {
        long total = 0L;
        // we could use a ContextState but it is easy enough that we avoid the object creation
        for (int offset = context.position() + headerLength(context); offset < context.limit(); offset += STEP_LENGTH)
            total += context.getLong(offset + CounterId.LENGTH + CLOCK_LENGTH);
        return total;
    }

    public boolean shouldClearLocal(ByteBuffer context)
    {
        // #elt being negative means we have to clean local shards.
        return context.getShort(context.position()) < 0;
    }

    /**
     * Detects whether or not the context has any legacy (local or remote) shards in it.
     */
    public boolean hasLegacyShards(ByteBuffer context)
    {
        int totalCount = (context.remaining() - headerLength(context)) / STEP_LENGTH;
        int localAndGlobalCount = Math.abs(context.getShort(context.position()));

        if (localAndGlobalCount < totalCount)
            return true; // remote shard(s) present

        for (int i = 0; i < localAndGlobalCount; i++)
            if (context.getShort(context.position() + HEADER_SIZE_LENGTH + i * HEADER_ELT_LENGTH) >= 0)
                return true; // found a local shard

        return false;
    }

    /**
     * Mark context to delete local references afterward.
     * Marking is done by multiply #elt by -1 to preserve header length
     * and #elt count in order to clear all local refs later.
     *
     * @param context a counter context
     * @return context that marked to delete local refs
     */
    public ByteBuffer markLocalToBeCleared(ByteBuffer context)
    {
        short count = context.getShort(context.position());
        if (count <= 0)
            return context; // already marked or all are remote.

        boolean hasLocalShards = false;
        for (int i = 0; i < count; i++)
        {
            if (context.getShort(context.position() + HEADER_SIZE_LENGTH + i * HEADER_ELT_LENGTH) >= 0)
            {
                hasLocalShards = true;
                break;
            }
        }

        if (!hasLocalShards)
            return context; // all shards are global or remote.

        ByteBuffer marked = ByteBuffer.allocate(context.remaining());
        marked.putShort(marked.position(), (short) (count * -1));
        ByteBufferUtil.arrayCopy(context,
                                 context.position() + HEADER_SIZE_LENGTH,
                                 marked,
                                 marked.position() + HEADER_SIZE_LENGTH,
                                 context.remaining() - HEADER_SIZE_LENGTH);
        return marked;
    }

    /**
     * Remove all the local of a context (but keep global).
     *
     * @param context a counter context
     * @return a version of {@code context} where no shards are local.
     */
    public ByteBuffer clearAllLocal(ByteBuffer context)
    {
        int count = Math.abs(context.getShort(context.position()));
        if (count == 0)
            return context; // no local or global shards present.

        List<Short> globalShardIndexes = new ArrayList<>(count);
        for (int i = 0; i < count; i++)
        {
            short elt = context.getShort(context.position() + HEADER_SIZE_LENGTH + i * HEADER_ELT_LENGTH);
            if (elt < 0)
                globalShardIndexes.add(elt);
        }

        if (count == globalShardIndexes.size())
            return context; // no local shards detected.

        // allocate a smaller BB for the cleared context - with no local header elts.
        ByteBuffer cleared = ByteBuffer.allocate(context.remaining() - (count - globalShardIndexes.size()) * HEADER_ELT_LENGTH);

        cleared.putShort(cleared.position(), (short) globalShardIndexes.size());
        for (int i = 0; i < globalShardIndexes.size(); i++)
            cleared.putShort(cleared.position() + HEADER_SIZE_LENGTH + i * HEADER_ELT_LENGTH, globalShardIndexes.get(i));

        int origHeaderLength = headerLength(context);
        ByteBufferUtil.arrayCopy(context,
                                 context.position() + origHeaderLength,
                                 cleared,
                                 cleared.position() + headerLength(cleared),
                                 context.remaining() - origHeaderLength);

        return cleared;
    }

    public void validateContext(ByteBuffer context) throws MarshalException
    {
        if ((context.remaining() - headerLength(context)) % STEP_LENGTH != 0)
            throw new MarshalException("Invalid size for a counter context");
    }

    /**
     * Update a MessageDigest with the content of a context.
     * Note that this skips the header entirely since the header information
     * has local meaning only, while digests are meant for comparison across
     * nodes. This means in particular that we always have:
     *  updateDigest(ctx) == updateDigest(clearAllLocal(ctx))
     */
    public void updateDigest(MessageDigest message, ByteBuffer context)
    {
        // context can be empty due to the optimization from CASSANDRA-10657
        if (!context.hasRemaining())
            return;
        ByteBuffer dup = context.duplicate();
        dup.position(context.position() + headerLength(context));
        message.update(dup);
    }

    /**
     * Returns the clock and the count associated with the local counter id, or (0, 0) if no such shard is present.
     */
    public ClockAndCount getLocalClockAndCount(ByteBuffer context)
    {
        return getClockAndCountOf(context, CounterId.getLocalId());
    }

    /**
     * Returns the clock and the count associated with the given counter id, or (0, 0) if no such shard is present.
     */
    @VisibleForTesting
    public ClockAndCount getClockAndCountOf(ByteBuffer context, CounterId id)
    {
        int position = findPositionOf(context, id);
        if (position == -1)
            return ClockAndCount.BLANK;

        long clock = context.getLong(position + CounterId.LENGTH);
        long count = context.getLong(position + CounterId.LENGTH + CLOCK_LENGTH);
        return ClockAndCount.create(clock, count);
    }

    /**
     * Finds the position of a shard with the given id within the context (via binary search).
     */
    @VisibleForTesting
    public int findPositionOf(ByteBuffer context, CounterId id)
    {
        int headerLength = headerLength(context);
        int offset = context.position() + headerLength;

        int left = 0;
        int right = (context.remaining() - headerLength) / STEP_LENGTH - 1;

        while (right >= left)
        {
            int middle = (left + right) / 2;
            int cmp = compareId(context, offset + middle * STEP_LENGTH, id.bytes(), id.bytes().position());

            if (cmp == -1)
                left = middle + 1;
            else if (cmp == 0)
                return offset + middle * STEP_LENGTH;
            else
                right = middle - 1;
        }

        return -1; // position not found
    }

    /**
     * Helper class to work on contexts (works by iterating over them).
     * A context being abstractly a list of tuple (counterid, clock, count), a
     * ContextState encapsulate a context and a position to one of the tuple.
     * It also allow to create new context iteratively.
     *
     * Note: this is intrinsically a private class intended for use by the
     * methods of CounterContext only. It is however public because it is
     * convenient to create handcrafted context for unit tests.
     */
    public static class ContextState
    {
        public final ByteBuffer context;
        public final int headerLength;

        private int headerOffset;        // offset from context.position()
        private int bodyOffset;          // offset from context.position()
        private boolean currentIsGlobal;
        private boolean currentIsLocal;

        private ContextState(ByteBuffer context)
        {
            this.context = context;
            this.headerLength = this.bodyOffset = headerLength(context);
            this.headerOffset = HEADER_SIZE_LENGTH;
            updateIsGlobalOrLocal();
        }

        public static ContextState wrap(ByteBuffer context)
        {
            return new ContextState(context);
        }

        /**
         * Allocate a new context big enough for globalCount + localCount + remoteCount elements
         * and return the initial corresponding ContextState.
         */
        public static ContextState allocate(int globalCount, int localCount, int remoteCount)
        {
            int headerLength = HEADER_SIZE_LENGTH + (globalCount + localCount) * HEADER_ELT_LENGTH;
            int bodyLength = (globalCount + localCount + remoteCount) * STEP_LENGTH;

            ByteBuffer buffer = ByteBuffer.allocate(headerLength + bodyLength);
            buffer.putShort(buffer.position(), (short) (globalCount + localCount));

            return ContextState.wrap(buffer);
        }

        public boolean isGlobal()
        {
            return currentIsGlobal;
        }

        public boolean isLocal()
        {
            return currentIsLocal;
        }

        public boolean isRemote()
        {
            return !(currentIsGlobal || currentIsLocal);
        }

        private void updateIsGlobalOrLocal()
        {
            if (headerOffset >= headerLength)
            {
                currentIsGlobal = currentIsLocal = false;
            }
            else
            {
                short headerElt = context.getShort(context.position() + headerOffset);
                currentIsGlobal = headerElt == getElementIndex() + Short.MIN_VALUE;
                currentIsLocal = headerElt == getElementIndex();
            }
        }

        public boolean hasRemaining()
        {
            return bodyOffset < context.remaining();
        }

        public void moveToNext()
        {
            bodyOffset += STEP_LENGTH;
            if (currentIsGlobal || currentIsLocal)
                headerOffset += HEADER_ELT_LENGTH;
            updateIsGlobalOrLocal();
        }

        public void copyTo(ContextState other)
        {
            other.writeElement(getCounterId(), getClock(), getCount(), currentIsGlobal, currentIsLocal);
        }

        public int compareIdTo(ContextState other)
        {
            return compareId(context, context.position() + bodyOffset, other.context, other.context.position() + other.bodyOffset);
        }

        public void reset()
        {
            this.headerOffset = HEADER_SIZE_LENGTH;
            this.bodyOffset = headerLength;
            updateIsGlobalOrLocal();
        }

        public int getElementIndex()
        {
            return (bodyOffset - headerLength) / STEP_LENGTH;
        }

        public CounterId getCounterId()
        {
            return CounterId.wrap(context, context.position() + bodyOffset);
        }

        public long getClock()
        {
            return context.getLong(context.position() + bodyOffset + CounterId.LENGTH);
        }

        public long getCount()
        {
            return context.getLong(context.position() + bodyOffset + CounterId.LENGTH + CLOCK_LENGTH);
        }

        public void writeGlobal(CounterId id, long clock, long count)
        {
            writeElement(id, clock, count, true, false);
        }

        // In 2.1 only used by the unit tests.
        public void writeLocal(CounterId id, long clock, long count)
        {
            writeElement(id, clock, count, false, true);
        }

        // In 2.1 only used by the unit tests.
        public void writeRemote(CounterId id, long clock, long count)
        {
            writeElement(id, clock, count, false, false);
        }

        private void writeElement(CounterId id, long clock, long count, boolean isGlobal, boolean isLocal)
        {
            writeElementAtOffset(context, context.position() + bodyOffset, id, clock, count);

            if (isGlobal)
                context.putShort(context.position() + headerOffset, (short) (getElementIndex() + Short.MIN_VALUE));
            else if (isLocal)
                context.putShort(context.position() + headerOffset, (short) getElementIndex());

            currentIsGlobal = isGlobal;
            currentIsLocal = isLocal;
            moveToNext();
        }

        // write a tuple (counter id, clock, count) at an absolute (bytebuffer-wise) offset
        private void writeElementAtOffset(ByteBuffer ctx, int offset, CounterId id, long clock, long count)
        {
            ctx = ctx.duplicate();
            ctx.position(offset);
            ctx.put(id.bytes().duplicate());
            ctx.putLong(clock);
            ctx.putLong(count);
        }
    }
}
