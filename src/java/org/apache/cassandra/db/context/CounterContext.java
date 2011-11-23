/**
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
import java.util.*;

import org.apache.log4j.Logger;

import org.apache.cassandra.db.marshal.MarshalException;
import org.apache.cassandra.db.DBConstants;
import org.apache.cassandra.utils.Allocator;
import org.apache.cassandra.utils.HeapAllocator;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.NodeId;

/**
 * An implementation of a partitioned counter context.
 *
 * A context is primarily a list of tuples (node id, clock, count) -- called
 * shard in the following. But with some shard are flagged as delta (with
 * special resolution rules in merge()).
 *
 * The data structure has two parts:
 *   a) a header containing the lists of "delta" (a list of references to the second parts)
 *   b) a list of shard -- (node id, logical clock, count) tuples -- (the so-called 'body' below)
 *
 * The exact layout is:
 *            | header  |   body   |
 * context :  |--|------|----------|
 *             ^     ^
 *             |   list of indices in the body list (2*#elt bytes)
 *    #elt in rest of header (2 bytes)
 *
 * The body layout being:
 *
 * body:     |----|----|----|----|----|----|....
 *             ^    ^    ^     ^   ^    ^
 *             |    |  count_1 |   |   count_2
 *             |  clock_1      |  clock_2
 *         nodeid_1          nodeid_2
 *
 * The rules when merging two shard with the same nodeid are:
 *   - delta + delta = sum counts (and logical clock)
 *   - delta + other = keep the delta one
 *   - other + other = keep the shard with highest logical clock
 */
public class CounterContext implements IContext
{
    private static final int HEADER_SIZE_LENGTH = DBConstants.shortSize;
    private static final int HEADER_ELT_LENGTH = DBConstants.shortSize;
    private static final int CLOCK_LENGTH = DBConstants.longSize;
    private static final int COUNT_LENGTH = DBConstants.longSize;
    private static final int STEP_LENGTH = NodeId.LENGTH + CLOCK_LENGTH + COUNT_LENGTH;

    private static final Logger logger = Logger.getLogger(CounterContext.class);

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
     * Creates an initial counter context with an initial value for the local node.
     *
     *
     * @param value the value for this initial update
     *
     * @param allocator
     * @return an empty counter context.
     */
    public ByteBuffer create(long value, Allocator allocator)
    {
        ByteBuffer context = allocator.allocate(HEADER_SIZE_LENGTH + HEADER_ELT_LENGTH + STEP_LENGTH);
        // The first (and only) elt is a delta
        context.putShort(context.position(), (short)1);
        context.putShort(context.position() + HEADER_SIZE_LENGTH, (short)0);
        writeElementAtOffset(context, context.position() + HEADER_SIZE_LENGTH + HEADER_ELT_LENGTH, NodeId.getLocalId(), 1L, value);
        return context;
    }

    // Provided for use by unit tests
    public ByteBuffer create(NodeId id, long clock, long value, boolean isDelta)
    {
        ByteBuffer context = ByteBuffer.allocate(HEADER_SIZE_LENGTH + (isDelta ? HEADER_ELT_LENGTH : 0) + STEP_LENGTH);
        context.putShort(context.position(), (short)(isDelta ? 1 : 0));
        if (isDelta)
        {
            context.putShort(context.position() + HEADER_SIZE_LENGTH, (short)0);
        }
        writeElementAtOffset(context, context.position() + HEADER_SIZE_LENGTH + (isDelta ? HEADER_ELT_LENGTH : 0), id, clock, value);
        return context;
    }

    // write a tuple (node id, clock, count) at an absolute (bytebuffer-wise) offset
    private static void writeElementAtOffset(ByteBuffer context, int offset, NodeId id, long clock, long count)
    {
        context = context.duplicate();
        context.position(offset);
        context.put(id.bytes().duplicate());
        context.putLong(clock);
        context.putLong(count);
    }

    private static int headerLength(ByteBuffer context)
    {
        return HEADER_SIZE_LENGTH + Math.abs(context.getShort(context.position())) * HEADER_ELT_LENGTH;
    }

    private static int compareId(ByteBuffer bb1, int pos1, ByteBuffer bb2, int pos2)
    {
        return ByteBufferUtil.compareSubArrays(bb1, pos1, bb2, pos2, NodeId.LENGTH);
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
     * @return the ContextRelationship between the contexts.
     */
    public ContextRelationship diff(ByteBuffer left, ByteBuffer right)
    {
        ContextRelationship relationship = ContextRelationship.EQUAL;
        ContextState leftState = new ContextState(left, headerLength(left));
        ContextState rightState = new ContextState(right, headerLength(right));

        while (leftState.hasRemaining() && rightState.hasRemaining())
        {
            // compare id bytes
            int compareId = leftState.compareIdTo(rightState);
            if (compareId == 0)
            {
                long leftClock  = leftState.getClock();
                long rightClock = rightState.getClock();

                // advance
                leftState.moveToNext();
                rightState.moveToNext();

                // process clock comparisons
                if (leftClock == rightClock)
                {
                    continue;
                }
                else if ((leftClock >= 0 && rightClock > 0 && leftClock > rightClock)
                      || (leftClock < 0 && (rightClock > 0 || leftClock < rightClock)))
                {
                    if (relationship == ContextRelationship.EQUAL)
                    {
                        relationship = ContextRelationship.GREATER_THAN;
                    }
                    else if (relationship == ContextRelationship.GREATER_THAN)
                    {
                        continue;
                    }
                    else
                    {
                        // relationship == ContextRelationship.LESS_THAN
                        return ContextRelationship.DISJOINT;
                    }
                }
                else
                {
                    if (relationship == ContextRelationship.EQUAL)
                    {
                        relationship = ContextRelationship.LESS_THAN;
                    }
                    else if (relationship == ContextRelationship.GREATER_THAN)
                    {
                        return ContextRelationship.DISJOINT;
                    }
                    else
                    {
                        // relationship == ContextRelationship.LESS_THAN
                        continue;
                    }
                }
            }
            else if (compareId > 0)
            {
                // only advance the right context
                rightState.moveToNext();

                if (relationship == ContextRelationship.EQUAL)
                {
                    relationship = ContextRelationship.LESS_THAN;
                }
                else if (relationship == ContextRelationship.GREATER_THAN)
                {
                    return ContextRelationship.DISJOINT;
                }
                else
                {
                    // relationship == ContextRelationship.LESS_THAN
                    continue;
                }
            }
            else // compareId < 0
            {
                // only advance the left context
                leftState.moveToNext();

                if (relationship == ContextRelationship.EQUAL)
                {
                    relationship = ContextRelationship.GREATER_THAN;
                }
                else if (relationship == ContextRelationship.GREATER_THAN)
                {
                    continue;
                }
                else
                // relationship == ContextRelationship.LESS_THAN
                {
                    return ContextRelationship.DISJOINT;
                }
            }
        }

        // check final lengths
        if (leftState.hasRemaining())
        {
            if (relationship == ContextRelationship.EQUAL)
            {
                return ContextRelationship.GREATER_THAN;
            }
            else if (relationship == ContextRelationship.LESS_THAN)
            {
                return ContextRelationship.DISJOINT;
            }
        }
        else if (rightState.hasRemaining())
        {
            if (relationship == ContextRelationship.EQUAL)
            {
                return ContextRelationship.LESS_THAN;
            }
            else if (relationship == ContextRelationship.GREATER_THAN)
            {
                return ContextRelationship.DISJOINT;
            }
        }

        return relationship;
    }

    /**
     * Return a context w/ an aggregated count for each node id.
     *
     * @param left counter context.
     * @param right counter context.
     * @param allocator An allocator for the merged value.
     */
    public ByteBuffer merge(ByteBuffer left, ByteBuffer right, Allocator allocator)
    {
        ContextState leftState = new ContextState(left, headerLength(left));
        ContextState rightState = new ContextState(right, headerLength(right));

        // Compute size of result
        int mergedHeaderLength = HEADER_SIZE_LENGTH;
        int mergedBodyLength = 0;

        while (leftState.hasRemaining() && rightState.hasRemaining())
        {
            int cmp = leftState.compareIdTo(rightState);
            if (cmp == 0)
            {
                mergedBodyLength += STEP_LENGTH;
                if (leftState.isDelta() || rightState.isDelta())
                    mergedHeaderLength += HEADER_ELT_LENGTH;
                leftState.moveToNext();
                rightState.moveToNext();
            }
            else if (cmp > 0)
            {
                mergedBodyLength += STEP_LENGTH;
                if (rightState.isDelta())
                    mergedHeaderLength += HEADER_ELT_LENGTH;
                rightState.moveToNext();
            }
            else // cmp < 0
            {
                mergedBodyLength += STEP_LENGTH;
                if (leftState.isDelta())
                    mergedHeaderLength += HEADER_ELT_LENGTH;
                leftState.moveToNext();
            }
        }
        mergedHeaderLength += leftState.remainingHeaderLength() + rightState.remainingHeaderLength();
        mergedBodyLength += leftState.remainingBodyLength() + rightState.remainingBodyLength();

        // Do the actual merge
        ByteBuffer merged = allocator.allocate(mergedHeaderLength + mergedBodyLength);
        merged.putShort(merged.position(), (short) ((mergedHeaderLength - HEADER_SIZE_LENGTH) / HEADER_ELT_LENGTH));
        ContextState mergedState = new ContextState(merged, mergedHeaderLength);
        leftState.reset();
        rightState.reset();
        while (leftState.hasRemaining() && rightState.hasRemaining())
        {
            int cmp = leftState.compareIdTo(rightState);
            if (cmp == 0)
            {
                if (leftState.isDelta() || rightState.isDelta())
                {
                    // Local id and at least one is a delta
                    if (leftState.isDelta() && rightState.isDelta())
                    {
                        // both delta, sum
                        long clock = leftState.getClock() + rightState.getClock();
                        long count = leftState.getCount() + rightState.getCount();
                        mergedState.writeElement(leftState.getNodeId(), clock, count, true);
                    }
                    else
                    {
                        // Only one have delta, keep that one
                        (leftState.isDelta() ? leftState : rightState).copyTo(mergedState);
                    }
                }
                else
                {
                    long leftClock = leftState.getClock();
                    long rightClock = rightState.getClock();
                    if ((leftClock >= 0 && rightClock > 0 && leftClock >= rightClock)
                     || (leftClock < 0 && (rightClock > 0 || leftClock < rightClock)))
                        leftState.copyTo(mergedState);
                    else
                        rightState.copyTo(mergedState);
                }
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

        return merged;
    }

    /**
     * Human-readable String from context.
     *
     * @param context counter context.
     * @return a human-readable String of the context.
     */
    public String toString(ByteBuffer context)
    {
        ContextState state = new ContextState(context, headerLength(context));
        StringBuilder sb = new StringBuilder();
        sb.append("[");

        while (state.hasRemaining())
        {
            if (state.elementIdx() > 0)
            {
                sb.append(",");
            }
            sb.append("{");
            sb.append(state.getNodeId().toString()).append(", ");
            sb.append(state.getClock()).append(", ");;
            sb.append(state.getCount());
            sb.append("}");
            if (state.isDelta())
            {
                sb.append("*");
            }
            state.moveToNext();
        }

        sb.append("]");
        return sb.toString();
    }

    /**
     * Returns the aggregated count across all node ids.
     *
     * @param context a counter context
     * @return the aggregated count represented by {@code context}
     */
    public long total(ByteBuffer context)
    {
        long total = 0L;

        // we could use a ContextState but it is easy enough that we avoid the object creation
        for (int offset = context.position() + headerLength(context); offset < context.limit(); offset += STEP_LENGTH)
        {
            long count = context.getLong(offset + NodeId.LENGTH + CLOCK_LENGTH);
            total += count;
        }

        return total;
    }

    /**
     * Mark context to delete delta afterward.
     * Marking is done by multiply #elt by -1 to preserve header length
     * and #elt count in order to clear all delta later.
     *
     * @param context a counter context
     * @return context that marked to delete delta
     */
    public ByteBuffer markDeltaToBeCleared(ByteBuffer context)
    {
        int headerLength = headerLength(context);
        if (headerLength == 0)
            return context;

        ByteBuffer marked = context.duplicate();
        short count = context.getShort(context.position());
        // negate #elt to mark as deleted, without changing its size.
        if (count > 0)
            marked.putShort(marked.position(), (short) (count * -1));
        return marked;
    }

    /**
     * Remove all the delta of a context (i.e, set an empty header).
     *
     * @param context a counter context
     * @return a version of {@code context} where no count are a delta.
     */
    public ByteBuffer clearAllDelta(ByteBuffer context)
    {
        int headerLength = headerLength(context);
        if (headerLength == 0)
            return context;

        ByteBuffer cleaned = ByteBuffer.allocate(context.remaining() - headerLength + HEADER_SIZE_LENGTH);
        cleaned.putShort(cleaned.position(), (short)0);
        ByteBufferUtil.arrayCopy(
                context,
                context.position() + headerLength,
                cleaned,
                cleaned.position() + HEADER_SIZE_LENGTH,
                context.remaining() - headerLength);
        return cleaned;
    }

    public void validateContext(ByteBuffer context) throws MarshalException
    {
        int headerLength = headerLength(context);
        if (headerLength < 0 || (context.remaining() - headerLength) %  STEP_LENGTH != 0)
            throw new MarshalException("Invalid size for a counter context");
    }

    /**
     * Update a MessageDigest with the content of a context.
     * Note that this skips the header entirely since the header information
     * has local meaning only, while digests a meant for comparison across
     * nodes. This means in particular that we always have:
     *  updateDigest(ctx) == updateDigest(clearAllDelta(ctx))
     */
    public void updateDigest(MessageDigest message, ByteBuffer context)
    {
        int hlength = headerLength(context);
        ByteBuffer dup = context.duplicate();
        dup.position(context.position() + hlength);
        message.update(dup);
    }

    /**
     * Checks whether the provided context has a count for the provided
     * NodeId.
     *
     * TODO: since the context is sorted, we could implement a binary search.
     * This is however not called in any critical path and contexts will be
     * fairly small so it doesn't matter much.
     */
    public boolean hasNodeId(ByteBuffer context, NodeId id)
    {
        // we could use a ContextState but it is easy enough that we avoid the object creation
        for (int offset = context.position() + headerLength(context); offset < context.limit(); offset += STEP_LENGTH)
        {
            if (id.equals(NodeId.wrap(context, offset)))
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Compute a new context such that if applied to context yields the same
     * total but with old local node ids nulified and there content merged to
     * the current localNodeId.
     */
    public ByteBuffer computeOldShardMerger(ByteBuffer context, List<NodeId.NodeIdRecord> oldIds, long mergeBefore)
    {
        long now = System.currentTimeMillis();
        int hlength = headerLength(context);
        NodeId localId = NodeId.getLocalId();

        Iterator<NodeId.NodeIdRecord> recordIterator = oldIds.iterator();
        NodeId.NodeIdRecord currRecord = recordIterator.hasNext() ? recordIterator.next() : null;

        ContextState state = new ContextState(context, hlength);
        ContextState foundState = null;

        List<NodeId> toMerge = new ArrayList<NodeId>();
        long mergeTotal = 0;
        while (state.hasRemaining() && currRecord != null)
        {
            assert !currRecord.id.equals(localId);

            NodeId nodeId = state.getNodeId();
            int c = nodeId.compareTo(currRecord.id);

            if (c > 0)
            {
                currRecord = recordIterator.hasNext() ? recordIterator.next() : null;
                continue;
            }

            if (state.isDelta())
            {
                if (state.getClock() < 0)
                {
                    // Already merged shard, waiting to be collected

                    if (nodeId.equals(localId))
                        // we should not get there, but we have been creating problematic context prior to #2968
                        throw new RuntimeException("Current nodeId with a negative clock (likely due to #2968). You need to restart this node with -Dcassandra.renew_counter_id=true to fix.");

                    if (state.getCount() != 0)
                    {
                        // This should not happen, but previous bugs have generated this (#2968 in particular) so fixing it.
                        logger.error(String.format("Invalid counter context (clock is %d and count is %d for NodeId %s), will fix", state.getCount(), state.getCount(), nodeId.toString()));
                        toMerge.add(nodeId);
                        mergeTotal += state.getCount();
                    }
                }
                else if (c == 0)
                {
                    // Found an old id. However, merging an oldId that has just been renewed isn't safe, so
                    // we check that it has been renewed before mergeBefore.
                    if (currRecord.timestamp < mergeBefore)
                    {
                        toMerge.add(nodeId);
                        mergeTotal += state.getCount();
                    }
                }
            }

            if (c == 0)
                currRecord = recordIterator.hasNext() ? recordIterator.next() : null;

            state.moveToNext();
        }
        // Continuing the iteration so that we can repair invalid shards
        while (state.hasRemaining())
        {
            NodeId nodeId = state.getNodeId();
            if (state.isDelta() && state.getClock() < 0)
            {
                if (nodeId.equals(localId))
                    // we should not get there, but we have been creating problematic context prior to #2968
                    throw new RuntimeException("Current nodeId with a negative clock (likely due to #2968). You need to restart this node with -Dcassandra.renew_counter_id=true to fix.");

                if (state.getCount() != 0)
                {
                    // This should not happen, but previous bugs have generated this (#2968 in particular) so fixing it.
                    logger.error(String.format("Invalid counter context (clock is %d and count is %d for NodeId %s), will fix", state.getClock(), state.getCount(), nodeId.toString()));
                    toMerge.add(nodeId);
                    mergeTotal += state.getCount();
                }
            }
            state.moveToNext();
        }

        if (toMerge.isEmpty())
            return null;

        ContextState merger = ContextState.allocate(toMerge.size() + 1, toMerge.size() + 1);
        state.reset();
        int i = 0;
        int removedTotal = 0;
        boolean localWritten = false;
        while (state.hasRemaining())
        {
            NodeId nodeId = state.getNodeId();
            if (nodeId.compareTo(localId) > 0)
            {
                merger.writeElement(localId, 1L, mergeTotal, true);
                localWritten = true;
            }
            else if (i < toMerge.size() && nodeId.compareTo(toMerge.get(i)) == 0)
            {
                long count = state.getCount();
                removedTotal += count;
                merger.writeElement(nodeId, -now - state.getClock(), -count, true);
                ++i;
            }
            state.moveToNext();
        }
        if (!localWritten)
            merger.writeElement(localId, 1L, mergeTotal, true);

        // sanity check
        assert mergeTotal == removedTotal;
        return merger.context;
    }

    /**
     * Remove shards that have been canceled through computeOldShardMerger
     * since a time older than gcBefore.
     * Used by compaction to strip context of unecessary information,
     * shrinking them.
     */
    public ByteBuffer removeOldShards(ByteBuffer context, int gcBefore)
    {
        int hlength = headerLength(context);
        ContextState state = new ContextState(context, hlength);
        int removedShards = 0;
        int removedDelta = 0;
        while (state.hasRemaining())
        {
            long clock = state.getClock();
            if (clock < 0)
            {
                // We should never have a count != 0 when clock < 0.
                // We know that previous may have created those situation though, so:
                //   * for delta shard: we throw an exception since computeOldShardMerger should
                //     have corrected that situation
                //   * for non-delta shard: it is a much more crappier situation because there is
                //     not much we can do since we are not responsible for that shard. So we simply
                //     ignore the shard.
                if (state.getCount() != 0)
                {
                    if (state.isDelta())
                    {
                        throw new IllegalStateException("Counter shard with negative clock but count != 0; context = " + toString(context));
                    }
                    else
                    {
                        logger.debug("Ignoring non-removable non-delta corrupted shard in context " + toString(context));
                        state.moveToNext();
                        continue;
                    }
                }

                if (-((int)(clock / 1000)) < gcBefore)
                {
                    removedShards++;
                    if (state.isDelta())
                        removedDelta++;
                }
            }
            state.moveToNext();
        }

        if (removedShards == 0)
            return context;

        int removedHeaderSize = removedDelta * HEADER_ELT_LENGTH;
        int removedBodySize = removedShards * STEP_LENGTH;
        int newSize = context.remaining() - removedHeaderSize - removedBodySize;
        int newHlength = hlength - removedHeaderSize;
        ByteBuffer cleanedContext = HeapAllocator.instance.allocate(newSize);
        cleanedContext.putShort(cleanedContext.position(), (short) ((newHlength - HEADER_SIZE_LENGTH) / HEADER_ELT_LENGTH));
        ContextState cleaned = new ContextState(cleanedContext, newHlength);

        state.reset();
        while (state.hasRemaining())
        {
            long clock = state.getClock();
            if (clock >= 0 || state.getCount() != 0 || -((int)(clock / 1000)) >= gcBefore)
            {
                state.copyTo(cleaned);
            }

            state.moveToNext();
        }
        return cleanedContext;
    }

    /**
     * Helper class to work on contexts (works by iterating over them).
     * A context being abstractly a list of tuple (nodeid, clock, count), a
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
        private int headerOffset;  // offset from context.position()
        private int bodyOffset;    // offset from context.position()
        private boolean currentIsDelta;

        public ContextState(ByteBuffer context, int headerLength)
        {
            this(context, headerLength, HEADER_SIZE_LENGTH, headerLength, false);
            updateIsDelta();
        }

        public ContextState(ByteBuffer context)
        {
            this(context, headerLength(context));
        }

        private ContextState(ByteBuffer context, int headerLength, int headerOffset, int bodyOffset, boolean currentIsDelta)
        {
            this.context = context;
            this.headerLength = headerLength;
            this.headerOffset = headerOffset;
            this.bodyOffset = bodyOffset;
            this.currentIsDelta = currentIsDelta;
        }

        public boolean isDelta()
        {
            return currentIsDelta;
        }

        private void updateIsDelta()
        {
            currentIsDelta = (headerOffset < headerLength) && context.getShort(context.position() + headerOffset) == (short) elementIdx();
        }

        public boolean hasRemaining()
        {
            return bodyOffset < context.remaining();
        }

        public int remainingHeaderLength()
        {
            return headerLength - headerOffset;
        }

        public int remainingBodyLength()
        {
            return context.remaining() - bodyOffset;
        }

        public void moveToNext()
        {
            bodyOffset += STEP_LENGTH;
            if (currentIsDelta)
            {
                headerOffset += HEADER_ELT_LENGTH;
            }
            updateIsDelta();
        }

        // This advance other to the next position (but not this)
        public void copyTo(ContextState other)
        {
            ByteBufferUtil.arrayCopy(context, context.position() + bodyOffset, other.context, other.context.position() + other.bodyOffset, STEP_LENGTH);
            if (currentIsDelta)
            {
                other.context.putShort(other.context.position() + other.headerOffset, (short) other.elementIdx());
            }
            other.currentIsDelta = currentIsDelta;
            other.moveToNext();
        }

        public int compareIdTo(ContextState other)
        {
            return compareId(context, context.position() + bodyOffset, other.context, other.context.position() + other.bodyOffset);
        }

        public void reset()
        {
            this.headerOffset = HEADER_SIZE_LENGTH;
            this.bodyOffset = headerLength;
            updateIsDelta();
        }

        public NodeId getNodeId()
        {
            return NodeId.wrap(context, context.position() + bodyOffset);
        }

        public long getClock()
        {
            return context.getLong(context.position() + bodyOffset + NodeId.LENGTH);
        }

        public long getCount()
        {
            return context.getLong(context.position() + bodyOffset + NodeId.LENGTH + CLOCK_LENGTH);
        }

        // Advance this to the next position
        public void writeElement(NodeId id, long clock, long count, boolean isDelta)
        {
            writeElementAtOffset(context, context.position() + bodyOffset, id, clock, count);
            if (isDelta)
            {
                context.putShort(context.position() + headerOffset, (short)elementIdx());
            }
            currentIsDelta = isDelta;
            moveToNext();
        }

        public void writeElement(NodeId id, long clock, long count)
        {
            writeElement(id, clock, count, false);
        }

        public int elementIdx()
        {
            return (bodyOffset - headerLength) / STEP_LENGTH;
        }

        public ContextState duplicate()
        {
            return new ContextState(context, headerLength, headerOffset, bodyOffset, currentIsDelta);
        }

        /*
         * Allocate a new context big enough for {@code elementCount} elements
         * with {@code deltaCount} of them being delta, and return the initial
         * ContextState corresponding.
         */
        public static ContextState allocate(int elementCount, int deltaCount)
        {
            return allocate(elementCount, deltaCount, HeapAllocator.instance);
        }

        public static ContextState allocate(int elementCount, int deltaCount, Allocator allocator)
        {
            assert deltaCount <= elementCount;
            int hlength = HEADER_SIZE_LENGTH + deltaCount * HEADER_ELT_LENGTH;
            ByteBuffer context = allocator.allocate(hlength + elementCount * STEP_LENGTH);
            context.putShort(context.position(), (short)deltaCount);
            return new ContextState(context, hlength);
        }
    }
}
