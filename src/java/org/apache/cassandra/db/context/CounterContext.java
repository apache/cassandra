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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.db.DBConstants;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

/**
 * An implementation of a partitioned counter context.
 *
 * The data structure is:
 *   a vector of (node id, logical clock, count) tuples.
 *
 * On update, the node will:
 *   1) increment the logical clock, and
 *   2) update the count associated w/ its tuple.
 *
 * The logical clock represents the # of operations executed
 * by the node.  The aggregated count can be determined
 * by rolling up all the counts from each tuple.
 *
 * NOTE: only a given node id may increment its associated count and
 * care must be taken to ensure that tuples are correctly made consistent.
 */
public class CounterContext implements IContext
{
    private static final int idLength;
    private static final byte[] localId;
    private static final ByteBuffer wrappedLocalId;
    private static final int clockLength = DBConstants.longSize_;
    private static final int countLength = DBConstants.longSize_;
    private static final int stepLength; // length: id + logical clock + count

    // lazy-load singleton
    private static class LazyHolder
    {
        private static final CounterContext counterContext = new CounterContext();
    }

    static
    {
        localId  = FBUtilities.getLocalAddress().getAddress();
        wrappedLocalId = ByteBuffer.wrap(localId);
        idLength   = localId.length;
        stepLength = idLength + clockLength + countLength;
    }

    public static CounterContext instance()
    {
        return LazyHolder.counterContext;
    }

    /**
     * Creates an initial counter context with an initial value for the local node with.
     *
     * @param value the value for this initial update
     *
     * @return an empty counter context.
     */
    public ByteBuffer create(long value)
    {
        ByteBuffer context = ByteBuffer.allocate(stepLength);
        writeElementAtOffset(context, 0, localId, 1L, value);
        return context;
    }

    // For testing purposes
    public ByteBuffer create(byte[] id, long clock, long value)
    {
        ByteBuffer context = ByteBuffer.allocate(stepLength);
        writeElementAtOffset(context, 0, id, clock, value);
        return context;
    }

    // write a tuple (node id, clock, count) at offset
    protected static void writeElementAtOffset(ByteBuffer context, int offset, byte[] id, long clock, long count)
    {
        System.arraycopy(id, 0, context.array(), offset + context.arrayOffset(), idLength);
        context.putLong(offset + idLength, clock);
        context.putLong(offset + idLength + clockLength, count);
    }

    /**
     * Determine the count relationship between two contexts.
     *
     * EQUAL:        Equal set of nodes and every count is equal.
     * GREATER_THAN: Superset of nodes and every count is equal or greater than its corollary.
     * LESS_THAN:    Subset of nodes and every count is equal or less than its corollary.
     * DISJOINT:     Node sets are not equal and/or counts are not all greater or less than.
     *
     * Strategy:
     *   compare node logical clocks (like a version vector).
     *
     * @param left
     *            counter context.
     * @param right
     *            counter context.
     * @return the ContextRelationship between the contexts.
     */
    public ContextRelationship diff(ByteBuffer left, ByteBuffer right)
    {
        ContextRelationship relationship = ContextRelationship.EQUAL;

        int leftIndex  = left.position();
        int rightIndex = right.position();
        while (leftIndex < left.remaining() && rightIndex < right.remaining())
        {
            // compare id bytes
            int compareId = ByteBufferUtil.compareSubArrays(left, leftIndex, right, rightIndex, idLength);
            if (compareId == 0)
            {
                long leftClock  = left.getLong(leftIndex + idLength);
                long rightClock = right.getLong(rightIndex + idLength);

                // advance indexes
                leftIndex  += stepLength;
                rightIndex += stepLength;

                // process clock comparisons
                if (leftClock == rightClock)
                {
                    continue;
                }
                else if (leftClock > rightClock)
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
                // leftClock < rightClock
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
                rightIndex += stepLength;

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
            else
            {
                // compareId < 0
                // only advance the left context
                leftIndex += stepLength;

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
        if (leftIndex < left.remaining())
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
        else if (rightIndex < right.remaining())
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
     * @param left
     *            counter context.
     * @param right
     *            counter context.
     */
    public ByteBuffer merge(ByteBuffer left, ByteBuffer right)
    {
        // Compute size of result
        int size = 0;
        int leftOffset  = left.position();
        int rightOffset = right.position();
        while ((leftOffset < left.limit()) && (rightOffset < right.limit()))
        {
            int cmp = ByteBufferUtil.compareSubArrays(left, leftOffset, right, rightOffset, idLength);
            if (cmp == 0)
            {
                ++size;
                rightOffset += stepLength;
                leftOffset += stepLength;
            }
            else if (cmp > 0)
            {
                ++size;
                rightOffset += stepLength;
            }
            else // cmp < 0
            {
                ++size;
                leftOffset += stepLength;
            }
        }
        size += (left.limit() - leftOffset)  / stepLength;
        size += (right.limit() - rightOffset) / stepLength;

        ByteBuffer merged = ByteBuffer.allocate(size * stepLength);

        // Do the actual merge:
        //   a) local id:  sum clocks, counts
        //   b) remote id: keep highest clock, count (reconcile)
        int mergedOffset = merged.position();
        leftOffset = left.position();
        rightOffset = right.position();
        while ((leftOffset < left.limit()) && (rightOffset < right.limit()))
        {
            int cmp = ByteBufferUtil.compareSubArrays(left, leftOffset, right, rightOffset, idLength);
            if (cmp == 0)
            {
                // sum for local id, keep highest othewise
                long leftClock = left.getLong(leftOffset + idLength);
                long rightClock = right.getLong(rightOffset + idLength);
                if (ByteBufferUtil.compareSubArrays(left, leftOffset, wrappedLocalId, 0, idLength) == 0)
                {
                    long leftCount = left.getLong(leftOffset + idLength + clockLength);
                    long rightCount = right.getLong(rightOffset + idLength + clockLength);
                    writeElementAtOffset(merged, mergedOffset, localId, leftClock + rightClock, leftCount + rightCount);
                }
                else
                {
                    if (leftClock >= rightClock)
                        ByteBufferUtil.arrayCopy(left, leftOffset, merged, mergedOffset, stepLength);
                    else
                        ByteBufferUtil.arrayCopy(right, rightOffset, merged, mergedOffset, stepLength);
                }
                mergedOffset += stepLength;
                rightOffset += stepLength;
                leftOffset += stepLength;
            }
            else if (cmp > 0)
            {
                ByteBufferUtil.arrayCopy(right, rightOffset, merged, mergedOffset, stepLength);
                mergedOffset += stepLength;
                rightOffset += stepLength;
            }
            else // cmp < 0
            {
                ByteBufferUtil.arrayCopy(left, leftOffset, merged, mergedOffset, stepLength);
                mergedOffset += stepLength;
                leftOffset += stepLength;
            }
        }
        if (leftOffset < left.limit())
            ByteBufferUtil.arrayCopy(
                left,
                leftOffset,
                merged,
                mergedOffset,
                left.limit() - leftOffset);
        if (rightOffset < right.limit())
            ByteBufferUtil.arrayCopy(
                right,
                rightOffset,
                merged,
                mergedOffset,
                right.limit() - rightOffset);

        return merged;
    }

    /**
     * Human-readable String from context.
     *
     * @param context
     *            version context.
     * @return a human-readable String of the context.
     */
    public String toString(ByteBuffer context)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int offset = context.position(); offset < context.limit(); offset += stepLength)
        {
            if (offset > context.position())
            {
                sb.append(",");
            }
            sb.append("{");
            try
            {
                int absOffset = context.arrayOffset() + offset;
                InetAddress address = InetAddress.getByAddress(
                            ArrayUtils.subarray(context.array(), absOffset, absOffset + idLength));
                sb.append(address.getHostAddress());
            }
            catch (UnknownHostException uhe)
            {
                sb.append("?.?.?.?");
            }
            sb.append(", ");
            sb.append(context.getLong(offset + idLength));
            sb.append(", ");
            sb.append(context.getLong(offset + idLength + clockLength));

            sb.append("}");
        }
        sb.append("]");
        return sb.toString();
    }

    // return an aggregated count across all node ids
    public long total(ByteBuffer context)
    {
        long total = 0L;

        for (int offset = context.position(); offset < context.limit(); offset += stepLength)
        {
            long count = context.getLong(offset + idLength + clockLength);
            total += count;
        }

        return total;
    }

    // remove the count for a given node id
    public ByteBuffer cleanNodeCounts(ByteBuffer context, InetAddress node)
    {
        // calculate node id
        ByteBuffer nodeId = ByteBuffer.wrap(node.getAddress());

        // look for this node id
        for (int offset = 0; offset < context.remaining(); offset += stepLength)
        {
            int cmp = ByteBufferUtil.compareSubArrays(context, context.position() + offset, nodeId, 0, idLength);
            if (cmp < 0)
                continue;
            else if (cmp == 0)
            {
                // node id found: remove node count
                ByteBuffer truncatedContext = ByteBuffer.allocate(context.remaining() - stepLength);
                ByteBufferUtil.arrayCopy(context, context.position(), truncatedContext, 0, offset);
                ByteBufferUtil.arrayCopy(
                        context,
                        context.position() + offset + stepLength,
                        truncatedContext,
                        offset,
                        context.remaining() - (offset + stepLength));
                return truncatedContext;
            }
            else // cmp > 0
            {
                break; // node id not present
            }
        }

        return context;
    }
}
