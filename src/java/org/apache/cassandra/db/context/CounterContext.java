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
import java.util.*;

import org.apache.commons.lang.ArrayUtils;

import org.apache.cassandra.db.DBConstants;
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
        idLength   = localId.length;
        stepLength = idLength + clockLength + countLength;
    }

    public static CounterContext instance()
    {
        return LazyHolder.counterContext;
    }

    /**
     * Creates an initial counter context.
     *
     * @return an empty counter context.
     */
    public byte[] create()
    {
        return new byte[0];
    }
    
    // write a tuple (node id, clock, count) at the front
    protected static void writeElement(byte[] context, byte[] id, long clock, long count)
    {
        writeElementAtStepOffset(context, 0, id, clock, count);
    }

    // write a tuple (node id, clock, count) at step offset
    protected static void writeElementAtStepOffset(byte[] context, int stepOffset, byte[] id, long clock, long count)
    {
        int offset = stepOffset * stepLength;
        System.arraycopy(id, 0, context, offset, idLength);
        FBUtilities.copyIntoBytes(context, offset + idLength, clock);
        FBUtilities.copyIntoBytes(context, offset + idLength + clockLength, count);
    }

    public byte[] insertElementAtStepOffset(byte[] context, int stepOffset, byte[] id, long clock, long count)
    {
        int offset = stepOffset * stepLength;
        byte[] newContext = new byte[context.length + stepLength];
        System.arraycopy(context, 0, newContext, 0, offset);
        writeElementAtStepOffset(newContext, stepOffset, id, clock, count);
        System.arraycopy(context, offset, newContext, offset + stepLength, context.length - offset);
        return newContext;
    }

    public byte[] update(byte[] context, InetAddress node, long delta)
    {
        // calculate node id
        byte[] nodeId = node.getAddress();
        int idCount = context.length / stepLength;

        // look for this node id
        for (int stepOffset = 0; stepOffset < idCount; ++stepOffset)
        {
            int offset = stepOffset * stepLength;
            int cmp = FBUtilities.compareByteSubArrays(nodeId, 0, context, offset, idLength);
            if (cmp == 0)
            {
                // node id found: increment clock, update count; shift to front
                long clock = FBUtilities.byteArrayToLong(context, offset + idLength);
                long count = FBUtilities.byteArrayToLong(context, offset + idLength + clockLength);

                writeElementAtStepOffset(context, stepOffset, nodeId, clock + 1L, count + delta);
                return context;
            }
            if (cmp < 0)
            {
                // id at offset is greater that the one we are updating, inserting
                return insertElementAtStepOffset(context, stepOffset, nodeId, 1L, delta);
            }
        }

        // node id not found: adding at the end
        return insertElementAtStepOffset(context, idCount, nodeId, 1L, delta);
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
    public ContextRelationship diff(byte[] left, byte[] right)
    {
        ContextRelationship relationship = ContextRelationship.EQUAL;

        int leftIndex  = 0;
        int rightIndex = 0;
        while (leftIndex < left.length && rightIndex < right.length)
        {
            // compare id bytes
            int compareId = FBUtilities.compareByteSubArrays(left, leftIndex, right, rightIndex, idLength);
            if (compareId == 0)
            {
                long leftClock  = FBUtilities.byteArrayToLong(left,  leftIndex + idLength);
                long rightClock = FBUtilities.byteArrayToLong(right, rightIndex + idLength);

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
        if (leftIndex < left.length)
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
        else if (rightIndex < right.length)
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

    private class CounterNode
    {
        public final long clock;
        public final long count;

        public CounterNode(long clock, long count)
        {
            this.clock = clock;
            this.count = count;
        }

        public int compareClockTo(CounterNode o)
        {
            if (clock == o.clock)
            {
                return 0;
            }
            else if (clock > o.clock)
            {
                return 1;
            }
            // clock < o.clock
            return -1;
        }

        @Override
        public String toString()
        {
            return "(" + clock + "," + count + ")";
        }
    }

    /**
     * Return a context w/ an aggregated count for each node id.
     *
     * @param left
     *            counter context.
     * @param right
     *            counter context.
     */
    public byte[] merge(byte[] left, byte[] right)
    {
        if (left.length > right.length)
        {
            byte[] tmp = right;
            right = left;
            left = tmp;
        }

        // Compute size of result
        int size = 0;
        int leftOffset  = 0;
        int rightOffset = 0;
        while ((leftOffset < left.length) && (rightOffset < right.length))
        {
            int cmp = FBUtilities.compareByteSubArrays(left, leftOffset, right, rightOffset, idLength);
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
        size += (left.length  - leftOffset)  / stepLength;
        size += (right.length - rightOffset) / stepLength;

        byte[] merged = new byte[size * stepLength];

        // Do the actual merge:
        //   a) local id:  sum clocks, counts
        //   b) remote id: keep highest clock, count (reconcile)
        int mergedOffset = 0; leftOffset = 0; rightOffset = 0;
        while ((leftOffset < left.length) && (rightOffset < right.length))
        {
            int cmp = FBUtilities.compareByteSubArrays(left, leftOffset, right, rightOffset, idLength);
            if (cmp == 0)
            {
                // sum for local id, keep highest othewise
                long leftClock = FBUtilities.byteArrayToLong(left, leftOffset + idLength);
                long rightClock = FBUtilities.byteArrayToLong(right, rightOffset + idLength);
                if (FBUtilities.compareByteSubArrays(left, leftOffset, localId, 0, idLength) == 0)
                {
                    long leftCount = FBUtilities.byteArrayToLong(left, leftOffset + idLength + clockLength);
                    long rightCount = FBUtilities.byteArrayToLong(right, rightOffset + idLength + clockLength);
                    writeElementAtStepOffset(merged, mergedOffset / stepLength, localId, leftClock + rightClock, leftCount + rightCount);
                }
                else
                {
                    if (leftClock >= rightClock)
                        System.arraycopy(left, leftOffset, merged, mergedOffset, stepLength);
                    else
                        System.arraycopy(right, rightOffset, merged, mergedOffset, stepLength);
                }
                mergedOffset += stepLength;
                rightOffset += stepLength;
                leftOffset += stepLength;
            }
            else if (cmp > 0)
            {
                System.arraycopy(right, rightOffset, merged, mergedOffset, stepLength);
                mergedOffset += stepLength;
                rightOffset += stepLength;
            }
            else // cmp < 0
            {
                System.arraycopy(left, leftOffset, merged, mergedOffset, stepLength);
                mergedOffset += stepLength;
                leftOffset += stepLength;
            }
        }
        if (leftOffset < left.length)
            System.arraycopy(
                left,
                leftOffset,
                merged,
                mergedOffset,
                left.length - leftOffset);
        if (rightOffset < right.length)
            System.arraycopy(
                right,
                rightOffset,
                merged,
                mergedOffset,
                right.length - rightOffset);

        return merged;
    }

    /**
     * Human-readable String from context.
     *
     * @param context
     *            version context.
     * @return a human-readable String of the context.
     */
    public String toString(byte[] context)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int offset = 0; offset < context.length; offset += stepLength)
        {
            if (offset > 0)
            {
                sb.append(",");
            }
            sb.append("{");
            try
            {
                InetAddress address = InetAddress.getByAddress(
                            ArrayUtils.subarray(context, offset, offset + idLength));
                sb.append(address.getHostAddress());
            }
            catch (UnknownHostException uhe)
            {
                sb.append("?.?.?.?");
            }
            sb.append(", ");
            sb.append(FBUtilities.byteArrayToLong(context, offset + idLength));
            sb.append(", ");
            sb.append(FBUtilities.byteArrayToLong(context, offset + idLength + clockLength));

            sb.append("}");
        }
        sb.append("]");
        return sb.toString();
    }

    // return an aggregated count across all node ids
    public byte[] total(byte[] context)
    {
        long total = 0L;

        for (int offset = 0; offset < context.length; offset += stepLength)
        {
            long count = FBUtilities.byteArrayToLong(context, offset + idLength + clockLength);
            total += count;
        }

        return FBUtilities.toByteArray(total);
    }

    // remove the count for a given node id
    public byte[] cleanNodeCounts(byte[] context, InetAddress node)
    {
        // calculate node id
        byte[] nodeId = node.getAddress();

        // look for this node id
        for (int offset = 0; offset < context.length; offset += stepLength)
        {
            int cmp = FBUtilities.compareByteSubArrays(context, offset, nodeId, 0, idLength);
            if (cmp < 0)
                continue;
            else if (cmp == 0)
            {
                // node id found: remove node count
                byte[] truncatedContext = new byte[context.length - stepLength];
                System.arraycopy(context, 0, truncatedContext, 0, offset);
                System.arraycopy(
                        context,
                        offset + stepLength,
                        truncatedContext,
                        offset,
                        context.length - (offset + stepLength));
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
