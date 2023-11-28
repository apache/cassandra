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
package org.apache.cassandra.io.tries;

import java.util.function.LongSupplier;
import java.util.function.Supplier;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * Thread-unsafe value iterator for on-disk tries. Uses the assumptions of {@link Walker}.
 * <p>
 * The main utility of this class is the {@link #nextPayloadedNode()} method, which lists all nodes that contain a
 * payload within the requested bounds. The treatment of the bounds is non-standard (see
 * {@link #ValueIterator(Rebufferer, long, ByteComparable, ByteComparable, LeftBoundTreatment, boolean)}), necessary to
 * properly walk tries of prefixes and separators.
 */
@NotThreadSafe
public class ValueIterator<CONCRETE extends ValueIterator<CONCRETE>> extends Walker<CONCRETE>
{
    private final ByteSource limit;
    private final TransitionBytesCollector collector;
    protected IterationPosition stack;
    private long next;

    protected enum LeftBoundTreatment
    {
        ADMIT_PREFIXES,
        ADMIT_EXACT,
        GREATER
    }

    protected static class IterationPosition
    {
        final long node;
        final int limit;
        final IterationPosition prev;
        int childIndex;

        IterationPosition(long node, int childIndex, int limit, IterationPosition prev)
        {
            super();
            this.node = node;
            this.childIndex = childIndex;
            this.limit = limit;
            this.prev = prev;
        }

        @Override
        public String toString()
        {
            return String.format("[Node %d, child %d, limit %d]", node, childIndex, limit);
        }
    }

    protected ValueIterator(Rebufferer source, long root)
    {
        this(source, root, false);
    }

    protected ValueIterator(Rebufferer source, long root, boolean collecting)
    {
        super(source, root);
        limit = null;
        collector = collecting ? new TransitionBytesCollector() : null;
        initializeNoLeftBound(root, 256);
    }

    protected ValueIterator(Rebufferer source, long root, ByteComparable start, ByteComparable end, LeftBoundTreatment admitPrefix)
    {
        this(source, root, start, end, admitPrefix, false);
    }

    /**
     * Constrained iterator. The end position is always treated as inclusive, and we have three possible treatments for
     * the start, specified in admitPrefix:
     * <ul>
     *   <li> When {@code GREATER}, exact matches and any prefixes of the start are excluded.
     *   <li> When {@code ADMIT_EXACT}, exact matches are included.
     *   <li> When {@code ADMIT_PREFIXES}, the longest prefix of the start present in the trie is also included,
     *        provided that there is no entry in the trie between that prefix and the start. An exact match also
     *        satisfies this and is included.
     * </ul>
     * This behaviour is shared with the reverse counterpart {@link ReverseValueIterator}.
     */
    protected ValueIterator(Rebufferer source, long root, ByteComparable start, ByteComparable end, LeftBoundTreatment admitPrefix, boolean collecting)
    {
        super(source, root);
        limit = end != null ? end.asComparableBytes(BYTE_COMPARABLE_VERSION) : null;
        collector = collecting ? new TransitionBytesCollector() : null;

        if (start != null)
            initializeWithLeftBound(root, start.asComparableBytes(BYTE_COMPARABLE_VERSION), admitPrefix, limit != null);
        else
            initializeNoLeftBound(root, limit != null ? limit.next() : 256);
    }

    private void initializeWithLeftBound(long root, ByteSource start, LeftBoundTreatment admitPrefix, boolean atLimit)
    {
        try
        {
            descendWith(start.next(), start, atLimit ? limit.next() : 256, null, root, admitPrefix);
        }
        catch (Throwable t)
        {
            super.close();
            throw t;
        }
    }

    private void initializeNoLeftBound(long root, int limitByte)
    {
        stack = new IterationPosition(root, -1, limitByte, null);

        try
        {
            go(root);
            if (hasPayload())
                next = root;
            else
                next = advanceNode();
        }
        catch (Throwable t)
        {
            super.close();
            throw t;
        }
    }

    /**
     * Returns the payload node position.
     *
     * This method must be async-read-safe, see {@link #advanceNode()}.
     */
    protected long nextPayloadedNode()
    {
        long toReturn = next;
        if (next != NONE)
            next = advanceNode();
        return toReturn;
    }

    protected boolean hasNext()
    {
        return next != NONE;
    }

    protected <VALUE> VALUE nextValue(Supplier<VALUE> supplier)
    {
        if (next == NONE)
            return null;
        go(next);
        VALUE result = supplier.get();
        next = advanceNode();
        return result;
    }

    protected long nextValueAsLong(LongSupplier supplier, long valueIfNone)
    {
        if (next == NONE)
            return valueIfNone;
        go(next);
        long result = supplier.getAsLong();
        next = advanceNode();
        return result;
    }

    protected ByteComparable collectedKey()
    {
        assert collector != null : "Cannot get a collected value from a non-collecting iterator";
        return collector.toByteComparable();
    }

    /**
     * Skip to the given key or the closest after it in iteration order. Inclusive when admitPrefix = ADMIT_EXACT,
     * exclusive when GREATER (ADMIT_PREFIXES is not supported).
     * Requires that the iterator is collecting bytes.
     * To get the next entry, use getNextPayloadedNode as normal.
     */
    protected void skipTo(ByteComparable skipTo, LeftBoundTreatment admitPrefix)
    {
        assert skipTo != null;
        assert collector != null : "Cannot skip without collecting bytes";
        // TODO: Figure out what you need to know to say if an earlier prefix would still be acceptable
        // to support skipping with ADMIT_PREFIXES.
        assert admitPrefix != LeftBoundTreatment.ADMIT_PREFIXES : "Skipping with ADMIT_PREFIXES is not supported";
        if (stack == null)
            return; // exhausted whole trie
        ByteSource skipToBytes = skipTo.asComparableBytes(BYTE_COMPARABLE_VERSION);
        int pos;
        int nextByte = skipToBytes.next();
        final int collectedLength = collector.pos;
        final byte[] collectedBytes = collector.bytes;
        for (pos = 0; pos < collectedLength; ++pos)
        {
            if (nextByte != collectedBytes[pos])
            {
                if (nextByte < collectedBytes[pos])
                    return;    // the position we are already advanced to is beyond skipTo
                else
                    break;     // matched a prefix of skipTo, now we need to advance through the rest of it
            }
            nextByte = skipToBytes.next();
        }
        int upLevels = collectedLength - pos;
        IterationPosition stack = this.stack;
        for (int i = 0; i < upLevels; ++i)
            stack = stack.prev;
        collector.pos = pos;

        descendWith(nextByte, skipToBytes, stack.limit, stack.prev, stack.node, admitPrefix);
    }

    private void descendWith(int skipToFirstByte, ByteSource skipToRest, int limitByte, IterationPosition stackPrev, long startNode, LeftBoundTreatment admitPrefix)
    {
        int childIndex;
        long payloadedNode = NONE;
        // Follow start position while we still have a prefix, stacking path and saving prefixes.
        go(startNode);
        while (true)
        {
            childIndex = search(skipToFirstByte);

            // For a separator trie the latest payload met along the prefix is a potential match for start
            payloadedNode = maybeCollectPayloadedNode(admitPrefix, childIndex, payloadedNode);

            if (childIndex < 0)
                break;

            stackPrev = new IterationPosition(position, childIndex, limitByte, stackPrev);
            if (collector != null)
                collector.add(skipToFirstByte);
            go(transition(childIndex)); // child index is positive, transition must exist

            if (limitByte < 256)
            {
                if (skipToFirstByte == limitByte)
                    limitByte = limit.next();
                else if (skipToFirstByte < limitByte)
                    limitByte = 256;
                else // went beyond the limit
                {
                    stack = null;
                    next = NONE;
                    return;
                }
            }
            skipToFirstByte = skipToRest.next();
        }

        childIndex = -1 - childIndex - 1;
        stack = new IterationPosition(position, childIndex, limitByte, stackPrev);

        // Advancing now gives us first match if we didn't find one already.
        payloadedNode = maybeAcceptPayloadedNode(admitPrefix, skipToFirstByte, payloadedNode);
        if (payloadedNode != NONE)
            next = payloadedNode;
        else
            next = advanceNode();
    }

    private long maybeAcceptPayloadedNode(LeftBoundTreatment admitPrefix, int trailingByte, long payloadedNode)
    {
        switch (admitPrefix)
        {
            case ADMIT_PREFIXES:
                return payloadedNode;
            case ADMIT_EXACT:
                if (trailingByte == ByteSource.END_OF_STREAM && hasPayload())
                    return position;
                // else fall through
            case GREATER:
            default:
                return NONE;
        }
    }

    private long maybeCollectPayloadedNode(LeftBoundTreatment admitPrefix, int childIndex, long payloadedNode)
    {
        if (admitPrefix == LeftBoundTreatment.ADMIT_PREFIXES)
        {
            if (childIndex == 0 || childIndex == -1)
            {
                if (hasPayload())
                    payloadedNode = position;
            }
            else
            {
                payloadedNode = NONE;
            }
        }
        return payloadedNode;
    }

    protected long advanceNode()
    {
        long child;
        int transitionByte;

        go(stack.node);
        while (true)
        {
            int childIndex = stack.childIndex + 1;
            transitionByte = transitionByte(childIndex);

            if (transitionByte > stack.limit)
            {
                // ascend
                stack = stack.prev;
                if (collector != null)
                    collector.pop();
                if (stack == null)        // exhausted whole trie
                    return NONE;
                go(stack.node);
                continue;
            }

            child = transition(childIndex);

            if (child != NONE)
            {
                assert child >= 0 : String.format("Expected value >= 0 but got %d - %s", child, this);

                // descend
                go(child);

                int l = 256;
                if (transitionByte == stack.limit)
                    l = limit.next();

                stack.childIndex = childIndex;
                stack = new IterationPosition(child, -1, l, stack);
                if (collector != null)
                    collector.add(transitionByte);

                if (payloadFlags() != 0)
                    return child;
            }
            else
            {
                stack.childIndex = childIndex;
            }
        }
    }
}
