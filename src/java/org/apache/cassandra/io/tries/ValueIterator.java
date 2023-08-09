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

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * Thread-unsafe value iterator for on-disk tries. Uses the assumptions of {@link Walker}.
 * <p>
 * The main utility of this class is the {@link #nextPayloadedNode()} method, which lists all nodes that contain a
 * payload within the requested bounds. The treatment of the bounds is non-standard (see
 * {@link #ValueIterator(Rebufferer, long, ByteComparable, ByteComparable, boolean)}), necessary to properly walk
 * tries of prefixes and separators.
 */
@NotThreadSafe
public class ValueIterator<CONCRETE extends ValueIterator<CONCRETE>> extends Walker<CONCRETE>
{
    private final ByteSource limit;
    private final TransitionBytesCollector collector;
    protected IterationPosition stack;
    private long next;

    public static class IterationPosition
    {
        final long node;
        final int limit;
        final IterationPosition prev;
        int childIndex;

        public IterationPosition(long node, int childIndex, int limit, IterationPosition prev)
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

    protected ValueIterator(Rebufferer source, long root, ByteComparable start, ByteComparable end, boolean admitPrefix)
    {
        this(source, root, start, end, admitPrefix, false);
    }

    /**
     * Constrained iterator. The end position is always treated as inclusive, and we have two possible treatments for
     * the start:
     * <ul>
     *   <li> When {@code admitPrefix=false}, exact matches and any prefixes of the start are excluded.
     *   <li> When {@code admitPrefix=true}, the longest prefix of the start present in the trie is also included,
     *        provided that there is no entry in the trie between that prefix and the start. An exact match also
     *        satisfies this and is included.
     * </ul>
     * This behaviour is shared with the reverse counterpart {@link ReverseValueIterator}.
     */
    protected ValueIterator(Rebufferer source, long root, ByteComparable start, ByteComparable end, boolean admitPrefix, boolean collecting)
    {
        super(source, root);
        limit = end != null ? end.asComparableBytes(BYTE_COMPARABLE_VERSION) : null;
        collector = collecting ? new TransitionBytesCollector() : null;

        if (start != null)
            initializeWithLeftBound(root, start.asComparableBytes(BYTE_COMPARABLE_VERSION), admitPrefix, limit != null);
        else
            initializeNoLeftBound(root, limit != null ? limit.next() : 256);
    }

    private void initializeWithLeftBound(long root, ByteSource startStream, boolean admitPrefix, boolean atLimit)
    {
        IterationPosition prev = null;
        int childIndex;
        int limitByte;
        long payloadedNode = -1;

        try
        {
            // Follow start position while we still have a prefix, stacking path and saving prefixes.
            go(root);
            while (true)
            {
                int s = startStream.next();
                childIndex = search(s);

                // For a separator trie the latest payload met along the prefix is a potential match for start
                if (admitPrefix)
                {
                    if (childIndex == 0 || childIndex == -1)
                    {
                        if (hasPayload())
                            payloadedNode = position;
                    }
                    else
                    {
                        payloadedNode = -1;
                    }
                }

                limitByte = 256;
                if (atLimit)
                {
                    limitByte = limit.next();
                    if (s < limitByte)
                        atLimit = false;
                }
                if (childIndex < 0)
                    break;

                prev = new IterationPosition(position, childIndex, limitByte, prev);
                go(transition(childIndex)); // child index is positive, transition must exist
            }

            childIndex = -1 - childIndex - 1;
            stack = new IterationPosition(position, childIndex, limitByte, prev);

            // Advancing now gives us first match if we didn't find one already.
            if (payloadedNode != -1)
                next = payloadedNode;
            else
                next = advanceNode();
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
     * Returns the payload node position without advancing.
     */
    protected long peekNode()
    {
        return next;
    }

    /**
     * Returns the position of the next node with payload contained in the iterated span.
     */
    protected long nextPayloadedNode()
    {
        long toReturn = next;
        if (next != -1)
            next = advanceNode();
        return toReturn;
    }

    protected ByteComparable nextCollectedValue()
    {
        assert collector != null : "Cannot get a collected value from a non-collecting iterator";
        return collector.toByteComparable();
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
                    return -1;
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
