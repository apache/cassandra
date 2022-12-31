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

import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * Thread-unsafe value iterator for on-disk tries. Uses the assumptions of Walker.
 */
public class ValueIterator<CONCRETE extends ValueIterator<CONCRETE>> extends Walker<CONCRETE>
{
    private final ByteSource limit;
    private IterationPosition stack;
    private long next;

    static class IterationPosition
    {
        long node;
        int childIndex;
        int limit;
        IterationPosition prev;

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
        super(source, root);
        limit = null;
        initializeNoLeftBound(root, 256);
    }

    protected ValueIterator(Rebufferer source, long root, ByteComparable start, ByteComparable end, boolean admitPrefix)
    {
        super(source, root);
        limit = end != null ? end.asComparableBytes(BYTE_COMPARABLE_VERSION) : null;

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
                        if (payloadFlags() != 0)
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
                go(transition(childIndex));
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
            if (payloadFlags() != 0)
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
        if (next != -1)
            next = advanceNode();
        return toReturn;
    }

    private long advanceNode()
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
                if (stack == null)        // exhausted whole trie
                    return -1;
                go(stack.node);
                continue;
            }

            child = transition(childIndex);

            if (child != -1)
            {
                assert child >= 0 : String.format("Expected value >= 0 but got %d - %s", child, this);

                // descend
                go(child);

                int l = 256;
                if (transitionByte == stack.limit)
                    l = limit.next();

                stack.childIndex = childIndex;
                stack = new IterationPosition(child, -1, l, stack);

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
