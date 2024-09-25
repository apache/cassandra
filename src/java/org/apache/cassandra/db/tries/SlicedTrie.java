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

package org.apache.cassandra.db.tries;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * Represents a sliced view of a trie, i.e. the content within the given pair of bounds.
 *
 * Applied by advancing three tries in parallel: the left bound, the source and the right bound. While the source
 * bound is smallest, we don't issue any content and skip over any children. As soon as the left bound becomes strictly
 * smaller, we stop processing it (as it's a singleton trie it will remain smaller until it's exhausted) and start
 * issuing the nodes and content from the source. As soon as the right bound becomes strictly smaller, we finish the
 * walk.
 *
 * We don't explicitly construct tries for the two bounds; tracking the current depth (= prefix length) and transition
 * as characters are requested from the key is sufficient as it is a trie with just a single descent path. Because we
 * need the next character to tell if it's been exhausted, we keep these one position ahead. The source is always
 * advanced, thus this gives us the thing to compare it against after the advance.
 *
 * We also track the current state to make some decisions a little simpler.
 *
 * See Trie.md for further details.
 */
public class SlicedTrie<T> extends Trie<T>
{
    private final Trie<T> source;

    /** Left-side boundary. The characters of this are requested as we descend along the left-side boundary. */
    private final ByteComparable left;

    /** Right-side boundary. The characters of this are requested as we descend along the right-side boundary. */
    private final ByteComparable right;

    private final boolean includeLeft;
    private final boolean includeRight;

    public SlicedTrie(Trie<T> source, ByteComparable left, boolean includeLeft, ByteComparable right, boolean includeRight)
    {
        this.source = source;
        this.left = left;
        this.right = right;
        this.includeLeft = includeLeft;
        this.includeRight = includeRight;
    }

    static ByteSource add0(ByteSource src)
    {
        return new ByteSource()
        {
            boolean done = false;

            @Override
            public int next()
            {
                if (done)
                    return END_OF_STREAM;
                int next = src.next();
                if (next != END_OF_STREAM)
                    return next;
                done = true;
                return 0;
            }
        };
    }

    static ByteSource openAndMaybeAdd0(ByteComparable key, boolean shouldAdd0)
    {
        if (key == null)
            return null;
        ByteSource src = key.asComparableBytes(Trie.BYTE_COMPARABLE_VERSION);
        if (shouldAdd0)
            return add0(src);
        else
            return src;
    }

    @Override
    protected Cursor<T> cursor(Direction direction)
    {
        // The cursor is left-inclusive and right-exclusive by default. If we need to change the inclusiveness, adjust
        // the bound to the next possible value by adding a 00 byte at the end.
        ByteSource leftSource = openAndMaybeAdd0(left, !includeLeft);
        ByteSource rightSource = openAndMaybeAdd0(right, includeRight);

        // Empty left bound is the same as having no left bound, adjust for that.
        int leftNext = -1;
        if (leftSource != null)
        {
            leftNext = leftSource.next();
            if (leftNext == ByteSource.END_OF_STREAM)
                leftSource = null;
        }

        // Empty right bound means the result can only be empty. Make things easier for the cursor by handling this.
        int rightNext = -1;
        if (rightSource != null)
        {
            rightNext = rightSource.next();
            if (rightNext == ByteSource.END_OF_STREAM)
            {
                assert leftSource == null : "Invalid range " + sliceString();
                return Trie.<T>empty().cursor(direction);
            }
        }

        return new SlicedCursor<>(source.cursor(direction),
                                  leftSource,
                                  leftNext,
                                  rightSource,
                                  rightNext,
                                  direction);
    }

    String sliceString()
    {
        return String.format("%s%s;%s%s",
                             includeLeft ? "[" : "(",
                             left.byteComparableAsString(Trie.BYTE_COMPARABLE_VERSION),
                             right.byteComparableAsString(Trie.BYTE_COMPARABLE_VERSION),
                             includeRight ? "]" : ")");
    }

    private enum State
    {
        /**
         * The cursor is at the initial phase while it is walking prefixes of both bounds.
         * Content is not to be reported.
         */
        COMMON_PREFIX,
        /**
         * The cursor is positioned on some prefix of the start bound, strictly before any prefix of the end bound in
         * iteration order.
         * Content should only be reported in the reverse direction (as these prefixes are prefixes of the right bound
         * and included in the slice).
         */
        START_PREFIX,
        /**
         * The cursor is positioned inside the range, i.e. strictly between any prefixes of the start and end bounds.
         * All content should be reported.
         */
        INSIDE,
        /**
         * The cursor is positioned on some prefix of the end bound, strictly after any prefix of the start bound.
         * Content should only be reported in the forward direction.
         */
        END_PREFIX,
        /** The cursor is positioned beyond the end bound. Exhaustion (depth -1) has been reported. */
        EXHAUSTED;
    }

    private static class SlicedCursor<T> implements Cursor<T>
    {
        private final ByteSource start;
        private final ByteSource end;
        private final Cursor<T> source;
        private final Direction direction;

        private State state;
        private int startNext;
        private int startNextDepth;
        private int endNext;
        private int endNextDepth;

        public SlicedCursor(Cursor<T> source,
                            ByteSource leftSource,
                            int leftNext,
                            ByteSource rightSource,
                            int rightNext,
                            Direction direction)
        {
            this.source = source;
            this.direction = direction;
            start = direction.select(leftSource, rightSource);
            end = direction.select(rightSource, leftSource);
            startNext = direction.select(leftNext, rightNext);
            endNext = direction.select(rightNext, leftNext);
            startNextDepth = start != null ? 1 : 0;
            endNextDepth = end != null ? 1 : 0;
            state = start != null
                    ? end != null
                      ? State.COMMON_PREFIX
                      : State.START_PREFIX
                    : end != null
                      ? State.END_PREFIX
                      : State.INSIDE;
        }

        @Override
        public int advance()
        {
            int newDepth = source.advance();
            int transition = source.incomingTransition();

            switch (state)
            {
                case COMMON_PREFIX:
                case START_PREFIX:
                    // Skip any transitions before the start bound
                    while (newDepth == startNextDepth && direction.lt(transition, startNext))
                    {
                        newDepth = source.skipChildren();
                        transition = source.incomingTransition();
                    }

                    // Check if we are still following the start bound
                    if (newDepth == startNextDepth && transition == startNext)
                    {
                        assert startNext != ByteSource.END_OF_STREAM;
                        startNext = start.next();
                        ++startNextDepth;
                        State currState = state;
                        // In the forward direction the exact match for the left bound and all descendant states are
                        // included in the set.
                        // In the reverse direction we will instead use the -1 as target transition and thus ascend on
                        // the next advance (skipping the exact right bound and all its descendants).
                        if (startNext == ByteSource.END_OF_STREAM && direction.isForward())
                            state = State.INSIDE; // checkEndBound may adjust this to END_PREFIX
                        if (currState == State.START_PREFIX)
                            return newDepth;   // there is no need to check the end bound as we descended along a
                                               // strictly earlier path
                    }
                    else // otherwise we are beyond the start bound
                        state = State.INSIDE; // checkEndBound may adjust this to END_PREFIX
                    // pass through
                case INSIDE:
                case END_PREFIX:
                    return checkEndBound(newDepth, transition);
                default:
                    throw new AssertionError();
            }
        }

        private int markDone()
        {
            state = State.EXHAUSTED;
            return -1;
        }

        private int checkEndBound(int newDepth, int transition)
        {
            // Cursor positions compare by depth descending and transition ascending.
            if (newDepth > endNextDepth)
                return newDepth;    // happy and quick path in the interior of the slice
                                    // (state == State.INSIDE can be asserted here (we skip it for efficiency))
            if (newDepth < endNextDepth)
                return markDone();
            // newDepth == endDepth
            if (direction.lt(transition, endNext))
            {
                adjustStateStrictlyBeforeEnd();
                return newDepth;
            }
            if (direction.lt(endNext, transition))
                return markDone();

            // Following end bound
            endNext = end.next();
            ++endNextDepth;
            if (endNext == ByteSource.END_OF_STREAM)
            {
                // At the exact end bound.
                if (direction.isForward())
                {
                    // In forward direction the right bound is not included in the slice.
                    return markDone();
                }
                else
                {
                    // In reverse, the left bound and all its descendants are included, thus we use the -1 as limiting
                    // transition. We can also see the bound as strictly ahead of our current position as the current
                    // branch should be fully included.
                    adjustStateStrictlyBeforeEnd();
                }
            }
            else
                adjustStateAtEndPrefix();
            return newDepth;
        }

        private void adjustStateAtEndPrefix()
        {
            switch (state)
            {
                case INSIDE:
                    state = State.END_PREFIX;
                    break;
            }
        }

        private void adjustStateStrictlyBeforeEnd()
        {
            switch (state)
            {
                case COMMON_PREFIX:
                    state = State.START_PREFIX;
                    break;
                case END_PREFIX:
                    state = State.INSIDE;
                    break;
            }
        }

        @Override
        public int advanceMultiple(TransitionsReceiver receiver)
        {
            switch (state)
            {
                case COMMON_PREFIX:
                case START_PREFIX:
                case END_PREFIX:
                    return advance();   // descend only one level to be able to compare cursors correctly
                case INSIDE:
                    int depth = source.depth();
                    int newDepth = source.advanceMultiple(receiver);
                    if (newDepth > depth)
                        return newDepth;    // successfully descended
                    // we ascended, check if we are still within boundaries
                    return checkEndBound(newDepth, source.incomingTransition());
                default:
                    throw new AssertionError();
            }
        }

        @Override
        public int skipChildren()
        {
            switch (state)
            {
                case START_PREFIX:
                    // Skipping children takes us beyond the start path.
                    state = State.INSIDE;
                case INSIDE:
                    // Check that we are still inside after we skip.
                    return checkEndBound(source.skipChildren(), source.incomingTransition());
                case COMMON_PREFIX:
                case END_PREFIX:
                    // The skip takes us beyond the end bound; we are done.
                    return markDone();
                default:
                    throw new AssertionError();
            }
        }

        @Override
        public int depth()
        {
            return state == State.EXHAUSTED ? -1 : source.depth();
        }

        @Override
        public int incomingTransition()
        {
            return source.incomingTransition();
        }

        @Override
        public T content()
        {
            switch (state)
            {
                case INSIDE:
                    return source.content();
                // Additionally, prefixes of the right bound (which are not prefixes of the left) need to be reported:
                case START_PREFIX:
                    // start prefixes in reverse direction (but making sure we don't report the exact match);
                    return !direction.isForward() && startNext != ByteSource.END_OF_STREAM ? source.content() : null;
                case END_PREFIX:
                    // end prefixes in forward direction.
                    return direction.isForward() ? source.content() : null;
                default:
                    return null;
            }
        }
    }
}
