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

    @Override
    protected Cursor<T> cursor()
    {
        return new SlicedCursor<>(this);
    }

    private enum State
    {
        /** The cursor is still positioned on some prefix of the left bound. Content should not be produced. */
        BEFORE_LEFT,
        /** The cursor is positioned inside the range, i.e. beyond the left bound, possibly on a prefix of the right. */
        INSIDE,
        /** The cursor is positioned beyond the right bound. Exhaustion (depth -1) has been reported. */
        AFTER_RIGHT
    }

    private static class SlicedCursor<T> implements Cursor<T>
    {
        private final ByteSource left;
        private final ByteSource right;
        private final boolean includeLeft;
        private final boolean excludeRight;
        private final Cursor<T> source;

        private State state;
        private int leftNext;
        private int leftNextDepth;
        private int rightNext;
        private int rightNextDepth;

        public SlicedCursor(SlicedTrie<T> slicedTrie)
        {
            source = slicedTrie.source.cursor();
            if (slicedTrie.left != null)
            {
                left = slicedTrie.left.asComparableBytes(BYTE_COMPARABLE_VERSION);
                includeLeft = slicedTrie.includeLeft;
                leftNext = left.next();
                leftNextDepth = 1;
                if (leftNext == ByteSource.END_OF_STREAM && includeLeft)
                    state = State.INSIDE;
                else
                    state = State.BEFORE_LEFT;
            }
            else
            {
                left = null;
                includeLeft = true;
                state = State.INSIDE;
            }

            if (slicedTrie.right != null)
            {
                right = slicedTrie.right.asComparableBytes(BYTE_COMPARABLE_VERSION);
                excludeRight = !slicedTrie.includeRight;
                rightNext = right.next();
                rightNextDepth = 1;
                if (rightNext == ByteSource.END_OF_STREAM && excludeRight)
                    state = State.BEFORE_LEFT;  // This is a hack, we are after the right bound but we don't want to
                                                // report depth -1 yet. So just make sure root's content is not reported.
            }
            else
            {
                right = null;
                excludeRight = true;
                rightNextDepth = 0;
            }
        }

        @Override
        public int advance()
        {
            assert (state != State.AFTER_RIGHT);

            int newDepth = source.advance();
            int transition = source.incomingTransition();

            if (state == State.BEFORE_LEFT)
            {
                // Skip any transitions before the left bound
                while (newDepth == leftNextDepth && transition < leftNext)
                {
                    newDepth = source.skipChildren();
                    transition = source.incomingTransition();
                }

                // Check if we are still following the left bound
                if (newDepth == leftNextDepth && transition == leftNext)
                {
                    assert leftNext != ByteSource.END_OF_STREAM;
                    leftNext = left.next();
                    ++leftNextDepth;
                    if (leftNext == ByteSource.END_OF_STREAM && includeLeft)
                        state = State.INSIDE; // report the content on the left bound
                }
                else // otherwise we are beyond it
                    state = State.INSIDE;
            }

            return checkRightBound(newDepth, transition);
        }

        private int markDone()
        {
            state = State.AFTER_RIGHT;
            return -1;
        }

        private int checkRightBound(int newDepth, int transition)
        {
            // Cursor positions compare by depth descending and transition ascending.
            if (newDepth > rightNextDepth)
                return newDepth;
            if (newDepth < rightNextDepth)
                return markDone();
            // newDepth == rightDepth
            if (transition < rightNext)
                return newDepth;
            if (transition > rightNext)
                return markDone();

            // Following right bound
            rightNext = right.next();
            ++rightNextDepth;
            if (rightNext == ByteSource.END_OF_STREAM && excludeRight)
                return markDone();  // do not report any content on the right bound
            return newDepth;
        }

        @Override
        public int advanceMultiple(TransitionsReceiver receiver)
        {
            switch (state)
            {
                case BEFORE_LEFT:
                    return advance();   // descend only one level to be able to compare cursors correctly
                case INSIDE:
                    int depth = source.depth();
                    if (depth == rightNextDepth - 1)  // this is possible because right is already advanced;
                        return advance();   // we need to check next byte against right boundary in this case
                    int newDepth = source.advanceMultiple(receiver);
                    if (newDepth > depth)
                        return newDepth;    // successfully advanced
                    // we ascended, check if we are still within boundaries
                    return checkRightBound(newDepth, source.incomingTransition());
                default:
                    throw new AssertionError();
            }
        }

        @Override
        public int skipChildren()
        {
            assert (state != State.AFTER_RIGHT);

            // We are either inside or following the left bound. In the latter case ascend takes us beyond it.
            state = State.INSIDE;
            return checkRightBound(source.skipChildren(), source.incomingTransition());
        }

        @Override
        public int depth()
        {
            return state == State.AFTER_RIGHT ? -1 : source.depth();
        }

        @Override
        public int incomingTransition()
        {
            return source.incomingTransition();
        }

        @Override
        public T content()
        {
            return state == State.INSIDE ? source.content() : null;
        }
    }
}
