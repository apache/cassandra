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
 * TrieSet representing the range between two keys.
 *
 * The keys must be correctly ordered, including with respect to the `includeLeft` and `includeRight` constraints.
 * (i.e. RangeTrieSet(x, false, x, false) is an invalid call but RangeTrieSet(x, true, x, false) is inefficient
 * but fine for an empty set).
 */
public class RangeTrieSet extends TrieSet
{
    /** Left-side boundary. The characters of this are requested as we descend along the left-side boundary. */
    private final ByteComparable left;

    /** Right-side boundary. The characters of this are requested as we descend along the right-side boundary. */
    private final ByteComparable right;

    private final boolean includeLeft;
    private final boolean includeRight;

    RangeTrieSet(ByteComparable left, boolean includeLeft, ByteComparable right, boolean includeRight)
    {
        this.left = left;
        this.includeLeft = includeLeft;
        this.right = right;
        this.includeRight = includeRight;
    }

    public SetNode root()
    {
        return makeNode(left == null ? null : left.asComparableBytes(Trie.BYTE_COMPARABLE_VERSION),
                        left != null,
                        right == null ? null : right.asComparableBytes(Trie.BYTE_COMPARABLE_VERSION),
                        right != null);
    }

    private SetNode makeNode(ByteSource lLimit, boolean atLLimit, ByteSource rLimit, boolean atRLimit)
    {
        // We only have a constraint on the branch if we are at one or both boundaries.
        // If the node falls completely between them, the whole branch (at any depth) is in the set.
        if (!atLLimit && !atRLimit)
            return FULL;

        return new RangeNode(lLimit, atLLimit, rLimit, atRLimit);
    }

    class RangeNode implements SetNode
    {
        /** Byte at the left boundary, inclusive. */
        final int llimit;
        final ByteSource remainingLLimit;
        /** Byte at the right boundary, inclusive. */
        final int rlimit;
        final ByteSource remainingRLimit;
        /** Whether or not we are descending along the left boundary. */
        final boolean atLLimit;
        /** Whether or not we are descending along the right boundary. */
        final boolean atRLimit;

        /** Whether the current path is in the covered set. */
        final boolean inSet;

        int currentTransition;


        RangeNode(ByteSource remainingLLimit, boolean atLLimit, ByteSource remainingRLimit, boolean atRLimit)
        {
            int llimit = 0;
            boolean inSet = true;
            if (atLLimit)
            {
                llimit = remainingLLimit.next();
                if (llimit == ByteSource.END_OF_STREAM)
                {
                    atLLimit = false;
                    llimit = 0;
                    inSet &= includeLeft; // The current path matches left boundary
                }
                else
                    inSet = false;  // The current path is a prefix of the left boundary, ie. smaller.
            }
            int rlimit = 255;
            if (atRLimit)
            {
                rlimit = remainingRLimit.next();
                if (rlimit == ByteSource.END_OF_STREAM)
                {
                    atRLimit = false;
                    rlimit = -1;    // no op, added for clarity. Node should have no children.
                    inSet &= includeRight; // The current path matches right boundary
                }
            }
            assert llimit <= rlimit || rlimit == -1 : "Bound " + left + " not <= " + right + " in range " + llimit + " vs " + rlimit;

            this.llimit = llimit;
            this.remainingLLimit = remainingLLimit;
            this.rlimit = rlimit;
            this.remainingRLimit = remainingRLimit;
            this.atLLimit = atLLimit;
            this.atRLimit = atRLimit;
            this.inSet = inSet;
        }

        public SetNode getCurrentChild()
        {
            return makeNode(remainingLLimit, atLLimit && (currentTransition == llimit),
                            remainingRLimit, atRLimit && (currentTransition == rlimit));
        }

        public int currentTransition()
        {
            return currentTransition;
        }

        public boolean startIteration()
        {
            currentTransition = llimit;
            return currentTransition <= rlimit;
        }

        public boolean advanceIteration()
        {
            return ++currentTransition <= rlimit;
        }

        public boolean inSet()
        {
            return inSet;
        }
    }
}
