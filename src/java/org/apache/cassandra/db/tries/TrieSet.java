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

/**
 * A simplified version of Trie used for sets (whose ultimate function is to intersect a Trie).
 *
 * Sets cannot be asynchronous and support a special value to denote a branch is fully included in the set,
 * which is used to speed up intersections.
 *
 * Like Trie nodes, set nodes are stateful and not thread-safe. If the consumer can use multiple threads when accessing
 * a node (e.g. with asynchronous trie walks), it must enforce a happens-before relationship between calls to the
 * methods of a node.
 */
public abstract class TrieSet
{
    public abstract SetNode root();

    interface SetNode
    {
        boolean startIteration();
        boolean advanceIteration();
        int currentTransition();
        SetNode getCurrentChild();

        /**
         * Returns true if this specific position is in the set (i.e. if content in the intersected node should be
         * returned).
         *
         * Note: Having a node produced by the trie set does not necessarily mean the relevant key is in the set.
         * Imagine a singleton set, e.g. {010203}. It will be represented as the following trie:
         *     root -01-> node1 -02-> node2 -03-> node3
         * where only node3 will have inSet() == true. Root (corresponding to empty key), node1 (key 01) and node2 (key
         * 0102) are not in the set and thus their inSet() will be false.
         */
        boolean inSet();
    }

    protected static final SetNode FULL = new SetNode()
    {
        public AssertionError error()
        {
            throw new AssertionError("SetNode FULL must be handled explicitly.");
        }

        public boolean startIteration()
        {
            throw error();
        }

        public boolean advanceIteration()
        {
            throw error();
        }

        public int currentTransition()
        {
            throw error();
        }

        public SetNode getCurrentChild()
        {
            throw error();
        }

        public boolean inSet()
        {
            throw error();
        }
    };

    private static final TrieSet FULL_SET = new TrieSet()
    {
        public SetNode root()
        {
            return FULL;
        }
    };

    private static final TrieSet EMPTY_SET = new TrieSet()
    {
        public SetNode root()
        {
            return null;
        }
    };

    /**
     * Range of keys between the given boundaries.
     * A null argument for any of the limits means that the set should be unbounded on that side.
     * The keys must be correctly ordered, including with respect to the `includeLeft` and `includeRight` constraints.
     * (i.e. range(x, false, x, false) is an invalid call but range(x, true, x, false) is inefficient
     * but fine for an empty set).
     */
    public static TrieSet range(ByteComparable left, boolean includeLeft, ByteComparable right, boolean includeRight)
    {
        return new RangeTrieSet(left, includeLeft, right, includeRight);
    }

    public static TrieSet full()
    {
        return FULL_SET;
    }

    public static TrieSet empty()
    {
        return EMPTY_SET;
    }
}
