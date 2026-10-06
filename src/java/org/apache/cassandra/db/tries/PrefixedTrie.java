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
 * Prefixed trie. Represents the content of the given trie with the prefix prepended to all keys.
 */
public class PrefixedTrie<T> extends Trie<T>
{
    final ByteComparable prefix;
    final Trie<T> trie;

    public PrefixedTrie(ByteComparable prefix, Trie<T> trie)
    {
        this.prefix = prefix;
        this.trie = trie;
    }

    @Override
    protected Trie.Cursor<T> cursor(Direction direction)
    {
        Trie.Cursor<T> sourceCursor = trie.cursor(direction);
        return new Cursor<>(prefix.asComparableBytes(sourceCursor.byteComparableVersion()), sourceCursor);
    }

    private static class Cursor<T> implements Trie.Cursor<T>
    {
        final Trie.Cursor<T> tail;
        ByteSource prefixBytes;
        int nextPrefixByte;
        int incomingTransition;
        int depthOfPrefix;

        Cursor(ByteSource prefix, Trie.Cursor<T> tail)
        {
            this.tail = tail;
            prefixBytes = prefix;
            incomingTransition = -1;
            nextPrefixByte = prefixBytes.next();
            depthOfPrefix = 0;
        }

        int completeAdvanceInTail(int depthInTail)
        {
            if (depthInTail < 0)
                return exhausted();

            incomingTransition = tail.incomingTransition();
            return depthInTail + depthOfPrefix;
        }

        boolean prefixDone()
        {
            return nextPrefixByte == ByteSource.END_OF_STREAM;
        }

        @Override
        public int depth()
        {
            if (prefixDone())
                return tail.depth() + depthOfPrefix;
            else
                return depthOfPrefix;
        }

        @Override
        public int incomingTransition()
        {
            return incomingTransition;
        }

        @Override
        public int advance()
        {
            if (prefixDone())
                return completeAdvanceInTail(tail.advance());

            ++depthOfPrefix;
            incomingTransition = nextPrefixByte;
            nextPrefixByte = prefixBytes.next();
            return depthOfPrefix;
        }

        @Override
        public int advanceMultiple(Trie.TransitionsReceiver receiver)
        {
            if (prefixDone())
                return completeAdvanceInTail(tail.advanceMultiple(receiver));

            while (!prefixDone())
            {
                receiver.addPathByte(incomingTransition);
                ++depthOfPrefix;
                incomingTransition = nextPrefixByte;
                nextPrefixByte = prefixBytes.next();
            }
            return depthOfPrefix;
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            // regardless if we exhausted prefix, if caller asks for depth <= prefix depth, we're done.
            if (skipDepth <= depthOfPrefix)
                return exhausted();
            if (prefixDone())
                return completeAdvanceInTail(tail.skipTo(skipDepth - depthOfPrefix, skipTransition));
            assert skipDepth == depthOfPrefix + 1 : "Invalid advance request to depth " + skipDepth + " to cursor at depth " + depthOfPrefix;
            if (tail.direction().gt(skipTransition, nextPrefixByte))
                return exhausted();
            return advance();
        }

        private int exhausted()
        {
            incomingTransition = -1;
            depthOfPrefix = -1;
            nextPrefixByte = 0; // to make prefixDone() false so incomingTransition/depth/content are -1/-1/null
            return depthOfPrefix;
        }

        public Direction direction()
        {
            return tail.direction();
        }

        public ByteComparable.Version byteComparableVersion()
        {
            return tail.byteComparableVersion();
        }

        @Override
        public T content()
        {
            return prefixDone() ? tail.content() : null;
        }

        @Override
        public Trie<T> tailTrie()
        {
            if (prefixDone())
                return tail.tailTrie();
            else
            {
                assert depthOfPrefix >= 0 : "tailTrie called on exhausted cursor";
                if (!(prefixBytes instanceof ByteSource.Duplicatable))
                    prefixBytes = ByteSource.duplicatable(prefixBytes);
                ByteSource.Duplicatable duplicatableSource = (ByteSource.Duplicatable) prefixBytes;

                return new PrefixedTrie<>(v -> duplicatableSource.duplicate(), tail.tailTrie());
            }
        }
    }
}
