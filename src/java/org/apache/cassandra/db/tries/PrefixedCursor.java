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

/// Prefixed cursor. Prepends the given prefix to all keys of the supplied cursor.
abstract class PrefixedCursor<T, C extends Cursor<T>> implements Cursor<T>
{
    final C tail;
    ByteSource prefixBytes;
    int nextPrefixByte;
    int incomingTransition;
    int depthOfPrefix;

    PrefixedCursor(ByteComparable prefix, C tail)
    {
        this(prefix.asComparableBytes(tail.byteComparableVersion()), tail);
    }

    PrefixedCursor(ByteSource prefix, C tail)
    {
        this(prefix.next(), prefix, tail);
    }

    PrefixedCursor(int firstPrefixByte, ByteSource prefix, C tail)
    {
        this.tail = tail;
        prefixBytes = prefix;
        incomingTransition = -1;
        nextPrefixByte = firstPrefixByte;
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
    public int advanceMultiple(TransitionsReceiver receiver)
    {
        if (prefixDone())
            return completeAdvanceInTail(tail.advanceMultiple(receiver));

        incomingTransition = nextPrefixByte;
        nextPrefixByte = prefixBytes.next();
        ++depthOfPrefix;

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

    ByteSource.Duplicatable duplicateSource()
    {
        if (!(prefixBytes instanceof ByteSource.Duplicatable))
            prefixBytes = ByteSource.duplicatable(prefixBytes);
        ByteSource.Duplicatable duplicatableSource = (ByteSource.Duplicatable) prefixBytes;
        return duplicatableSource.duplicate();
    }

    static class Plain<T> extends PrefixedCursor<T, Cursor<T>> implements Cursor<T>
    {
        Plain(ByteComparable prefix, Cursor<T> tail)
        {
            super(prefix, tail);
        }

        Plain(int firstPrefixByte, ByteSource prefix, Cursor<T> source)
        {
            super(firstPrefixByte, prefix, source);
        }

        @Override
        public Cursor<T> tailCursor(Direction direction)
        {
            if (prefixDone())
                return tail.tailCursor(direction);
            else
            {
                assert depthOfPrefix >= 0 : "tailTrie called on exhausted cursor";
                return new Plain<>(nextPrefixByte, duplicateSource(), tail.tailCursor(direction));
            }
        }
    }

    static class Range<S extends RangeState<S>> extends PrefixedCursor<S, RangeCursor<S>> implements RangeCursor<S>
    {
        Range(ByteComparable prefix, RangeCursor<S> tail)
        {
            super(prefix, tail);
        }

        Range(int firstPrefixByte, ByteSource prefix, RangeCursor<S> source)
        {
            super(firstPrefixByte, prefix, source);
        }

        @Override
        public S state()
        {
            if (prefixDone() && tail.depth() >= 0)
                return tail.state();
            return null;
        }

        @Override
        public RangeCursor<S> tailCursor(Direction direction)
        {
            if (prefixDone())
                return tail.tailCursor(direction);
            else
            {
                assert depthOfPrefix >= 0 : "tailTrie called on exhausted cursor";
                return new Range<>(nextPrefixByte, duplicateSource(), tail.tailCursor(direction));
            }
        }
    }

    static class DeletionAware<T, D extends RangeState<D>>
    extends PrefixedCursor<T, DeletionAwareCursor<T, D>> implements DeletionAwareCursor<T, D>
    {
        DeletionAware(ByteComparable prefix, DeletionAwareCursor<T, D> tail)
        {
            super(prefix, tail);
        }

        DeletionAware(int firstPrefixByte, ByteSource prefix, DeletionAwareCursor<T, D> tail)
        {
            super(firstPrefixByte, prefix, tail);
        }

        @Override
        public RangeCursor<D> deletionBranchCursor(Direction direction)
        {
            return prefixDone() ? tail.deletionBranchCursor(direction) : null;
        }

        @Override
        public DeletionAwareCursor<T, D> tailCursor(Direction direction)
        {
            if (prefixDone())
                return tail.tailCursor(direction);
            else
            {
                assert depthOfPrefix >= 0 : "tailTrie called on exhausted cursor";
                return new DeletionAware<>(nextPrefixByte, duplicateSource(), tail.tailCursor(direction));
            }
        }
    }
}
