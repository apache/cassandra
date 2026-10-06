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
abstract class PrefixedCursor<T, C extends Cursor<T>> extends DepthAdjustedCursor<T, C>
{
    ByteSource prefixBytes;
    int nextPrefixByte;
    long currentPosition;

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
        super(tail, 0);
        prefixBytes = prefix;
        nextPrefixByte = firstPrefixByte;
        tail.assertFresh();
        setPositionAndCheckPrefixDone(tail.encodedPosition()); // initial position with the correct direction
    }

    long completeAdvanceInTail(long position)
    {
        return currentPosition = position;
    }

    boolean prefixDone()
    {
        return nextPrefixByte == ByteSource.END_OF_STREAM;
    }

    @Override
    public long encodedPosition()
    {
        return currentPosition;
    }

    @Override
    public long advance()
    {
        if (prefixDone())
            return completeAdvanceInTail(super.advance());

        long nextPosition = Cursor.positionForDescentWithByte(currentPosition, nextPrefixByte);
        nextPrefixByte = prefixBytes.next();
        return setPositionAndCheckPrefixDone(nextPosition);
    }

    @Override
    public long advanceMultiple(TransitionsReceiver receiver)
    {
        if (prefixDone())
            return completeAdvanceInTail(super.advanceMultiple(receiver));

        long pos = currentPosition;
        int incomingTransition = nextPrefixByte;
        nextPrefixByte = prefixBytes.next();

        while (nextPrefixByte != ByteSource.END_OF_STREAM)
        {
            receiver.addPathByte(incomingTransition);
            pos += DEPTH_ADJUSTMENT_ONE;
            incomingTransition = nextPrefixByte;
            nextPrefixByte = prefixBytes.next();
        }
        // Note: It's tempting to do an advance in the tail too, but its root main contain content or other features
        // that we can't skip over.

        return setPositionAndCheckPrefixDone(Cursor.positionForDescentWithByte(pos, incomingTransition));
    }

    @Override
    public long skipTo(long encodedSkipPosition)
    {
        if (prefixDone())
            return completeAdvanceInTail(super.skipTo(encodedSkipPosition));

        long nextPosition = Cursor.positionForDescentWithByte(currentPosition, nextPrefixByte);
        if (Cursor.compare(encodedSkipPosition, nextPosition) > 0)
            return exhausted();
        assert Cursor.depth(encodedSkipPosition) == Cursor.depth(nextPosition)
            : "Invalid advance request to " + Cursor.toString(encodedSkipPosition) +
              " to cursor at " + Cursor.toString(currentPosition);
        nextPrefixByte = prefixBytes.next();
        return setPositionAndCheckPrefixDone(nextPosition);
    }

    private long setPositionAndCheckPrefixDone(long position)
    {
        if (nextPrefixByte == ByteSource.END_OF_STREAM)
            setAttachmentPoint(position);

        currentPosition = position;
        return position;
    }

    private long exhausted()
    {
        currentPosition = Cursor.exhaustedPosition(currentPosition);
        nextPrefixByte = 0; // make sure prefixDone is not engaged (we could return content or tail if it is)
        return currentPosition;
    }

    @Override
    public T content()
    {
        return prefixDone() ? source.content() : null;
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
            assert !Cursor.isExhausted(currentPosition) : "tailTrie called on exhausted cursor";

            if (prefixDone())
                return source.tailCursor(direction);
            else
                return new PrefixedCursor.Plain<>(nextPrefixByte, duplicateSource(), source.tailCursor(direction));
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
            if (prefixDone())
                return source.state();
            return null;
        }

        @Override
        public S precedingState()
        {
            if (prefixDone())
                return source.precedingState();
            return null;
        }

        @Override
        public RangeCursor<S> tailCursor(Direction direction)
        {
            assert !Cursor.isExhausted(currentPosition) : "tailTrie called on exhausted cursor";

            if (prefixDone())
                return source.tailCursor(direction);
            else
                return new PrefixedCursor.Range<>(nextPrefixByte, duplicateSource(), source.tailCursor(direction));
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
            return prefixDone() ? source.deletionBranchCursor(direction) : null;
        }

        @Override
        public DeletionAwareCursor<T, D> tailCursor(Direction direction)
        {
            assert !Cursor.isExhausted(currentPosition) : "tailTrie called on exhausted cursor";

            if (prefixDone())
                return source.tailCursor(direction);
            else
            {
                return new DeletionAware<>(nextPrefixByte, duplicateSource(), source.tailCursor(direction));
            }
        }
    }
}
