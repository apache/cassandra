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

/// Trie cursor for a singleton trie, mapping a given key to a value.
class SingletonCursor<T> implements Cursor<T>
{
    ByteSource src;
    final ByteComparable.Version byteComparableVersion;
    final T value;
    protected long currentPosition;
    protected long nextPosition;


    public SingletonCursor(Direction direction,
                           ByteSource src,
                           ByteComparable.Version byteComparableVersion,
                           T value)
    {
        this.src = src;
        this.byteComparableVersion = byteComparableVersion;
        this.value = value;
        this.currentPosition = Cursor.rootPosition(direction);
        prepareNextPosition(currentPosition);
    }

    /// Constructor for tail tries.
    ///
    /// Note: the positions given may have different direction from the direction of the constructed tail.
    SingletonCursor(Direction direction,
                    long currentPosition,
                    long nextPosition,
                    ByteSource src,
                    ByteComparable.Version byteComparableVersion,
                    T value)
    {
        assert !Cursor.isOnReturnPath(currentPosition) : "tailCursor cannot be called on return path positions";
        this.src = src;
        this.byteComparableVersion = byteComparableVersion;
        this.value = value;
        this.currentPosition = Cursor.rootPosition(direction);
        if (!Cursor.isExhausted(nextPosition))
            this.nextPosition = nextPosition - Cursor.depthCorrectionValue(currentPosition);
        else
            this.nextPosition = nextPosition;

        if (direction != Cursor.direction(currentPosition))
            this.nextPosition ^= Cursor.TRANSITION_MASK;
    }

    void prepareNextPosition(long currentPosition)
    {
        int nextTransition = src.next();

        if (nextTransition != ByteSource.END_OF_STREAM)
            nextPosition = Cursor.positionForDescentWithByte(currentPosition, nextTransition);
        else
            nextPosition = Cursor.exhaustedPosition(currentPosition);
    }

    @Override
    public long advance()
    {
        currentPosition = nextPosition;
        if (!Cursor.isExhausted(nextPosition))
            prepareNextPosition(currentPosition);
        return currentPosition;
    }

    @Override
    public long advanceMultiple(TransitionsReceiver receiver)
    {
        if (Cursor.isExhausted(nextPosition))
            return currentPosition = nextPosition;

        int nextTransition = Cursor.incomingTransition(nextPosition);
        int current = nextTransition;
        long pos = currentPosition;
        int next = src.next();
        while (next != ByteSource.END_OF_STREAM)
        {
            if (receiver != null)
                receiver.addPathByte(current);
            current = next;
            next = src.next();
            pos += DEPTH_ADJUSTMENT_ONE;
        }
        currentPosition = Cursor.positionForDescentWithByte(pos, current);
        nextPosition = Cursor.exhaustedPosition(currentPosition);
        return currentPosition;
    }

    @Override
    public long skipTo(long encodedSkipPosition)
    {
        if (Cursor.compare(encodedSkipPosition, nextPosition) <= 0)
            return advance();
        else
            return currentPosition = Cursor.exhaustedPosition(currentPosition);
    }

    protected boolean atEnd()
    {
        return Cursor.isExhausted(nextPosition) && !Cursor.isExhausted(currentPosition);
    }

    @Override
    public T content()
    {
        return atEnd() ? value : null;
    }

    @Override
    public long encodedPosition()
    {
        return currentPosition;
    }

    @Override
    public ByteComparable.Version byteComparableVersion()
    {
        return byteComparableVersion;
    }

    @Override
    public SingletonCursor<T> tailCursor(Direction dir)
    {
        return new SingletonCursor<>(dir,
                                     currentPosition, nextPosition,
                                     duplicateSource(),
                                     byteComparableVersion,
                                     value);
    }

    ByteSource.Duplicatable duplicateSource()
    {
        if (!(src instanceof ByteSource.Duplicatable))
            src = ByteSource.duplicatable(src);
        ByteSource.Duplicatable duplicatableSource = (ByteSource.Duplicatable) src;
        return duplicatableSource.duplicate();
    }

    static class DeletionAware<T, D extends RangeState<D>>
    extends SingletonCursor<T> implements DeletionAwareCursor<T, D>
    {
        DeletionAware(Direction direction,
                      ByteSource src,
                      ByteComparable.Version byteComparableVersion,
                      T value)
        {
            super(direction, src, byteComparableVersion, value);
        }

        DeletionAware(Direction direction,
                      long currentPosition,
                      long nextPosition,
                      ByteSource src,
                      ByteComparable.Version byteComparableVersion,
                      T value)
        {
            super(direction, currentPosition, nextPosition, src, byteComparableVersion, value);
        }

        @Override
        public RangeCursor<D> deletionBranchCursor(Direction direction)
        {
            return null;
        }

        @Override
        public DeletionAware<T, D> tailCursor(Direction dir)
        {
            return new DeletionAware<>(dir, currentPosition, nextPosition, duplicateSource(), byteComparableVersion, value);
        }
    }

    static class DeletionBranch<T, D extends RangeState<D>>
    extends SingletonCursor<T> implements DeletionAwareCursor<T, D>
    {
        RangeTrie<D> deletionBranch;

        DeletionBranch(Direction direction, ByteSource src, ByteComparable.Version byteComparableVersion, RangeTrie<D> deletionBranch)
        {
            super(direction, src, byteComparableVersion, null);
            this.deletionBranch = deletionBranch;
        }

        DeletionBranch(Direction direction,
                       long currentPosition,
                       long nextPosition,
                       ByteSource src,
                       ByteComparable.Version byteComparableVersion,
                       RangeTrie<D> deletionBranch)
        {
            super(direction, currentPosition, nextPosition, src, byteComparableVersion, null);
            this.deletionBranch = deletionBranch;
        }

        @Override
        public RangeCursor<D> deletionBranchCursor(Direction direction)
        {
            return atEnd() ? deletionBranch.cursor(direction) : null;
        }

        @Override
        public DeletionBranch<T, D> tailCursor(Direction dir)
        {
            return new DeletionBranch<>(dir, currentPosition, nextPosition, duplicateSource(), byteComparableVersion, deletionBranch);
        }
    }
}
