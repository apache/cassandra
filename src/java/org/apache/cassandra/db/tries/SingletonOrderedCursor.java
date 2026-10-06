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

/// Trie cursor for a singleton trie, mapping a given key to a value. This version places the content before or after
/// the branch, which is useful for range boundaries and ordered tries (see [Trie#singletonOrdered]).
class SingletonOrderedCursor<T> extends SingletonCursor<T>
{
    final boolean presentOnReturnPath;


    public SingletonOrderedCursor(Direction direction,
                                  ByteSource.Peekable src,
                                  ByteComparable.Version byteComparableVersion,
                                  boolean presentOnReturnPath,
                                  T value)
    {
        super(direction, src, byteComparableVersion, value);
        this.presentOnReturnPath = presentOnReturnPath;
        adjustNextPosition(src);
    }

    /// Constructor for tail tries.
    ///
    /// Note: the positions given may have different direction from the direction of the constructed tail.
    SingletonOrderedCursor(Direction direction,
                           long currentPosition,
                           long nextPosition,
                           ByteSource.Peekable src,
                           ByteComparable.Version byteComparableVersion,
                           boolean presentOnReturnPath,
                           T value)
    {
        super(direction, currentPosition, nextPosition, src, byteComparableVersion, value);
        // We need to do some additional steps if the directions differ.
        if (direction != Cursor.direction(currentPosition))
        {
            if (!Cursor.isExhausted(nextPosition) && src.peek() == ByteSource.END_OF_STREAM)
                this.nextPosition ^= ON_RETURN_PATH_BIT;
            this.presentOnReturnPath = !presentOnReturnPath;
        }
        else
            this.presentOnReturnPath = presentOnReturnPath;
    }

    @Override
    void prepareNextPosition(long currentPosition)
    {
        super.prepareNextPosition(currentPosition);
        adjustNextPosition(((ByteSource.Peekable) src));
    }

    private void adjustNextPosition(ByteSource.Peekable src)
    {
        if (presentOnReturnPath)
        {
            if (Cursor.isExhausted(nextPosition))
            {
                if (Cursor.isRootPosition(currentPosition))
                    nextPosition = currentPosition | ON_RETURN_PATH_BIT;
            }
            else if (src.peek() == ByteSource.END_OF_STREAM)
                nextPosition |= ON_RETURN_PATH_BIT;
        }
    }

    @Override
    public long advanceMultiple(TransitionsReceiver receiver)
    {
        if (Cursor.depth(nextPosition) <= 0) // exhausted or root return path
            return advance();

        super.advanceMultiple(receiver);
        if (presentOnReturnPath)
            currentPosition |= ON_RETURN_PATH_BIT;
        return currentPosition;
    }

    @Override
    public SingletonOrderedCursor<T> tailCursor(Direction dir)
    {
        return new SingletonOrderedCursor<>(dir,
                                            currentPosition, nextPosition,
                                            duplicateSource(),
                                            byteComparableVersion,
                                            presentOnReturnPath,
                                            value);
    }

    static class Range<S extends RangeState<S>> extends SingletonOrderedCursor<S> implements RangeCursor<S>
    {
        public Range(Direction direction,
                     ByteSource.Peekable src,
                     ByteComparable.Version byteComparableVersion,
                     boolean presentOnReturnPath,
                     S value)
        {
            super(direction, src, byteComparableVersion, presentOnReturnPath, value);
        }

        public Range(Direction direction,
                     long currentPosition,
                     long nextPosition,
                     ByteSource.Peekable src,
                     ByteComparable.Version byteComparableVersion,
                     boolean presentOnReturnPath,
                     S value)
        {
            super(direction, currentPosition, nextPosition, src, byteComparableVersion, presentOnReturnPath, value);
        }

        @Override
        public S precedingState()
        {
            return null;
        }

        @Override
        public S state()
        {
            return content();
        }

        @Override
        public Range<S> tailCursor(Direction dir)
        {
            return new Range<>(dir, currentPosition, nextPosition, duplicateSource(), byteComparableVersion, presentOnReturnPath, value);
        }
    }
}
