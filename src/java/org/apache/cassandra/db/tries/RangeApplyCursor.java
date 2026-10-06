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

import java.util.function.BiFunction;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/// A cursor applying a range to a data cursor. The cursor will present the content of the data trie modified by any
/// applicable/covering range of the range trie.
///
/// This is very similar to a normal merge cursor but, because it only presents content from the data trie, it does not
/// need to walk the range trie unless it matches positions from the data cursor and thus skips the range cursor
/// whenever the data one ends up ahead.
class RangeApplyCursor<T, S extends RangeState<S>> implements Cursor<T>
{
    final BiFunction<S, T, T> resolver;
    final Direction direction;
    final RangeCursor<S> range;
    final Cursor<T> data;

    boolean atRange;

    RangeApplyCursor(BiFunction<S, T, T> resolver, RangeCursor<S> range, Cursor<T> data)
    {
        this.direction = data.direction();
        this.resolver = resolver;
        this.range = range;
        this.data = data;
        assert data.depth() == 0;
        assert range.depth() == 0;
        atRange = true;
    }

    @Override
    public int depth()
    {
        return data.depth();
    }

    @Override
    public int incomingTransition()
    {
        return data.incomingTransition();
    }

    @Override
    public Direction direction()
    {
        return direction;
    }

    @Override
    public ByteComparable.Version byteComparableVersion()
    {
        assert range.byteComparableVersion() == data.byteComparableVersion() :
            "Merging cursors with different byteComparableVersions: " +
            range.byteComparableVersion() + " vs " + data.byteComparableVersion();
        return range.byteComparableVersion();
    }

    @Override
    public int advance()
    {
        int dataDepth = data.advance();
        if (atRange)
            return skipRangeToDataPosition(dataDepth);
        else
            return maybeSkipRange(dataDepth);
    }

    @Override
    public int skipTo(int skipDepth, int skipTransition)
    {
        int dataDepth = data.skipTo(skipDepth, skipTransition);
        if (atRange) // if both cursors were at the same position, always advance the range cursor to catch up.
            return skipRangeToDataPosition(dataDepth);
        else // otherwise skip range to the new data position only if it advances past the range's current position.
            return maybeSkipRange(dataDepth);
    }

    @Override
    public int advanceMultiple(TransitionsReceiver receiver)
    {
        // While we are on a shared position, we must descend one byte at a time to maintain the cursor ordering.
        if (atRange)
            return skipRangeToDataPosition(data.advance());
        else // atData only
            return maybeSkipRange(data.advanceMultiple(receiver));
    }

    int maybeSkipRange(int dataDepth)
    {
        int rangeDepth = range.depth();
        // If data position is at or before the range position, we are good.
        if (rangeDepth < dataDepth)
            return setAtRangeAndReturnDepth(false, dataDepth);

        if (rangeDepth == dataDepth)
        {
            int dataTrans = data.incomingTransition();
            int rangeTrans = range.incomingTransition();
            if (direction.le(dataTrans, rangeTrans))
                return setAtRangeAndReturnDepth(dataTrans == rangeTrans, dataDepth);
        }

        // Range cursor is before data cursor. Skip it ahead so that we are positioned on data.
        return skipRangeToDataPosition(dataDepth);
    }

    private int skipRangeToDataPosition(int dataDepth)
    {
        int dataTrans = data.incomingTransition();
        int rangeDepth = range.skipTo(dataDepth, dataTrans);
        return setAtRangeAndReturnDepth(rangeDepth == dataDepth && range.incomingTransition() == dataTrans,
                                        dataDepth);
    }

    private int setAtRangeAndReturnDepth(boolean atRange, int depth)
    {
        this.atRange = atRange;
        return depth;
    }

    @Override
    public T content()
    {
        T content = data.content();
        if (content == null)
            return null;

        S applicableRange = atRange ? range.content() : null;
        if (applicableRange == null)
        {
            applicableRange = range.precedingState();
            if (applicableRange == null)
                return content;
        }

        return resolver.apply(applicableRange, content);
    }

    @Override
    public Cursor<T> tailCursor(Direction direction)
    {
        if (atRange)
            return new RangeApplyCursor<>(resolver, range.tailCursor(direction), data.tailCursor(direction));
        else
            return data.tailCursor(direction);
    }

    static class DeletionAwareDataBranch<T, D extends RangeState<D>> extends RangeApplyCursor<T, D> implements DeletionAwareCursor<T, D>
    {
        DeletionAwareDataBranch(BiFunction<D, T, T> resolver, RangeCursor<D> range, Cursor<T> data)
        {
            super(resolver, range, data);
        }

        @Override
        public RangeCursor<D> deletionBranchCursor(Direction direction)
        {
            return null;
        }

        @Override
        public DeletionAwareCursor<T, D> tailCursor(Direction direction)
        {
            return new DeletionAwareDataBranch<>(resolver,
                                                 atRange ? range.tailCursor(direction)
                                                         : RangeCursor.empty(direction, byteComparableVersion()),
                                                 data.tailCursor(direction));
        }
    }
}
