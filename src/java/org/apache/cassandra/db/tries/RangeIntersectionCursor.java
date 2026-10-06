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

class RangeIntersectionCursor<S extends RangeState<S>> implements RangeCursor<S>
{
    enum State
    {
        MATCHING,
        SET_AHEAD,
        SOURCE_AHEAD
    }

    final RangeCursor<S> src;
    final TrieSetCursor set;
    long currentPosition;
    S currentState;
    State state;

    public RangeIntersectionCursor(RangeCursor<S> src, TrieSetCursor set)
    {
        this.set = set;
        this.src = src;
        assert Cursor.compare(src.encodedPosition(), set.encodedPosition()) == 0;
        matchingPosition(set.encodedPosition());
    }

    @Override
    public long encodedPosition()
    {
        return currentPosition;
    }

    @Override
    public ByteComparable.Version byteComparableVersion()
    {
        return set.byteComparableVersion();
    }

    @Override
    public S state()
    {
        return currentState;
    }

    @Override
    public long advance()
    {
        switch(state)
        {
            case MATCHING:
            {
                long lposition = set.advance();
                if (set.precedingIncluded())
                    return advanceWithSetAhead(src.advance());
                else
                    return advanceSourceToIntersection(lposition);
            }
            case SET_AHEAD:
                return advanceWithSetAhead(src.advance());
            case SOURCE_AHEAD:
                return advanceWithSourceAhead(set.advance());
            default:
                throw new AssertionError();
        }
    }

    @Override
    public long skipTo(long skipPosition)
    {
        switch(state)
        {
            case MATCHING:
                return skipBoth(skipPosition);
            case SET_AHEAD:
            {
                // if the cursor ahead is at the skip point or beyond, we can advance the other cursor to the skip point
                long setPosition = set.encodedPosition();
                if (Cursor.compare(skipPosition, setPosition) <= 0)
                    return advanceWithSetAhead(src.skipTo(skipPosition));
                // otherwise we must perform a full advance
                return skipBoth(skipPosition);
            }
            case SOURCE_AHEAD:
            {
                // if the cursor ahead is at the skip point or beyond, we can advance the other cursor to the skip point
                long sourcePosition = src.encodedPosition();
                if (Cursor.compare(skipPosition, sourcePosition) <= 0)
                    return advanceWithSourceAhead(set.skipTo(skipPosition));
                // otherwise we must perform a full advance
                return skipBoth(skipPosition);
            }
            default:
                throw new AssertionError();
        }
    }

    private long skipBoth(long skipPosition)
    {
        long lposition = set.skipTo(skipPosition);
        if (set.precedingIncluded())
            return advanceWithSetAhead(src.skipTo(skipPosition));
        else
            return advanceSourceToIntersection(lposition);
    }

    @Override
    public long advanceMultiple(Cursor.TransitionsReceiver receiver)
    {
        switch(state)
        {
            case MATCHING:
            {
                // Cannot do multi-advance when cursors are at the same position. Applying advance().
                long lposition = set.advance();
                if (set.precedingIncluded())
                    return advanceWithSetAhead(src.advance());
                else
                    return advanceSourceToIntersection(lposition);
            }
            case SET_AHEAD:
                return advanceWithSetAhead(src.advanceMultiple(receiver));
            case SOURCE_AHEAD:
                return advanceWithSourceAhead(set.advanceMultiple(receiver));
            default:
                throw new AssertionError();
        }
    }

    private long advanceWithSetAhead(long sourcePosition)
    {
        long setPosition = set.encodedPosition();
        long cmp = Cursor.compare(sourcePosition, setPosition);
        if (cmp < 0)
            return coveredAreaWithSetAhead(sourcePosition);
        if (cmp == 0)
            return matchingPosition(sourcePosition);

        // Advancing cursor moved beyond the ahead cursor. Check if roles have reversed.
        if (src.precedingState() != null)
            return coveredAreaWithSourceAhead(setPosition);
        else
            return advanceSetToIntersection(sourcePosition);
    }

    private long advanceWithSourceAhead(long setPosition)
    {
        long sourcePosition = src.encodedPosition();
        long cmp = Cursor.compare(setPosition, sourcePosition);
        if (cmp < 0)
            return coveredAreaWithSourceAhead(setPosition);
        if (cmp == 0)
            return matchingPosition(setPosition);

        // Advancing cursor moved beyond the ahead cursor. Check if roles have reversed.
        if (set.precedingIncluded())
            return coveredAreaWithSetAhead(sourcePosition);
        else
            return advanceSourceToIntersection(setPosition);
    }

    private long advanceSourceToIntersection(long setPosition)
    {
        while (true)
        {
            // Set is ahead of source, but outside the covered area. Skip source to set's position.
            long sourcePosition = src.skipTo(setPosition);
            if (Cursor.compare(sourcePosition, setPosition) == 0)
                return matchingPosition(setPosition);
            if (src.precedingState() != null)
                return coveredAreaWithSourceAhead(setPosition);

            // Source is ahead of set, but outside the covered area. Skip set to source's position.
            setPosition = set.skipTo(sourcePosition);
            if (Cursor.compare(setPosition, sourcePosition) == 0)
                return matchingPosition(sourcePosition);
            if (set.precedingIncluded())
                return coveredAreaWithSetAhead(sourcePosition);
        }
    }

    private long advanceSetToIntersection(long sourcePosition)
    {
        while (true)
        {
            // Source is ahead of set, but outside the covered area. Skip set to source's position.
            long setPosition = set.skipTo(sourcePosition);
            if (Cursor.compare(setPosition, sourcePosition) == 0)
                return matchingPosition(sourcePosition);
            if (set.precedingIncluded())
                return coveredAreaWithSetAhead(sourcePosition);

            // Set is ahead of source, but outside the covered area. Skip source to set's position.
            sourcePosition = src.skipTo(setPosition);
            if (Cursor.compare(setPosition, sourcePosition) == 0)
                return matchingPosition(setPosition);
            if (src.precedingState() != null)
                return coveredAreaWithSourceAhead(setPosition);
        }
    }

    private long coveredAreaWithSetAhead(long position)
    {
        return setState(State.SET_AHEAD, position, src.state());
    }

    private long coveredAreaWithSourceAhead(long position)
    {
        return setState(State.SOURCE_AHEAD, position, restrict(src.precedingState(), set.state()));
    }

    private long matchingPosition(long position)
    {
        return setState(State.MATCHING, position, restrict(src.state(), set.state()));
    }

    private S restrict(S srcState, TrieSetCursor.RangeState setState)
    {
        if (srcState == null)
            return null;
        if (srcState.isBoundary())
            return srcState.restrict(setState.applicableBefore, setState.applicableAfter);

        return setState.applyToCoveringState(srcState);
    }

    private long setState(State state, long position, S cursorState)
    {
        this.state = state;
        this.currentPosition = position;
        this.currentState = cursorState;
        return position;
    }

    @Override
    public RangeCursor<S> tailCursor(Direction direction)
    {
        switch (state)
        {
            case MATCHING:
                return new RangeIntersectionCursor<>(src.tailCursor(direction), set.tailCursor(direction));
            case SET_AHEAD:
                return src.tailCursor(direction);
            case SOURCE_AHEAD:
                return new RangeIntersectionCursor<>(src.precedingStateCursor(direction), set.tailCursor(direction));
            default:
                throw new AssertionError();
        }
    }

    static class TrieSet extends RangeIntersectionCursor<TrieSetCursor.RangeState> implements TrieSetCursor
    {
        public TrieSet(TrieSetCursor src, TrieSetCursor set)
        {
            super(src, set);
        }

        @Override
        public RangeState state()
        {
            RangeState s = super.state();
            return s != null ? s : RangeState.NOT_CONTAINED;
        }

        @Override
        public TrieSetCursor tailCursor(Direction direction)
        {
            TrieSetCursor source = (TrieSetCursor) src;
            switch (state)
            {
                case MATCHING:
                    return new TrieSet(source.tailCursor(direction), set.tailCursor(direction));
                case SET_AHEAD:
                    return source.tailCursor(direction);
                case SOURCE_AHEAD:
                    return set.tailCursor(direction);
                default:
                    throw new AssertionError();
            }
        }
    }
}
