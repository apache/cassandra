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
        SOURCE_AHEAD;
    }

    final Direction direction;
    final RangeCursor<S> src;
    final TrieSetCursor set;
    int currentDepth;
    int currentTransition;
    S currentState;
    State state;

    public RangeIntersectionCursor(RangeCursor<S> src, TrieSetCursor set)
    {
        this.direction = src.direction();
        this.set = set;
        this.src = src;
        matchingPosition(set.depth(), set.incomingTransition());
    }

    @Override
    public int depth()
    {
        return currentDepth;
    }

    @Override
    public int incomingTransition()
    {
        return currentTransition;
    }

    @Override
    public Direction direction()
    {
        return direction;
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
    public int advance()
    {
        switch(state)
        {
            case MATCHING:
            {
                int ldepth = set.advance();
                if (set.precedingIncluded())
                    return advanceWithSetAhead(src.advance());
                else
                    return advanceSourceToIntersection(ldepth);
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
    public int skipTo(int skipDepth, int skipTransition)
    {
        switch(state)
        {
            case MATCHING:
                return skipBoth(skipDepth, skipTransition);
            case SET_AHEAD:
            {
                // if the cursor ahead is at the skip point or beyond, we can advance the other cursor to the skip point
                int setDepth = set.depth();
                if (setDepth < skipDepth || setDepth == skipDepth && direction.ge(set.incomingTransition(), skipTransition))
                    return advanceWithSetAhead(src.skipTo(skipDepth, skipTransition));
                // otherwise we must perform a full advance
                return skipBoth(skipDepth, skipTransition);
            }
            case SOURCE_AHEAD:
            {
                // if the cursor ahead is at the skip point or beyond, we can advance the other cursor to the skip point
                int sourceDepth = src.depth();
                if (sourceDepth < skipDepth || sourceDepth == skipDepth && direction.ge(src.incomingTransition(), skipTransition))
                    return advanceWithSourceAhead(set.skipTo(skipDepth, skipTransition));
                // otherwise we must perform a full advance
                return skipBoth(skipDepth, skipTransition);
            }
            default:
                throw new AssertionError();
        }
    }

    private int skipBoth(int skipDepth, int skipTransition)
    {
        int ldepth = set.skipTo(skipDepth, skipTransition);
        if (set.precedingIncluded())
            return advanceWithSetAhead(src.skipTo(skipDepth, skipTransition));
        else
            return advanceSourceToIntersection(ldepth);
    }

    @Override
    public int advanceMultiple(Cursor.TransitionsReceiver receiver)
    {
        switch(state)
        {
            case MATCHING:
            {
                // Cannot do multi-advance when cursors are at the same position. Applying advance().
                int ldepth = set.advance();
                if (set.precedingIncluded())
                    return advanceWithSetAhead(src.advance());
                else
                    return advanceSourceToIntersection(ldepth);
            }
            case SET_AHEAD:
                return advanceWithSetAhead(src.advanceMultiple(receiver));
            case SOURCE_AHEAD:
                return advanceWithSourceAhead(set.advanceMultiple(receiver));
            default:
                throw new AssertionError();
        }
    }

    private int advanceWithSetAhead(int sourceDepth)
    {
        int sourceTransition = src.incomingTransition();
        int setDepth = set.depth();
        int setTransition = set.incomingTransition();
        if (sourceDepth > setDepth)
            return coveredAreaWithSetAhead(sourceDepth, sourceTransition);
        if (sourceDepth == setDepth)
        {
            if (direction.lt(sourceTransition, setTransition))
                return coveredAreaWithSetAhead(sourceDepth, sourceTransition);
            if (sourceTransition == setTransition)
                return matchingPosition(sourceDepth, sourceTransition);
        }

        // Advancing cursor moved beyond the ahead cursor. Check if roles have reversed.
        if (src.precedingState() != null)
            return coveredAreaWithSourceAhead(setDepth, setTransition);
        else
            return advanceSetToIntersection(sourceDepth);
    }

    private int advanceWithSourceAhead(int setDepth)
    {
        int setTransition = set.incomingTransition();
        int sourceDepth = src.depth();
        int sourceTransition = src.incomingTransition();
        if (setDepth > sourceDepth)
            return coveredAreaWithSourceAhead(setDepth, setTransition);
        if (setDepth == sourceDepth)
        {
            if (direction.lt(setTransition, sourceTransition))
                return coveredAreaWithSourceAhead(setDepth, setTransition);
            if (setTransition == sourceTransition)
                return matchingPosition(setDepth, setTransition);
        }

        // Advancing cursor moved beyond the ahead cursor. Check if roles have reversed.
        if (set.precedingIncluded())
            return coveredAreaWithSetAhead(sourceDepth, sourceTransition);
        else
            return advanceSourceToIntersection(setDepth);
    }

    private int advanceSourceToIntersection(int setDepth)
    {
        int setTransition = set.incomingTransition();
        while (true)
        {
            // Set is ahead of source, but outside the covered area. Skip source to set's position.
            int sourceDepth = src.skipTo(setDepth, setTransition);
            int sourceTransition = src.incomingTransition();
            if (sourceDepth == setDepth && sourceTransition == setTransition)
                return matchingPosition(setDepth, setTransition);
            if (src.precedingState() != null)
                return coveredAreaWithSourceAhead(setDepth, setTransition);

            // Source is ahead of set, but outside the covered area. Skip set to source's position.
            setDepth = set.skipTo(sourceDepth, sourceTransition);
            setTransition = set.incomingTransition();
            if (setDepth == sourceDepth && setTransition == sourceTransition)
                return matchingPosition(sourceDepth, sourceTransition);
            if (set.precedingIncluded())
                return coveredAreaWithSetAhead(sourceDepth, sourceTransition);
        }
    }

    private int advanceSetToIntersection(int sourceDepth)
    {
        int sourceTransition = src.incomingTransition();
        while (true)
        {
            // Source is ahead of set, but outside the covered area. Skip set to source's position.
            int setDepth = set.skipTo(sourceDepth, sourceTransition);
            int setTransition = set.incomingTransition();
            if (setDepth == sourceDepth && setTransition == sourceTransition)
                return matchingPosition(sourceDepth, sourceTransition);
            if (set.precedingIncluded())
                return coveredAreaWithSetAhead(sourceDepth, sourceTransition);

            // Set is ahead of source, but outside the covered area. Skip source to set's position.
            sourceDepth = src.skipTo(setDepth, setTransition);
            sourceTransition = src.incomingTransition();
            if (sourceDepth == setDepth && sourceTransition == setTransition)
                return matchingPosition(setDepth, setTransition);
            if (src.precedingState() != null)
                return coveredAreaWithSourceAhead(setDepth, setTransition);
        }
    }

    private int coveredAreaWithSetAhead(int depth, int transition)
    {
        return setState(State.SET_AHEAD, depth, transition, src.state());
    }

    private int coveredAreaWithSourceAhead(int depth, int transition)
    {
        return setState(State.SOURCE_AHEAD, depth, transition, restrict(src.precedingState(), set.state()));
    }

    private int matchingPosition(int depth, int transition)
    {
        return setState(State.MATCHING, depth, transition, restrict(src.state(), set.state()));
    }

    private S restrict(S srcState, TrieSetCursor.RangeState setState)
    {
        if (srcState == null)
            return null;
        if (srcState.isBoundary())
            return srcState.restrict(setState.applicableBefore, setState.applicableAfter);

        return setState.applyToCoveringState(srcState, direction());
    }

    private int setState(State state, int depth, int transition, S cursorState)
    {
        this.state = state;
        this.currentDepth = depth;
        this.currentTransition = transition;
        this.currentState = cursorState;
        return depth;
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
            return s != null ? s : RangeState.START_END_PREFIX;
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
