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
import java.util.function.Predicate;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

abstract class RangeIntersectionCursor<S extends RangeState<S>, C extends RangeCursor<S>,
                                       R extends RangeState<R>, D extends RangeCursor<R>,
                                       Q extends RangeState<Q>>
implements RangeCursor<Q>
{
    enum State
    {
        MATCHING,
        SET_AHEAD,
        SOURCE_AHEAD
    }

    final C src;
    final D set;
    long currentPosition;
    Q currentState;
    State state;

    public RangeIntersectionCursor(C src, D set)
    {
        this.set = set;
        this.src = src;
        assert Cursor.compare(src.encodedPosition(), set.encodedPosition()) == 0;
        // concrete class must call setInitialState()
    }

    void setInitialState()
    {
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
    public long advance()
    {
        switch(state)
        {
            case MATCHING:
            {
                long lposition = set.advance();
                if (precedingIncludedBySet())
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

    protected abstract boolean precedingIncludedBySet();
    protected abstract boolean precedingIncludedBySource();

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
        if (precedingIncludedBySet())
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
                if (precedingIncludedBySet())
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
        if (precedingIncludedBySource())
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
        if (precedingIncludedBySet())
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
            if (precedingIncludedBySource())
                return coveredAreaWithSourceAhead(setPosition);

            // Source is ahead of set, but outside the covered area. Skip set to source's position.
            setPosition = set.skipTo(sourcePosition);
            if (Cursor.compare(setPosition, sourcePosition) == 0)
                return matchingPosition(sourcePosition);
            if (precedingIncludedBySet())
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
            if (precedingIncludedBySet())
                return coveredAreaWithSetAhead(sourcePosition);

            // Set is ahead of source, but outside the covered area. Skip source to set's position.
            sourcePosition = src.skipTo(setPosition);
            if (Cursor.compare(setPosition, sourcePosition) == 0)
                return matchingPosition(setPosition);
            if (precedingIncludedBySource())
                return coveredAreaWithSourceAhead(setPosition);
        }
    }

    private long coveredAreaWithSetAhead(long position)
    {
        return setState(State.SET_AHEAD, position, restrict(src.state(), set.precedingState()));
    }

    private long coveredAreaWithSourceAhead(long position)
    {
        return setState(State.SOURCE_AHEAD, position, restrict(src.precedingState(), set.state()));
    }

    private long matchingPosition(long position)
    {
        return setState(State.MATCHING, position, restrict(src.state(), set.state()));
    }

    protected abstract Q restrict(S srcState, R setState);

    private long setState(State state, long position, Q cursorState)
    {
        this.state = state;
        this.currentPosition = position;
        this.currentState = cursorState;
        return position;
    }

    static class RangeBySet<S extends RangeState<S>> extends RangeIntersectionCursor<S, RangeCursor<S>, TrieSetCursor.RangeState, TrieSetCursor, S>
    {
        public RangeBySet(RangeCursor<S> src, TrieSetCursor set)
        {
            super(src, set);
            setInitialState();
        }

        @Override
        public S state()
        {
            return currentState;
        }

        @Override
        protected boolean precedingIncludedBySet()
        {
            return set.precedingIncluded();
        }

        @Override
        protected boolean precedingIncludedBySource()
        {
            return src.precedingState() != null;
        }

        protected S restrict(S srcState, TrieSetCursor.RangeState setState)
        {
            if (srcState == null || setState == null)
                return null;
            if (srcState.isBoundary())
                return srcState.restrict(setState.applicableBefore, setState.applicableAfter);

            return setState.applyToCoveringState(srcState);
        }

        @Override
        public RangeCursor<S> tailCursor(Direction direction)
        {
            switch (state)
            {
                case MATCHING:
                    return new RangeBySet<>(src.tailCursor(direction), set.tailCursor(direction));
                case SET_AHEAD:
                    return src.tailCursor(direction);
                case SOURCE_AHEAD:
                    return new RangeBySet<>(src.precedingStateCursor(direction), set.tailCursor(direction));
                default:
                    throw new AssertionError();
            }
        }
    }

    static class TrieSet
    extends RangeIntersectionCursor<TrieSetCursor.RangeState, TrieSetCursor, TrieSetCursor.RangeState, TrieSetCursor, TrieSetCursor.RangeState>
    implements TrieSetCursor
    {
        public TrieSet(TrieSetCursor src, TrieSetCursor set)
        {
            super(src, set);
            setInitialState();
        }

        @Override
        protected boolean precedingIncludedBySet()
        {
            return set.precedingIncluded();
        }

        @Override
        protected boolean precedingIncludedBySource()
        {
            return src.precedingIncluded();
        }

        protected TrieSetCursor.RangeState restrict(TrieSetCursor.RangeState srcState, TrieSetCursor.RangeState setState)
        {
            if (srcState == null || setState == null)
                return RangeState.NOT_CONTAINED;

            return srcState.intersect(setState);
        }

        @Override
        public RangeState nonNullState()
        {
            return currentState;
        }

        @Override
        public TrieSetCursor tailCursor(Direction direction)
        {
            switch (state)
            {
                case MATCHING:
                    return new TrieSet(src.tailCursor(direction), set.tailCursor(direction));
                case SET_AHEAD:
                    return src.tailCursor(direction);
                case SOURCE_AHEAD:
                    return set.tailCursor(direction);
                default:
                    throw new AssertionError();
            }
        }
    }

    static class WithResolver<S extends RangeState<S>, R extends RangeState<R>, Q extends RangeState<Q>>
    extends RangeIntersectionCursor<S, RangeCursor<S>, R, RangeCursor<R>, Q>
    {
        final Predicate<? super S> includedBySource;
        final Predicate<? super R> includedBySet;
        final BiFunction<? super S, ? super R, ? extends Q> resolver;

        public WithResolver(RangeCursor<S> src, RangeCursor<R> set,
                            Predicate<? super S> includedBySource,
                            Predicate<? super R> includedBySet,
                            BiFunction<? super S, ? super R, ? extends Q> resolver)
        {
            super(src, set);
            this.includedBySet = includedBySet;
            this.includedBySource = includedBySource;
            this.resolver = resolver;
            setInitialState();
        }

        @Override
        public Q state()
        {
            return currentState;
        }

        @Override
        protected boolean precedingIncludedBySet()
        {
            return includedBySet.test(set.precedingState());
        }

        @Override
        protected boolean precedingIncludedBySource()
        {
            return includedBySource.test(src.precedingState());
        }

        @Override
        protected Q restrict(S srcState, R setState)
        {
            return resolver.apply(srcState, setState);
        }

        @Override
        public RangeCursor<Q> tailCursor(Direction direction)
        {
            switch (state)
            {
                case MATCHING:
                    return new WithResolver<>(src.tailCursor(direction), set.tailCursor(direction), includedBySource, includedBySet, resolver);
                case SET_AHEAD:
                    return new WithResolver<>(src.tailCursor(direction), set.precedingStateCursor(direction), includedBySource, includedBySet, resolver);
                case SOURCE_AHEAD:
                    return new WithResolver<>(src.precedingStateCursor(direction), set.tailCursor(direction), includedBySource, includedBySet, resolver);
                default:
                    throw new AssertionError();
            }
        }
    }
}
