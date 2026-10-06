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

import javax.annotation.Nullable;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

abstract class FlexibleMergeCursor<C extends Cursor<?>, D extends Cursor<?>, T> implements Cursor<T>
{
    final C c1;
    @Nullable D c2;
    long c2depthCorrection;
    long currentPosition;

    enum State
    {
        AT_C1,
        AT_C2,
        AT_BOTH,
        C1_ONLY,    // c2 is null
    }
    State state;

    FlexibleMergeCursor(C c1)
    {
        this.c1 = c1;
        this.c2 = null;
        state = State.C1_ONLY;
        currentPosition = c1.getPositionAndAssertFresh();
        // We can't call postAdvance here because the class may not be completely initialized.
        // The concrete class should do that instead
    }

    FlexibleMergeCursor(C c1, D c2)
    {
        c1.assertFresh();
        if (c2 != null)
            c2.assertFresh();
        this.c1 = c1;
        this.c2 = c2;
        this.c2depthCorrection = 0;
        state = c2 != null ? State.AT_BOTH : State.C1_ONLY;
        currentPosition = c1.encodedPosition();
        if (c2 != null)
            currentPosition = Cursor.unionFlagsMatchingPositions(currentPosition, c2.encodedPosition());
        // We can't call postAdvance here because the class may not be completely initialized.
        // The concrete class should do that instead
    }

    public void addCursor(D c2)
    {
        assert state == State.C1_ONLY : "Attempting to add further cursors to a cursor that already has two sources";
        this.c2 = c2;
        this.c2depthCorrection = Cursor.depthCorrectionValue(currentPosition);
        this.state = State.AT_BOTH;
        currentPosition = Cursor.unionFlags(currentPosition, c2.getPositionAndAssertFresh(), Cursor.FLAGS_MASK);
    }

    abstract long postAdvance(long depth);

    @Override
    public long advance()
    {
        switch (state)
        {
            case C1_ONLY:
                return inC1Only(c1.advance());
            case AT_C1:
                return checkOrder(c1.advance(), c2.encodedPosition());
            case AT_C2:
                return checkOrder(c1.encodedPosition(), c2.advance());
            case AT_BOTH:
                return checkOrder(c1.advance(), c2.advance());
            default:
                throw new AssertionError();
        }
    }

    @Override
    public long skipTo(long encodedSkipPosition)
    {
        if (state == State.C1_ONLY)
            return inC1Only(c1.skipTo(encodedSkipPosition));

        long c2encodedSkipPosition = encodedSkipPosition - c2depthCorrection;
        // Handle request to exit c2 branch separately for simplicity
        if (Cursor.isExhausted(c2encodedSkipPosition))
        {
            switch (state)
            {
                case AT_C1:
                case AT_BOTH:
                    return leaveC2(c1.skipTo(encodedSkipPosition));
                case AT_C2:
                    return leaveC2(c1.skipToWhenAhead(encodedSkipPosition));
                default:
                    throw new AssertionError();
            }
        }

        switch (state)
        {
            case AT_C1:
                return checkOrder(c1.skipTo(encodedSkipPosition), c2.skipToWhenAhead(c2encodedSkipPosition));
            case AT_C2:
                return checkOrder(c1.skipToWhenAhead(encodedSkipPosition), c2.skipTo(c2encodedSkipPosition));
            case AT_BOTH:
                return checkOrder(c1.skipTo(encodedSkipPosition), c2.skipTo(c2encodedSkipPosition));
            default:
                throw new AssertionError();
        }
    }

    @Override
    public long advanceMultiple(TransitionsReceiver receiver)
    {
        switch (state)
        {
            case C1_ONLY:
                return inC1Only(c1.advanceMultiple(receiver));
            // If we are in a branch that's only covered by one of the sources, we can use its advanceMultiple as it is
            // only different from advance if it takes multiple steps down, which does not change the order of the
            // cursors.
            // Since it might ascend, we still have to check the order after the call.
            case AT_C1:
                return checkOrder(c1.advanceMultiple(receiver), c2.encodedPosition());
            case AT_C2:
                return checkOrder(c1.encodedPosition(), c2.advanceMultiple(receiver));
            // While we are on a shared position, we must descend one byte at a time to maintain the cursor ordering.
            case AT_BOTH:
                return checkOrder(c1.advance(), c2.advance());
            default:
                throw new AssertionError();
        }
    }

    private long inC1Only(long c1pos)
    {
        return postAdvance(currentPosition = c1pos);
    }

    private long checkOrder(long c1pos, long c2posUncorrected)
    {
        if (Cursor.isExhausted(c2posUncorrected))
            return leaveC2(c1pos);

        long c2pos = c2posUncorrected + c2depthCorrection;
        long cmp = Cursor.compare(c1pos, c2pos);
        if (cmp < 0)
        {
            state = State.AT_C1;
            return postAdvance(currentPosition = c1pos);
        }
        if (cmp > 0)
        {
            state = State.AT_C2;
            return postAdvance(currentPosition = c2pos);
        }
        // c1pos == c2pos
        state = State.AT_BOTH;
        currentPosition = Cursor.unionFlagsMatchingPositions(c1pos, c2pos);
        return postAdvance(currentPosition);
    }

    private long leaveC2(long c1pos)
    {
        state = State.C1_ONLY;
        c2 = null;
        return postAdvance(currentPosition = c1pos);
    }

    @Override
    public long encodedPosition()
    {
        return currentPosition;
    }

    @Override
    public ByteComparable.Version byteComparableVersion()
    {
        return c1.byteComparableVersion();
    }

    static abstract class WithMappedContent<T, U, C extends Cursor<T>, D extends Cursor<U>, Z> extends FlexibleMergeCursor<C, D, Z>
    {
        final BiFunction<T, U, Z> resolver;

        WithMappedContent(BiFunction<T, U, Z> resolver, C c1)
        {
            super(c1);
            this.resolver = resolver;
        }

        WithMappedContent(BiFunction<T, U, Z> resolver, C c1, D c2)
        {
            super(c1, c2);
            this.resolver = resolver;
        }

        @Override
        public Z content()
        {
            U mc = null;
            T nc = null;
            switch (state)
            {
                case C1_ONLY:
                case AT_C1:
                    nc = c1.content();
                    break;
                case AT_C2:
                    mc = c2.content();
                    break;
                case AT_BOTH:
                    mc = c2.content();
                    nc = c1.content();
                    break;
                default:
                    throw new AssertionError();
            }
            if (nc == null && mc == null)
                return null;
            return resolver.apply(nc, mc);
        }
    }
}
