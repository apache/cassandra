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
    final Direction direction;
    final C c1;
    @Nullable D c2;
    int c2depthCorrection;
    int incomingTransition;
    int depth;

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
        assert c1.depth() == 0;
        this.direction = c1.direction();
        this.c1 = c1;
        this.c2 = null;
        state = State.C1_ONLY;
        incomingTransition = c1.incomingTransition();
        depth = c1.depth();
        // We can't call postAdvance here because the class may not be completely initialized.
        // The concrete class should do that instead
    }

    FlexibleMergeCursor(C c1, D c2)
    {
        assert c1.depth() == 0;
        assert c2.depth() == 0;
        this.direction = c1.direction();
        this.c1 = c1;
        this.c2 = c2;
        this.c2depthCorrection = 0;
        state = c2 != null ? State.AT_BOTH : State.C1_ONLY;
        incomingTransition = c1.incomingTransition();
        depth = c1.depth();
        // We can't call postAdvance here because the class may not be completely initialized.
        // The concrete class should do that instead
    }

    public void addCursor(D c2)
    {
        assert state == State.C1_ONLY : "Attempting to add further cursors to a cursor that already has two sources";
        assert c2.depth() == 0 : "Only cursors rooted at the current position can be added";
        this.c2 = c2;
        this.c2depthCorrection = c1.depth();
        this.state = State.AT_BOTH;
    }

    abstract int postAdvance(int depth);

    @Override
    public int advance()
    {
        switch (state)
        {
            case C1_ONLY:
                return inC1Only(c1.advance());
            case AT_C1:
                return checkOrder(c1.advance(), c2.depth());
            case AT_C2:
                return checkOrder(c1.depth(), c2.advance());
            case AT_BOTH:
                return checkOrder(c1.advance(), c2.advance());
            default:
                throw new AssertionError();
        }
    }

    @Override
    public int skipTo(int skipDepth, int skipTransition)
    {
        if (state == State.C1_ONLY)
            return inC1Only(c1.skipTo(skipDepth, skipTransition));

        // Handle request to exit c2 branch separately for simplicity
        if (skipDepth <= c2depthCorrection)
        {
            switch (state)
            {
                case AT_C1:
                case AT_BOTH:
                    return leaveC2(c1.skipTo(skipDepth, skipTransition));
                case AT_C2:
                    return leaveC2(c1.skipToWhenAhead(skipDepth, skipTransition));
                default:
                    throw new AssertionError();
            }
        }

        int c2skipDepth = skipDepth - c2depthCorrection;
        switch (state)
        {
            case AT_C1:
                return checkOrder(c1.skipTo(skipDepth, skipTransition), c2.skipToWhenAhead(c2skipDepth, skipTransition));
            case AT_C2:
                return checkOrder(c1.skipToWhenAhead(skipDepth, skipTransition), c2.skipTo(c2skipDepth, skipTransition));
            case AT_BOTH:
                return checkOrder(c1.skipTo(skipDepth, skipTransition), c2.skipTo(c2skipDepth, skipTransition));
            default:
                throw new AssertionError();
        }
    }

    @Override
    public int advanceMultiple(TransitionsReceiver receiver)
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
                return checkOrder(c1.advanceMultiple(receiver), c2.depth());
            case AT_C2:
                return checkOrder(c1.depth(), c2.advanceMultiple(receiver));
            // While we are on a shared position, we must descend one byte at a time to maintain the cursor ordering.
            case AT_BOTH:
                return checkOrder(c1.advance(), c2.advance());
            default:
                throw new AssertionError();
        }
    }

    private int inC1Only(int c1depth)
    {
        incomingTransition = c1.incomingTransition();
        depth = c1depth;
        return postAdvance(c1depth);
    }

    private int checkOrder(int c1depth, int c2depthUncorrected)
    {
        if (c2depthUncorrected < 0)
            return leaveC2(c1depth);

        int c2depth = c2depthUncorrected + c2depthCorrection;
        if (c1depth > c2depth)
        {
            state = State.AT_C1;
            incomingTransition = c1.incomingTransition();
            depth = c1depth;
            return postAdvance(c1depth);
        }
        if (c1depth < c2depth)
        {
            state = State.AT_C2;
            incomingTransition = c2.incomingTransition();
            depth = c2depth;
            return postAdvance(c2depth);
        }
        // c1depth == c2depth
        int c1trans = c1.incomingTransition();
        int c2trans = c2.incomingTransition();
        boolean c1ahead = direction.gt(c1trans, c2trans);
        boolean c2ahead = direction.lt(c1trans, c2trans);
        state = c2ahead ? State.AT_C1 : c1ahead ? State.AT_C2 : State.AT_BOTH;
        incomingTransition = c1ahead ? c2trans : c1trans;
        depth = c1depth;
        return postAdvance(c1depth);
    }

    private int leaveC2(int c1depth)
    {
        state = State.C1_ONLY;
        c2 = null;
        incomingTransition = c1.incomingTransition();
        depth = c1depth;
        return postAdvance(c1depth);
    }

    @Override
    public int depth()
    {
        return depth;
    }

    @Override
    public int incomingTransition()
    {
        return incomingTransition;
    }

    @Override
    public Direction direction()
    {
        return direction;
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
