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

class DepthAdjustedCursor<T, C extends Cursor<T>> implements Cursor<T>
{
    final C source;
    final int depthAdjustment;
    final int incomingTransitionAtRoot;

    DepthAdjustedCursor(C source, int depthAdjustment, int incomingTransitionAtRoot)
    {
        this.source = source;
        this.depthAdjustment = depthAdjustment;
        this.incomingTransitionAtRoot = incomingTransitionAtRoot;
    }

    int toAdjustedDepth(int depth)
    {
        return depth < 0 ? depth : depth + depthAdjustment;
    }

    int fromAdjustedDepth(int depth)
    {
        return depth < 0 ? depth : depth - depthAdjustment;
    }

    @Override
    public int depth()
    {
        return toAdjustedDepth(source.depth());
    }

    @Override
    public int incomingTransition()
    {
        return source.depth() == 0 ? incomingTransitionAtRoot : source.incomingTransition();
    }

    @Override
    public T content()
    {
        return source.content();
    }

    @Override
    public Direction direction()
    {
        return source.direction();
    }

    @Override
    public ByteComparable.Version byteComparableVersion()
    {
        return source.byteComparableVersion();
    }

    @Override
    public int advance()
    {
        return toAdjustedDepth(source.advance());
    }

    @Override
    public int advanceMultiple(TransitionsReceiver receiver)
    {
        return toAdjustedDepth(source.advanceMultiple(receiver));
    }

    @Override
    public int skipTo(int skipDepth, int skipTransition)
    {
        return toAdjustedDepth(source.skipTo(fromAdjustedDepth(skipDepth), skipTransition));
    }

    @Override
    public int skipToWhenAhead(int skipDepth, int skipTransition)
    {
        int requestedDepth = fromAdjustedDepth(skipDepth);
        if (requestedDepth == 0)
            return direction().lt(skipTransition, incomingTransitionAtRoot) ? depthAdjustment : toAdjustedDepth(source.skipTo(requestedDepth, skipTransition));
        else
            return toAdjustedDepth(source.skipToWhenAhead(fromAdjustedDepth(skipDepth), skipTransition));
    }

    @Override
    public boolean descendAlong(ByteSource bytes)
    {
        return source.descendAlong(bytes);
    }

    @Override
    public Cursor<T> tailCursor(Direction direction)
    {
        return source.tailCursor(direction);
    }

    @Override
    public <R> R process(Walker<? super T, R> walker)
    {
        throw new AssertionError("Depth-adjusted cursors cannot be walked directly.");
    }

    @Override
    public <R> R processSkippingBranches(Walker<? super T, R> walker)
    {
        throw new AssertionError("Depth-adjusted cursors cannot be walked directly.");
    }

    static <T> Cursor<T> make(Cursor<T> source, int depthAdjustment, int incomingTransitionAtRoot)
    {
        return depthAdjustment == 0 ? source : new Plain<>(source, depthAdjustment, incomingTransitionAtRoot);
    }

    static <S extends RangeState<S>> RangeCursor<S> make(RangeCursor<S> source, int depthAdjustment, int incomingTransitionAtRoot)
    {
        return depthAdjustment == 0 ? source : new Range<>(source, depthAdjustment, incomingTransitionAtRoot);
    }

    static class Plain<T> extends DepthAdjustedCursor<T, Cursor<T>>
    {
        public Plain(Cursor<T> source, int depthAdjustment, int incomingTransitionAtRoot)
        {
            super(source, depthAdjustment, incomingTransitionAtRoot);
        }
    }

    static class Range<S extends RangeState<S>> extends DepthAdjustedCursor<S, RangeCursor<S>> implements RangeCursor<S>
    {
        Range(RangeCursor<S> source, int depthAdjustment, int incomingTransitionAtRoot)
        {
            super(source, depthAdjustment, incomingTransitionAtRoot);
        }

        @Override
        public S state()
        {
            return source.state();
        }

        @Override
        public S precedingState()
        {
            return source.precedingState();
        }

        @Override
        public RangeCursor<S> precedingStateCursor(Direction direction)
        {
            return source.precedingStateCursor(direction);
        }

        @Override
        public RangeCursor<S> tailCursor(Direction direction)
        {
            return source.tailCursor(direction);
        }
    }
}
