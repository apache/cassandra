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
    private long depthAdjustment;
    private long matchingPositionAtRoot;

    DepthAdjustedCursor(C source, long matchingPositionAtRoot)
    {
        this.source = source;
        setAttachmentPoint(matchingPositionAtRoot);
    }

    void setAttachmentPoint(long matchingPositionAtRoot)
    {
        this.matchingPositionAtRoot = matchingPositionAtRoot;
        this.depthAdjustment = Cursor.depthCorrectionValue(matchingPositionAtRoot);
    }

    long toAdjustedDepth(long position)
    {
        if (Cursor.depth(position) > 0)
            return position + depthAdjustment;
        else if (Cursor.isExhausted(position))
            return position;
        else
            return matchingPositionAtRoot | (position & Cursor.ON_RETURN_PATH_BIT);
    }

    long fromAdjustedDepth(long position)
    {
        // matchingPositionAtRoot | ON_RETURN_PATH_BIT should map to rootPosition | ON_RETURN_PATH_BIT
        long adjusted = position - depthAdjustment;
        if (Cursor.depth(adjusted) > 0)
            return adjusted;

        // The only non-exhausted position that can be requested with this depth is the return path stop for the root.
        if (position == (matchingPositionAtRoot | Cursor.ON_RETURN_PATH_BIT))
            return Cursor.rootReturnPosition(adjusted);
        else
            return Cursor.exhaustedPosition(adjusted);

    }

    @Override
    public long encodedPosition()
    {
        return toAdjustedDepth(source.encodedPosition());
    }

    @Override
    public T content()
    {
        return source.content();
    }

    @Override
    public ByteComparable.Version byteComparableVersion()
    {
        return source.byteComparableVersion();
    }

    @Override
    public long advance()
    {
        return toAdjustedDepth(source.advance());
    }

    @Override
    public long advanceMultiple(TransitionsReceiver receiver)
    {
        return toAdjustedDepth(source.advanceMultiple(receiver));
    }

    @Override
    public long skipTo(long encodedSkipPosition)
    {
        return toAdjustedDepth(source.skipTo(fromAdjustedDepth(encodedSkipPosition)));
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

    static <T> Cursor<T> make(Cursor<T> source, long matchingPositionAtRoot)
    {
        return Cursor.depth(matchingPositionAtRoot) == 0 ? source : new Plain<>(source, matchingPositionAtRoot);
    }

    static <S extends RangeState<S>> RangeCursor<S> make(RangeCursor<S> source, long matchingPositionAtRoot)
    {
        return Cursor.depth(matchingPositionAtRoot) == 0 ? source : new Range<>(source, matchingPositionAtRoot);
    }

    static class Plain<T> extends DepthAdjustedCursor<T, Cursor<T>>
    {
        public Plain(Cursor<T> source, long matchingPositionAtRoot)
        {
            super(source, matchingPositionAtRoot);
        }
    }

    static class Range<S extends RangeState<S>> extends DepthAdjustedCursor<S, RangeCursor<S>> implements RangeCursor<S>
    {
        Range(RangeCursor<S> source, long matchingPositionAtRoot)
        {
            super(source, matchingPositionAtRoot);
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
