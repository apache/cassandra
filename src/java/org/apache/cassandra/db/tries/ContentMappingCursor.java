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

import java.util.function.Function;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/// Cursor that sends all values through a specified content mapper.
abstract class ContentMappingCursor<T, C extends Cursor<T>, V> implements Cursor<V>
{
    final Function<T, V> mapper;
    final C source;

    ContentMappingCursor(Function<T, V> mapper, C source)
    {
        this.mapper = mapper;
        this.source = source;
    }

    @Override
    public long encodedPosition()
    {
        return source.encodedPosition();
    }

    @Override
    public long advance()
    {
        return source.advance();
    }

    @Override
    public long advanceMultiple(TransitionsReceiver receiver)
    {
        return source.advanceMultiple(receiver);
    }

    @Override
    public long skipTo(long encodedSkipPosition)
    {
        return source.skipTo(encodedSkipPosition);
    }

    public ByteComparable.Version byteComparableVersion()
    {
        return source.byteComparableVersion();
    }

    @Override
    public V content()
    {
        T content = source.content();
        if (content != null)
            return mapper.apply(content);
        else
            return null;
    }

    static class Plain<T, V> extends ContentMappingCursor<T, Cursor<T>, V> implements Cursor<V>
    {
        Plain(Function<T, V> mapper, Cursor<T> source)
        {
            super(mapper, source);
        }

        @Override
        public Cursor<V> tailCursor(Direction direction)
        {
            return new Plain<>(mapper, source.tailCursor(direction));
        }
    }

    static class Range<S extends RangeState<S>, V extends RangeState<V>> extends ContentMappingCursor<S, RangeCursor<S>, V> implements RangeCursor<V>
    {
        Range(Function<S, V> mapper, RangeCursor<S> source)
        {
            super(mapper, source);
        }

        @Override
        public V state()
        {
            S state = source.state();
            if (state == null)
                return null;
            return mapper.apply(state);
        }

        @Override
        public V precedingState()
        {
            S state = source.precedingState();
            if (state == null)
                return null;
            return mapper.apply(state);
        }

        @Override
        public RangeCursor<V> tailCursor(Direction direction)
        {
            return new Range<>(mapper, source.tailCursor(direction));
        }
    }

    static class DeletionAware<T, D extends RangeState<D>, V, E extends RangeState<E>>
    extends ContentMappingCursor<T, DeletionAwareCursor<T, D>, V> implements DeletionAwareCursor<V, E>
    {
        final Function<D, E> deletionMapper;
        DeletionAware(Function<T, V> mapper, Function<D, E> deletionMapper, DeletionAwareCursor<T, D> source)
        {
            super(mapper, source);
            this.deletionMapper = deletionMapper;
        }

        @Override
        public RangeCursor<E> deletionBranchCursor(Direction direction)
        {
            RangeCursor<D> branch = source.deletionBranchCursor(direction);
            if (branch == null)
                return null;
            return new Range<>(deletionMapper, branch);
        }

        @Override
        public DeletionAwareCursor<V, E> tailCursor(Direction direction)
        {
            return new DeletionAware<>(mapper, deletionMapper, source.tailCursor(direction));
        }
    }


    static class DeletionAwareDataOnly<T, D extends RangeState<D>, V>
    extends ContentMappingCursor<T, DeletionAwareCursor<T, D>, V> implements DeletionAwareCursor<V, D>
    {
        DeletionAwareDataOnly(Function<T, V> mapper, DeletionAwareCursor<T, D> source)
        {
            super(mapper, source);
        }

        @Override
        public RangeCursor<D> deletionBranchCursor(Direction direction)
        {
            return source.deletionBranchCursor(direction);
        }

        @Override
        public DeletionAwareCursor<V, D> tailCursor(Direction direction)
        {
            return new DeletionAwareDataOnly<>(mapper, source.tailCursor(direction));
        }
    }
}
