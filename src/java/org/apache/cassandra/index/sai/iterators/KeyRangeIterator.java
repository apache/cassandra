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
package org.apache.cassandra.index.sai.iterators;

import java.io.Closeable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.utils.AbstractGuavaIterator;

/**
 * An abstract implementation of {@link AbstractGuavaIterator} that supports the building and management of
 * concatanation, union and intersection iterators.
 * <p>
 * Only certain methods are designed to be overriden.  The others are marked private or final.
 */
public abstract class KeyRangeIterator extends AbstractGuavaIterator<PrimaryKey> implements Closeable
{
    private final PrimaryKey min, max;
    private final long count;

    protected KeyRangeIterator(Builder.Statistics statistics)
    {
        this(statistics.min, statistics.max, statistics.count);
    }

    public KeyRangeIterator(KeyRangeIterator range)
    {
        this(range == null ? null : range.min,
             range == null ? null : range.max,
             range == null ? -1 : range.count);
    }

    public KeyRangeIterator(PrimaryKey min, PrimaryKey max, long count)
    {
        boolean isComplete = min != null && max != null && count != 0;
        boolean isEmpty = min == null && max == null && (count == 0 || count == -1);
        Preconditions.checkArgument(isComplete || isEmpty, "Range: [%s,%s], Count: %d", min, max, count);

        if (isEmpty)
          endOfData();

        this.min = min;
        this.max = max;
        this.count = count;
    }

    public final PrimaryKey getMinimum()
    {
        return min;
    }

    public final PrimaryKey getMaximum()
    {
        return max;
    }

    /**
     * @return an upper bound on the number of keys that can be returned by this iterator.
     */
    public final long getMaxKeys()
    {
        return count;
    }

    /**
     * When called, this iterator's current position will
     * be skipped forwards until finding either:
     *   1) an element equal to or bigger than nextKey
     *   2) the end of the iterator
     *
     * @param nextKey value to skip the iterator forward until matching
     */
    public final void skipTo(PrimaryKey nextKey)
    {
        if (state == State.DONE)
            return;

        if (state == State.READY && next.compareTo(nextKey) >= 0)
            return;

        if (max.compareTo(nextKey) < 0)
        {
            endOfData();
            return;
        }

        performSkipTo(nextKey);
        state = State.NOT_READY;
    }

    /**
     * Skip to nextKey.
     * <p>
     * That is, implementations should set up the iterator state such that
     * calling computeNext() will return nextKey if present,
     * or the first one after it if not present.
     */
    protected abstract void performSkipTo(PrimaryKey nextKey);

    public static KeyRangeIterator empty()
    {
        return EmptyRangeIterator.instance;
    }

    private static class EmptyRangeIterator extends KeyRangeIterator
    {
        static final KeyRangeIterator instance = new EmptyRangeIterator();
        EmptyRangeIterator() { super(null, null, 0); }
        public PrimaryKey computeNext() { return endOfData(); }
        protected void performSkipTo(PrimaryKey nextKey) { }
        public void close() { }
    }

    @VisibleForTesting
    public static abstract class Builder
    {
        protected final Statistics statistics;

        public Builder(Statistics statistics)
        {
            this.statistics = statistics;
        }

        public PrimaryKey getMinimum()
        {
            return statistics.min;
        }

        public PrimaryKey getMaximum()
        {
            return statistics.max;
        }

        public long getCount()
        {
            return statistics.count;
        }

        public Builder add(Iterable<KeyRangeIterator> ranges)
        {
            if (ranges == null || Iterables.isEmpty(ranges))
                return this;

            ranges.forEach(this::add);
            return this;
        }

        public final KeyRangeIterator build()
        {
            if (rangeCount() == 0)
                return empty();
            else
                return buildIterator();
        }

        public abstract Builder add(KeyRangeIterator range);

        public abstract int rangeCount();

        public abstract void cleanup();

        protected abstract KeyRangeIterator buildIterator();

        public static abstract class Statistics
        {
            protected PrimaryKey min, max;
            protected long count;

            public abstract void update(KeyRangeIterator range);
        }
    }

    protected static PrimaryKey nullSafeMin(PrimaryKey a, PrimaryKey b)
    {
        if (a == null) return b;
        if (b == null) return a;

        return a.compareToStrict(b) > 0 ? b : a;
    }

    protected static PrimaryKey nullSafeMax(PrimaryKey a, PrimaryKey b)
    {
        if (a == null) return b;
        if (b == null) return a;

        return a.compareToStrict(b) > 0 ? a : b;
    }
}
