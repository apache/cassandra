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

package org.apache.cassandra.service.accord;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.function.Predicate;

import javax.annotation.Nullable;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.MaxDecidedRX.DecidedRX;
import accord.local.Node;
import accord.primitives.FullRangeRoute;
import accord.primitives.Range;
import accord.primitives.Routable;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.Property;
import accord.utils.RandomSource;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongArrayList;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.index.accord.RouteIndexFormat;
import org.apache.cassandra.index.accord.TxnRange;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.utils.CloseableIterator;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.commands;
import static accord.utils.Property.stateful;
import static org.apache.cassandra.index.accord.AccordIndexUtil.normalize;

public class RouteInMemoryIndexTest
{
    static
    {
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    private static final Logger logger = LoggerFactory.getLogger(RouteInMemoryIndexTest.class);

    private static final Node.Id N1 = new Node.Id(1);
    private static final TableId TABLE_ID = TableId.UNDEFINED;
    private static final long MIN_TOKEN = 0;
    private static final long MAX_TOKEN = 1 << 16;

    @Test
    public void minMaxFilter()
    {
        stateful().check(commands(() -> State::new)
                         .add(State::update)
                         .add(State::bumpSegment)
                         .add(State::remove)
                         .add(State::rangeSearch)
                         .add(State::keySearch)
                         .onSuccess((state, sut, history) -> logger.info("Successful for the following:\nState {}\nHistory:\n{}", state, Property.formatList("\t\t", history)))
                         .build());
    }


    private static TokenKey tokenKey(long token)
    {
        return new TokenKey(TABLE_ID, new LongToken(token));
    }

    private static TokenRange nextRange(RandomSource rs)
    {
        long token = rs.nextLong(MIN_TOKEN, MAX_TOKEN + 1);
        long a, b;
        if (token + 10 > MAX_TOKEN)
        {
            a = token - 10;
            b = token;
        }
        else
        {
            a = token;
            b = token + 10;
        }
        return TokenRange.createUnsafe(tokenKey(a), tokenKey(b));
    }

    static TxnId idFor(long operation)
    {
        return new TxnId(1, operation, Txn.Kind.ExclusiveSyncPoint, Routable.Domain.Range, N1);
    }

    private static TxnRange nextTxnRange(RandomSource rs, State state)
    {
        if (rs.decide(state.unfiltered))
            return TxnRange.FULL;

        long maxKnown = state.operations;
        long minKnown = state.model.isEmpty() ? maxKnown : state.model.minTime();
        return TxnRange.next(rs, minKnown, maxKnown, RouteInMemoryIndexTest::idFor);
    }

    private static @Nullable DecidedRX nextDecidedRX(RandomSource rs, State state)
    {
        if (rs.decide(state.minDecidedIdNull)) return null;
        long maxKnown = state.operations;
        long minKnown = state.model.isEmpty() ? maxKnown : state.model.minTime();
        TxnId txnId = minKnown == maxKnown ? idFor(maxKnown)
                                           : idFor(rs.nextLong(minKnown, maxKnown));
        return new DecidedRX(txnId, txnId);
    }

    private static class State
    {
        private final RouteInMemoryIndex<?> index = new RouteInMemoryIndex<>();
        private final Model model = new Model();
        private final float unfiltered;
        private final float minDecidedIdNull;
        private long currentSegment = 0;
        private LongArrayList activeSegments = new LongArrayList();
        private long operations = 0;

        State(RandomSource rs)
        {
            activeSegments.add(currentSegment);

            unfiltered = rs.nextFloat();
            minDecidedIdNull = rs.nextFloat();
        }

        public static Property.Command<State, Void, ?> update(RandomSource rs, State state)
        {
            long segment = state.currentSegment;
            TxnId txnId = idFor(++state.operations);
            TokenRange range = nextRange(rs);

            FullRangeRoute route = new FullRangeRoute(range.start(), new Range[] {range});
            return new Property.SimpleCommand<>("update(" + segment + ", " + normalize(txnId) + ", " + normalize(range) + ')', s2 -> {
                s2.index.update(segment, 0, txnId, route);
                s2.model.update(segment, txnId, range);
            });
        }

        public static Property.Command<State, Void, ?> remove(RandomSource rs, State state)
        {
            if (state.activeSegments.size() == 1)
                return Property.ignoreCommand();
            int allowedSize = state.activeSegments.size() - 1; // need to keep the current segment
            int size = allowedSize == 1 ? 1 : rs.nextInt(1, allowedSize);

            // if the view is used, then it gets corrupted while removing, so copy the view result
            var sublist = new ArrayList<>(state.activeSegments.subList(0, size));
            Assertions.assertThat(sublist).doesNotContain(state.currentSegment);

            return new Property.SimpleCommand<>("Remove " + sublist, s2 -> {
                s2.index.removeForTests(sublist);
                sublist.forEach(s2.model::remove);
                state.activeSegments.removeAll(sublist);
            }) {
                @Override
                public void checkPostconditions(State state, Void sut) throws Throwable
                {
                    Assertions.assertThat(state.activeSegments).isNotEmpty();
                }
            };
        }

        public static Property.Command<State, Void, ?> bumpSegment(RandomSource rs, State state)
        {
            long before = state.currentSegment;
            long after = before + 1;
            return new Property.SimpleCommand<>("Segment " + before + "->" + after, s2 -> {
                s2.activeSegments.add(after);
                state.currentSegment = after;
            });
        }

        public static Property.Command<State, Void, ?> rangeSearch(RandomSource rs, State state)
        {
            var range = nextRange(rs);
            var txnRange = nextTxnRange(rs, state);
            @Nullable DecidedRX decidedRX = nextDecidedRX(rs, state);
            return new Property.SimpleCommand<>("Search " + normalize(range) + ", txn_id range " + txnRange + ", minDecidedId " + normalize(decidedRX), s2 -> s2.assertSearchMatch(range, txnRange.minTxnId, txnRange.maxTxnId, decidedRX));
        }

        public static Property.Command<State, Void, ?> keySearch(RandomSource rs, State state)
        {
            TokenKey key = tokenKey(rs.nextLong(MIN_TOKEN, MAX_TOKEN + 1));
            var txnRange = nextTxnRange(rs, state);
            @Nullable DecidedRX decidedRX = nextDecidedRX(rs, state);
            return new Property.SimpleCommand<>("Search " + normalize(key) + ", txn_id range " + txnRange + ", decidedRX " + normalize(decidedRX), s2 -> s2.assertSearchMatch(key, txnRange.minTxnId, txnRange.maxTxnId, decidedRX));
        }

        private void assertSearchMatch(TokenRange range, TxnId minTxnId, TxnId maxTxnId, @Nullable DecidedRX decidedRX)
        {
            List<TxnId> actual = new ArrayList<>();
            index.search(0, range, minTxnId, maxTxnId, decidedRX).consume(actual::add);
            List<TxnId> expected = new ArrayList<>();
            model.search(range, minTxnId, maxTxnId, decidedRX).consume(expected::add);
            Assertions.assertThat(actual).isEqualTo(expected);
        }

        private void assertSearchMatch(TokenKey key, TxnId minTxnId, TxnId maxTxnId, @Nullable DecidedRX decidedRX)
        {
            List<TxnId> actual = new ArrayList<>();
            index.search(0, key, minTxnId, maxTxnId, decidedRX).consume(actual::add);
            List<TxnId> expected = new ArrayList<>();
            model.search(key, minTxnId, maxTxnId, decidedRX).consume(expected::add);
            Assertions.assertThat(actual).isEqualTo(expected);
        }
    }

    private static class Model
    {
        private static class Value
        {
            final TokenRange range;
            final TxnId txnId;

            private Value(TokenRange range, TxnId txnId)
            {
                this.range = range;
                this.txnId = txnId;
            }
        }
        private static class Segment
        {
            private final List<Value> values = new ArrayList<>();
            private TxnId maxRXId = TxnId.NONE;

            void add(TokenRange range, TxnId txnId)
            {
                values.add(new Value(range, txnId));
                if (txnId.is(Txn.Kind.ExclusiveSyncPoint) && txnId.compareTo(maxRXId) > 0)
                    maxRXId = txnId;
            }
        }

        public long minTime()
        {
            long min = Long.MAX_VALUE;
            for (var segment : segments.values())
            {
                for (var value : segment.values)
                    min = Math.min(min, value.txnId.hlc());
            }
            return min;
        }

        public boolean isEmpty()
        {
            return segments.isEmpty();
        }
        private final Long2ObjectHashMap<Segment> segments = new Long2ObjectHashMap<>();

        void update(long segment, TxnId txnId, TokenRange range)
        {
            segments.computeIfAbsent(segment, i -> new Segment()).add(range, txnId);
        }

        public RangeSearcher.Result search(TokenRange range, TxnId minTxnId, TxnId maxTxnId, @Nullable DecidedRX decidedRX)
        {
            return search(vrange -> range.compareIntersecting(vrange) == 0, minTxnId, maxTxnId, decidedRX);
        }

        public RangeSearcher.Result search(TokenKey key, TxnId minTxnId, TxnId maxTxnId, @Nullable DecidedRX decidedRX)
        {
            return search(range -> range.contains(key), minTxnId, maxTxnId, decidedRX);
        }

        public RangeSearcher.Result search(Predicate<TokenRange> test, TxnId minTxnId, TxnId maxTxnId, @Nullable DecidedRX decidedRX)
        {
            TreeSet<TxnId> result = new TreeSet<>();
            for (var segment: segments.values())
            {
                if (!RouteIndexFormat.includeByDecidedRX(decidedRX, segment.maxRXId)) continue;
                for (var value : segment.values)
                {
                    if (value.txnId.compareTo(minTxnId) < 0 || value.txnId.compareTo(maxTxnId) > 0) continue;
                    if (test.test(value.range))
                        result.add(value.txnId);
                }
            }
            return new RangeSearcher.DefaultResult(minTxnId, maxTxnId, decidedRX, CloseableIterator.wrap(result.iterator()));
        }

        void remove(long segment)
        {
            segments.remove(segment);
        }
    }
}