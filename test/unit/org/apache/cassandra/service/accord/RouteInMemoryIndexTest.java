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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import org.apache.cassandra.dht.Murmur3Partitioner.LongToken;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.utils.CloseableIterator;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.commands;
import static accord.utils.Property.stateful;

public class RouteInMemoryIndexTest
{
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
                         .add(State::unfilteredSearch)
                         .add(State::search)
                         .onSuccess((state, sut, history) -> logger.info("Successful for the following:\nState {}\nHistory:\n{}", state, Property.formatList("\t\t", history)))
                         .build());
    }

    private static class State
    {
        private final RouteInMemoryIndex<?> index = new RouteInMemoryIndex<>();
        private final Model model = new Model();
        private long currentSegment = 0;
        private LongArrayList activeSegments = new LongArrayList();
        private long operations = 0;

        State(RandomSource rs)
        {
            activeSegments.add(currentSegment);
        }

        TxnId idFor(long operation)
        {
            return new TxnId(1, operation, Txn.Kind.Write, Routable.Domain.Range, N1);
        }

        public static Property.Command<State, Void, ?> update(RandomSource rs, State state)
        {
            long segment = state.currentSegment;
            TxnId txnId = state.idFor(++state.operations);
            TokenRange range = nextRange(rs);

            FullRangeRoute route = new FullRangeRoute(range.start(), new Range[] {range});
            return new Property.SimpleCommand<>("update(" + segment + ", " + txnId + ", " + range + ')', s2 -> {
                s2.index.update(segment, 0, txnId, route);
                s2.model.update(segment, range, txnId);
            });
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
            return TokenRange.createUnsafe(new TokenKey(TABLE_ID, new LongToken(a)), new TokenKey(TABLE_ID, new LongToken(b)));
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

        private void assertSearchMatch(TokenRange range, TxnId minTxnId, TxnId maxTxnId)
        {
            try (var actual = index.search(0, range, minTxnId, maxTxnId).results();
                 var expected = model.search(range, minTxnId, maxTxnId).results())
            {
                while (actual.hasNext() && expected.hasNext())
                {
                    Assertions.assertThat(actual.next()).isEqualTo(expected.next());
                }
                Assertions.assertThat(actual).describedAs("Expected iterator was exhausted first!").isExhausted();
                Assertions.assertThat(expected).describedAs("Actual iterator was exhausted first!").isExhausted();
            }
        }

        public static Property.Command<State, Void, ?> unfilteredSearch(RandomSource rs, State state)
        {
            var range = nextRange(rs);
            TxnId minTxnId = TxnId.NONE;
            TxnId maxTxnId = TxnId.MAX;
            return new Property.SimpleCommand<>("Search " + range, s2 -> s2.assertSearchMatch(range, minTxnId, maxTxnId));
        }

        public static Property.Command<State, Void, ?> search(RandomSource rs, State state)
        {
            var range = nextRange(rs);
            TxnId minTxnId;
            TxnId maxTxnId;
            if (state.model.isEmpty())
            {
                // just do random
                minTxnId = state.idFor(rs.nextLong(1, 1 << 16));
                maxTxnId = state.idFor(rs.nextLong(1, 1 << 16));
            }
            else
            {
                long minKnown = state.model.minTime();
                long maxKnown = state.operations;
                switch (rs.nextInt(0, 3))
                {
                    case 0: // future
                    {
                        minTxnId = state.idFor(state.operations + 10);
                        maxTxnId = state.idFor(state.operations + 100);
                    }
                    break;
                    case 1: // past
                    {
                        minTxnId = state.idFor(Math.max(1, minKnown - 100));
                        maxTxnId = state.idFor(Math.max(1, minKnown - 10));
                    }
                    break;
                    case 2: // present-ish
                    {
                        // this can cause min/max to be reversed!
                        minTxnId = state.idFor(Math.max(1, minKnown + 10));
                        maxTxnId = state.idFor(Math.max(1, maxKnown - 10));
                    }
                    break;
                    default:
                        throw new UnsupportedOperationException();
                }
            }
            return new Property.SimpleCommand<>("Search " + range + ", txn_id range " + minTxnId + ',' + maxTxnId, s2 -> s2.assertSearchMatch(range, minTxnId, maxTxnId));
        }
    }

    private static class Model
    {
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
        }
        private final Long2ObjectHashMap<Segment> segments = new Long2ObjectHashMap<>();

        void update(long segment, TokenRange range, TxnId txnId)
        {
            segments.computeIfAbsent(segment, i -> new Segment()).values.add(new Value(range, txnId));
        }

        public RangeSearcher.Result search(TokenRange range, TxnId minTxnId, TxnId maxTxnId)
        {
            TreeSet<TxnId> result = new TreeSet<>();
            for (var segment: segments.values())
            {
                for (var value : segment.values)
                {
                    if (value.txnId.compareTo(minTxnId) < 0 || value.txnId.compareTo(maxTxnId) > 0) continue;
                    if (range.compareIntersecting(value.range) == 0)
                        result.add(value.txnId);
                }
            }
            return new RangeSearcher.DefaultResult(minTxnId, maxTxnId, CloseableIterator.wrap(result.iterator()));
        }

        void remove(long segment)
        {
            segments.remove(segment);
        }
    }
}