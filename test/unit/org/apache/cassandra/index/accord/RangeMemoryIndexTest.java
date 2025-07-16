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

package org.apache.cassandra.index.accord;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.function.Predicate;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.local.Node;
import accord.primitives.Routable;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.utils.Property;
import accord.utils.RandomSource;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.JournalKey;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.assertj.core.api.Assertions;

import static accord.utils.Property.commands;
import static accord.utils.Property.stateful;
import static org.apache.cassandra.index.accord.AccordIndexUtil.*;

public class RangeMemoryIndexTest
{
    private static final Logger logger = LoggerFactory.getLogger(RangeMemoryIndexTest.class);

    private static final Node.Id N1 = new Node.Id(1);
    private static final TableId TABLE_ID = TableId.UNDEFINED;
    private static final long MIN_TOKEN = 0;
    private static final long MAX_TOKEN = 1 << 16;
    private static final int STORE = 0;

    @Test
    public void minMaxFilter()
    {
        stateful().check(commands(() -> State::new)
                         .add(State::update)
                         .add(State::rangeSearch)
                         .add(State::keySearch)
                         .onSuccess((state, sut, history) -> logger.info("Successful for the following:\nState {}\nHistory:\n{}", state, Property.formatList("\t\t", history)))
                         .build());
    }

    private static TokenKey tokenKey(long token)
    {
        return new TokenKey(TABLE_ID, new Murmur3Partitioner.LongToken(token));
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
        return new TxnId(1, operation, Txn.Kind.Write, Routable.Domain.Range, N1);
    }

    private static TxnRange nextTxnRange(RandomSource rs, State state)
    {
        if (rs.decide(state.unfiltered))
            return TxnRange.FULL;

        long maxKnown = state.operations;
        long minKnown = state.model.isEmpty() ? maxKnown : state.model.minTime();
        return TxnRange.next(rs, minKnown, maxKnown, RangeMemoryIndexTest::idFor);
    }

    private static DecoratedKey pk(TxnId txnId)
    {
        return AccordKeyspace.JournalColumns.decorate(new JournalKey(txnId, JournalKey.Type.COMMAND_DIFF, STORE));
    }

    private static class State
    {
        private final RangeMemoryIndex index = new RangeMemoryIndex();
        private final Model model = new Model();
        private final float unfiltered;
        private long operations = 0;

        State(RandomSource rs)
        {
            operations = rs.nextLong(0, 1 << 20);
            unfiltered = rs.nextFloat();
        }

        public static Property.Command<State, Void, ?> update(RandomSource rs, State state)
        {
            TxnId txnId = idFor(++state.operations);
            TokenRange range = nextRange(rs);
            DecoratedKey key = pk(txnId);
            return new Property.SimpleCommand<>("update(" + normalize(txnId) + ", " + normalize(range) + ')', s2 -> {
                s2.index.add(key, txnId, range);
                s2.model.add(txnId, range);
            });
        }

        public static Property.Command<State, Void, ?> rangeSearch(RandomSource rs, State state)
        {
            var range = nextRange(rs);
            var txnRange = nextTxnRange(rs, state);
            byte[] start = OrderedRouteSerializer.serializeTokenOnly(range.start());
            byte[] end = OrderedRouteSerializer.serializeTokenOnly(range.end());
            return new Property.SimpleCommand<>("search(" + normalize(range) + ", " + txnRange + ')', s2 -> {
                var actual = normalizePks(state.index.search(STORE, TABLE_ID, start, end,  txnRange.minTxnId, txnRange.maxTxnId));
                var expected = state.model.search(range, txnRange.minTxnId, txnRange.maxTxnId);
                Assertions.assertThat(actual).isEqualTo(expected);
            });
        }

        public static Property.Command<State, Void, ?> keySearch(RandomSource rs, State state)
        {
            TokenKey key = tokenKey(rs.nextLong(MIN_TOKEN, MAX_TOKEN + 1));
            var txnRange = nextTxnRange(rs, state);
            var start = OrderedRouteSerializer.serializeTokenOnly(key);
            return new Property.SimpleCommand<>("search(" + normalize(key) + ", " + txnRange + ')', s2 -> {
                var actual = normalizePks(state.index.search(STORE, TABLE_ID, start, txnRange.minTxnId, txnRange.maxTxnId));
                var expected = state.model.search(key, txnRange.minTxnId, txnRange.maxTxnId);
                Assertions.assertThat(actual).isEqualTo(expected);
            });
        }
    }

    private static NavigableSet<TxnId> normalizePks(NavigableSet<ByteBuffer> set)
    {
        if (set.isEmpty()) return Collections.emptyNavigableSet();
        TreeSet<TxnId> result = new TreeSet<>();
        for (var bb : set)
            result.add(AccordKeyspace.JournalColumns.getJournalKey(bb).id);
        return result;
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

        private final List<Model.Value> values = new ArrayList<>();
        private long minTime = Long.MAX_VALUE;

        public void add(TxnId txnId, TokenRange range)
        {
            values.add(new Model.Value(range, txnId));
            minTime = Math.min(minTime, txnId.hlc());
        }

        public long minTime()
        {
            return minTime;
        }

        public boolean isEmpty()
        {
            return values.isEmpty();
        }

        public NavigableSet<TxnId> search(TokenRange range, TxnId minTxnId, TxnId maxTxnId)
        {
            return search(r -> r.compareIntersecting(range) == 0, minTxnId, maxTxnId);
        }

        public NavigableSet<TxnId> search(TokenKey key, TxnId minTxnId, TxnId maxTxnId)
        {
            return search(r -> r.contains(key), minTxnId, maxTxnId);
        }

        public NavigableSet<TxnId> search(Predicate<TokenRange> test, TxnId minTxnId, TxnId maxTxnId)
        {
            NavigableSet<TxnId> result = new TreeSet<>();
            for (var value : values)
            {
                if (value.txnId.compareTo(minTxnId) < 0 || value.txnId.compareTo(maxTxnId) > 0) continue;
                if (test.test(value.range))
                    result.add(value.txnId);
            }
            return result;
        }
    }
}