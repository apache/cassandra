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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Nullable;

import com.google.common.collect.AbstractIterator;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.impl.CommandTimeseries;
import accord.impl.CommandTimeseriesHolder;
import accord.local.Command;
import accord.local.SaveStatus;
import accord.primitives.AbstractKeys;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import accord.primitives.Seekable;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.IntervalTree;

public class CommandsForRanges
{
    public static final class RangeCommandSummary
    {
        public final TxnId txnId;
        public final SaveStatus status;
        public final @Nullable Timestamp executeAt;
        public final List<TxnId> deps;

        RangeCommandSummary(TxnId txnId, SaveStatus status, @Nullable Timestamp executeAt, List<TxnId> deps)
        {
            this.txnId = txnId;
            this.status = status;
            this.executeAt = executeAt;
            this.deps = deps;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RangeCommandSummary that = (RangeCommandSummary) o;
            return txnId.equals(that.txnId);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(txnId);
        }

        @Override
        public String toString()
        {
            return "RangeCommandSummary{" +
                   "txnId=" + txnId +
                   ", status=" + status +
                   '}';
        }
    }

    private enum RangeCommandSummaryLoader implements CommandTimeseries.CommandLoader<RangeCommandSummary>
    {
        INSTANCE;

        @Override
        public RangeCommandSummary saveForCFK(Command command)
        {
            //TODO split write from read?
            throw new UnsupportedOperationException();
        }

        @Override
        public TxnId txnId(RangeCommandSummary data)
        {
            return data.txnId;
        }

        @Override
        public Timestamp executeAt(RangeCommandSummary data)
        {
            return data.executeAt;
        }

        @Override
        public SaveStatus saveStatus(RangeCommandSummary data)
        {
            return data.status;
        }

        @Override
        public List<TxnId> depsIds(RangeCommandSummary data)
        {
            return data.deps;
        }
    }

    public class Builder
    {
        private final IntervalTree.Builder<RoutableKey, RangeCommandSummary, Interval<RoutableKey, RangeCommandSummary>> builder;

        private Builder(IntervalTree.Builder<RoutableKey, RangeCommandSummary, Interval<RoutableKey, RangeCommandSummary>> builder)
        {
            this.builder = builder;
        }

        public Builder put(Ranges ranges, TxnId txnId, SaveStatus status, Timestamp execteAt, List<TxnId> dependsOn)
        {
            remove(txnId);
            return put(ranges, new RangeCommandSummary(txnId, status, execteAt, dependsOn));
        }

        private Builder put(Ranges ranges, RangeCommandSummary summary)
        {
            for (Range range : ranges)
                put(range, summary);
            return this;
        }

        private Builder put(Range range, RangeCommandSummary summary)
        {
            builder.add(Interval.create(normalize(range.start(), range.startInclusive(), true),
                                        normalize(range.end(), range.endInclusive(), false),
                                        summary));
            return this;
        }

        private Builder remove(TxnId txnId)
        {
            return removeIf(data -> data.txnId.equals(txnId));
        }

        private Builder removeIf(Predicate<RangeCommandSummary> predicate)
        {
            return removeIf((i1, i2, data) -> predicate.test(data));
        }

        private Builder removeIf(IntervalTree.Builder.TriPredicate<RoutableKey, RoutableKey, RangeCommandSummary> predicate)
        {
            builder.removeIf(predicate);
            return this;
        }

        public void apply()
        {
            rangesToCommands = builder.build();
        }
    }
    
    private IntervalTree<RoutableKey, RangeCommandSummary, Interval<RoutableKey, RangeCommandSummary>> rangesToCommands = IntervalTree.emptyTree();

    public Iterable<CommandTimeseriesHolder> search(AbstractKeys<Key, ?> keys)
    {
        // group by the keyspace, as ranges are based off TokenKey, which is scoped to a range
        Map<String, List<Key>> groupByKeyspace = new HashMap<>();
        for (Key key : keys)
            groupByKeyspace.computeIfAbsent(((PartitionKey) key).keyspace(), ignore -> new ArrayList<>()).add(key);
        // TODO (determinism) : this can break simulator as its not deteramnistic
        return () -> new AbstractIterator<CommandTimeseriesHolder>()
        {
            Iterator<String> ksIt = groupByKeyspace.keySet().iterator();
            Iterator<Map.Entry<Range, Set<RangeCommandSummary>>> rangeIt;

            @Override
            protected CommandTimeseriesHolder computeNext()
            {
                while (true)
                {
                    if (rangeIt != null && rangeIt.hasNext())
                    {
                        Map.Entry<Range, Set<RangeCommandSummary>> next = rangeIt.next();
                        return result(next.getKey(), next.getValue());
                    }
                    rangeIt = null;
                    if (!ksIt.hasNext())
                    {
                        ksIt = null;
                        return endOfData();
                    }
                    String ks = ksIt.next();
                    List<Key> keys = groupByKeyspace.get(ks);
                    // TODO (determinism) : this can break simulator as its not deteramnistic
                    Map<Range, Set<RangeCommandSummary>> groupByRange = new HashMap<>();
                    for (Key key : keys)
                    {
                        List<Interval<RoutableKey, RangeCommandSummary>> matches = rangesToCommands.matches(key);
                        if (matches.isEmpty())
                            continue;
                        for (Interval<RoutableKey, RangeCommandSummary> interval : matches)
                            groupByRange.computeIfAbsent(toRange(interval), ignore -> new HashSet<>()).add(interval.data);
                    }
                    // TODO (determinism) : this can break simulator as its not deteramnistic
                    rangeIt = groupByRange.entrySet().iterator();
                }
            }
        };
    }

    private static Range toRange(Interval<RoutableKey, RangeCommandSummary> interval)
    {
        TokenKey start = (TokenKey) interval.min;
        TokenKey end = (TokenKey) interval.max;
        // TODO (correctness) : accord doesn't support wrap around, so decreaseSlightly may fail in some cases
        return new TokenRange(start.withToken(start.token().decreaseSlightly()), end);
    }

    @Nullable
    public CommandTimeseriesHolder search(Range range)
    {
        List<RangeCommandSummary> matches = rangesToCommands.search(Interval.create(normalize(range.start(), range.startInclusive(), true),
                                                                                    normalize(range.end(), range.endInclusive(), false)));
        return result(range, matches);
    }

    private CommandTimeseriesHolder result(Seekable seekable, Collection<RangeCommandSummary> matches)
    {
        if (matches.isEmpty())
            return null;
        return new Holder(seekable, matches);
    }

    public int size()
    {
        return rangesToCommands.intervalCount();
    }

    public Builder unbuild()
    {
        return new Builder(rangesToCommands.unbuild());
    }

    @Override
    public String toString()
    {
        return rangesToCommands.unbuild().toString();
    }

    private static RoutingKey normalize(RoutingKey key, boolean inclusive, boolean upOrDown)
    {
        if (inclusive) return key;
        AccordRoutingKey ak = (AccordRoutingKey) key;
        switch (ak.kindOfRoutingKey())
        {
            case SENTINEL:
                return normalize(ak.asSentinelKey().toTokenKey(), inclusive, upOrDown);
            case TOKEN:
                TokenKey tk = ak.asTokenKey();
                return tk.withToken(upOrDown ? tk.token().increaseSlightly() : tk.token().decreaseSlightly());
            default:
                throw new IllegalArgumentException("Unknown kind: " + ak.kindOfRoutingKey());
        }
    }

    private static class Holder implements CommandTimeseriesHolder
    {
        private final Seekable keyOrRange;
        private final Collection<RangeCommandSummary> matches;

        private Holder(Seekable keyOrRange, Collection<RangeCommandSummary> matches)
        {
            this.keyOrRange = keyOrRange;
            this.matches = matches;
        }

        @Override
        public CommandTimeseries<?> byId()
        {
            return build(m -> m.txnId);
        }

        @Override
        public CommandTimeseries<?> byExecuteAt()
        {
            return build(m -> m.executeAt != null ? m.executeAt : m.txnId);
        }

        private CommandTimeseries<?> build(Function<RangeCommandSummary, Timestamp> fn)
        {
            CommandTimeseries.Update<RangeCommandSummary> builder = new CommandTimeseries.Update<>(keyOrRange, RangeCommandSummaryLoader.INSTANCE);
            builder.ignoreTestKind(true);
            for (RangeCommandSummary m : matches)
            {
                if (m.status == SaveStatus.Invalidated)
                    continue;
                builder.add(fn.apply(m), m);
            }
            return builder.build();
        }

        @Override
        public String toString()
        {
            return "Holder{" +
                   "keyOrRange=" + keyOrRange +
                   ", matches=" + matches +
                   '}';
        }
    }
}
