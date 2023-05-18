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

import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import javax.annotation.Nullable;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.impl.CommandTimeseries;
import accord.impl.CommandTimeseriesHolder;
import accord.local.Command;
import accord.local.SaveStatus;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import accord.primitives.Seekable;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
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

        public Builder add(Ranges ranges, TxnId txnId, SaveStatus status, Timestamp execteAt, List<TxnId> dependsOn)
        {
            removeIf(txnId);
            return add(ranges, new RangeCommandSummary(txnId, status, execteAt, dependsOn));
        }

        private Builder add(Ranges ranges, RangeCommandSummary summary)
        {
            for (Range range : ranges)
                add(range, summary);
            return this;
        }

        private Builder add(Range range, RangeCommandSummary summary)
        {
            builder.add(Interval.create(normalize(range.start(), range.startInclusive(), true),
                                        normalize(range.end(), range.endInclusive(), false),
                                        summary));
            return this;
        }

        private Builder removeIf(TxnId txnId)
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

    @Nullable
    public CommandTimeseriesHolder search(Key key)
    {
        List<RangeCommandSummary> matches = rangesToCommands.search(key);
        return result(key, matches);
    }

    @Nullable
    public CommandTimeseriesHolder search(Range range)
    {
        List<RangeCommandSummary> matches = rangesToCommands.search(Interval.create(normalize(range.start(), range.startInclusive(), true),
                                                                                    normalize(range.end(), range.endInclusive(), false)));
        return result(range, matches);
    }

    private CommandTimeseriesHolder result(Seekable seekable, List<RangeCommandSummary> matches)
    {
        if (matches.isEmpty())
            return null;
        return fromRangeSummary(seekable, matches);
    }

    public int size()
    {
        return rangesToCommands.intervalCount();
    }

    public Builder unbuild()
    {
        return new Builder(rangesToCommands.unbuild());
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
                AccordRoutingKey.TokenKey tk = ak.asTokenKey();
                return tk.withToken(upOrDown ? tk.token().increaseSlightly() : tk.token().decreaseSlightly());
            default:
                throw new IllegalArgumentException("Unknown kind: " + ak.kindOfRoutingKey());
        }
    }

    private static CommandTimeseriesHolder fromRangeSummary(Seekable keyOrRange, List<RangeCommandSummary> matches)
    {
        return new CommandTimeseriesHolder()
        {
            @Override
            public CommandTimeseries<?> byId()
            {
                CommandTimeseries.Update<RangeCommandSummary> builder = new CommandTimeseries.Update<>(keyOrRange, RangeCommandSummaryLoader.INSTANCE);
                for (RangeCommandSummary m : matches)
                {
                    if (m.status == SaveStatus.Invalidated)
                        continue;
                    builder.add(m.txnId, m);
                }
                return builder.build();
            }

            @Override
            public CommandTimeseries<?> byExecuteAt()
            {
                CommandTimeseries.Update<RangeCommandSummary> builder = new CommandTimeseries.Update<>(null, RangeCommandSummaryLoader.INSTANCE);
                for (RangeCommandSummary m : matches)
                {
                    if (m.status == SaveStatus.Invalidated)
                        continue;
                    builder.add(m.executeAt != null ? m.executeAt : m.txnId, m);
                }
                return builder.build();
            }
        };
    }
}
