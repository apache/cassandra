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
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

import org.junit.Test;

import accord.api.RoutingKey;
import accord.impl.IntKey;
import accord.local.SaveStatus;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.TxnId;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;

import static accord.utils.Property.qt;
import static org.assertj.core.api.Assertions.assertThat;

public class CommandsForRangesTest
{
    private static final AccordGens.RangeFactory<IntKey.Routing> ROUTING_RANGE_FACTORY = (i, a, b) -> IntKey.range(a, b);
    private static final Gen.IntGen SMALL_INTS = Gens.ints().between(1, 10);
    private static final Gen<Ranges> RANGES_GEN = AccordGens.ranges(SMALL_INTS, AccordGens.intRoutingKey(), ROUTING_RANGE_FACTORY);
    private static final Gen<TxnId> TXN_ID_GEN = AccordGens.txnIds();
    private static final Gen<CommandsForRanges> CFK_GEN = rs -> {
        Ranges ranges = RANGES_GEN.next(rs);
        int numTxn = 10;
        TreeMap<TxnId, CommandsForRangesLoader.Summary> map = new TreeMap<>();
        for (int i = 0; i < numTxn; i++)
        {
            TxnId id = TXN_ID_GEN.next(rs);
            map.put(id, new CommandsForRangesLoader.Summary(id, id, SaveStatus.ReadyToExecute, ranges, Collections.emptyList()));
        }
        return CommandsForRanges.create(ranges, map);
    };
    private static final IntKey.Routing MIN = IntKey.routing(Integer.MIN_VALUE);
    private static final IntKey.Routing MAX = IntKey.routing(Integer.MAX_VALUE);

    @Test
    public void sliceEmptyWhenOutside()
    {
        qt().check(rs -> {
            CommandsForRanges cfr = CFK_GEN.next(rs);

            for (Range range : allOutside(cfr.ranges))
            {
                Ranges slice = Ranges.single(range);
                CommandsForRanges subset = cfr.slice(slice);
                assertThat(subset.ranges).isEmpty();
                assertThat(subset.size()).isEqualTo(0);
            }
        });
    }

    @Test
    public void sliceSameNoop()
    {
        qt().check(rs -> {
            CommandsForRanges cfr = CFK_GEN.next(rs);
            CommandsForRanges subset = cfr.slice(cfr.ranges);
            assertThat(subset.ranges).isEqualTo(cfr.ranges);
            assertThat(subset.size()).isEqualTo(cfr.size());
        });
    }

    private static List<Range> allOutside(Ranges ranges)
    {
        if (ranges.isEmpty()) return Collections.emptyList();
        List<Range> matches = new ArrayList<>();
        {
            Range first = ranges.get(0);
            if (!first.start().equals(MIN))
            {
                int start = Integer.MIN_VALUE;
                int end = key(first.start());
                matches.add(IntKey.range(start, end));
            }
        }
        if (ranges.size() > 1)
        {
            {
                Range last = ranges.get(ranges.size() - 1);
                if (!last.end().equals(MAX))
                {
                    int start = key(last.end());
                    int end = Integer.MAX_VALUE;
                    matches.add(IntKey.range(start, end));
                }
            }
            for (int i = 1; i < ranges.size(); i++)
            {
                Range previous = ranges.get(i - 1);
                Range next = ranges.get(i - 1);
                int start = key(previous.end());
                int end = key(next.start());
                if (start < end)
                    matches.add(IntKey.range(start, end));
            }
        }
        return matches;
    }

    private static int key(RoutingKey key)
    {
        return ((IntKey.Routing) key).key;
    }
}
