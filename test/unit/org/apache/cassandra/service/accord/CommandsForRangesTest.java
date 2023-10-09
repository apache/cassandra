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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import accord.local.SaveStatus;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import accord.primitives.TxnId;
import accord.utils.AccordGens;
import accord.utils.Gen;
import accord.utils.Gens;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.utils.AccordGenerators;
import org.apache.cassandra.utils.Interval;
import org.apache.cassandra.utils.IntervalTree;

import static accord.utils.Property.qt;
import static org.apache.cassandra.simulator.RandomSource.Choices.choose;
import static org.assertj.core.api.Assertions.assertThat;

public class CommandsForRangesTest
{
    private static Ranges FULL_RANGE = Ranges.of(new TokenRange(AccordRoutingKey.SentinelKey.min("test"), AccordRoutingKey.SentinelKey.max("test")));

    @BeforeClass
    public static void setup() throws NoSuchFieldException, IllegalAccessException
    {
        DatabaseDescriptor.clientInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Test
    public void prune()
    {
        qt().forAll(cfr()).check(cfr -> {
            // public void prune(TxnId pruneBefore, Ranges pruneRanges)
            // private Timestamp maxRedundant;
            List<TxnId> knownIds = new ArrayList<>(cfr.knownIds());
            knownIds.sort(Comparator.naturalOrder());

            assertThat(cfr.maxRedundant()).isNull();

            TxnId min = knownIds.get(0);
            TxnId max = knownIds.get(knownIds.size() - 1);

            // should do nothing
            IntervalTree<RoutableKey, CommandsForRanges.RangeCommandSummary, Interval<RoutableKey, CommandsForRanges.RangeCommandSummary>> tree = cfr.tree();
            cfr.prune(min, FULL_RANGE);
            assertThat(cfr.maxRedundant()).isNull();
            assertThat(cfr.tree()).isEqualTo(tree);

            cfr.prune(max, FULL_RANGE);
            assertThat(cfr.knownIds()).containsExactly(max);
            assertThat(cfr.maxRedundant()).isEqualTo(knownIds.size() == 1 ? null : knownIds.get(knownIds.size() - 2));

            cfr.prune(new TxnId(max.logicalNext(max.node), max.rw(), max.domain()), FULL_RANGE);
            assertThat(cfr.knownIds()).isEmpty();
            assertThat(cfr.maxRedundant()).isEqualTo(max);
        });
    }

    private static Gen<CommandsForRanges> cfr()
    {
        // TODO (coverage): once all partitioners work with regard to splitting, then should test all
        Gen<IPartitioner> partitionerGen = rs -> choose(rs, Murmur3Partitioner.instance, RandomPartitioner.instance);
        Gen<SaveStatus> statusGen = Gens.enums().all(SaveStatus.class);
        return rs -> {
            IPartitioner partitioner = partitionerGen.next(rs);
            // some code reaches to the DD for partitioner...
            DatabaseDescriptor.setPartitionerUnsafe(partitioner);
            Gen<Ranges> rangesGen = AccordGenerators.ranges(ignore -> Collections.singleton("test"), ignore -> partitioner);
            CommandsForRanges.Builder builder = new CommandsForRanges.Builder();
            int numTxn = rs.nextInt(1, 10);
            Set<TxnId> uniq = new HashSet<>();
            for (int i = 0; i < numTxn; i++)
            {
                TxnId id;
                while (!uniq.add(id = AccordGens.txnIds().next(rs))) {}
                builder.put(id, rangesGen.next(rs), statusGen.next(rs), id, Collections.emptyList());
            }
            return builder.build();
        };
    }
}