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

package org.apache.cassandra.simulator.paxos;

import java.io.IOException;

import org.apache.cassandra.simulator.ClusterSimulation;
import org.apache.cassandra.simulator.RandomSource;
import org.apache.cassandra.simulator.utils.IntRange;
import org.apache.cassandra.simulator.utils.KindOfSequence;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.SERIAL;

class AccordClusterSimulation extends ClusterSimulation<PaxosSimulation> implements AutoCloseable
{
    @SuppressWarnings("UnusedReturnValue")
    static class Builder extends ClusterSimulation.Builder<PaxosSimulation>
    {
        public AccordClusterSimulation create(long seed) throws IOException
        {
            RandomSource random = randomSupplier.get();
            random.reset(seed);
            return new AccordClusterSimulation(random, seed, uniqueNum, this);
        }

        public void applyHandicaps()
        {
            /**
             * TODO: remove after partial replication patch
             * The current homekey implementation isn't compatible with the C* commands per key implementation when
             * a non-replica coordinates a query.
             *
             * This creates a few problems.
             *
             * First when a non-replica coordinator chooses a home key, it chooses the end of one of it's ranges and
             * adds it to the txn. This doesn't work with the C* CFK implementation, because it expects a partition
             * key. This will change with the partial replication patch, so we can re-evaluate then.
             *
             * Second, nodes that haven't joined the ring have no ranges to pull home keys from, so they npe
             */
            dcCount = new IntRange(1, 1);
            nodeCount = new IntRange(3, 3);
        }
    }

    AccordClusterSimulation(RandomSource random, long seed, int uniqueNum, Builder builder) throws IOException
    {
        super(random, seed, uniqueNum, builder,
              config -> {},
              (simulated, schedulers, cluster, options) -> {
                  int[] primaryKeys = primaryKeys(seed, builder.primaryKeyCount());
                  KindOfSequence.Period jitter = RandomSource.Choices.uniform(KindOfSequence.values()).choose(random)
                                                                     .period(builder.schedulerJitterNanos(), random);
                  return new PairOfSequencesAccordSimulation(simulated, cluster, options,
                                                            builder.readChance().select(random), builder.concurrency(), builder.primaryKeySeconds(), builder.withinKeyConcurrency(),
                                                            SERIAL, schedulers, builder.debug(), seed,
                                                            primaryKeys, builder.secondsToSimulate() >= 0 ? SECONDS.toNanos(builder.secondsToSimulate()) : -1,
                                                            () -> jitter.get(random));
              });
    }

    private static int[] primaryKeys(long seed, int count)
    {
        int primaryKey = (int) (seed);
        int[] primaryKeys = new int[count];
        for (int i = 0 ; i < primaryKeys.length ; ++i)
            primaryKeys[i] = primaryKey += 1 << 20;
        return primaryKeys;
    }
}
