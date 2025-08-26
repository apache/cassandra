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
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.config.Config.PaxosVariant;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.simulator.AccordNetworkScheduler;
import org.apache.cassandra.simulator.AlwaysDeliverNetworkScheduler;
import org.apache.cassandra.simulator.ClusterSimulation;
import org.apache.cassandra.simulator.FutureActionScheduler;
import org.apache.cassandra.simulator.RandomSource;
import org.apache.cassandra.simulator.systems.SimulatedTime;
import org.apache.cassandra.simulator.utils.KindOfSequence;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.SERIAL;
import static org.apache.cassandra.net.Verb.CLEANUP_MSG;
import static org.apache.cassandra.net.Verb.FAILED_SESSION_MSG;
import static org.apache.cassandra.net.Verb.FINALIZE_COMMIT_MSG;
import static org.apache.cassandra.net.Verb.FINALIZE_PROMISE_MSG;
import static org.apache.cassandra.net.Verb.FINALIZE_PROPOSE_MSG;
import static org.apache.cassandra.net.Verb.PAXOS2_UPDATE_LOW_BALLOT_REQ;
import static org.apache.cassandra.net.Verb.PAXOS2_UPDATE_LOW_BALLOT_RSP;
import static org.apache.cassandra.net.Verb.PREPARE_CONSISTENT_REQ;
import static org.apache.cassandra.net.Verb.PREPARE_CONSISTENT_RSP;
import static org.apache.cassandra.net.Verb.PREPARE_MSG;
import static org.apache.cassandra.net.Verb.REPAIR_RSP;
import static org.apache.cassandra.net.Verb.SNAPSHOT_MSG;
import static org.apache.cassandra.net.Verb.STATUS_REQ;
import static org.apache.cassandra.net.Verb.STATUS_RSP;
import static org.apache.cassandra.net.Verb.SYNC_REQ;
import static org.apache.cassandra.net.Verb.SYNC_RSP;
import static org.apache.cassandra.net.Verb.VALIDATION_REQ;
import static org.apache.cassandra.net.Verb.VALIDATION_RSP;

class PaxosClusterSimulation extends ClusterSimulation<PaxosSimulation> implements AutoCloseable
{
    @SuppressWarnings("UnusedReturnValue")
    static class Builder extends ClusterSimulation.Builder<PaxosSimulation>
    {
        PaxosVariant initialPaxosVariant = PaxosVariant.v2;
        PaxosVariant finalPaxosVariant = null;
        Boolean stateCache;
        ConsistencyLevel serialConsistency = SERIAL;

        public Builder consistency(ConsistencyLevel serialConsistency)
        {
            this.serialConsistency = serialConsistency;
            return this;
        }

        public Builder stateCache(Boolean stateCache)
        {
            this.stateCache = stateCache;
            return this;
        }

        public Builder initialPaxosVariant(PaxosVariant variant)
        {
            initialPaxosVariant = variant;
            return this;
        }

        public Builder finalPaxosVariant(PaxosVariant variant)
        {
            finalPaxosVariant = variant;
            return this;
        }

        public PaxosClusterSimulation create(long seed) throws IOException
        {
            RandomSource random = randomSupplier.get();
            random.reset(seed);
            return new PaxosClusterSimulation(random, seed, uniqueNum, this);
        }

        @Override
        public Map<Verb, FutureActionScheduler> perVerbFutureActionSchedulers(int nodeCount, SimulatedTime time, RandomSource random, FutureActionScheduler defaultScheduler)
        {
            // Mark just the verbs for repair reliable so that other things can continue to be unreliable while repair runs
            AlwaysDeliverNetworkScheduler scheduler = new AlwaysDeliverNetworkScheduler(time);
            AccordNetworkScheduler accordScheduler = new AccordNetworkScheduler(defaultScheduler, scheduler);
            ImmutableMap.Builder<Verb, FutureActionScheduler> builder = ImmutableMap.builder();
            Verb[] repairVerbs = new Verb[] {
                REPAIR_RSP,
                VALIDATION_RSP,
                VALIDATION_REQ,
                SYNC_RSP,
                SYNC_REQ,
                PREPARE_MSG,
                SNAPSHOT_MSG,
                CLEANUP_MSG,
                PREPARE_CONSISTENT_RSP,
                PREPARE_CONSISTENT_REQ,
                FINALIZE_PROPOSE_MSG,
                FINALIZE_PROMISE_MSG,
                FINALIZE_COMMIT_MSG,
                FAILED_SESSION_MSG,
                STATUS_RSP,
                STATUS_REQ,
                PAXOS2_UPDATE_LOW_BALLOT_REQ,
                PAXOS2_UPDATE_LOW_BALLOT_RSP
            };
            for (Verb verb : repairVerbs)
                builder.put(verb, scheduler);
            for (Verb verb : AccordNetworkScheduler.ACCORD_VERBS)
                builder.put(verb, accordScheduler);
            return builder.build();
        }
    }

    PaxosClusterSimulation(RandomSource random, long seed, int uniqueNum, Builder builder) throws IOException
    {
        super(random, seed, uniqueNum, builder,
              config -> config.set("paxos_variant", builder.initialPaxosVariant.name())
                              .set("storage_compatibility_mode", "NONE")
                              .set("paxos_cache_size", (builder.stateCache != null ? builder.stateCache : random.uniformFloat() < 0.5) ? null : "0MiB")
                              .set("paxos_state_purging", "repaired")
                              .set("paxos_on_linearizability_violations", "log")
                              .set("storage_compatibility_mode", "NONE")
        ,
              (simulated, schedulers, cluster, options) -> {
                  int[] primaryKeys = primaryKeys(seed, builder.primaryKeyCount());
                  KindOfSequence.Period jitter = RandomSource.Choices.uniform(KindOfSequence.values()).choose(random)
                                                                     .period(builder.schedulerJitterNanos(), random);
                  return new PairOfSequencesPaxosSimulation(simulated, cluster, options.changePaxosVariantTo(builder.finalPaxosVariant), builder.transactionalMode(),
                                                            builder.readChance().select(random), builder.concurrency(), builder.primaryKeySeconds(), builder.withinKeyConcurrency(),
                                                            builder.serialConsistency, schedulers, builder.debug(), seed,
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
