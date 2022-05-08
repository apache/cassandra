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

package org.apache.cassandra.simulator.systems;

import java.util.BitSet;

import org.apache.cassandra.simulator.FutureActionScheduler;
import org.apache.cassandra.simulator.RandomSource;
import org.apache.cassandra.simulator.cluster.Topology;
import org.apache.cassandra.simulator.cluster.TopologyListener;
import org.apache.cassandra.simulator.systems.NetworkConfig.PhaseConfig;
import org.apache.cassandra.simulator.utils.KindOfSequence;
import org.apache.cassandra.simulator.utils.KindOfSequence.Decision;
import org.apache.cassandra.simulator.utils.KindOfSequence.LinkLatency;
import org.apache.cassandra.simulator.utils.KindOfSequence.NetworkDecision;
import org.apache.cassandra.simulator.utils.KindOfSequence.Period;

import static org.apache.cassandra.simulator.FutureActionScheduler.Deliver.DELIVER;
import static org.apache.cassandra.simulator.FutureActionScheduler.Deliver.DELIVER_AND_TIMEOUT;
import static org.apache.cassandra.simulator.FutureActionScheduler.Deliver.FAILURE;
import static org.apache.cassandra.simulator.FutureActionScheduler.Deliver.TIMEOUT;

public class SimulatedFutureActionScheduler implements FutureActionScheduler, TopologyListener
{
    static class Network
    {
        final LinkLatency normalLatency;
        final LinkLatency delayLatency;
        final NetworkDecision dropMessage;
        final NetworkDecision delayMessage;

        public Network(int nodes, PhaseConfig config, RandomSource random, KindOfSequence kind)
        {
            normalLatency = kind.linkLatency(nodes, config.normalLatency, random);
            delayLatency = kind.linkLatency(nodes, config.delayLatency, random);
            dropMessage = kind.networkDecision(nodes, config.dropChance, random);
            delayMessage = kind.networkDecision(nodes, config.delayChance, random);
        }
    }

    static class Scheduler extends SchedulerConfig
    {
        final Decision delayChance;
        public Scheduler(SchedulerConfig config, RandomSource random, KindOfSequence kind)
        {
            super(config.longDelayChance, config.delayNanos, config.longDelayNanos);
            delayChance = kind.decision(config.longDelayChance, random);
        }
    }

    final int nodeCount;
    final RandomSource random;
    final SimulatedTime time;

    // TODO (feature): should we produce more than two simultaneous partitions?
    final BitSet isInDropPartition = new BitSet();
    final BitSet isInFlakyPartition = new BitSet();

    Topology topology;

    final Network normal;
    final Network flaky;
    final Scheduler scheduler;

    final Decision decidePartition;
    final Decision decideFlaky;
    final Period recomputePeriod;

    long recomputeAt;

    public SimulatedFutureActionScheduler(KindOfSequence kind, int nodeCount, RandomSource random, SimulatedTime time, NetworkConfig network, SchedulerConfig scheduler)
    {
        this.nodeCount = nodeCount;
        this.random = random;
        this.time = time;
        this.normal = new Network(nodeCount, network.normal, random, kind);
        this.flaky = new Network(nodeCount, network.flaky, random, kind);
        this.scheduler = new Scheduler(scheduler, random, kind);
        this.decidePartition = kind.decision(network.partitionChance, random);
        this.decideFlaky = kind.decision(network.flakyChance, random);
        this.recomputePeriod = kind.period(network.reconfigureInterval, random);
    }

    private void maybeRecompute()
    {
        if (time.nanoTime() < recomputeAt)
            return;

        if (topology == null)
            return;

        recompute();
    }

    private void recompute()
    {
        isInDropPartition.clear();
        isInFlakyPartition.clear();

        if (decidePartition.get(random))
            computePartition(isInDropPartition);

        if (decideFlaky.get(random))
            computePartition(isInFlakyPartition);

        recomputeAt = time.nanoTime() + recomputePeriod.get(random);
    }

    private void computePartition(BitSet compute)
    {
        int size = topology.quorumRf <= 4 ? 1 : random.uniform(1, (topology.quorumRf - 1)/2);
        while (size > 0)
        {
            int next = random.uniform(0, topology.membersOfQuorum.length);
            if (compute.get(next))
                continue;
            compute.set(next);
            --size;
        }
    }

    Network config(int from, int to)
    {
        maybeRecompute();
        return isInFlakyPartition.get(from) != isInFlakyPartition.get(to) ? flaky : normal;
    }

    @Override
    public FutureActionScheduler.Deliver shouldDeliver(int from, int to)
    {
        Network config = config(from, to);

        if (isInDropPartition.get(from) != isInDropPartition.get(to))
            return TIMEOUT;

        if (!config.dropMessage.get(random, from, to))
            return DELIVER;

        if (random.decide(0.5f))
            return DELIVER_AND_TIMEOUT;

        if (random.decide(0.5f))
            return TIMEOUT;

        return FAILURE;
    }

    @Override
    public long messageDeadlineNanos(int from, int to)
    {
        Network config = config(from, to);
        return time.nanoTime() + (config.delayMessage.get(random, from, to)
                                  ? config.normalLatency.get(random, from, to)
                                  : config.delayLatency.get(random, from, to));
    }

    @Override
    public long messageTimeoutNanos(long expiresAtNanos, long expirationIntervalNanos)
    {
        return expiresAtNanos + random.uniform(0, expirationIntervalNanos / 2);
    }

    @Override
    public long messageFailureNanos(int from, int to)
    {
        return messageDeadlineNanos(from, to);
    }

    @Override
    public long schedulerDelayNanos()
    {
        return (scheduler.delayChance.get(random) ? scheduler.longDelayNanos : scheduler.delayNanos).select(random);
    }

    @Override
    public void onChange(Topology newTopology)
    {
        Topology oldTopology = topology;
        topology = newTopology;
        if (oldTopology == null || (newTopology.quorumRf < oldTopology.quorumRf && newTopology.quorumRf < isInDropPartition.cardinality()))
            recompute();
    }
}
