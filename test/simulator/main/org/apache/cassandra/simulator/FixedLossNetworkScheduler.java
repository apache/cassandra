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

package org.apache.cassandra.simulator;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.simulator.systems.SimulatedTime;
import org.apache.cassandra.simulator.utils.ChanceRange;
import org.apache.cassandra.simulator.utils.KindOfSequence;
import org.apache.cassandra.simulator.utils.LongRange;

/**
 * A scheduler that allows to configure network loss chance, that applies this chance to
 * _all_ operations. Useful for testing severe loss scenarios to make sure your retry
 * mechanisms work well.
 */
public class FixedLossNetworkScheduler implements FutureActionScheduler
{
    private static final Logger logger = LoggerFactory.getLogger(FixedLossNetworkScheduler.class);
    final RandomSource random;
    final SimulatedTime time;

    final KindOfSequence.LinkLatency normalLatency;
    final KindOfSequence.LinkLatency delayLatency;

    final KindOfSequence.NetworkDecision dropMessage;
    final KindOfSequence.NetworkDecision delayMessage;

    final KindOfSequence.Decision delayChance;
    final LongRange delayNanos, longDelayNanos;

    public FixedLossNetworkScheduler(int nodes, RandomSource random, SimulatedTime time, KindOfSequence kind, float min, float max)
    {
        this.random = random;
        this.time = time;
        this.normalLatency = kind.linkLatency(nodes,
                                              new LongRange(2, 20, TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS),
                                              random);
        this.delayLatency = kind.linkLatency(nodes,
                                             new LongRange(20, 40, TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS),
                                             random);

        this.dropMessage = kind.networkDecision(nodes,
                                                new ChanceRange(randomSource -> randomSource.qlog2uniformFloat(4), min, max),
                                                random);

        this.delayMessage = kind.networkDecision(nodes,
                                                 new ChanceRange(randomSource -> randomSource.qlog2uniformFloat(4), min, max),
                                                 random);

        this.delayChance = kind.decision(new ChanceRange(randomSource -> randomSource.qlog2uniformFloat(4), min, max),
                                         random);

        this.delayNanos = new LongRange(2, 100, TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS);
        this.longDelayNanos = new LongRange(2, 100, TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS);
    }

    private static class DeliveryPair
    {
        final int from;
        final int to;

        private DeliveryPair(int from, int to)
        {
            this.from = from;
            this.to = to;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DeliveryPair that = (DeliveryPair) o;
            return from == that.from && to == that.to;
        }

        public int hashCode()
        {
            return Objects.hash(from, to);
        }

        public String toString()
        {
            return "" + from +
                   "->" + to;
        }
    }

    private Map<DeliveryPair, Integer> pairs = new HashMap<>();
    private static final int TIMEOUTS_IN_A_ROW = 1;
    public Deliver shouldDeliver(int from, int to)
    {
        DeliveryPair pair = new DeliveryPair(from, to);
        if (!dropMessage.get(random, from, to))
        {
            pairs.put(pair, 0);
            return Deliver.DELIVER;
        }

        int subsequentFailures = pairs.compute(pair, (k, v) -> v == null ? 1 : v+1);

        if (subsequentFailures > TIMEOUTS_IN_A_ROW)
        {
            logger.info("Delivering {} after {} failures in a row", pair, TIMEOUTS_IN_A_ROW);
            pairs.put(pair, 0);
            return Deliver.DELIVER;
        }

        logger.info("Timing out {} for the {}th time", pair, subsequentFailures);
        return Deliver.TIMEOUT;
    }

    public long messageDeadlineNanos(int from, int to)
    {
        return time.nanoTime() + (delayMessage.get(random, from, to)
                                  ? normalLatency.get(random, from, to)
                                  : delayLatency.get(random, from, to));
    }

    public long messageTimeoutNanos(long expiresAfterNanos, long expirationIntervalNanos)
    {
        return expiresAfterNanos + random.uniform(0, expirationIntervalNanos / 2);
    }

    public long messageFailureNanos(int from, int to)
    {
        return messageDeadlineNanos(from, to);
    }

    public long schedulerDelayNanos()
    {
        return (delayChance.get(random) ? longDelayNanos : delayNanos).select(random);
    }
}
