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

package org.apache.cassandra.distributed.test.accord.load;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;
import java.util.function.LongFunction;
import java.util.function.Supplier;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;

import accord.utils.DefaultRandom;

import static java.util.concurrent.TimeUnit.SECONDS;

class LoadSettings
{
    enum ClusterChaos {
        RESTART,
        RESTART_AND_REBOOTSTRAP_INCOMPLETE, RESTART_AND_REBOOTSTRAP_RESET,
        RESTART_AND_REBOOTSTRAP_AFTER_TIMEOUT,

        REBOOTSTRAP_INCOMPLETE, REBOOTSTRAP_RESET,
        REBOOTSTRAP_IF_BEHIND
    }

    final int repairInterval;
    final int compactionInterval;
    final int journalFlushInterval;
    final int cfkFlushInterval;
    final int cfkCompactionPeriodSeconds;
    final int dataFlushInterval;
    final int clusterChaosInterval;
    final int clusterChaosDecay;
    final int clusterChaosConcurrency;
    final LongFunction<Supplier<ClusterChaos>> clusterChaos;
    final int batchSize;
    final long batchPeriodNanos;
    final int clientConcurrency;
    final int clients;
    final int ratePerSecond;
    final int minRatePerSecond;
    final int increaseRatePerSecondInterval;
    final int keysPerOperation;
    final float readRatio;
    final IntSupplier keySelector;
    final boolean readBeforeWrite;
    final float traceSlowest;
    final int traceLast;
    final long totalTransactions;
    final int totalClusterChaos;
    final int[][] artificialLatencies;

    LoadSettings(Builder builder)
    {
        this.repairInterval = builder.repairInterval;
        this.compactionInterval = builder.compactionInterval;
        this.journalFlushInterval = builder.journalFlushInterval;
        this.cfkFlushInterval = builder.cfkFlushInterval;
        this.cfkCompactionPeriodSeconds = builder.cfkCompactionPeriodSeconds;
        this.dataFlushInterval = builder.dataFlushInterval;
        this.clusterChaosInterval = builder.clusterChaosInterval;
        this.clusterChaosDecay = builder.clusterChaosDecay;
        this.clusterChaosConcurrency = builder.clusterChaosConcurrency;
        this.clusterChaos = builder.clusterChaos;
        this.batchSize = builder.batchSize;
        this.batchPeriodNanos = builder.batchPeriodNanos;
        this.clientConcurrency = builder.clientConcurrency;
        this.clients = builder.clients;
        this.ratePerSecond = builder.ratePerSecond;
        this.minRatePerSecond = builder.minRatePerSecond;
        this.increaseRatePerSecondInterval = builder.increaseRatePerSecondInterval;
        this.keysPerOperation = builder.keysPerOperation;
        this.readRatio = builder.readRatio;
        this.keySelector = builder.keySelector;
        this.readBeforeWrite = builder.readBeforeWrite;
        this.artificialLatencies = builder.artificialLatencies;
        this.traceSlowest = builder.traceSlowest;
        this.traceLast = builder.traceLast;
        this.totalTransactions = builder.totalTransactions;
        this.totalClusterChaos = builder.totalClusterChaos;
    }

    // interval is measured in terms of *operations* unless otherwise specified
    public static class Builder
    {
        int repairInterval = Integer.MAX_VALUE;
        int compactionInterval = Integer.MAX_VALUE;
        int journalFlushInterval = Integer.MAX_VALUE;
        int cfkFlushInterval = Integer.MAX_VALUE;
        int cfkCompactionPeriodSeconds = 0;
        int dataFlushInterval = Integer.MAX_VALUE;
        int clusterChaosInterval = Integer.MAX_VALUE;
        int clusterChaosDecay = 1;
        int clusterChaosConcurrency = 1;
        LongFunction<Supplier<ClusterChaos>> clusterChaos = seed -> new DefaultRandom(seed).randomWeightedPicker(LoadSettings.ClusterChaos.values());
        int batchSize = 1000;
        long batchPeriodNanos = SECONDS.toNanos(10);
        int clientConcurrency = 50;
        int clients = -1;
        int ratePerSecond = 1000;
        int minRatePerSecond = 50;
        int increaseRatePerSecondInterval = 1000;
        int keysPerOperation = 1;
        float readRatio = 0.5f;
        IntSupplier keySelector;
        boolean readBeforeWrite;
        float traceSlowest;
        int traceLast;
        int[][] artificialLatencies;
        long totalTransactions = Long.MAX_VALUE;
        int totalClusterChaos = Integer.MAX_VALUE;

        public Builder setRepairInterval(int repairInterval)
        {
            this.repairInterval = repairInterval;
            return this;
        }

        public Builder setCompactionInterval(int compactionInterval)
        {
            this.compactionInterval = compactionInterval;
            return this;
        }

        public Builder setJournalFlushInterval(int journalFlushInterval)
        {
            this.journalFlushInterval = journalFlushInterval;
            return this;
        }

        public Builder setCfkFlushInterval(int cfkFlushInterval)
        {
            this.cfkFlushInterval = cfkFlushInterval;
            return this;
        }

        public Builder setCfkCompactionPeriodSeconds(int cfkCompactionPeriodSeconds)
        {
            this.cfkCompactionPeriodSeconds = cfkCompactionPeriodSeconds;
            return this;
        }

        public Builder setDataFlushInterval(int dataFlushInterval)
        {
            this.dataFlushInterval = dataFlushInterval;
            return this;
        }

        public Builder setClusterChaosInterval(int clusterChaosInterval)
        {
            this.clusterChaosInterval = clusterChaosInterval;
            return this;
        }

        public Builder setClusterChaosDecay(int clusterChaosDecay)
        {
            this.clusterChaosDecay = clusterChaosDecay;
            return this;
        }

        public Builder setClusterChaosConcurrency(int clusterChaosConcurrency)
        {
            this.clusterChaosConcurrency = clusterChaosConcurrency;
            return this;
        }

        public Builder setClusterChaos(LongFunction<Supplier<ClusterChaos>> clusterChaos)
        {
            this.clusterChaos = clusterChaos;
            return this;
        }

        public Builder setTotalTransactions(long totalTransactions)
        {
            this.totalTransactions = totalTransactions;
            return this;
        }

        public Builder setTotalClusterChaos(int totalClusterChaos)
        {
            this.totalClusterChaos = totalClusterChaos;
            return this;
        }

        public Builder setBatchSize(int batchSize)
        {
            this.batchSize = batchSize;
            return this;
        }

        public Builder setBatchPeriodNanos(long batchPeriodNanos)
        {
            this.batchPeriodNanos = batchPeriodNanos;
            return this;
        }

        public Builder setClientConcurrency(int clientConcurrency)
        {
            this.clientConcurrency = clientConcurrency;
            return this;
        }

        public Builder setClients(int clients)
        {
            this.clients = clients;
            return this;
        }

        public Builder setRatePerSecond(int ratePerSecond)
        {
            this.ratePerSecond = ratePerSecond;
            return this;
        }

        public Builder setMinRatePerSecond(int minRatePerSecond)
        {
            this.minRatePerSecond = minRatePerSecond;
            return this;
        }

        public Builder setIncreaseRatePerSecondInterval(int increaseRatePerSecondInterval)
        {
            this.increaseRatePerSecondInterval = increaseRatePerSecondInterval;
            return this;
        }

        public Builder setKeysPerOperation(int keysPerOperation)
        {
            this.keysPerOperation = keysPerOperation;
            return this;
        }

        public Builder setReadRatio(float readRatio)
        {
            this.readRatio = readRatio;
            return this;
        }

        public Builder setReadBeforeWrite(boolean readBeforeWrite)
        {
            this.readBeforeWrite = readBeforeWrite;
            return this;
        }

        public Builder setTraceSlowest(float traceSlowest)
        {
            this.traceSlowest = traceSlowest;
            return this;
        }

        public Builder setTraceLast(int traceLast)
        {
            this.traceLast = traceLast;
            return this;
        }

        public Builder setKeySelector(IntSupplier keySelector)
        {
            this.keySelector = keySelector;
            return this;
        }

        public Builder setArtificialLatencies(int[][] artificialLatencies)
        {
            this.artificialLatencies = artificialLatencies;
            return this;
        }

        public LoadSettings build()
        {
            return new LoadSettings(this);
        }
    }

    static IntSupplier ycsbZipfian(int keyCount)
    {
        ZipfDistribution distribution = new ZipfDistribution(new JDKRandomGenerator(), keyCount, 0.99);
        int count = distribution.inverseCumulativeProbability(0.65f);
        float[] probs = new float[count];
        for (int i = 0 ; i < probs.length ; ++i)
            probs[i] = (float) distribution.cumulativeProbability(i);
        // zipf is slow to compute, so we cache the first 65% of the distribution then use uniform probability; this is good enough for our purposes
        float max = probs[probs.length - 1];
        float inv_incr = probs.length >= keyCount ? 0f : 1f / ((1f-max)/(keyCount - probs.length));
        Random random = new Random();
        return () -> {
            float v = random.nextFloat();
            if (v < max)
            {
                int i = Arrays.binarySearch(probs, v);
                if (i < 0) i = -1 - i;
                return i;
            }
            else
            {
                return (int)((v - max)*inv_incr);
            }
        };
    }

    static IntSupplier roundrobin(int keyCount)
    {
        AtomicInteger next = new AtomicInteger();
        return () -> {
            int v = next.incrementAndGet();
            if (v < keyCount)
                return v;
            return next.updateAndGet(i -> i > keyCount ? 0 : i + 1);
        };
    }

    static IntSupplier uniform(int keyCount)
    {
        Random random = new Random();
        return () -> random.nextInt(keyCount);
    }
}
