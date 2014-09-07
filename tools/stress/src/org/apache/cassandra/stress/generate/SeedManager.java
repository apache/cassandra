/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.stress.generate;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.stress.Operation;
import org.apache.cassandra.stress.settings.StressSettings;
import org.apache.cassandra.stress.util.DynamicList;

public class SeedManager
{

    final Distribution visits;
    final Generator writes;
    final Generator reads;
    final ConcurrentHashMap<Seed, Seed> managing = new ConcurrentHashMap<>();
    final DynamicList<Seed> sampleFrom;
    final Distribution sample;

    public SeedManager(StressSettings settings)
    {
        Generator writes, reads;
        if (settings.generate.sequence != null)
        {
            long[] seq = settings.generate.sequence;
            if (settings.generate.readlookback != null)
            {
                LookbackableWriteGenerator series = new LookbackableWriteGenerator(seq[0], seq[1], settings.generate.wrap, settings.generate.readlookback.get());
                writes = series;
                reads = series.reads;
            }
            else
            {
                writes = reads = new SeriesGenerator(seq[0], seq[1], settings.generate.wrap);
            }
        }
        else
        {
            writes = reads = new RandomGenerator(settings.generate.distribution.get());
        }
        this.visits = settings.insert.visits.get();
        this.writes = writes;
        this.reads = reads;
        this.sample = DistributionInverted.invert(settings.insert.revisit.get());
        if (sample.maxValue() > Integer.MAX_VALUE || sample.minValue() < 0)
            throw new IllegalArgumentException();
        this.sampleFrom = new DynamicList<>((int) sample.maxValue());
    }

    public Seed next(Operation op)
    {
        if (!op.isWrite())
        {
            Seed seed = reads.next(-1);
            if (seed == null)
                return null;
            Seed managing = this.managing.get(seed);
            return managing == null ? seed : managing;
        }

        while (true)
        {
            int index = (int) sample.next();
            Seed seed = sampleFrom.get(index);
            if (seed != null && seed.take())
                return seed;

            seed = writes.next((int) visits.next());
            if (seed == null)
                return null;
            // seeds are created HELD, so if we insert it successfully we have it exclusively for our write
            if (managing.putIfAbsent(seed, seed) == null)
                return seed;
        }
    }

    public void markVisited(Seed seed, int[] position)
    {
        boolean first = seed.position == null;
        seed.position = position;
        finishedWriting(seed, first, false);
    }

    public void markFinished(Seed seed)
    {
        finishedWriting(seed, seed.position == null, true);
    }

    void finishedWriting(Seed seed, boolean first, boolean completed)
    {
        if (!completed)
        {
            if (first)
                seed.poolNode = sampleFrom.append(seed);
            seed.yield();
        }
        else
        {
            if (!first)
                sampleFrom.remove(seed.poolNode);
            managing.remove(seed);
        }
        if (first)
            writes.finishWrite(seed);
    }

    private abstract class Generator
    {
        abstract Seed next(int visits);
        void finishWrite(Seed seed) { }
    }

    private class RandomGenerator extends Generator
    {

        final Distribution distribution;

        public RandomGenerator(Distribution distribution)
        {
            this.distribution = distribution;
        }

        public Seed next(int visits)
        {
            return new Seed(distribution.next(), visits);
        }
    }

    private class SeriesGenerator extends Generator
    {

        final long start;
        final long totalCount;
        final boolean wrap;
        final AtomicLong next = new AtomicLong();

        public SeriesGenerator(long start, long end, boolean wrap)
        {
            this.wrap = wrap;
            if (start > end)
                throw new IllegalStateException();
            this.start = start;
            this.totalCount = 1 + end - start;
        }

        public Seed next(int visits)
        {
            long next = this.next.getAndIncrement();
            if (!wrap && next >= totalCount)
                return null;
            return new Seed(start + (next % totalCount), visits);
        }
    }

    private class LookbackableWriteGenerator extends SeriesGenerator
    {

        final AtomicLong writeCount = new AtomicLong();
        final ConcurrentSkipListMap<Seed, Seed> afterMin = new ConcurrentSkipListMap<>();
        final LookbackReadGenerator reads;

        public LookbackableWriteGenerator(long start, long end, boolean wrap, Distribution readLookback)
        {
            super(start, end, wrap);
            this.writeCount.set(0);
            reads = new LookbackReadGenerator(readLookback);
        }

        public Seed next(int visits)
        {
            long next = this.next.getAndIncrement();
            if (!wrap && next >= totalCount)
                return null;
            return new Seed(start + (next % totalCount), visits);
        }

        void finishWrite(Seed seed)
        {
            if (seed.seed <= writeCount.get())
                return;
            afterMin.put(seed, seed);
            while (true)
            {
                Map.Entry<Seed, Seed> head = afterMin.firstEntry();
                if (head == null)
                    return;
                long min = this.writeCount.get();
                if (head.getKey().seed <= min)
                    return;
                if (head.getKey().seed == min + 1 && this.writeCount.compareAndSet(min, min + 1))
                {
                    afterMin.remove(head.getKey());
                    continue;
                }
                return;
            }
        }

        private class LookbackReadGenerator extends Generator
        {

            final Distribution lookback;

            public LookbackReadGenerator(Distribution lookback)
            {
                this.lookback = lookback;
                if (lookback.maxValue() > start + totalCount)
                    throw new IllegalArgumentException("Invalid lookback distribution; max value is " + lookback.maxValue()
                                                       + ", but series only ranges from " + writeCount + " to " + (start + totalCount));
            }

            public Seed next(int visits)
            {
                long lookback = this.lookback.next();
                long range = writeCount.get();
                long startOffset = range - lookback;
                if (startOffset < 0)
                {
                    if (range == totalCount && !wrap)
                        return null;
                    startOffset = range == 0 ? 0 : lookback % range;
                }
                return new Seed(start + startOffset, visits);
            }
        }

    }

}
