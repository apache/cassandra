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
import org.apache.cassandra.utils.LockedDynamicList;

public class SeedManager
{

    final Distribution visits;
    final Generator writes;
    final Generator reads;
    final ConcurrentHashMap<Long, Seed> managing = new ConcurrentHashMap<>();
    final LockedDynamicList<Seed> sampleFrom;
    final Distribution sample;
    final long sampleOffset;
    final int sampleSize;
    final long sampleMultiplier;
    final boolean updateSampleImmediately;

    public SeedManager(StressSettings settings)
    {
        Distribution tSample = settings.insert.revisit.get();
        this.sampleOffset = Math.min(tSample.minValue(), tSample.maxValue());
        long sampleSize = 1 + Math.max(tSample.minValue(), tSample.maxValue()) - sampleOffset;
        if (sampleOffset < 0 || sampleSize > Integer.MAX_VALUE)
            throw new IllegalArgumentException("sample range is invalid");

        // need to get a big numerical range even if a small number of discrete values
        // one plus so we still get variation at the low order numbers as well as high
        this.sampleMultiplier = 1 + Math.round(Math.pow(10D, 22 - Math.log10(sampleSize)));

        Generator writes, reads;
        if (settings.generate.sequence != null)
        {
            long[] seq = settings.generate.sequence;
            if (settings.generate.readlookback != null)
            {
                LookbackableWriteGenerator series = new LookbackableWriteGenerator(seq[0], seq[1], settings.generate.wrap, settings.generate.readlookback.get(), sampleMultiplier);
                writes = series;
                reads = series.reads;
            }
            else
            {
                writes = reads = new SeriesGenerator(seq[0], seq[1], settings.generate.wrap, sampleMultiplier);
            }
        }
        else
        {
            writes = reads = new RandomGenerator(settings.generate.distribution.get(), sampleMultiplier);
        }
        this.visits = settings.insert.visits.get();
        this.writes = writes;
        this.reads = reads;
        this.sampleFrom = new LockedDynamicList<>((int) sampleSize);
        this.sample = DistributionInverted.invert(tSample);
        this.sampleSize = (int) sampleSize;
        this.updateSampleImmediately = visits.average() > 1;
    }

    public Seed next(Operation op)
    {
        if (!op.isWrite())
        {
            Seed seed = reads.next(-1);
            if (seed == null)
                return null;
            Seed managing = this.managing.get(seed.seed);
            return managing == null ? seed : managing;
        }

        while (true)
        {
            int index = (int) (sample.next() - sampleOffset);
            Seed seed = sampleFrom.get(index);
            if (seed != null && seed.isSaved())
                return seed;

            seed = writes.next((int) visits.next());
            if (seed == null)
                return null;
            if (managing.putIfAbsent(seed.seed, seed) == null)
            {
                if (!updateSampleImmediately || seed.save(sampleFrom, sampleSize))
                    return seed;
                managing.remove(seed.seed, seed);
            }
        }
    }

    public void markLastWrite(Seed seed, boolean first)
    {
        // we could have multiple iterators mark the last write simultaneously,
        // so we ensure we remove conditionally, and only remove the exact seed we were operating over
        // this is important because, to ensure correctness, we do not support calling remove multiple
        // times on the same DynamicList.Node
        if (managing.remove(seed.seed, seed) && !first)
            seed.remove(sampleFrom);
    }

    public void markFirstWrite(Seed seed, boolean last)
    {
        if (!last && !updateSampleImmediately)
            seed.save(sampleFrom, Integer.MAX_VALUE);
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
        final long multiplier;

        public RandomGenerator(Distribution distribution, long multiplier)
        {

            this.distribution = distribution;
            this.multiplier = multiplier;
        }

        public Seed next(int visits)
        {
            return new Seed(distribution.next() * multiplier, visits);
        }
    }

    private class SeriesGenerator extends Generator
    {

        final long start;
        final long totalCount;
        final boolean wrap;
        final long multiplier;
        final AtomicLong next = new AtomicLong();

        public SeriesGenerator(long start, long end, boolean wrap, long multiplier)
        {
            this.wrap = wrap;
            if (start > end)
                throw new IllegalStateException();
            this.start = start;
            this.totalCount = 1 + end - start;
            this.multiplier = multiplier;

        }

        public Seed next(int visits)
        {
            long next = this.next.getAndIncrement();
            if (!wrap && next >= totalCount)
                return null;
            return new Seed((start + (next % totalCount))*multiplier, visits);
        }
    }

    private class LookbackableWriteGenerator extends SeriesGenerator
    {

        final AtomicLong writeCount = new AtomicLong();
        final ConcurrentSkipListMap<Seed, Seed> afterMin = new ConcurrentSkipListMap<>();
        final LookbackReadGenerator reads;

        public LookbackableWriteGenerator(long start, long end, boolean wrap, Distribution readLookback, long multiplier)
        {
            super(start, end, wrap, multiplier);
            this.writeCount.set(0);
            reads = new LookbackReadGenerator(readLookback);
        }

        public Seed next(int visits)
        {
            long next = this.next.getAndIncrement();
            if (!wrap && next >= totalCount)
                return null;
            return new Seed((start + (next % totalCount)) * multiplier, visits);
        }

        void finishWrite(Seed seed)
        {
            if (seed.seed/multiplier <= writeCount.get())
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
