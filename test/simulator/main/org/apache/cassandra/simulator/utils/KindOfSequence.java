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

package org.apache.cassandra.simulator.utils;

import java.util.Arrays;

import org.apache.cassandra.simulator.RandomSource;

public enum KindOfSequence
{
    UNIFORM, UNIFORM_STEP, RANDOMWALK;

    public interface LinkLatency
    {
        long get(RandomSource random, int from, int to);
    }

    public interface NetworkDecision
    {
        boolean get(RandomSource random, int from, int to);
    }

    public interface Period
    {
        long get(RandomSource random);
    }

    public interface Decision
    {
        boolean get(RandomSource random);
    }

    static class UniformPeriod implements LinkLatency, Period
    {
        final LongRange nanos;

        UniformPeriod(LongRange nanos)
        {
            this.nanos = nanos;
        }

        @Override
        public long get(RandomSource random, int from, int to)
        {
            return nanos.select(random);
        }

        @Override
        public long get(RandomSource random)
        {
            return nanos.select(random);
        }
    }

    static class UniformStepPeriod implements LinkLatency, Period
    {
        final LongRange range;
        long target;
        long cur;
        long step;

        UniformStepPeriod(LongRange range, RandomSource random)
        {
            this.range = range;
            cur = range.select(random);
            next(random);
        }

        void next(RandomSource random)
        {
            target = range.select(random);
            step = (target - cur) / random.uniform(16, 128);
            if (step == 0) step = target > cur ? 1 : -1;
        }

        @Override
        public long get(RandomSource random, int from, int to)
        {
            return get(random);
        }

        @Override
        public long get(RandomSource random)
        {
            long result = cur;
            cur += step;
            if (step < 0 && cur <= target) next(random);
            else if (step > 0 && cur >= target) next(random);
            return result;
        }
    }

    static class RandomWalkPeriod implements Period
    {
        final LongRange nanos;
        final long maxStepSize;
        long cur;

        RandomWalkPeriod(LongRange nanos, RandomSource random)
        {
            this.nanos = nanos;
            this.maxStepSize = maxStepSize(nanos, random);
            this.cur = nanos.select(random);
        }

        @Override
        public long get(RandomSource random)
        {
            long step = random.uniform(-maxStepSize, maxStepSize);
            long cur = this.cur;
            this.cur = step > 0 ? Math.min(nanos.max, cur + step)
                                : Math.max(nanos.min, cur + step);
            return cur;
        }
    }

    static class RandomWalkLinkLatency implements LinkLatency
    {
        final LongRange nanos;
        final long maxStepSize;
        final long[][] curs;

        RandomWalkLinkLatency(int nodes, LongRange nanos, RandomSource random)
        {
            this.nanos = nanos;
            this.maxStepSize = maxStepSize(nanos, random);
            this.curs = new long[nodes][nodes];
            for (long[] c : curs)
                Arrays.fill(c, nanos.min);
        }

        @Override
        public long get(RandomSource random, int from, int to)
        {
            --from;--to;
            long cur = curs[from][to];
            long step = random.uniform(-maxStepSize, maxStepSize);
            curs[from][to] = step > 0 ? Math.min(nanos.max, cur + step)
                                      : Math.max(nanos.min, cur + step);
            return cur;
        }
    }

    static class UniformStepLinkLatency implements LinkLatency
    {
        private static final int CUR = 0, TARGET = 1, STEP = 2;
        final LongRange range;
        final long[][][] state;

        UniformStepLinkLatency(int nodes, LongRange range, RandomSource random)
        {
            this.range = range;
            this.state = new long[nodes][nodes][3];
            for (int i = 0 ; i < nodes ; ++i)
            {
                for (int j = 0 ; j < nodes ; ++j)
                {
                    long[] state = this.state[i][j];
                    state[CUR] = range.select(random);
                    next(random, state);
                }
            }
        }

        void next(RandomSource random, long[] state)
        {
            state[TARGET] = range.select(random);
            state[STEP] = (state[TARGET] - state[CUR]) / random.uniform(16, 128);
            if (state[STEP] == 0) state[STEP] = state[TARGET] > state[CUR] ? 1 : -1;
        }

        @Override
        public long get(RandomSource random, int from, int to)
        {
            --from;--to;
            long[] state = this.state[from][to];
            long cur = state[CUR];
            state[CUR] += state[STEP];
            if (state[STEP] < 0 && cur <= state[TARGET]) next(random, state);
            else if (state[STEP] > 0 && cur >= state[TARGET]) next(random, state);
            return cur;
        }
    }

    static class FixedChance implements NetworkDecision, Decision
    {
        final float chance;

        FixedChance(float chance)
        {
            this.chance = chance;
        }

        @Override
        public boolean get(RandomSource random, int from, int to)
        {
            return random.decide(chance);
        }

        @Override
        public boolean get(RandomSource random)
        {
            return random.decide(chance);
        }
    }

    static class RandomWalkNetworkDecision implements NetworkDecision
    {
        final ChanceRange range;
        final float maxStepSize;
        final float[][] curs;

        RandomWalkNetworkDecision(int nodes, ChanceRange range, RandomSource random)
        {
            this.range = range;
            this.maxStepSize = maxStepSize(range, random);
            this.curs = new float[nodes][nodes];
            for (float[] c : curs)
                Arrays.fill(c, range.select(random));
        }

        @Override
        public boolean get(RandomSource random, int from, int to)
        {
            --from;--to;
            float cur = curs[from][to];
            float step = (2*random.uniformFloat() - 1f) * maxStepSize;
            curs[from][to] = step > 0 ? Math.min(range.max, cur + step)
                                      : Math.max(range.min, cur + step);
            return random.decide(cur);
        }
    }

    static class UniformStepNetworkDecision implements NetworkDecision
    {
        private static final int CUR = 0, TARGET = 1, STEP = 2;
        final ChanceRange range;
        final float[][][] state;

        UniformStepNetworkDecision(int nodes, ChanceRange range, RandomSource random)
        {
            this.range = range;
            this.state = new float[nodes][nodes][3];
            for (int i = 0 ; i < nodes ; ++i)
            {
                for (int j = 0 ; j < nodes ; ++j)
                {
                    float[] state = this.state[i][j];
                    state[CUR] = range.select(random);
                    next(random, state);
                }
            }
        }

        void next(RandomSource random, float[] state)
        {
            state[TARGET] = range.select(random);
            state[STEP] = (state[TARGET] - state[CUR]) / random.uniform(16, 128);
            if (state[STEP] == 0) state[STEP] = state[TARGET] > state[CUR] ? 1 : -1;
        }

        @Override
        public boolean get(RandomSource random, int from, int to)
        {
            --from;--to;
            float[] state = this.state[from][to];
            float cur = state[CUR];
            state[CUR] += state[STEP];
            if (state[STEP] < 0 && cur <= state[TARGET]) next(random, state);
            else if (state[STEP] > 0 && cur >= state[TARGET]) next(random, state);
            return random.decide(cur);
        }
    }

    static class RandomWalkDecision implements Decision
    {
        final ChanceRange range;
        final float maxStepSize;
        float cur;

        RandomWalkDecision(ChanceRange range, RandomSource random)
        {
            this.range = range;
            this.maxStepSize = maxStepSize(range, random);
            this.cur = range.select(random);
        }

        @Override
        public boolean get(RandomSource random)
        {
            float step = (2*random.uniformFloat() - 1f) * maxStepSize;
            float cur = this.cur;
            this.cur = step > 0 ? Math.min(range.max, cur + step)
                                : Math.max(range.min, cur + step);
            return random.decide(cur);
        }
    }

    static class UniformStepDecision implements Decision
    {
        final ChanceRange range;
        float target;
        float cur;
        float step;

        UniformStepDecision(ChanceRange range, RandomSource random)
        {
            this.range = range;
            cur = range.select(random);
            next(random);
        }

        void next(RandomSource random)
        {
            target = range.select(random);
            step = (target - cur) / random.uniform(16, 128);
            if (step == 0) step = target > cur ? 1 : -1;
        }

        @Override
        public boolean get(RandomSource random)
        {
            float chance = cur;
            cur += step;
            if (step < 0 && cur <= target) next(random);
            else if (step > 0 && cur >= target) next(random);
            return random.decide(chance);
        }
    }

    public LinkLatency linkLatency(int nodes, LongRange nanos, RandomSource random)
    {
        switch (this)
        {
            default:throw new AssertionError();
            case UNIFORM: return new UniformPeriod(nanos);
            case UNIFORM_STEP: return new UniformStepLinkLatency(nodes, nanos, random);
            case RANDOMWALK: return new RandomWalkLinkLatency(nodes, nanos, random);
        }
    }

    public Period period(LongRange nanos, RandomSource random)
    {
        switch (this)
        {
            default: throw new AssertionError();
            case UNIFORM: return new UniformPeriod(nanos);
            case UNIFORM_STEP: return new UniformStepPeriod(nanos, random);
            case RANDOMWALK: return new RandomWalkPeriod(nanos, random);
        }
    }

    public NetworkDecision networkDecision(int nodes, ChanceRange range, RandomSource random)
    {
        switch (this)
        {
            default:throw new AssertionError();
            // TODO: support a uniform per node variant?
            case UNIFORM: return new FixedChance(range.select(random));
            case UNIFORM_STEP: return new UniformStepNetworkDecision(nodes, range, random);
            case RANDOMWALK: return new RandomWalkNetworkDecision(nodes, range, random);
        }
    }

    public Decision decision(ChanceRange range, RandomSource random)
    {
        switch (this)
        {
            default:throw new AssertionError();
            case UNIFORM: return new FixedChance(range.select(random));
            case UNIFORM_STEP: return new UniformStepDecision(range, random);
            case RANDOMWALK: return new RandomWalkDecision(range, random);
        }
    }


    public static float maxStepSize(ChanceRange range, RandomSource random)
    {
        return maxStepSize(range.min, range.max, random);
    }

    public static float maxStepSize(float min, float max, RandomSource random)
    {
        switch (random.uniform(0, 3))
        {
            case 0:
                return Math.max(Float.MIN_VALUE, (max/32) - (min/32));
            case 1:
                return Math.max(Float.MIN_VALUE, (max/256) - (min/256));
            case 2:
                return Math.max(Float.MIN_VALUE, (max/2048) - (min/2048));
            default:
                return Math.max(Float.MIN_VALUE, (max/16384) - (min/16384));
        }
    }

    private static long maxStepSize(LongRange range, RandomSource random)
    {
        switch (random.uniform(0, 3))
        {
            case 0:
                return Math.max(1, (range.max/32) - (range.min/32));
            case 1:
                return Math.max(1, (range.max/256) - (range.min/256));
            case 2:
                return Math.max(1, (range.max/2048) - (range.min/2048));
            default:
                return Math.max(1, (range.max/16384) - (range.min/16384));
        }
    }
}
