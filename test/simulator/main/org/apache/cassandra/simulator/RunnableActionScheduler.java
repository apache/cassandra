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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.cassandra.simulator.utils.KindOfSequence;

public abstract class RunnableActionScheduler implements Consumer<Action>
{
    public enum Kind { RANDOM_WALK, UNIFORM, SEQUENTIAL }

    public static class Immediate extends RunnableActionScheduler
    {
        private final AtomicLong id = new AtomicLong();

        public Immediate() { }

        @Override
        public double priority()
        {
            return id.incrementAndGet();
        }
    }

    public static class Sequential extends RunnableActionScheduler
    {
        final AtomicInteger next = new AtomicInteger();

        @Override
        public double priority()
        {
            return next.incrementAndGet();
        }

        public Sequential() { }
    }

    public abstract static class AbstractRandom extends RunnableActionScheduler
    {
        protected final RandomSource random;

        public AbstractRandom(RandomSource random)
        {
            this.random = random;
        }
    }

    public static class RandomUniform extends AbstractRandom
    {
        final double min, range;

        RandomUniform(RandomSource random, double min, double range)
        {
            super(random);
            this.min = min;
            this.range = range;
        }

        public RandomUniform(RandomSource random)
        {
            this(random, 0d, 1d);
        }

        @Override
        public double priority()
        {
            return min + random.uniformDouble() * range;
        }
    }

    static class RandomWalk extends AbstractRandom
    {
        final double maxStepSize;
        double cur;

        @Override
        public double priority()
        {
            double result = cur;
            double step = 2 * (random.uniformDouble() - 1f) * maxStepSize;
            this.cur = step > 0 ? Math.min(1d, cur + step)
                                : Math.max(0d, cur + step);
            return result;
        }

        @Override
        protected RunnableActionScheduler next()
        {
            return new RunnableActionScheduler.RandomWalk(random, cur);
        }

        RandomWalk(RandomSource random, double cur)
        {
            super(random);
            this.maxStepSize = KindOfSequence.maxStepSize(0f, 1f, random);
            this.cur = cur;
        }

        RandomWalk(RandomSource random)
        {
            this(random, 0.5d);
        }
    }

    public abstract double priority();

    protected RunnableActionScheduler next()
    {
        return this;
    }

    public void attachTo(ActionList actions)
    {
        actions.forEach(next());
    }

    @Override
    public void accept(Action action)
    {
        action.setScheduler(this);
    }
}
