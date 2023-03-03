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
package org.apache.cassandra.service.paxos;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.DoubleSupplier;
import java.util.function.LongBinaryOperator;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.nicoulaj.compilecommand.annotations.Inline;
import org.apache.cassandra.config.DatabaseDescriptor;

import static org.apache.cassandra.service.paxos.ContentionStrategy.*;
import static org.apache.cassandra.service.paxos.ContentionStrategy.WaitRandomizerFactory.*;
import static org.apache.cassandra.service.paxos.ContentionStrategyTest.WaitRandomizerType.*;

public class ContentionStrategyTest
{
    private static final Logger logger = LoggerFactory.getLogger(ContentionStrategyTest.class);

    static
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static final long MAX = maxQueryTimeoutMicros()/2;

    private static final WaitParseValidator DEFAULT_WAIT_RANDOMIZER_VALIDATOR = new WaitParseValidator(defaultWaitRandomizer(), QEXP, 1.5);
    private static final BoundParseValidator DEFAULT_MIN_VALIDATOR = new BoundParseValidator(defaultMinWait(), true, assertBound(0, MAX, 0, selectors.maxReadWrite(0f).getClass(), 0.50, 0, modifiers.multiply(0f).getClass(), 0.66));
    private static final BoundParseValidator DEFAULT_MAX_VALIDATOR = new BoundParseValidator(defaultMaxWait(), false, assertBound(10000, 100000, 100000, selectors.maxReadWrite(0f).getClass(), 0.95, 0, modifiers.multiplyByAttemptsExp(0f).getClass(), 1.8));
    private static final BoundParseValidator DEFAULT_MIN_DELTA_VALIDATOR = new BoundParseValidator(defaultMinDelta(), true, assertBound(5000, MAX, 5000, selectors.maxReadWrite(0f).getClass(), 0.50, 0, modifiers.multiply(0f).getClass(), 0.5));

    private static List<BoundParseValidator> VALIDATE = ImmutableList.of(
            new BoundParseValidator("p95(rw)", false, assertBound(0, MAX, MAX, selectors.maxReadWrite(0f).getClass(), 0.95, 0, modifiers.identity().getClass(), 1)),
            new BoundParseValidator("5ms<=p50(rw)*0.66", false, assertBound(5000, MAX, MAX, selectors.maxReadWrite(0f).getClass(), 0.50, 0, modifiers.multiply(0).getClass(), 0.66)),
            new BoundParseValidator("5us <= p50(r)*1.66*attempts", true, assertBound(5, MAX, 5, selectors.read(0f).getClass(), 0.50, 0, modifiers.multiplyByAttempts(0f).getClass(), 1.66)),
            new BoundParseValidator("0<=p50(w)*0.66^attempts", true, assertBound(0, MAX, 0, selectors.write(0f).getClass(), 0.50, 0, modifiers.multiplyByAttemptsExp(0f).getClass(), 0.66)),
            new BoundParseValidator("125us", true, assertBound(125, 125, 125, selectors.constant(0).getClass(), 0.0f, 125, modifiers.identity().getClass(), 1)),
            new BoundParseValidator("5us <= p95(r)*1.8^attempts <= 100us", true, assertBound(5, 100, 5, selectors.read(0f).getClass(), 0.95, 0, modifiers.multiplyByAttemptsExp(0f).getClass(), 1.8)),
            DEFAULT_MIN_VALIDATOR, DEFAULT_MAX_VALIDATOR, DEFAULT_MIN_DELTA_VALIDATOR
    );

    private static List<WaitParseValidator> VALIDATE_RANDOMIZER = ImmutableList.of(
            new WaitParseValidator("quantizedexponential(0.5)", QEXP, 0.5),
            new WaitParseValidator("exponential(2.5)", EXP, 2.5),
            new WaitParseValidator("exp(10)", EXP, 10),
            new WaitParseValidator("uniform", UNIFORM, 0),
            DEFAULT_WAIT_RANDOMIZER_VALIDATOR
    );

    static class BoundParseValidator
    {
        final String spec;
        final boolean isMin;
        final Consumer<Bound> validator;

        BoundParseValidator(String spec, boolean isMin, Consumer<Bound> validator)
        {
            this.spec = spec;
            this.isMin = isMin;
            this.validator = validator;
        }

        void validate(Bound bound)
        {
            validator.accept(bound);
        }
    }

    enum WaitRandomizerType
    {
        UNIFORM(Uniform.class, (p, f) -> f.uniform()),
        EXP(Exponential.class, (p, f) -> f.exponential(p)),
        QEXP(QuantizedExponential.class, (p, f) -> f.quantizedExponential(p));

        final Class<? extends WaitRandomizer> clazz;
        final BiFunction<Double, WaitRandomizerFactory, WaitRandomizer> getter;

        WaitRandomizerType(Class<? extends WaitRandomizer> clazz, BiFunction<Double, WaitRandomizerFactory, WaitRandomizer> getter)
        {
            this.clazz = clazz;
            this.getter = getter;
        }
    }


    static class WaitParseValidator
    {
        final String spec;
        final WaitRandomizerType type;
        final double power;

        WaitParseValidator(String spec, WaitRandomizerType type, double power)
        {
            this.spec = spec;
            this.type = type;
            this.power = power;
        }

        void validate(WaitRandomizer randomizer)
        {
            Assert.assertSame(type.clazz, randomizer.getClass());
            if (AbstractExponential.class.isAssignableFrom(type.clazz))
                Assert.assertEquals(power, ((AbstractExponential) randomizer).power, 0.00001);
        }
    }

    private static class WaitRandomizerOutputValidator
    {
        static void validate(WaitRandomizerType type, long seed, int trials, int samplesPerTrial)
        {
            Random random = new Random(seed);
            WaitRandomizer randomizer = type.getter.apply(2d, new WaitRandomizerFactory()
            {
                @Override public LongBinaryOperator uniformLongSupplier() { return (min, max) -> min + random.nextInt((int) (max - min)); }
                @Override public DoubleSupplier uniformDoubleSupplier() { return random::nextDouble; }
            });

            for (int i = 0 ; i < trials ; ++i)
            {
                int min = random.nextInt(1 << 20);
                int max = min + 1024 + random.nextInt(1 << 20);
                double minMean = minMean(type, min, max);
                double maxMean = maxMean(type, min, max);
                double sampleMean = sampleMean(samplesPerTrial, min, max, randomizer);
                Assert.assertTrue(minMean <= sampleMean);
                Assert.assertTrue(maxMean >= sampleMean);
            }
        }

        private static double minMean(WaitRandomizerType type, int min, int max)
        {
            switch (type)
            {
                case UNIFORM: return min + (max - min) * (4d/10);
                case EXP: case QEXP: return min + (max - min) * (6d/10);
                default: throw new IllegalStateException();
            }
        }

        private static double maxMean(WaitRandomizerType type, int min, int max)
        {
            switch (type)
            {
                case UNIFORM: return min + (max - min) * (6d/10);
                case EXP: case QEXP: return min + (max - min) * (8d/10);
                default: throw new IllegalStateException();
            }
        }

        private static double sampleMean(int samples, int min, int max, WaitRandomizer randomizer)
        {
            double sum = 0;
            int attempts = 1;
            for (int i = 0 ; i < samples ; ++i)
            {
                long wait = randomizer.wait(min, max, attempts = (attempts & 15) + 1);
                Assert.assertTrue(wait >= min);
                Assert.assertTrue(wait <= max);
                sum += wait;
            }
            double mean = sum / samples;
            Assert.assertTrue(mean >= min);
            Assert.assertTrue(mean <= max);
            return mean;
        }
    }

    private static Consumer<Bound> assertBound(
                             long min, long max, long onFailure,
                             Class<? extends LatencySelector> selectorClass,
                             double selectorPercentile,
                             long selectorConst,
                             Class<? extends LatencyModifier> modifierClass,
                             double modifierVal
    )
    {
        return bound -> {
            Assert.assertEquals(min, bound.min);
            Assert.assertEquals(max, bound.max);
            Assert.assertEquals(onFailure, bound.onFailure);
            Assert.assertSame(selectorClass, bound.selector.getClass());
            if (selectorClass == selectors.constant(0).getClass())
            {
                LatencySupplier fail = v -> { throw new UnsupportedOperationException(); };
                Assert.assertEquals(selectorConst, bound.selector.select(fail, fail));
            }
            else
            {
                AtomicReference<Double> percentile = new AtomicReference<>();
                LatencySupplier set = v -> { percentile.set(v); return 0; };
                bound.selector.select(set, set);
                Assert.assertNotNull(percentile.get());
                Assert.assertEquals(selectorPercentile, percentile.get(), 0.00001);
            }
            Assert.assertSame(modifierClass, bound.modifier.getClass());
            Assert.assertEquals(1000000L * modifierVal, bound.modifier.modify(1000000, 1), 0.00001);
        };
    }

    private static void assertParseFailure(String spec)
    {

        try
        {
            Bound bound = parseBound(spec, false);
            Assert.fail("expected parse failure, but got " + bound);
        }
        catch (IllegalArgumentException e)
        {
            // expected
        }
    }

    @Test
    public void strategyParseTest()
    {
        for (BoundParseValidator min : VALIDATE.stream().filter(v -> v.isMin).toArray(BoundParseValidator[]::new))
        {
            for (BoundParseValidator max : VALIDATE.stream().filter(v -> !v.isMin).toArray(BoundParseValidator[]::new))
            {
                for (BoundParseValidator minDelta : VALIDATE.stream().filter(v -> v.isMin).toArray(BoundParseValidator[]::new))
                {
                    for (WaitParseValidator random : VALIDATE_RANDOMIZER)
                    {
                        {
                            ParsedStrategy parsed = parseStrategy("min=" + min.spec + ",max=" + max.spec + ",delta=" + minDelta.spec + ",random=" + random.spec);
                            Assert.assertEquals(parsed.min, min.spec);
                            min.validate(parsed.strategy.min);
                            Assert.assertEquals(parsed.max, max.spec);
                            max.validate(parsed.strategy.max);
                            Assert.assertEquals(parsed.minDelta, minDelta.spec);
                            minDelta.validate(parsed.strategy.minDelta);
                            Assert.assertEquals(parsed.waitRandomizer, random.spec);
                            random.validate(parsed.strategy.waitRandomizer);
                        }
                        ParsedStrategy parsed = parseStrategy("random=" + random.spec);
                        Assert.assertEquals(parsed.min, DEFAULT_MIN_VALIDATOR.spec);
                        DEFAULT_MIN_VALIDATOR.validate(parsed.strategy.min);
                        Assert.assertEquals(parsed.max, DEFAULT_MAX_VALIDATOR.spec);
                        DEFAULT_MAX_VALIDATOR.validate(parsed.strategy.max);
                        Assert.assertEquals(parsed.minDelta, DEFAULT_MIN_DELTA_VALIDATOR.spec);
                        DEFAULT_MIN_DELTA_VALIDATOR.validate(parsed.strategy.minDelta);
                        Assert.assertEquals(parsed.waitRandomizer, random.spec);
                        random.validate(parsed.strategy.waitRandomizer);
                    }
                    ParsedStrategy parsed = parseStrategy("delta=" + minDelta.spec);
                    Assert.assertEquals(parsed.min, DEFAULT_MIN_VALIDATOR.spec);
                    DEFAULT_MIN_VALIDATOR.validate(parsed.strategy.min);
                    Assert.assertEquals(parsed.max, DEFAULT_MAX_VALIDATOR.spec);
                    DEFAULT_MAX_VALIDATOR.validate(parsed.strategy.max);
                    Assert.assertEquals(parsed.minDelta, minDelta.spec);
                    minDelta.validate(parsed.strategy.minDelta);
                }
                ParsedStrategy parsed = parseStrategy("max=" + max.spec);
                Assert.assertEquals(parsed.min, DEFAULT_MIN_VALIDATOR.spec);
                DEFAULT_MIN_VALIDATOR.validate(parsed.strategy.min);
                Assert.assertEquals(parsed.max, max.spec);
                max.validate(parsed.strategy.max);
                Assert.assertEquals(parsed.minDelta, DEFAULT_MIN_DELTA_VALIDATOR.spec);
                DEFAULT_MIN_DELTA_VALIDATOR.validate(parsed.strategy.minDelta);
            }
            ParsedStrategy parsed = parseStrategy("min=" + min.spec);
            Assert.assertEquals(parsed.min, min.spec);
            min.validate(parsed.strategy.min);
            Assert.assertEquals(parsed.max, DEFAULT_MAX_VALIDATOR.spec);
            DEFAULT_MAX_VALIDATOR.validate(parsed.strategy.max);
            Assert.assertEquals(parsed.minDelta, DEFAULT_MIN_DELTA_VALIDATOR.spec);
            DEFAULT_MIN_DELTA_VALIDATOR.validate(parsed.strategy.minDelta);
        }
    }

    @Test
    public void testParseRoundTrip()
    {
        LatencySelectorFactory selectorFactory = new LatencySelectorFactory()
        {
            LatencySelectorFactory delegate = ContentionStrategy.selectors;
            public LatencySelector constant(long latency) { return selector(delegate.constant(latency), String.format("%dms", latency)); }
            public LatencySelector read(double percentile) { return selector(delegate.read(percentile), String.format("p%d(r)", (int) (percentile * 100))); }
            public LatencySelector write(double percentile) { return selector(delegate.write(percentile), String.format("p%d(w)", (int) (percentile * 100))); }
            public LatencySelector maxReadWrite(double percentile) { return selector(delegate.maxReadWrite(percentile), String.format("p%d(rw)", (int) percentile * 100)); }

            private LatencySelector selector(LatencySelector selector, String str) {
                return new LatencySelector()
                {
                    public long select(LatencySupplier read, LatencySupplier write)
                    {
                        return selector.select(read, write);
                    }

                    public String toString()
                    {
                        return str;
                    }
                };
            }
        };

        LatencyModifierFactory modifierFactory = new LatencyModifierFactory()
        {
            LatencyModifierFactory delegate = ContentionStrategy.modifiers;
            public LatencyModifier identity() { return modifier(delegate.identity(), ""); }
            public LatencyModifier multiply(double constant) { return modifier(delegate.multiply(constant), String.format(" * %.2f", constant)); }
            public LatencyModifier multiplyByAttempts(double multiply) { return modifier(delegate.multiplyByAttempts(multiply), String.format(" * %.2f * attempts", multiply)); }
            public LatencyModifier multiplyByAttemptsExp(double base) { return modifier(delegate.multiplyByAttemptsExp(base), String.format(" * %.2f ^ attempts", base)); }

            private LatencyModifier modifier(LatencyModifier modifier, String str) {
                return new LatencyModifier()
                {
                    @Inline
                    public long modify(long latency, int attempts)
                    {
                        return modifier.modify(latency, attempts);
                    }

                    public String toString()
                    {
                        return str;
                    }
                };
            }
        };

        LatencyModifier[] latencyModifiers = new LatencyModifier[]{
        modifierFactory.multiply(0.5),
        modifierFactory.multiplyByAttempts(0.5),
        modifierFactory.multiplyByAttemptsExp(0.5)
        };

        LatencySelector[] latencySelectors = new LatencySelector[]{
        selectorFactory.read(0.5),
        selectorFactory.write(0.5),
        selectorFactory.maxReadWrite(0.99)
        };

        for (boolean min : new boolean[] { true, false})
        {
            String left = min ? "10ms <= " : "";
            for (boolean max : new boolean[] { true, false})
            {
                String right = max ? " <= 10ms" : "";

                for (LatencySelector selector : latencySelectors)
                {
                    for (LatencyModifier modifier : latencyModifiers)
                    {
                        String mid = String.format("%s%s", selector, modifier);
                        String input = left + mid + right;
                        Bound bound = parseBound(input, false, selectorFactory, modifierFactory);
                        Assert.assertTrue(String.format("Bound: %d" , bound.min), !min || bound.min == 10000);
                        Assert.assertTrue(String.format("Bound: %d" , bound.max), !max || bound.max == 10000);
                        Assert.assertEquals(selector.toString(), bound.selector.toString());
                        Assert.assertEquals(modifier.toString(), bound.modifier.toString());
                    }
                }
            }
        }
    }

    @Test
    public void boundParseTest()
    {
        VALIDATE.forEach(v -> v.validate(parseBound(v.spec, v.isMin)));
    }

    @Test
    public void waitRandomizerParseTest()
    {
        VALIDATE_RANDOMIZER.forEach(v -> v.validate(parseWaitRandomizer(v.spec)));
    }

    @Test
    public void waitRandomizerSampleTest()
    {
        waitRandomizerSampleTest(2);
    }

    private void waitRandomizerSampleTest(int count)
    {
        while (count-- > 0)
        {
            long seed = ThreadLocalRandom.current().nextLong();
            logger.info("Seed {}", seed);
            for (WaitRandomizerType type : WaitRandomizerType.values())
            {
                WaitRandomizerOutputValidator.validate(type, seed, 100, 1000000);
            }
        }
    }

    @Test
    public void boundParseFailureTest()
    {
        assertParseFailure("10ms <= p95(r) <= 5ms");
        assertParseFailure("10 <= p95(r)");
        assertParseFailure("10 <= 20 <= 30");
        assertParseFailure("p95(r) < 5");
        assertParseFailure("p95(x)");
        assertParseFailure("p95()");
        assertParseFailure("p95");
        assertParseFailure("p50(rw)+0.66");
    }

    @Test
    public void testBackoffTime()
    {
        ContentionStrategy strategy = parseStrategy("min=0ms,max=100ms,random=uniform").strategy;
        double total = 0;
        int count = 100000;
        for (int i = 0 ; i < count ; ++i)
        {
            long now = System.nanoTime();
            long waitUntil = strategy.computeWaitUntilForContention(1, null, null, null, null);
            long waitLength = Math.max(waitUntil - now, 0);
            total += waitLength;
        }
        Assert.assertTrue(Math.abs(TimeUnit.MILLISECONDS.toNanos(50) - (total / count)) < TimeUnit.MILLISECONDS.toNanos(1L));
    }

    @Test
    public void testBackoffTimeElapsed()
    {
        ContentionStrategy strategy = parseStrategy("min=0ms,max=10ms,random=uniform").strategy;
        double total = 0;
        int count = 1000;
        for (int i = 0 ; i < count ; ++i)
        {
            long start = System.nanoTime();
            strategy.doWaitForContention(Long.MAX_VALUE, 1, null, null, null, null);
            long end = System.nanoTime();
            total += end - start;
        }
        // make sure we have slept at least 4ms on average, given a mean wait time of 5ms
        double avg = total / count;
        double nanos = avg - TimeUnit.MILLISECONDS.toNanos(4);
        Assert.assertTrue(nanos > 0);
    }
}
