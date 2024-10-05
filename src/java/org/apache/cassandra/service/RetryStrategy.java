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

package org.apache.cassandra.service;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.TimeoutStrategy.LatencySourceFactory;
import org.apache.cassandra.service.TimeoutStrategy.Wait;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;
import java.util.function.LongBinaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Math.*;
import static java.util.Arrays.stream;
import static java.util.concurrent.TimeUnit.*;
import static org.apache.cassandra.service.TimeoutStrategy.parseWait;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * <p>A strategy for making retry timing decisions for operations.
 * The strategy is defined by four factors: <ul>
 * <li> {@link #min}
 * <li> {@link #max}
 * <li> {@link #spread}
 * <li> {@link #waitRandomizer}
 * </ul>
 *
 * <p>The first three represent time periods, and may be defined dynamically based on a simple calculation over: <ul>
 * <li> {@code pX()} recent experienced latency distribution for successful operations,
 *                 e.g. {@code p50(rw)} the maximum of read and write median latencies,
 *                      {@code p999(r)} the 99.9th percentile of read latencies
 * <li> {@code attempts} the number of failed attempts made by the operation so far
 * <li> {@code constant} a user provided floating point constant
 * </ul>
 *
 * <p>Their calculation may take any of these forms
 * <li> constant            {@code $constant$[mu]s}
 * <li> dynamic constant    {@code pX() * constant}
 * <li> dynamic linear      {@code pX() * constant * attempts}
 * <li> dynamic exponential {@code pX() * constant ^ attempts}
 *
 * <p>Furthermore, the dynamic calculations can be bounded with a min/max, like so:
 *  {@code min[mu]s <= dynamic expr <= max[mu]s}
 *
 * e.g.
 * <li> {@code 10ms <= p50(rw)*0.66}
 * <li> {@code 10ms <= p95(rw)*1.8^attempts <= 100ms}
 * <li> {@code 5ms <= p50(rw)*0.5}
 *
 * <p>These calculations are put together to construct a range from which we draw a random number.
 * The period we wait for {@code X} will be drawn so that {@code min <= X < max}.
 *
 * <p>With the constraint that {@code max} must be {@code spread} greater than {@code min},
 * but no greater than its expression-defined maximum. {@code max} will be increased up until
 * this point, after which {@code min} will be decreased until this gap is imposed.
 *
 * <p>The {@link #waitRandomizer} property specifies the manner in which a random value is drawn from the range.
 * It is defined using one of the following specifiers:
 * <li> uniform
 * <li> exp($power$) or exponential($power$)
 * <li> qexp($power$) or qexponential($power$) or quantizedexponential($power$)
 *
 * The uniform specifier is self-explanatory, selecting all values in the range with equal probability.
 * The exponential specifier draws values towards the end of the range with higher probability, raising
 * a floating point number in the range [0..1.0) to the power provided, and translating the resulting value
 * to a uniform value in the range.
 * The quantized exponential specifier partitions the range into {@code attempts} buckets, then applies the pure
 * exponential approach to draw values from [0..attempts), before drawing a uniform value from the corresponding bucket
 */
public class RetryStrategy
{
    private static final Pattern RANDOMIZER = Pattern.compile(
                "uniform|exp(onential)?[(](?<exp>[0-9.]+)[)]|q(uantized)?exp(onential)?[(](?<qexp>[0-9.]+)[)]");

    final static WaitRandomizerFactory randomizers = new WaitRandomizerFactory(){};

    protected interface WaitRandomizer
    {
        long wait(long min, long max, int attempts);
    }

    interface WaitRandomizerFactory
    {
        default LongBinaryOperator uniformLongSupplier() { return (min, max) -> ThreadLocalRandom.current().nextLong(min, max); } // DO NOT USE METHOD HANDLES (want to fetch afresh each time)
        default DoubleSupplier uniformDoubleSupplier() { return () -> ThreadLocalRandom.current().nextDouble(); }
        
        default WaitRandomizer uniform() { return new Uniform(uniformLongSupplier()); }
        default WaitRandomizer exponential(double power) { return new Exponential(uniformLongSupplier(), uniformDoubleSupplier(), power); }
        default WaitRandomizer quantizedExponential(double power) { return new QuantizedExponential(uniformLongSupplier(), uniformDoubleSupplier(), power); }

        static class Uniform implements WaitRandomizer
        {
            final LongBinaryOperator uniformLong;

            public Uniform(LongBinaryOperator uniformLong)
            {
                this.uniformLong = uniformLong;
            }

            @Override
            public long wait(long min, long max, int attempts)
            {
                return uniformLong.applyAsLong(min, max);
            }
        }

        static abstract class AbstractExponential implements WaitRandomizer
        {
            final LongBinaryOperator uniformLong;
            final DoubleSupplier uniformDouble;
            final double power;

            public AbstractExponential(LongBinaryOperator uniformLong, DoubleSupplier uniformDouble, double power)
            {
                this.uniformLong = uniformLong;
                this.uniformDouble = uniformDouble;
                this.power = power;
            }
        }

        static class Exponential extends AbstractExponential
        {
            public Exponential(LongBinaryOperator uniformLong, DoubleSupplier uniformDouble, double power)
            {
                super(uniformLong, uniformDouble, power);
            }

            @Override
            public long wait(long min, long max, int attempts)
            {
                if (attempts == 1)
                    return uniformLong.applyAsLong(min, max);

                double p = uniformDouble.getAsDouble();
                long delta = max - min;
                delta *= Math.pow(p, power);
                return max - delta;
            }
        }

        static class QuantizedExponential extends AbstractExponential
        {
            public QuantizedExponential(LongBinaryOperator uniformLong, DoubleSupplier uniformDouble, double power)
            {
                super(uniformLong, uniformDouble, power);
            }

            @Override
            public long wait(long min, long max, int attempts)
            {
                long quanta = (max - min) / attempts;
                if (attempts == 1 || quanta == 0)
                    return uniformLong.applyAsLong(min, max);

                double p = uniformDouble.getAsDouble();
                int base = (int) (attempts * Math.pow(p, power));
                return max - ThreadLocalRandom.current().nextLong(quanta * base, quanta * (base + 1));
            }
        }
    }

    public final WaitRandomizer waitRandomizer;
    public final Wait min, max, spread;

    public RetryStrategy(String waitRandomizer, String min, String max, String spread, LatencySourceFactory latencies)
    {
        this.waitRandomizer = parseWaitRandomizer(waitRandomizer);
        this.min = parseBound(min, true, latencies);
        this.max = parseBound(max, false, latencies);
        this.spread = parseBound(spread, true, latencies);
    }

    protected RetryStrategy(WaitRandomizer waitRandomizer, Wait min, Wait max, Wait spread)
    {
        this.waitRandomizer = waitRandomizer;
        this.min = min;
        this.max = max;
        this.spread = spread;
    }

    protected Wait parseBound(String spec, boolean isMin, LatencySourceFactory latencies)
    {
        long defaultMaxMicros = DatabaseDescriptor.getRpcTimeout(MICROSECONDS);
        return parseWait(spec, 0, defaultMaxMicros, isMin ? 0 : defaultMaxMicros, latencies);
    }

    protected long computeWaitUntil(int attempts)
    {
        long wait = computeWait(attempts);
        return nanoTime() + MICROSECONDS.toNanos(wait);
    }

    protected long computeWait(int attempts)
    {
        long minWaitMicros = min.get(attempts);
        long maxWaitMicros = max.get(attempts);
        long spreadMicros = spread.get(attempts);

        if (minWaitMicros + spreadMicros > maxWaitMicros)
        {
            maxWaitMicros = minWaitMicros + spreadMicros;
            if (maxWaitMicros > this.max.max)
            {
                maxWaitMicros = this.max.max;
                minWaitMicros = max(this.min.min, min(this.min.max, maxWaitMicros - spreadMicros));
            }
        }

        return waitRandomizer.wait(minWaitMicros, maxWaitMicros, attempts);
    }

    public static class ParsedStrategy
    {
        public final String waitRandomizer, min, max, spread;
        public final RetryStrategy strategy;

        protected ParsedStrategy(String waitRandomizer, String min, String max, String spread, RetryStrategy strategy)
        {
            this.waitRandomizer = waitRandomizer;
            this.min = min;
            this.max = max;
            this.spread = spread;
            this.strategy = strategy;
        }

        public String toString()
        {
            return "min=" + min + ",max=" + max + ",spread=" + spread + ",random=" + waitRandomizer;
        }
    }

    @VisibleForTesting
    public static ParsedStrategy parseStrategy(String spec, LatencySourceFactory latencies, ParsedStrategy defaultStrategy)
    {
        String[] args = spec.split(",");
        String waitRandomizer = find(args, "random");
        String min = find(args, "min");
        String max = find(args, "max");
        String spread = find(args, "spread");
        if (spread == null)
            spread = find(args, "delta");

        if (waitRandomizer == null) waitRandomizer = defaultStrategy.waitRandomizer;
        if (min == null) min = defaultStrategy.min;
        if (max == null) max = defaultStrategy.max;
        if (spread == null) spread = defaultStrategy.spread;

        RetryStrategy strategy = new RetryStrategy(waitRandomizer, min, max, spread, latencies);
        return new ParsedStrategy(waitRandomizer, min, max, spread, strategy);
    }

    protected static String find(String[] args, String param)
    {
        return stream(args).filter(s -> s.startsWith(param + '='))
                .map(s -> s.substring(param.length() + 1))
                .findFirst().orElse(null);
    }

    static WaitRandomizer parseWaitRandomizer(String input)
    {
        return parseWaitRandomizer(input, randomizers);
    }

    static WaitRandomizer parseWaitRandomizer(String input, WaitRandomizerFactory randomizers)
    {
        Matcher m = RANDOMIZER.matcher(input);
        if (!m.matches())
            throw new IllegalArgumentException(input + " does not match" + RANDOMIZER);

        String exp;
        exp = m.group("exp");
        if (exp != null)
            return randomizers.exponential(Double.parseDouble(exp));
        exp = m.group("qexp");
        if (exp != null)
            return randomizers.quantizedExponential(Double.parseDouble(exp));
        return randomizers.uniform();
    }
}
