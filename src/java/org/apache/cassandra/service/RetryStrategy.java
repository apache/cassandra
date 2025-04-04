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

import accord.utils.Invariants;
import accord.utils.RandomSource;
import org.apache.cassandra.service.TimeoutStrategy.LatencySourceFactory;
import org.apache.cassandra.service.TimeoutStrategy.Wait;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleSupplier;
import java.util.function.LongBinaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.cassandra.service.TimeoutStrategy.parseInMicros;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * <p>A strategy for making retry timing decisions for operations.
 * The strategy is defined by six factors: <ul>
 * <li> {@link #minMinMicros}
 * <li> {@link #maxMaxMicros}
 * <li> {@link #min}
 * <li> {@link #max}
 * <li> {@link #waitRandomizer}
 * <li> {@link #maxAttempts}
 * </ul>
 *
 * <p>The first two represent the absolute upper and lower bound times we are permitted to produce as constants</p>
 * <p>The next two represent time periods, and may be defined dynamically based on a simple calculation over: <ul>
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
 * e.g.
 * <li> {@code 10ms <= p50(rw)*0.66...p99(rw)}
 * <li> {@code 10ms <= p95(rw)*1.8^attempts <= 100ms}
 * <li> {@code 5ms <= p50(rw)*0.5}
 *
 * <p>These calculations are put together to construct a range from which we draw a random number.
 * The period we wait for {@code X} will be drawn so that {@code minMin <= min <= max <= maxMax}.
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
public class RetryStrategy implements WaitStrategy
{
    private static final Pattern RANDOMIZER = Pattern.compile(
                "uniform|exp(onential)?[(](?<exp>[0-9.]+)[)]|q(uantized)?exp(onential)?[(](?<qexp>[0-9.]+)[)]");

    static final Pattern PARSE = Pattern.compile(
          "(\\s*(?<minmin>0|[0-9]+[mu]?s)\\s*<=)?" +
                "(\\s*(?<min>[^=]+)([(]?\\s*<=\\s*(?<maxmin>0|[0-9]+[mu]?s)\\s*[)]?)?\\s*[.]{3})?" +
                "(\\s*(?<max>[^=]+))" +
                "(\\s*<=\\s*(?<maxmax>0|[0-9]+[mu]?s))?");

    public static final WaitRandomizerFactory randomizers = new WaitRandomizerFactory(){};

    public interface WaitRandomizer
    {
        long wait(long min, long max, int attempts);
    }

    public static WaitRandomizerFactory randomizers(RandomSource random)
    {
        return new WaitRandomizerFactory()
        {
            @Override public LongBinaryOperator uniformLongSupplier() { return random::nextLong; }
            @Override public DoubleSupplier uniformDoubleSupplier() { return random::nextDouble; }
        };
    }

    public interface WaitRandomizerFactory
    {
        default LongBinaryOperator uniformLongSupplier() { return (min, max) -> ThreadLocalRandom.current().nextLong(min, max); } // DO NOT USE METHOD HANDLES (want to fetch afresh each time)
        default DoubleSupplier uniformDoubleSupplier() { return () -> ThreadLocalRandom.current().nextDouble(); }
        
        default WaitRandomizer uniform() { return new Uniform(uniformLongSupplier()); }
        default WaitRandomizer exponential(double power) { return new Exponential(uniformLongSupplier(), uniformDoubleSupplier(), power); }
        default WaitRandomizer quantizedExponential(double power) { return new QuantizedExponential(uniformLongSupplier(), uniformDoubleSupplier(), power); }

        class Uniform implements WaitRandomizer
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

        abstract class AbstractExponential implements WaitRandomizer
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

        class Exponential extends AbstractExponential
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

        class QuantizedExponential extends AbstractExponential
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
    public final long minMinMicros, maxMinMicros, maxMaxMicros;
    public final @Nullable Wait min;
    public final @Nonnull Wait max;
    public final int maxAttempts;

    protected RetryStrategy(WaitRandomizer waitRandomizer, long minMinMicros, Wait min, long maxMinMicros, Wait max, long maxMaxMicros, int retries)
    {
        this.waitRandomizer = waitRandomizer;
        this.minMinMicros = minMinMicros;
        this.min = min;
        this.maxMinMicros = maxMinMicros;
        this.max = max;
        this.maxMaxMicros = maxMaxMicros;
        this.maxAttempts = retries == Integer.MAX_VALUE ? Integer.MAX_VALUE : retries + 1;
        Invariants.require(maxAttempts >= 1);
    }

    public long computeWaitUntil(int attempts)
    {
        long wait = computeWait(attempts, NANOSECONDS);
        if (wait < 0)
            return -1;
        return nanoTime() + wait;
    }

    public long computeWait(int attempt, TimeUnit units)
    {
        if (attempt > maxAttempts)
            return -1;

        long result;
        if (min == null)
        {
             result = max.getMicros(attempt);
        }
        else
        {
            long min = this.min.getMicros(attempt);
            if (min > maxMinMicros)
                min = maxMinMicros;
            long max = this.max.getMicros(attempt);
            result = min >= max ? min : waitRandomizer.wait(min, max, attempt);
        }

        if (result > maxMaxMicros) result = maxMaxMicros;
        if (result < minMinMicros) result = minMinMicros;
        return units.convert(result, MICROSECONDS);
    }

    public static RetryStrategy parse(String spec, LatencySourceFactory latencies)
    {
        return parse(spec, latencies, null);
    }

    public static RetryStrategy parse(String spec, LatencySourceFactory latencies, WaitRandomizer randomizer)
    {
        String original = spec;
        int retries = Integer.MAX_VALUE;
        int end = spec.length();
        {
            int next;
            while ((next = spec.lastIndexOf(',', end - 1)) >= 0)
            {
                int mid = spec.indexOf('=', next + 1);
                if (mid <= next || mid >= end)
                    throw new IllegalArgumentException("Invalid modifier specification: '" + spec.substring(next, end) + "'; expecting '=' for value assignment");
                String key = spec.substring(next + 1, mid).trim();
                String value = spec.substring(mid + 1, end).trim();
                switch (key)
                {
                    default: throw new IllegalArgumentException("Invalid modifier specification: unrecognised property '" + key + '\'');
                    case "retries":
                        retries = Integer.parseInt(value);
                        break;
                    case "rnd":
                        if (randomizer != null)
                            throw new IllegalArgumentException("Randomizer already specified, cannot re-specify: " + value);
                        randomizer = parseWaitRandomizer(value);
                        break;
                }
                end = next;
            }
            if (end != spec.length())
                spec = spec.substring(0, end);
        }

        Matcher m = PARSE.matcher(spec);
        if (!m.matches())
            throw new IllegalArgumentException("Invalid specification: '" + spec + "'; does not match " + PARSE);

        long minMin = parseInMicros(m.group("minmin"), 0);
        long maxMax = parseInMicros(m.group("maxmax"), Long.MAX_VALUE);
        Wait max = TimeoutStrategy.parseWait(m.group("max"), latencies);
        String minSpec = m.group("min");
        Wait min = minSpec == null ? null : TimeoutStrategy.parseWait(minSpec, latencies);
        if (min == null && randomizer != null)
            throw new IllegalArgumentException("Invalid to specify randomiser when no range specified: '" + original + '\'');
        if (min instanceof Wait.Constant && minMin != 0)
            throw new IllegalArgumentException("Invalid to specify an absolute minimum constant when the min bound is itself a constant: '" + original + '\'');
        long maxMin = parseInMicros(m.group("maxmin"), Long.MAX_VALUE);
        if (min instanceof Wait.Constant && maxMin != Long.MAX_VALUE)
            throw new IllegalArgumentException("Invalid to specify an absolute max(min) constant when the min bound is itself a constant: '" + original + '\'');
        if (max instanceof Wait.Constant && maxMax != Long.MAX_VALUE)
            throw new IllegalArgumentException("Invalid to specify an absolute maximum constant when the max bound is itself a constant: '" + original + '\'');
        if (randomizer == null)
            randomizer = randomizers.uniform();
        return new RetryStrategy(randomizer, minMin, min, maxMin, max, maxMax, retries);
    }

    @VisibleForTesting
    protected static WaitRandomizer parseWaitRandomizer(String input)
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
