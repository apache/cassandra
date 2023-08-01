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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import com.codahale.metrics.Snapshot;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.NoSpamLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.DoubleSupplier;
import java.util.function.LongBinaryOperator;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;
import static java.lang.Math.*;
import static java.util.Arrays.stream;
import static java.util.concurrent.TimeUnit.*;
import static org.apache.cassandra.config.DatabaseDescriptor.*;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.casReadMetrics;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.casWriteMetrics;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.Clock.waitUntil;

/**
 * <p>A strategy for making back-off decisions for Paxos operations that fail to make progress because of other paxos operations.
 * The strategy is defined by four factors: <ul>
 * <li> {@link #min}
 * <li> {@link #max}
 * <li> {@link #minDelta}
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
 * <p>With the constraint that {@code max} must be {@code minDelta} greater than {@code min},
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
 *
 * <p>Finally, there is also a {@link #traceAfterAttempts} property that permits initiating tracing of operations
 * that experience a certain minimum number of failed paxos rounds due to contention. A setting of 0 or 1 will initiate
 * a trace session after the first failed ballot.
 */
public class ContentionStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(ContentionStrategy.class);

    private static final Pattern BOUND = Pattern.compile(
                "(?<const>0|[0-9]+[mu]s)" +
                "|((?<min>0|[0-9]+[mu]s) *<= *)?" +
                    "(p(?<perc>[0-9]+)\\((?<rw>r|w|rw|wr)\\)|(?<constbase>0|[0-9]+[mu]s))" +
                    "\\s*([*]\\s*(?<mod>[0-9.]+)?\\s*(?<modkind>[*^]\\s*attempts)?)?" +
                "( *<= *(?<max>0|[0-9]+[mu]s))?");
    private static final Pattern TIME = Pattern.compile(
                "0|([0-9]+)ms|([0-9]+)us");
    private static final Pattern RANDOMIZER = Pattern.compile(
                "uniform|exp(onential)?[(](?<exp>[0-9.]+)[)]|q(uantized)?exp(onential)?[(](?<qexp>[0-9.]+)[)]");
    private static final String DEFAULT_WAIT_RANDOMIZER = "qexp(1.5)"; // at least 0ms, and at least 66% of median latency
    private static final String DEFAULT_MIN = "0 <= p50(rw)*0.66"; // at least 0ms, and at least 66% of median latency
    private static final String DEFAULT_MAX = "10ms <= p95(rw)*1.8^attempts <= 100ms"; // p95 latency with exponential back-off at rate of 1.8^attempts
    private static final String DEFAULT_MIN_DELTA = "5ms <= p50(rw)*0.5"; // at least 5ms, and at least 50% of median latency

    private static volatile ContentionStrategy current;

    // Factories can be useful for testing purposes, to supply custom implementations of selectors and modifiers.
    final static LatencySelectorFactory selectors = new LatencySelectorFactory(){};
    final static LatencyModifierFactory modifiers = new LatencyModifierFactory(){};
    final static WaitRandomizerFactory randomizers = new WaitRandomizerFactory(){};

    static
    {
        current = new ContentionStrategy(defaultWaitRandomizer(), defaultMinWait(), defaultMaxWait(), defaultMinDelta(), Integer.MAX_VALUE);
    }

    static interface LatencyModifierFactory
    {
        default LatencyModifier identity() { return (l, a) -> l; }
        default LatencyModifier multiply(double constant) { return (l, a) -> saturatedCast(l * constant); }
        default LatencyModifier multiplyByAttempts(double multiply) { return (l, a) -> saturatedCast(l * multiply * a); }
        default LatencyModifier multiplyByAttemptsExp(double base) { return (l, a) -> saturatedCast(l * pow(base, a)); }
    }

    static interface LatencySupplier
    {
        abstract long get(double percentile);
    }

    static interface LatencySelector
    {
        abstract long select(LatencySupplier readLatencyHistogram, LatencySupplier writeLatencyHistogram);
    }

    static interface LatencySelectorFactory
    {
        default LatencySelector constant(long latency) { return (read, write) -> latency; }
        default LatencySelector read(double percentile) { return (read, write) -> read.get(percentile); }
        default LatencySelector write(double percentile) { return (read, write) -> write.get(percentile); }
        default LatencySelector maxReadWrite(double percentile) { return (read, write) -> max(read.get(percentile), write.get(percentile)); }
    }

    static interface LatencyModifier
    {
        long modify(long latency, int attempts);
    }

    static interface WaitRandomizer
    {
        abstract long wait(long min, long max, int attempts);
    }

    static interface WaitRandomizerFactory
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

    static class SnapshotAndTime
    {
        final long validUntil;
        final Snapshot snapshot;

        SnapshotAndTime(long validUntil, Snapshot snapshot)
        {
            this.validUntil = validUntil;
            this.snapshot = snapshot;
        }
    }

    static class TimeLimitedLatencySupplier extends AtomicReference<SnapshotAndTime> implements LatencySupplier
    {
        final Supplier<Snapshot> snapshotSupplier;
        final long validForNanos;

        TimeLimitedLatencySupplier(Supplier<Snapshot> snapshotSupplier, long time, TimeUnit units)
        {
            this.snapshotSupplier = snapshotSupplier;
            this.validForNanos = units.toNanos(time);
        }

        private Snapshot getSnapshot()
        {
            long now = nanoTime();

            SnapshotAndTime cur = get();
            if (cur != null && cur.validUntil > now)
                return cur.snapshot;

            Snapshot newSnapshot = snapshotSupplier.get();
            SnapshotAndTime next = new SnapshotAndTime(now + validForNanos, newSnapshot);
            if (compareAndSet(cur, next))
                return next.snapshot;

            return accumulateAndGet(next, (a, b) -> a.validUntil > b.validUntil ? a : b).snapshot;
        }

        @Override
        public long get(double percentile)
        {
            return (long)getSnapshot().getValue(percentile);
        }
    }

    static class Bound
    {
        final long min, max, onFailure;
        final LatencyModifier modifier;
        final LatencySelector selector;
        final LatencySupplier reads, writes;

        Bound(long min, long max, long onFailure, LatencyModifier modifier, LatencySelector selector)
        {
            Preconditions.checkArgument(min<=max, "min (%s) must be less than or equal to max (%s)", min, max);
            this.min = min;
            this.max = max;
            this.onFailure = onFailure;
            this.modifier = modifier;
            this.selector = selector;
            this.reads = new TimeLimitedLatencySupplier(casReadMetrics.latency::getSnapshot, 10L, SECONDS);
            this.writes = new TimeLimitedLatencySupplier(casWriteMetrics.latency::getSnapshot, 10L, SECONDS);
        }

        long get(int attempts)
        {
            try
            {
                long base = selector.select(reads, writes);
                return max(min, min(max, modifier.modify(base, attempts)));
            }
            catch (Throwable t)
            {
                NoSpamLogger.getLogger(logger, 1L, MINUTES).info("", t);
                return onFailure;
            }
        }

        public String toString()
        {
            return "Bound{" +
                   "min=" + min +
                   ", max=" + max +
                   ", onFailure=" + onFailure +
                   ", modifier=" + modifier +
                   ", selector=" + selector +
                   '}';
        }
    }

    final WaitRandomizer waitRandomizer;
    final Bound min, max, minDelta;
    final int traceAfterAttempts;

    public ContentionStrategy(String waitRandomizer, String min, String max, String minDelta, int traceAfterAttempts)
    {
        this.waitRandomizer = parseWaitRandomizer(waitRandomizer);
        this.min = parseBound(min, true);
        this.max = parseBound(max, false);
        this.minDelta = parseBound(minDelta, true);
        this.traceAfterAttempts = traceAfterAttempts;
    }

    public enum Type
    {
        READ("Contended Paxos Read"), WRITE("Contended Paxos Write"), REPAIR("Contended Paxos Repair");

        final String traceTitle;
        final String lowercase;

        Type(String traceTitle)
        {
            this.traceTitle = traceTitle;
            this.lowercase = name().toLowerCase();
        }
    }

    long computeWaitUntilForContention(int attempts, TableMetadata table, DecoratedKey partitionKey, ConsistencyLevel consistency, Type type)
    {
        if (attempts >= traceAfterAttempts && !Tracing.isTracing())
        {
            Tracing.instance.newSession(Tracing.TraceType.QUERY);
            Tracing.instance.begin(type.traceTitle,
                                   ImmutableMap.of(
                                       "keyspace", table.keyspace,
                                       "table", table.name,
                                       "partitionKey", table.partitionKeyType.getString(partitionKey.getKey()),
                                       "consistency", consistency.name(),
                                       "kind", type.lowercase
                                   ));

            logger.info("Tracing contended paxos {} for key {} on {}.{} with trace id {}",
                        type.lowercase,
                        ByteBufferUtil.bytesToHex(partitionKey.getKey()),
                        table.keyspace, table.name,
                        Tracing.instance.getSessionId());
        }

        long minWaitMicros = min.get(attempts);
        long maxWaitMicros = max.get(attempts);
        long minDeltaMicros = minDelta.get(attempts);

        if (minWaitMicros + minDeltaMicros > maxWaitMicros)
        {
            maxWaitMicros = minWaitMicros + minDeltaMicros;
            if (maxWaitMicros > this.max.max)
            {
                maxWaitMicros = this.max.max;
                minWaitMicros = max(this.min.min, min(this.min.max, maxWaitMicros - minDeltaMicros));
            }
        }

        long wait = waitRandomizer.wait(minWaitMicros, maxWaitMicros, attempts);
        return nanoTime() + MICROSECONDS.toNanos(wait);
    }

    boolean doWaitForContention(long deadline, int attempts, TableMetadata table, DecoratedKey partitionKey, ConsistencyLevel consistency, Type type)
    {
        long until = computeWaitUntilForContention(attempts, table, partitionKey, consistency, type);
        if (until >= deadline)
            return false;

        try
        {
            waitUntil(until);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            return false;
        }
        return true;
    }

    static boolean waitForContention(long deadline, int attempts, TableMetadata table, DecoratedKey partitionKey, ConsistencyLevel consistency, Type type)
    {
        return current.doWaitForContention(deadline, attempts, table, partitionKey, consistency, type);
    }

    static long waitUntilForContention(int attempts, TableMetadata table, DecoratedKey partitionKey, ConsistencyLevel consistency, Type type)
    {
        return current.computeWaitUntilForContention(attempts, table, partitionKey, consistency, type);
    }

    static class ParsedStrategy
    {
        final String waitRandomizer, min, max, minDelta;
        final ContentionStrategy strategy;

        ParsedStrategy(String waitRandomizer, String min, String max, String minDelta, ContentionStrategy strategy)
        {
            this.waitRandomizer = waitRandomizer;
            this.min = min;
            this.max = max;
            this.minDelta = minDelta;
            this.strategy = strategy;
        }
    }

    @VisibleForTesting
    static ParsedStrategy parseStrategy(String spec)
    {
        String[] args = spec.split(",");
        String waitRandomizer = find(args, "random");
        String min = find(args, "min");
        String max = find(args, "max");
        String minDelta = find(args, "delta");
        String trace = find(args, "trace");

        if (waitRandomizer == null) waitRandomizer = defaultWaitRandomizer();
        if (min == null) min = defaultMinWait();
        if (max == null) max = defaultMaxWait();
        if (minDelta == null) minDelta = defaultMinDelta();
        int traceAfterAttempts = trace == null ? current.traceAfterAttempts: Integer.parseInt(trace);

        ContentionStrategy strategy = new ContentionStrategy(waitRandomizer, min, max, minDelta, traceAfterAttempts);
        return new ParsedStrategy(waitRandomizer, min, max, minDelta, strategy);
    }


    public static void setStrategy(String spec)
    {
        ParsedStrategy parsed = parseStrategy(spec);
        current = parsed.strategy;
        setPaxosContentionWaitRandomizer(parsed.waitRandomizer);
        setPaxosContentionMinWait(parsed.min);
        setPaxosContentionMaxWait(parsed.max);
        setPaxosContentionMinDelta(parsed.minDelta);
    }

    public static String getStrategySpec()
    {
        return "min=" + defaultMinWait()
                + ",max=" + defaultMaxWait()
                + ",delta=" + defaultMinDelta()
                + ",random=" + defaultWaitRandomizer()
                + ",trace=" + current.traceAfterAttempts;
    }

    private static String find(String[] args, String param)
    {
        return stream(args).filter(s -> s.startsWith(param + '='))
                .map(s -> s.substring(param.length() + 1))
                .findFirst().orElse(null);
    }

    private static LatencySelector parseLatencySelector(Matcher m, LatencySelectorFactory selectors)
    {
        String perc = m.group("perc");
        if (perc == null)
            return selectors.constant(parseInMicros(m.group("constbase")));

        double percentile = parseDouble("0." + perc);
        String rw = m.group("rw");
        if (rw.length() == 2)
            return selectors.maxReadWrite(percentile);
        else if ("r".equals(rw))
            return selectors.read(percentile);
        else
            return selectors.write(percentile);
    }

    private static LatencyModifier parseLatencyModifier(Matcher m, LatencyModifierFactory modifiers)
    {
        String mod = m.group("mod");
        if (mod == null)
            return modifiers.identity();

        double modifier = parseDouble(mod);

        String modkind = m.group("modkind");
        if (modkind == null)
            return modifiers.multiply(modifier);

        if (modkind.startsWith("*"))
            return modifiers.multiplyByAttempts(modifier);
        else if (modkind.startsWith("^"))
            return modifiers.multiplyByAttemptsExp(modifier);
        else
            throw new IllegalArgumentException("Unrecognised attempt modifier: " + modkind);
    }

    static long saturatedCast(double v)
    {
        if (v > Long.MAX_VALUE)
            return Long.MAX_VALUE;
        return (long) v;
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

    static Bound parseBound(String input, boolean isMin)
    {
        return parseBound(input, isMin, selectors, modifiers);
    }

    @VisibleForTesting
    static Bound parseBound(String input, boolean isMin, LatencySelectorFactory selectors, LatencyModifierFactory modifiers)
    {
        Matcher m = BOUND.matcher(input);
        if (!m.matches())
            throw new IllegalArgumentException(input + " does not match " + BOUND);

        String maybeConst = m.group("const");
        if (maybeConst != null)
        {
            long v = parseInMicros(maybeConst);
            return new Bound(v, v, v, modifiers.identity(), selectors.constant(v));
        }

        long min = parseInMicros(m.group("min"), 0);
        long max = parseInMicros(m.group("max"), maxQueryTimeoutMicros() / 2);
        return new Bound(min, max, isMin ? min : max, parseLatencyModifier(m, modifiers), parseLatencySelector(m, selectors));
    }

    private static long parseInMicros(String input, long orElse)
    {
        if (input == null)
            return orElse;

        return parseInMicros(input);
    }

    private static long parseInMicros(String input)
    {
        Matcher m = TIME.matcher(input);
        if (!m.matches())
            throw new IllegalArgumentException(input + " does not match " + TIME);

        String text;
        if (null != (text = m.group(1)))
            return parseInt(text) * 1000;
        else if (null != (text = m.group(2)))
            return parseInt(text);
        else
            return 0;
    }

    @VisibleForTesting
    static String defaultWaitRandomizer()
    {
        return orElse(DatabaseDescriptor::getPaxosContentionWaitRandomizer, DEFAULT_WAIT_RANDOMIZER);
    }

    @VisibleForTesting
    static String defaultMinWait()
    {
        return orElse(DatabaseDescriptor::getPaxosContentionMinWait, DEFAULT_MIN);
    }

    @VisibleForTesting
    static String defaultMaxWait()
    {
        return orElse(DatabaseDescriptor::getPaxosContentionMaxWait, DEFAULT_MAX);
    }

    @VisibleForTesting
    static String defaultMinDelta()
    {
        return orElse(DatabaseDescriptor::getPaxosContentionMinDelta, DEFAULT_MIN_DELTA);
    }

    @VisibleForTesting
    static long maxQueryTimeoutMicros()
    {
        return max(max(getCasContentionTimeout(MICROSECONDS), getWriteRpcTimeout(MICROSECONDS)), getReadRpcTimeout(MICROSECONDS));
    }

    private static String orElse(Supplier<String> get, String orElse)
    {
        String result = get.get();
        return result != null ? result : orElse;
    }
}
