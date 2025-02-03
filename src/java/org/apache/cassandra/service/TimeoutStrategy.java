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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import accord.utils.Invariants;
import com.codahale.metrics.Snapshot;
import org.apache.cassandra.metrics.ClientRequestMetrics;
import org.apache.cassandra.service.TimeoutStrategy.LatencySupplier.Constant;
import org.apache.cassandra.service.TimeoutStrategy.LatencySupplier.Percentile;

import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;
import static java.lang.Math.max;
import static java.lang.Math.pow;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.config.DatabaseDescriptor.getCasContentionTimeout;
import static org.apache.cassandra.config.DatabaseDescriptor.getReadRpcTimeout;
import static org.apache.cassandra.config.DatabaseDescriptor.getWriteRpcTimeout;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * <p>A strategy for making timeout decisions for operations. This is a simplified single-value version of
 * the RetryStrategy
 *
 * <p>This represent a computed time period, that may be defined dynamically based on a simple calculation over: <ul>
 * <li> {@code pX()} recent experienced latency distribution for successful operations,
 *                 e.g. {@code p50(rw)} the maximum of read and write median latencies,
 *                      {@code p999(r)} the 99.9th percentile of read latencies
 * <li> {@code attempts} the number of failed attempts made by the operation so far
 * <li> {@code constant} a user provided floating point constant
 * </ul>
 *
 * <p>The calculation may take any of these forms
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
 * TODO (expected): permit simple constant addition (e.g. p50+5ms)
 * TODO (required): track separate stats per-DC as inputs to these decisions
 */
public class TimeoutStrategy implements WaitStrategy
{
    static final Pattern PARSE = Pattern.compile(
                "(\\s*(?<min>0|[0-9]+[mu]?s)\\s*<=)?" +
                "(\\s*(?<wait>[^=]+))" +
                "(\\s*<=\\s*(?<max>0|[0-9]+[mu]?s))?");

    static final Pattern WAIT = Pattern.compile(
                "\\s*(?<const>0|[0-9]+[mu]?s)" +
                "|\\s*((p(?<perc>[0-9]+)(\\((?<rw>r|w|rw|wr)\\))?)?|(?<constbase>0|[0-9]+[mu]?s))" +
                    "\\s*(([*]\\s*(?<mod>[0-9.]+))?\\s*(?<modkind>[*^]\\s*attempts)?)?\\s*");
    static final Pattern TIME = Pattern.compile(
                "0|[0-9]+[mu]?s");

    // Factories can be useful for testing purposes, to supply custom implementations of selectors and modifiers.
    final static LatencyModifierFactory modifiers = new LatencyModifierFactory(){};

    interface LatencyModifierFactory
    {
        default LatencyModifier identity() { return (l, a) -> l; }
        default LatencyModifier multiply(double constant) { return (l, a) -> saturatedCast(l * constant); }
        default LatencyModifier multiplyByAttempts(double multiply) { return (l, a) -> saturatedCast(l * multiply * a); }
        default LatencyModifier multiplyByAttemptsExp(double base) { return (l, a) -> saturatedCast(l * pow(base, a)); }
    }

    public interface Wait
    {
        long getMicros(int attempts);

        class Constant implements Wait
        {
            final long micros;
            public Constant(long micros) { this.micros = micros; }
            @Override public long getMicros(int attempts) { return micros; }
        }

        class Modifying implements Wait
        {
            final LatencySupplier supplier;
            final LatencyModifier modifier;

            Modifying(LatencySupplier supplier, LatencyModifier modifier)
            {
                this.supplier = supplier;
                this.modifier = modifier;
            }

            @Override
            public long getMicros(int attempts)
            {
                return modifier.modify(supplier.getMicros(), attempts);
            }
        }
    }

    public interface LatencySupplier
    {
        long getMicros();

        class Constant implements LatencySupplier
        {
            final long micros;
            public Constant(long micros) {this.micros = micros; }
            @Override public long getMicros() { return micros; }
        }

        class Percentile implements LatencySupplier
        {
            final LatencySource latencies;
            final double percentile;

            public Percentile(LatencySource latencies, double percentile)
            {
                this.latencies = latencies;
                this.percentile = percentile;
            }

            @Override
            public long getMicros()
            {
                return latencies.get(percentile);
            }
        }
    }

    public interface LatencySource
    {
        long get(double percentile);
    }

    public interface LatencySourceFactory
    {
        LatencySource source(String params);

        static LatencySourceFactory rw(ClientRequestMetrics reads, ClientRequestMetrics writes)
        {
            return new ReadWriteLatencySourceFactory(reads, writes);
        }

        static LatencySourceFactory of(ClientRequestMetrics latencies)
        {
            LatencySource source = new TimeLimitedLatencySupplier(latencies.latency::getSnapshot, 10, SECONDS);
            return ignore -> source;
        }

        static LatencySourceFactory none()
        {
            return ignore -> ignore2 -> { throw new UnsupportedOperationException(); };
        }
    }

    public static class ReadWriteLatencySourceFactory implements LatencySourceFactory
    {
        final LatencySource reads, writes;

        public ReadWriteLatencySourceFactory(ClientRequestMetrics reads, ClientRequestMetrics writes)
        {
            this(reads.latency::getSnapshot, writes.latency::getSnapshot);
        }

        public ReadWriteLatencySourceFactory(Supplier<Snapshot> reads, Supplier<Snapshot> writes)
        {
            this.reads = new TimeLimitedLatencySupplier(reads, 10, SECONDS);
            this.writes = new TimeLimitedLatencySupplier(writes, 10, SECONDS);
        }

        @Override
        public LatencySource source(String rw)
        {
            if (rw.length() == 2)
                return percentile -> Math.max(reads.get(percentile), writes.get(percentile));
            else if ("r".equals(rw))
                return reads;
            else
                return writes;
        }
    }

    interface LatencyModifier
    {
        long modify(long latency, int attempts);
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

    static class TimeLimitedLatencySupplier extends AtomicReference<SnapshotAndTime> implements LatencySource
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

    final Wait wait;
    final long minMicros, maxMicros;

    public TimeoutStrategy(Wait wait, long minMicros, long maxMicros)
    {
        this.minMicros = minMicros;
        this.maxMicros = maxMicros;
        this.wait = wait;
    }

    public long computeWait(int attempts, TimeUnit units)
    {
        long wait = this.wait.getMicros(attempts);
        if (wait < minMicros) wait = minMicros;
        else if (wait > maxMicros) wait = maxMicros;
        return units.convert(wait, MICROSECONDS);
    }

    public long computeWaitUntil(int attempts)
    {
        long nanos = computeWait(attempts, NANOSECONDS);
        return nanoTime() + nanos;
    }

    private static LatencySupplier parseLatencySupplier(Matcher m, LatencySourceFactory latenciesFactory)
    {
        String perc = m.group("perc");
        if (perc == null)
            return new Constant(parseInMicros(m.group("constbase")));

        String rw = m.group("rw");
        if (rw == null) rw = "rw";
        LatencySource latencies = latenciesFactory.source(rw);
        double percentile = parseDouble("0." + perc);
        return new Percentile(latencies, percentile);
    }

    private static @Nullable LatencyModifier parseLatencyModifier(String spec, Matcher m, LatencyModifierFactory modifiers)
    {
        String mod = m.group("mod");
        String modkind = m.group("modkind");
        double modifier = 1.0;
        if (mod != null) modifier = Double.parseDouble(mod);
        else if (modkind == null) return null;
        else if (!modkind.startsWith("*"))
            throw new IllegalArgumentException("Invalid latency modifier specification: " + spec + ". Expect constant factor as base for exponent.");

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

    public static TimeoutStrategy parse(String input, LatencySourceFactory latencies)
    {
        Matcher m = PARSE.matcher(input);
        if (!m.matches())
            throw new IllegalArgumentException("Invalid specification: '" + input + "'; does not match " + PARSE);
        long min = parseInMicros(m.group("min"), 0);
        long max = parseInMicros(m.group("max"), Long.MAX_VALUE);
        Wait wait = TimeoutStrategy.parseWait(m.group("wait"), latencies);
        return new TimeoutStrategy(wait, min, max);
    }

    public static Wait parseWait(String input, LatencySourceFactory latencies)
    {
        return parseWait(input, latencies, modifiers);
    }

    @VisibleForTesting
    static Wait parseWait(String input, LatencySourceFactory latencies, LatencyModifierFactory modifiers)
    {
        Matcher m = WAIT.matcher(input);
        if (!m.matches())
            throw new IllegalArgumentException(input + " does not match " + WAIT);

        String maybeConst = m.group("const");
        if (maybeConst != null)
        {
            long v = parseInMicros(maybeConst);
            return new Wait.Constant(v);
        }

        LatencySupplier supplier = parseLatencySupplier(m, latencies);
        LatencyModifier modifier = parseLatencyModifier(input, m, modifiers);
        if (modifier == null && supplier instanceof LatencySupplier.Constant)
            return new Wait.Constant(((Constant) supplier).micros);
        if (modifier == null)
            modifier = modifiers.identity();
        return new Wait.Modifying(supplier, modifier);
    }

    public static long parseInMicros(String input, long orElse)
    {
        if (input == null)
            return orElse;

        return parseInMicros(input);
    }

    public static long parseInMicros(String text)
    {
        Matcher m = TIME.matcher(text);
        if (!m.matches())
            throw new IllegalArgumentException(text + " does not match " + TIME);

        if (text.length() == 1)
        {
            Invariants.require(text.charAt(0) == '0');
            return 0;
        }

        char penultimate = text.charAt(text.length() - 2);
        switch (penultimate)
        {
            default:  return parseInt(text.substring(0, text.length() - 1)) * 1000_000L;
            case 'm': return parseInt(text.substring(0, text.length() - 2)) * 1000L;
            case 'u': return parseInt(text.substring(0, text.length() - 2));
        }
    }

    private static String orElse(Supplier<String> get, String orElse)
    {
        String result = get.get();
        return result != null ? result : orElse;
    }

    @VisibleForTesting
    public static long maxQueryTimeoutMicros()
    {
        return max(max(getCasContentionTimeout(MICROSECONDS), getWriteRpcTimeout(MICROSECONDS)), getReadRpcTimeout(MICROSECONDS));
    }
}
