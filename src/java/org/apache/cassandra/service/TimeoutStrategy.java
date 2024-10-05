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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Snapshot;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.ClientRequestMetrics;
import org.apache.cassandra.utils.NoSpamLogger;

import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.pow;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
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
public class TimeoutStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(TimeoutStrategy.class);

    static final Pattern BOUND = Pattern.compile(
                "(?<const>0|[0-9]+[mu]s)" +
                "|((?<min>0|[0-9]+[mu]s) *<= *)?" +
                    "(p(?<perc>[0-9]+)(\\((?<rw>r|w|rw|wr)\\))?|(?<constbase>0|[0-9]+[mu]s))" +
                    "\\s*([*]\\s*(?<mod>[0-9.]+)?\\s*(?<modkind>[*^]\\s*attempts)?)?" +
                "( *<= *(?<max>0|[0-9]+[mu]s))?");
    static final Pattern TIME = Pattern.compile(
                "0|([0-9]+)ms|([0-9]+)us");

    // Factories can be useful for testing purposes, to supply custom implementations of selectors and modifiers.
    final static LatencySupplierFactory selectors = new LatencySupplierFactory(){};
    final static LatencyModifierFactory modifiers = new LatencyModifierFactory(){};

    interface LatencyModifierFactory
    {
        default LatencyModifier identity() { return (l, a) -> l; }
        default LatencyModifier multiply(double constant) { return (l, a) -> saturatedCast(l * constant); }
        default LatencyModifier multiplyByAttempts(double multiply) { return (l, a) -> saturatedCast(l * multiply * a); }
        default LatencyModifier multiplyByAttemptsExp(double base) { return (l, a) -> saturatedCast(l * pow(base, a)); }
    }

    interface LatencySupplier
    {
        long get();
    }

    public interface LatencySource
    {
        long get(double percentile);
    }

    interface LatencySupplierFactory
    {
        default LatencySupplier constant(long latency) { return () -> latency; }
        default LatencySupplier percentile(double percentile, LatencySource latencies) { return () -> latencies.get(percentile); }
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

    public static class Wait
    {
        final long min, max, onFailure;
        final LatencyModifier modifier;
        final LatencySupplier supplier;

        Wait(long min, long max, long onFailure, LatencyModifier modifier, LatencySupplier supplier)
        {
            Preconditions.checkArgument(min<=max, "min (%s) must be less than or equal to max (%s)", min, max);
            this.min = min;
            this.max = max;
            this.onFailure = onFailure;
            this.modifier = modifier;
            this.supplier = supplier;
        }

        long get(int attempts)
        {
            try
            {
                long base = supplier.get();
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
                   ", supplier=" + supplier +
                   '}';
        }
    }

    final Wait wait;

    public TimeoutStrategy(String spec, LatencySourceFactory latencies)
    {
        this.wait = parseWait(spec, latencies);
    }

    public long computeWait(int attempts, TimeUnit units)
    {
        return units.convert(wait.get(attempts), MICROSECONDS);
    }

    public long computeWaitUntil(int attempts)
    {
        long nanos = computeWait(attempts, NANOSECONDS);
        return nanoTime() + nanos;
    }

    protected Wait parseWait(String spec, LatencySourceFactory latencies)
    {
        long defaultMicros = DatabaseDescriptor.getRpcTimeout(MICROSECONDS);
        return parseWait(spec, 0, defaultMicros, defaultMicros, latencies);
    }

    private static LatencySupplier parseLatencySupplier(Matcher m, LatencySupplierFactory selectors, LatencySourceFactory latenciesFactory)
    {
        String perc = m.group("perc");
        if (perc == null)
            return selectors.constant(parseInMicros(m.group("constbase")));

        LatencySource latencies = latenciesFactory.source(m.group("rw"));
        double percentile = parseDouble("0." + perc);
        return selectors.percentile(percentile, latencies);
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

    public static Wait parseWait(String input, long defaultMin, long defaultMax, long onFailure, LatencySourceFactory latencies)
    {
        return parseWait(input, defaultMin, defaultMax, onFailure, latencies, selectors, modifiers);
    }

    @VisibleForTesting
    public static Wait parseWait(String input, long defaultMinMicros, long defaultMaxMicros, long onFailure, LatencySourceFactory latencies, LatencySupplierFactory selectors, LatencyModifierFactory modifiers)
    {
        Matcher m = BOUND.matcher(input);
        if (!m.matches())
            throw new IllegalArgumentException(input + " does not match " + BOUND);

        String maybeConst = m.group("const");
        if (maybeConst != null)
        {
            long v = parseInMicros(maybeConst);
            return new Wait(v, v, v, modifiers.identity(), selectors.constant(v));
        }

        long min = parseInMicros(m.group("min"), defaultMinMicros);
        long max = parseInMicros(m.group("max"), defaultMaxMicros);
        return new Wait(min, max, onFailure, parseLatencyModifier(m, modifiers), parseLatencySupplier(m, selectors, latencies));
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

    private static String orElse(Supplier<String> get, String orElse)
    {
        String result = get.get();
        return result != null ? result : orElse;
    }
}
