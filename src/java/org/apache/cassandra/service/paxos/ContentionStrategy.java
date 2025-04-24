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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.RetryStrategy;
import org.apache.cassandra.service.TimeoutStrategy;
import org.apache.cassandra.service.TimeoutStrategy.LatencySourceFactory;
import org.apache.cassandra.service.TimeoutStrategy.ReadWriteLatencySourceFactory;
import org.apache.cassandra.service.TimeoutStrategy.Wait;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.NoSpamLogger;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Arrays.stream;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.cassandra.config.DatabaseDescriptor.*;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.casReadMetrics;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.casWriteMetrics;
import static org.apache.cassandra.service.TimeoutStrategy.parseInMicros;
import static org.apache.cassandra.utils.Clock.waitUntil;
import static org.apache.cassandra.utils.LocalizeString.toLowerCaseLocalized;

/**
 * See {@link RetryStrategy}
 * TODO (expected): deprecate in favour of pure RetryStrategy
 */
public class ContentionStrategy extends RetryStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(RetryStrategy.class);

    public enum Type
    {
        READ("Contended Paxos Read"), WRITE("Contended Paxos Write"), REPAIR("Contended Paxos Repair");

        final String traceTitle;
        final String lowercase;

        Type(String traceTitle)
        {
            this.traceTitle = traceTitle;
            this.lowercase = toLowerCaseLocalized(name());
        }
    }

    static final Pattern LEGACY = Pattern.compile(
    "(?<const>0|[0-9]+[mu]?s)" +
            "|((?<min>0|[0-9]+[mu]?s) *<= *)?" +
                "(?<delegate>[^=]+)?" +
            "( *<= *(?<max>0|[0-9]+[mu]?s))?");

    private static final String DEFAULT_WAIT_RANDOMIZER = "uniform";
    private static final String DEFAULT_MIN = "0";
    private static final String DEFAULT_MAX = "100ms";
    private static final String DEFAULT_SPREAD = "100ms";
    private static final String MAX_INT = "" + Integer.MAX_VALUE;
    private static final LatencySourceFactory LATENCIES = new ReadWriteLatencySourceFactory(casReadMetrics, casWriteMetrics);

    private static volatile ContentionStrategy current;
    private static volatile ParsedStrategy currentParsed;
    static
    {
        String waitRandomizer = orElse(DatabaseDescriptor::getPaxosContentionWaitRandomizer, DEFAULT_WAIT_RANDOMIZER);
        String min = orElse(DatabaseDescriptor::getPaxosContentionMinWait, DEFAULT_MIN);
        String max = orElse(DatabaseDescriptor::getPaxosContentionMaxWait, DEFAULT_MAX);
        String spread = orElse(DatabaseDescriptor::getPaxosContentionMinDelta, DEFAULT_SPREAD);
        current = parse(waitRandomizer, min, max, spread, MAX_INT, MAX_INT);
        currentParsed = new ParsedStrategy(waitRandomizer, min, max, spread, MAX_INT, MAX_INT, current);
    }

    final @Nullable LegacyWait spread;
    final int traceAfterAttempts;

    public ContentionStrategy(WaitRandomizer waitRandomizer, LegacyWait min, LegacyWait max, LegacyWait spread, int retries, int traceAfterAttempts)
    {
        super(waitRandomizer, min.min, min, min.max, max, max.max, retries);
        this.traceAfterAttempts = traceAfterAttempts;
        this.spread = spread;
    }

    public long computeWait(int attempt, TimeUnit units)
    {
        if (attempt > maxAttempts)
            return -1;

        long minWaitMicros = min.getMicros(attempt);
        long maxWaitMicros = max.getMicros(attempt);
        long spreadMicros = spread == null ? 0 : spread.getMicros(attempt);

        if (minWaitMicros + spreadMicros >= maxWaitMicros)
        {
            if (spreadMicros == 0)
                return units.convert(maxWaitMicros, MICROSECONDS);

            if (maxWaitMicros < minWaitMicros)
                maxWaitMicros = minWaitMicros;
            long newMaxWaitMicros = minWaitMicros + spreadMicros;
            if (newMaxWaitMicros > maxMaxMicros)
            {
                newMaxWaitMicros = maxMaxMicros;
                minWaitMicros = max(this.minMinMicros, maxWaitMicros - spreadMicros);
            }
            maxWaitMicros = newMaxWaitMicros;
            if (minWaitMicros >= maxWaitMicros)
                return minWaitMicros;
        }

        return units.convert(waitRandomizer.wait(minWaitMicros, maxWaitMicros, attempt), MICROSECONDS);
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

        return super.computeWaitUntil(attempts);
    }

    public boolean doWaitForContention(long deadline, int attempts, TableMetadata table, DecoratedKey partitionKey, ConsistencyLevel consistency, Type type)
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

    public static class ParsedStrategy
    {
        public final String waitRandomizer, min, max, spread, retries, trace;
        public final ContentionStrategy strategy;

        protected ParsedStrategy(String waitRandomizer, String min, String max, String spread, String retries, String trace, ContentionStrategy strategy)
        {
            this.waitRandomizer = waitRandomizer;
            this.min = min;
            this.max = max;
            this.spread = spread;
            this.retries = retries;
            this.trace = trace;
            this.strategy = strategy;
        }

        public String toString()
        {
            return "min=" + min + ",max=" + max + ",spread=" + spread + ",retries=" + retries
                   + ",random=" + waitRandomizer + ",trace=" + current.traceAfterAttempts;
        }
    }

    @VisibleForTesting
    public static ParsedStrategy parseStrategy(String spec, ParsedStrategy defaultStrategy)
    {
        String[] args = spec.split(",");
        String waitRandomizer = find(args, "random");
        String min = find(args, "min");
        String max = find(args, "max");
        String spread = find(args, "spread");
        String trace = find(args, "trace");
        if (spread == null)
            spread = find(args, "delta");
        String retries = find(args, "retries");

        if (waitRandomizer == null) waitRandomizer = defaultStrategy.waitRandomizer;
        if (min == null) min = defaultStrategy.min;
        if (max == null) max = defaultStrategy.max;
        if (spread == null) spread = defaultStrategy.spread;
        if (retries == null) retries = defaultStrategy.retries;
        if (trace == null) trace = defaultStrategy.trace;

        ContentionStrategy strategy = parse(waitRandomizer, min, max, spread, retries, trace);
        return new ParsedStrategy(waitRandomizer, min, max, spread, retries, trace, strategy);
    }

    private static ContentionStrategy parse(String waitRandomizerString, String minString, String maxString, String spreadString, String retriesString, String traceString)
    {
        return new ContentionStrategy(parseWaitRandomizer(waitRandomizerString),
                                      parseLegacy(minString, true), parseLegacy(maxString, false), parseLegacy(spreadString, false),
                                      Integer.parseInt(retriesString), Integer.parseInt(traceString));
    }

    private static String find(String[] args, String param)
    {
        return stream(args).filter(s -> s.startsWith(param + '='))
                           .map(s -> s.substring(param.length() + 1))
                           .findFirst().orElse(null);
    }


    public static synchronized void setStrategy(String spec)
    {
        ParsedStrategy parsed = parseStrategy(spec, currentParsed);
        currentParsed = parsed;
        current = parsed.strategy;
        setPaxosContentionWaitRandomizer(parsed.waitRandomizer);
        setPaxosContentionMinWait(parsed.min);
        setPaxosContentionMaxWait(parsed.max);
        setPaxosContentionMinDelta(parsed.spread);
    }

    public static String getStrategySpec()
    {
        return currentParsed.toString();
    }

    private static String orElse(Supplier<String> get, String orElse)
    {
        String result = get.get();
        return result != null ? result : orElse;
    }

    @VisibleForTesting
    static LegacyWait parseLegacy(String spec, boolean isMin)
    {
        long defaultMaxMicros = getRpcTimeout(MICROSECONDS);
        return parseLegacy(spec, 0, defaultMaxMicros, isMin ? 0 : defaultMaxMicros, LATENCIES);
    }

    public static LegacyWait parseLegacy(String input, long defaultMinMicros, long defaultMaxMicros, long onFailure, LatencySourceFactory latencies)
    {
        Matcher m = LEGACY.matcher(input);
        if (!m.matches())
            throw new IllegalArgumentException(input + " does not match " + LEGACY);

        String maybeConst = m.group("const");
        if (maybeConst != null)
        {
            long v = parseInMicros(maybeConst);
            return new LegacyWait(v, v, v, new Wait.Constant(v));
        }

        long min = parseInMicros(m.group("min"), defaultMinMicros);
        long max = parseInMicros(m.group("max"), defaultMaxMicros);
        return new LegacyWait(min, max, onFailure, TimeoutStrategy.parseWait(m.group("delegate"), latencies));
    }


    private static class LegacyWait implements Wait
    {
        final long min, max, onFailure;
        final Wait delegate;

        LegacyWait(long min, long max, long onFailure, Wait delegate)
        {
            Preconditions.checkArgument(min <= max, "min (%s) must be less than or equal to max (%s)", min, max);
            this.min = min;
            this.max = max;
            this.onFailure = onFailure;
            this.delegate = delegate;
        }

        public long getMicros(int attempts)
        {
            try
            {
                return max(min, min(max, delegate.getMicros(attempts)));
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
                   ", delegate=" + delegate +
                   '}';
        }
    }

}
