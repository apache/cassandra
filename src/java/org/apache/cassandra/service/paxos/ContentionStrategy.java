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

import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Math.*;
import static java.util.concurrent.TimeUnit.*;
import static org.apache.cassandra.config.DatabaseDescriptor.*;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.casReadMetrics;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.casWriteMetrics;
import static org.apache.cassandra.utils.Clock.waitUntil;

/**
 * See {@link RetryStrategy}
 */
public class ContentionStrategy extends RetryStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(RetryStrategy.class);

    private static final String DEFAULT_WAIT_RANDOMIZER = "uniform";
    private static final String DEFAULT_MIN = "0";
    private static final String DEFAULT_MAX = "100ms";
    private static final String DEFAULT_SPREAD = "100ms";
    private static final LatencySourceFactory LATENCIES = new ReadWriteLatencySourceFactory(casReadMetrics, casWriteMetrics);

    private static volatile ContentionStrategy current;
    private static volatile ParsedStrategy currentParsed;
    private static final RetryStrategy.ParsedStrategy defaultStrategy;
    static
    {
        defaultStrategy = new ParsedStrategy(DEFAULT_WAIT_RANDOMIZER, DEFAULT_MIN, DEFAULT_MAX, DEFAULT_SPREAD, Integer.MAX_VALUE,
                                             new ContentionStrategy(DEFAULT_WAIT_RANDOMIZER, DEFAULT_MIN, DEFAULT_MAX, DEFAULT_SPREAD, Integer.MAX_VALUE));

        String waitRandomizer = orElse(DatabaseDescriptor::getPaxosContentionWaitRandomizer, DEFAULT_WAIT_RANDOMIZER);
        String min = orElse(DatabaseDescriptor::getPaxosContentionMinWait, DEFAULT_MIN);
        String max = orElse(DatabaseDescriptor::getPaxosContentionMaxWait, DEFAULT_MAX);
        String spread = orElse(DatabaseDescriptor::getPaxosContentionMinDelta, DEFAULT_SPREAD);

        current = new ContentionStrategy(waitRandomizer, min, max, spread, Integer.MAX_VALUE);
        currentParsed = new ParsedStrategy(waitRandomizer, min, max, spread, Integer.MAX_VALUE, current);
    }

    final int traceAfterAttempts;

    public ContentionStrategy(String waitRandomizer, String min, String max, String spread, int traceAfterAttempts)
    {
        super(waitRandomizer, min, max, spread, LATENCIES);
        this.traceAfterAttempts = traceAfterAttempts;
    }

    public ContentionStrategy(WaitRandomizer waitRandomizer, Wait min, Wait max, Wait spread, int traceAfterAttempts)
    {
        super(waitRandomizer, min, max, spread);
        this.traceAfterAttempts = traceAfterAttempts;
    }

    @Override
    protected Wait parseBound(String spec, boolean isMin, LatencySourceFactory latencies)
    {
        return TimeoutStrategy.parseWait(spec, 0, maxQueryTimeoutMicros(), isMin ? 0 : maxQueryTimeoutMicros(), latencies);
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

    public static class ParsedStrategy extends RetryStrategy.ParsedStrategy
    {
        public final int trace;
        public final ContentionStrategy strategy;

        ParsedStrategy(String waitRandomizer, String min, String max, String minDelta, int trace, ContentionStrategy strategy)
        {
            super(waitRandomizer, min, max, minDelta, strategy);
            this.trace = trace;
            this.strategy = strategy;
        }

        @Override
        public String toString()
        {
            return super.toString() + (trace == Integer.MAX_VALUE ? "" : ",trace=" + current.traceAfterAttempts);
        }
    }

    @VisibleForTesting
    public static ParsedStrategy parseStrategy(String spec)
    {
        RetryStrategy.ParsedStrategy parsed = RetryStrategy.parseStrategy(spec, LATENCIES, defaultStrategy);
        String[] args = spec.split(",");

        String trace = find(args, "trace");
        int traceAfterAttempts = trace == null ? current.traceAfterAttempts: Integer.parseInt(trace);

        ContentionStrategy strategy = new ContentionStrategy(parsed.strategy.waitRandomizer, parsed.strategy.min, parsed.strategy.max, parsed.strategy.spread, traceAfterAttempts);
        return new ParsedStrategy(parsed.waitRandomizer, parsed.min, parsed.max, parsed.spread, traceAfterAttempts, strategy);
    }

    public static synchronized void setStrategy(String spec)
    {
        ParsedStrategy parsed = parseStrategy(spec);
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
