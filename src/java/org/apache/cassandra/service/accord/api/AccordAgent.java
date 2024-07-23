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

package org.apache.cassandra.service.accord.api;

import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.EventsListener;
import accord.api.ProgressLog.BlockedUntil;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.local.Command;
import accord.local.Node;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.messages.ReplyContext;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.Txn.Kind;
import accord.primitives.TxnId;
import accord.topology.Shard;
import accord.utils.DefaultRandom;
import accord.utils.Invariants;
import accord.utils.RandomSource;
import accord.utils.SortedList;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.AccordMetrics;
import org.apache.cassandra.net.ResponseContext;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.txn.TxnKeyRead;
import org.apache.cassandra.service.accord.txn.TxnQuery;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static accord.primitives.Routable.Domain.Key;
import static accord.utils.Invariants.illegalState;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.config.DatabaseDescriptor.getReadRpcTimeout;

// TODO (expected): merge with AccordService
public class AccordAgent implements Agent
{
    private static final Logger logger = LoggerFactory.getLogger(AccordAgent.class);

    protected Node.Id self;

    // TODO (required): this should be configurable and have exponential back-off, escaping to operator input past a certain number of retries
    private long retryBootstrapDelayMicros = SECONDS.toMicros(1L);
    private final RandomSource random = new DefaultRandom();

    public void setNodeId(Node.Id id)
    {
        self = id;
    }

    public void setRetryBootstrapDelay(long delay, TimeUnit units)
    {
        retryBootstrapDelayMicros = units.toMicros(delay);
    }

    @Override
    public void onRecover(Node node, Result success, Throwable fail)
    {
        // TODO: this
    }

    @Override
    public void onInconsistentTimestamp(Command command, Timestamp prev, Timestamp next)
    {
        // TODO: this
        AssertionError error = new AssertionError("Inconsistent execution timestamp detected for txnId " + command.txnId() + ": " + prev + " != " + next);
        onUncaughtException(error);
        throw error;
    }

    public void onFailedBarrier(TxnId id, Seekables<?, ?> keysOrRanges, Throwable cause)
    {

    }

    @Override
    public void onFailedBootstrap(String phase, Ranges ranges, Runnable retry, Throwable failure)
    {
        logger.error("Failed bootstrap at {} for {}", phase, ranges, failure);
        AccordService.instance().scheduler().once(retry, retryBootstrapDelayMicros, MICROSECONDS);
    }

    @Override
    public void onStale(Timestamp staleSince, Ranges ranges)
    {
        // TODO (required): decide how to handle this - maybe do nothing besides log? Maybe configurably try some number of repair attempts to catch up.
    }

    @Override
    public void onUncaughtException(Throwable t)
    {
        logger.error("Uncaught accord exception", t);
        JVMStabilityInspector.uncaughtException(Thread.currentThread(), t);
    }

    @Override
    public void onHandledException(Throwable t, String context)
    {
        logger.warn(context, t);
        JVMStabilityInspector.uncaughtException(Thread.currentThread(), t);
    }

    @Override
    public long preAcceptTimeout()
    {
        // TODO: should distinguish between reads and writes (Aleksey: why? and why read rpc timeout is being used?)
        return getReadRpcTimeout(MICROSECONDS);
    }

    // TODO (expected): we probably want additional configuration here so we can prune on shorter time horizons when we have a lot of transactions on a single key
    @Override
    public long cfkHlcPruneDelta()
    {
        return SECONDS.toMicros(10L);
    }

    @Override
    public int cfkPruneInterval()
    {
        return 32;
    }

    // TODO (expected): we probably want additional configuration here
    @Override
    public long maxConflictsHlcPruneDelta()
    {
        return SECONDS.toMicros(1);
    }

    @Override
    public long maxConflictsPruneInterval()
    {
        return 100;
    }

    /**
     * Create an empty transaction that Accord can use for its internal transactions. This is not suitable
     * for tests since it skips validation done by regular transactions.
     */
    @Override
    public Txn emptySystemTxn(Kind kind, Routable.Domain domain)
    {
        return new Txn.InMemory(kind, domain == Key ? Keys.EMPTY : Ranges.EMPTY, TxnKeyRead.EMPTY, TxnQuery.UNSAFE_EMPTY, null);
    }

    @Override
    public EventsListener metricsEventsListener()
    {
        return AccordMetrics.Listener.instance;
    }

    @Override
    public long attemptCoordinationDelay(Node node, SafeCommandStore safeStore, TxnId txnId, TimeUnit units, int retryCount)
    {
        SafeCommand safeCommand = safeStore.unsafeGetNoCleanup(txnId);
        Invariants.nonNull(safeCommand);

        Command command = safeCommand.current();
        Invariants.nonNull(command);

        Timestamp mostRecentAttempt = Timestamp.max(command.txnId(), command.promised());
        RoutingKey homeKey = command.route().homeKey();
        Shard shard = node.topology().forEpochIfKnown(homeKey, command.txnId().epoch());

        // TODO (expected): make this a configurable calculation on normal request latencies (like ContentionStrategy)
        long oneSecond = SECONDS.toMicros(1L);
        long startTime = mostRecentAttempt.hlc() + DatabaseDescriptor.getAccord().recoveryDelayFor(txnId, MICROSECONDS)
                         + (retryCount == 0 ? 0 : random.nextLong(oneSecond << Math.min(retryCount, 4)));

        startTime = nonClashingStartTime(startTime, shard == null ? null : shard.nodes, node.id(), oneSecond, random);
        long nowMicros = MILLISECONDS.toMicros(Clock.Global.currentTimeMillis());
        return units.convert(Math.max(1, startTime - nowMicros), MICROSECONDS);
    }

    @VisibleForTesting
    public static long nonClashingStartTime(long startTime, SortedList<Node.Id> nodes, Node.Id id, long granularity, RandomSource random)
    {
        long perSecondStartTime;
        if (nodes != null)
        {
            int position = nodes.indexOf(id);
            perSecondStartTime = position * (SECONDS.toMicros(1) / nodes.size());
        }
        else
        {
            // we've raced with topology update, this should be rare so just pick a random start time
            perSecondStartTime = random.nextLong(granularity);
        }

        // TODO (expected): make this a configurable calculation on normal request latencies (like ContentionStrategy)
        long subSecondRemainder = startTime % granularity;
        long newStartTime = startTime - subSecondRemainder + perSecondStartTime;
        if (newStartTime < startTime)
            newStartTime += granularity;
        return newStartTime;
    }

    @Override
    public long seekProgressDelay(Node node, SafeCommandStore safeStore, TxnId txnId, int retryCount, BlockedUntil blockedUntil, TimeUnit units)
    {
        // TODO (required): make this configurable and dependent upon normal request latencies, and perhaps offset from txnId.hlc()
        return units.convert((1L << Math.min(retryCount, 4)), SECONDS);
    }

    @Override
    public long retryAwaitTimeout(Node node, SafeCommandStore safeStore, TxnId txnId, int retryCount, BlockedUntil retrying, TimeUnit units)
    {
        // TODO (expected): integrate with contention backoff
        return units.convert((1L << Math.min(retryCount, 4)), SECONDS);
    }

    @Override
    public long expiresAt(ReplyContext replyContext, TimeUnit unit)
    {
        return unit.convert(((ResponseContext)replyContext).expiresAtNanos(), NANOSECONDS);
    }

    @Override
    public void onViolation(String message)
    {
        try { throw illegalState(message); }
        catch (Throwable t) { logger.error("Consistency violation", t); }
    }
}
