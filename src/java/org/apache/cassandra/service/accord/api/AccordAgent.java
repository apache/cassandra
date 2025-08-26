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

import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.CoordinatorEventListener;
import accord.api.LocalEventListener;
import accord.api.ProgressLog.BlockedUntil;
import accord.api.RoutingKey;
import accord.api.Tracing;
import accord.api.TraceEventType;
import accord.local.Command;
import accord.local.Node;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.TimeService;
import accord.messages.ReplyContext;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.Txn.Kind;
import accord.primitives.TxnId;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.utils.DefaultRandom;
import accord.utils.Invariants;
import accord.utils.RandomSource;
import accord.utils.SortedList;
import accord.utils.UnhandledEnum;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.metrics.AccordMetrics;
import org.apache.cassandra.net.ResponseContext;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.AccordTracing;
import org.apache.cassandra.service.accord.serializers.TableMetadatasAndKeys;
import org.apache.cassandra.service.accord.txn.TxnQuery;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static accord.primitives.Routable.Domain.Key;
import static accord.utils.SortedArrays.SortedArrayList.ofSorted;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.config.DatabaseDescriptor.getAccordScheduleDurabilityTxnIdLag;
import static org.apache.cassandra.config.DatabaseDescriptor.getReadRpcTimeout;
import static org.apache.cassandra.service.accord.api.AccordWaitStrategies.expireEpochWait;
import static org.apache.cassandra.service.accord.api.AccordWaitStrategies.fetch;
import static org.apache.cassandra.service.accord.api.AccordWaitStrategies.recover;
import static org.apache.cassandra.service.accord.api.AccordWaitStrategies.retryBootstrap;
import static org.apache.cassandra.service.accord.api.AccordWaitStrategies.retryDurability;
import static org.apache.cassandra.service.accord.api.AccordWaitStrategies.retrySyncPoint;
import static org.apache.cassandra.service.accord.api.AccordWaitStrategies.slowTxnPreaccept;
import static org.apache.cassandra.service.accord.api.AccordWaitStrategies.slowRead;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

// TODO (expected): merge with AccordService
public class AccordAgent implements Agent
{
    private static final Logger logger = LoggerFactory.getLogger(AccordAgent.class);

    private static BiConsumer<TxnId, Throwable> onFailedBarrier;
    public static void setOnFailedBarrier(BiConsumer<TxnId, Throwable> newOnFailedBarrier) { onFailedBarrier = newOnFailedBarrier; }
    public static void onFailedBarrier(TxnId txnId, Throwable cause)
    {
        BiConsumer<TxnId, Throwable> invoke = onFailedBarrier;
        if (invoke != null) invoke.accept(txnId, cause);
    }

    private final AccordTracing tracing = new AccordTracing();
    private final RandomSource random = new DefaultRandom();
    protected Node.Id self;

    public AccordAgent()
    {
    }

    public AccordTracing tracing()
    {
        return tracing;
    }

    @Override
    public @Nullable Tracing trace(TxnId txnId, TraceEventType eventType)
    {
        return tracing.trace(txnId, eventType);
    }

    public void setNodeId(Node.Id id)
    {
        self = id;
    }

    @Override
    public void onInconsistentTimestamp(Command command, Timestamp prev, Timestamp next)
    {
        // TODO (expected): better reporting
        AssertionError error = new AssertionError("Inconsistent execution timestamp detected for txnId " + command.txnId() + ": " + prev + " != " + next);
        onUncaughtException(error);
        throw error;
    }

    @Override
    public void onFailedBootstrap(int attempts, String phase, Ranges ranges, Runnable retry, Throwable failure)
    {
        logger.error("Failed bootstrap at {} for {}", phase, ranges, failure);
        AccordService.instance().scheduler().once(retry, retryBootstrap.computeWait(attempts, MICROSECONDS), MICROSECONDS);
    }

    @Override
    public void onStale(Timestamp staleSince, Ranges ranges)
    {
        logger.error("This replica has become stale for {} as of {}", ranges, staleSince);
    }

    @Override
    public void onUncaughtException(Throwable t)
    {
        handleUncaughtException(t);
    }

    public static void handleUncaughtException(Throwable t)
    {
        if (t instanceof RequestTimeoutException || t instanceof CancellationException)
            return;
        JVMStabilityInspector.uncaughtException(Thread.currentThread(), t);
    }

    @Override
    public void onCaughtException(Throwable t, String context)
    {
        logger.warn(context, t);
        JVMStabilityInspector.inspectThrowable(t);
    }

    @Override
    public Topologies selectPreferred(Node.Id from, Topologies to)
    {
        SortedList<Node.Id> nodes = to.nodes();
        int i = nodes.indexOf(from);
        Node.Id node = i <= 0 ? nodes.get(nodes.size() - 1) : to.nodes().get(i - 1);
        return to.select(ofSorted(node));
    }

    @Override
    public boolean rejectPreAccept(TimeService time, TxnId txnId)
    {
        return time.now() - getReadRpcTimeout(MICROSECONDS) > txnId.hlc();
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

    // TODO (expected): I don't think we even need this - just prune each time we have doubled in size
    @Override
    public long maxConflictsPruneInterval()
    {
        return 1024;
    }

    /**
     * Create an empty transaction that Accord can use for its internal transactions. This is not suitable
     * for tests since it skips validation done by regular transactions.
     */
    @Override
    public Txn emptySystemTxn(Kind kind, Routable.Domain domain)
    {
        return new Txn.InMemory(kind, (domain == Key ? Keys.EMPTY : Ranges.EMPTY), TxnRead.empty(domain), TxnQuery.UNSAFE_EMPTY, null, TableMetadatasAndKeys.none(domain));
    }

    @Override
    public CoordinatorEventListener coordinatorEvents()
    {
        return AccordMetrics.Listener.instance;
    }

    @Override
    public LocalEventListener localEvents()
    {
        return AccordMetrics.Listener.instance;
    }

    @Override
    public long slowCoordinatorDelay(Node node, SafeCommandStore safeStore, TxnId txnId, TimeUnit units, int retryCount)
    {
        SafeCommand safeCommand = safeStore.unsafeGetNoCleanup(txnId);
        Invariants.nonNull(safeCommand);

        Command command = safeCommand.current();
        Invariants.nonNull(command);

        RoutingKey homeKey = command.route().homeKey();
        Shard shard = node.topology().forEpochIfKnown(homeKey, command.txnId().epoch());

        // TODO (expected): make this a configurable calculation on normal request latencies (like ContentionStrategy)
        long nowMicros = MILLISECONDS.toMicros(Clock.Global.currentTimeMillis());
        long oneSecond = SECONDS.toMicros(1L);
        long promisedHlc = command.promised().hlc();
        if (promisedHlc > nowMicros + TimeUnit.MINUTES.toMicros(1))
            promisedHlc = 0;
        long mostRecentStart = Math.max(command.txnId().hlc(), promisedHlc);
        long waitMicros = recover(txnId).computeWait(retryCount, MICROSECONDS);
        if (mostRecentStart > nowMicros + SECONDS.toMicros(1L))
            logger.warn("max({},{})>{}", command.txnId(), command.promised(), nowMicros);
        long startTime = mostRecentStart + waitMicros;
        if (startTime < nowMicros)
            startTime = nowMicros + waitMicros/2;

        startTime = nonClashingStartTime(startTime, shard == null ? null : shard.nodes, node.id(), oneSecond, random);
        long delayMicros = Math.max(1, startTime - nowMicros);
        Invariants.require(delayMicros < TimeUnit.HOURS.toMicros(1L), "unexpectedly long coordination recovery delay proposed: %d (start %d, now %d)", delayMicros, startTime, nowMicros, command.txnId(), command.promised());
        return units.convert(delayMicros, MICROSECONDS);
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
    public long slowReplicaDelay(Node node, SafeCommandStore safeStore, TxnId txnId, int attempt, BlockedUntil blockedUntil, TimeUnit units)
    {
        return fetch(txnId).computeWait(attempt, units);
    }

    @Override
    public long slowAwaitDelay(Node node, SafeCommandStore safeStore, TxnId txnId, int attempt, BlockedUntil retrying, TimeUnit units)
    {
        // TODO (desired): separate config?
        return fetch(txnId).computeWait(attempt, units);
    }

    @Override
    public long retrySyncPointDelay(Node node, int attempt, TimeUnit units)
    {
        return retrySyncPoint.computeWait(attempt, units);
    }

    @Override
    public long retryDurabilityDelay(Node node, int attempt, TimeUnit units)
    {
        return retryDurability.computeWait(attempt, units);
    }

    @Override
    public long expireEpochWait(TimeUnit units)
    {
        return expireEpochWait.computeWait(1, units);
    }

    @Override
    public long expiresAt(ReplyContext replyContext, TimeUnit unit)
    {
        return unit.convert(((ResponseContext)replyContext).expiresAtNanos(), NANOSECONDS);
    }

    @Override
    public long selfSlowAt(TxnId txnId, Status.Phase phase, TimeUnit unit)
    {
        switch (phase)
        {
            default: throw new UnhandledEnum(phase);
            case PreAccept: return unit.convert(slowTxnPreaccept.computeWaitUntil(1), NANOSECONDS);
            case Execute:   return unit.convert(slowRead.computeWaitUntil(1), NANOSECONDS);
        }
    }

    @Override
    public long selfExpiresAt(TxnId txnId, Status.Phase phase, TimeUnit unit)
    {
        long delayNanos;
        switch (txnId.kind())
        {
            default: throw new UnhandledEnum(txnId.kind());
            case Write:
                delayNanos = DatabaseDescriptor.getWriteRpcTimeout(NANOSECONDS);
                break;
            case EphemeralRead:
            case Read:
                delayNanos = DatabaseDescriptor.getReadRpcTimeout(NANOSECONDS);
                break;
            case ExclusiveSyncPoint:
                delayNanos = DatabaseDescriptor.getAccordRangeSyncPointTimeoutNanos();
        }
        return unit.convert(nanoTime() + delayNanos, NANOSECONDS);
    }

    @Override
    public AsyncChain<TxnId> awaitStaleId(Node node, TxnId staleId, boolean isRequested)
    {
        long waitMicros = (staleId.hlc() + getAccordScheduleDurabilityTxnIdLag(MICROSECONDS)) - node.now();
        if (waitMicros <= 0)
            return AsyncChains.success(staleId);

        logger.debug("Waiting {} micros for {} to be stale", waitMicros, staleId);
        AsyncResult.Settable<TxnId> result = AsyncResults.settable();
        node.scheduler().once(() -> result.setSuccess(staleId), waitMicros, MICROSECONDS);
        return result;
    }

    @Override
    public long minStaleHlc(Node node, boolean requested)
    {
        return node.now() - (100 + getAccordScheduleDurabilityTxnIdLag(MICROSECONDS));
    }
}
