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
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.CoordinatorEventListener;
import accord.api.OwnershipEventListener;
import accord.api.ProgressLog.BlockedUntil;
import accord.api.ReplicaEventListener;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.api.Tracing;
import accord.coordinate.Coordination;
import accord.coordinate.Exhausted;
import accord.coordinate.Preempted;
import accord.coordinate.Timeout;
import accord.local.Command;
import accord.local.LogUnavailableException;
import accord.local.Node;
import accord.local.SafeCommand;
import accord.local.SafeCommandStore;
import accord.local.TimeService;
import accord.messages.MessageType;
import accord.primitives.Keys;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Routable;
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
import accord.utils.async.Cancellable;

import org.apache.cassandra.config.AccordConfig;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.metrics.AccordReplicaMetrics;
import org.apache.cassandra.metrics.AccordSystemMetrics;
import org.apache.cassandra.service.RetryStrategy;
import org.apache.cassandra.service.accord.AccordService;
import org.apache.cassandra.service.accord.debug.AccordTracing;
import org.apache.cassandra.service.accord.serializers.TableMetadatasAndKeys;
import org.apache.cassandra.service.accord.txn.TxnQuery;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.service.accord.txn.TxnResult;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.NoSpamLogger;

import static accord.primitives.Routable.Domain.Key;
import static accord.utils.SortedArrays.SortedArrayList.ofSorted;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.config.DatabaseDescriptor.getAccordScheduleDurabilityTxnIdLag;
import static org.apache.cassandra.config.DatabaseDescriptor.getReadRpcTimeout;
import static org.apache.cassandra.service.accord.api.AccordWaitStrategies.expire;
import static org.apache.cassandra.service.accord.api.AccordWaitStrategies.expireEpochWait;
import static org.apache.cassandra.service.accord.api.AccordWaitStrategies.expireSyncPoint;
import static org.apache.cassandra.service.accord.api.AccordWaitStrategies.expireTxn;
import static org.apache.cassandra.service.accord.api.AccordWaitStrategies.fetch;
import static org.apache.cassandra.service.accord.api.AccordWaitStrategies.recover;
import static org.apache.cassandra.service.accord.api.AccordWaitStrategies.retryBootstrap;
import static org.apache.cassandra.service.accord.api.AccordWaitStrategies.retryDurability;
import static org.apache.cassandra.service.accord.api.AccordWaitStrategies.retryFetchTopology;
import static org.apache.cassandra.service.accord.api.AccordWaitStrategies.retryJoinBootstrap;
import static org.apache.cassandra.service.accord.api.AccordWaitStrategies.retrySyncPoint;
import static org.apache.cassandra.service.accord.api.AccordWaitStrategies.slowRead;
import static org.apache.cassandra.service.accord.api.AccordWaitStrategies.slowTxnPreaccept;
import static org.apache.cassandra.service.accord.txn.TxnResult.Kind.txn_data;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

// TODO (expected): merge with AccordService
public class AccordAgent implements Agent, OwnershipEventListener
{
    private static final Logger logger = LoggerFactory.getLogger(AccordAgent.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1L, MINUTES);
    private static final ReplicaEventListener replicaEventListener = new AccordReplicaMetrics.Listener();

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
    protected AccordConfig config;

    public AccordAgent()
    {
    }

    public AccordTracing tracing()
    {
        return tracing;
    }

    @Override
    public @Nullable Tracing trace(TxnId txnId, Participants<?> participants, Coordination.CoordinationKind eventType)
    {
        return tracing.trace(txnId, participants, eventType);
    }

    @Override
    public OwnershipEventListener ownershipEvents()
    {
        return this;
    }

    public void setup(Node.Id id)
    {
        self = id;
        config = DatabaseDescriptor.getAccord();
    }

    @Override
    public void onFailedBootstrap(int attempts, String phase, Ranges ranges, Runnable retry, Runnable fail, Throwable failure)
    {
        RetryStrategy strategy;
        String message;
        SystemKeyspace.BootstrapState bootstrapState = SystemKeyspace.getBootstrapState();
        switch (bootstrapState)
        {
            default: throw new UnhandledEnum(bootstrapState);
            case IN_PROGRESS:
            case NEEDS_BOOTSTRAP:
                message = "Failed bootstrap (for joining) at {} for {}{}";
                strategy = retryJoinBootstrap;
                break;
            case COMPLETED:
            case DECOMMISSIONED:
                message = "Failed bootstrap at {} for {}{}";
                strategy = retryBootstrap;
                break;
        }
        long retryDelayMicros = strategy.computeWait(attempts, MICROSECONDS);
        if (retryDelayMicros < 0)
        {
            if (strategy == retryJoinBootstrap)
            {
                logger.error(message, phase, ranges, ". Retry strategy giving up. Not yet joined, so failing bootstrap.", failure);
                fail.run();
            }
            else
            {
                // TODO (expected): we should be able to resume these without restarting (but for now we just shouldn't configure a retry limit)
                // failing would prevent the node processing all epochs (as this feeds into the epoch readiness), so we just drop in this case
                logger.error(message, phase, ranges, ". Retry strategy giving up. To resume you will need to restart.", failure);
            }
        }
        else
        {
            logger.error(message, phase, ranges, ". Retrying in " + retryDelayMicros + "us.", failure);
            AccordService.unsafeInstance().scheduler().once(() -> {
                logger.info("Retrying bootstrap of {}", ranges);
                retry.run();
            }, retryDelayMicros, MICROSECONDS);
        }
    }

    @Override
    public void onStale(Timestamp staleSince, Ranges ranges)
    {
        logger.error("This replica has become stale for {} as of {}", ranges, staleSince);
    }

    public static void handleException(Throwable t)
    {
        if (t instanceof RequestTimeoutException)
            return;

        AccordSystemMetrics.metrics.errors.inc();
        if (t instanceof CancellationException || t instanceof TimeoutException || t instanceof Timeout || t instanceof Preempted || t instanceof Exhausted || t instanceof LogUnavailableException)
            // TODO (required): leaky logger, permitting multiple messages per time period and reporting how many were dropped
            noSpamLogger.warn("", t);
        else
            JVMStabilityInspector.uncaughtException(Thread.currentThread(), t);
    }

    @Override
    public void onException(Throwable t)
    {
        handleException(t);
    }

    @Override
    public void onException(Throwable t, String context)
    {
        handleException(t);
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
        return config.commands_for_key_prune_delta.to(MICROSECONDS);
    }

    @Override
    public int cfkPruneInterval()
    {
        return config.commands_for_key_prune_interval;
    }

    @Override
    public long maxConflictsHlcPruneDelta()
    {
        return config.max_conflicts_prune_delta.to(MICROSECONDS);
    }

    @Override
    public boolean softReject(long unappliedCount, long maxUnappliedAge, long cumulativeUnappliedAge)
    {
        return unappliedCount > config.min_soft_reject_count
               && (unappliedCount > config.max_soft_reject_count
                || maxUnappliedAge > config.soft_reject_age.toMicroseconds()
                || cumulativeUnappliedAge > config.soft_reject_cumulative_age.toMicroseconds());
    }

    @Override
    public boolean hardReject(int softRejectCount, int totalCount)
    {
        return (softRejectCount / (float) totalCount) >= config.hard_reject_ratio;
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
        return tracing;
    }

    @Override
    public ReplicaEventListener replicaEvents()
    {
        return replicaEventListener;
    }

    private static final long ONE_SECOND = SECONDS.toMicros(1L);
    private static final long ONE_MINUTE = MINUTES.toMicros(1L);

    @Override
    public long slowCoordinatorDelay(Node node, SafeCommandStore safeStore, TxnId txnId, TimeUnit units, int attempt)
    {
        SafeCommand safeCommand = safeStore.unsafeGetNoCleanup(txnId);
        if (safeCommand == null)
        {
            noSpamLogger.warn("{} invoked slowCoordinatorDelay for {} without having it in cache", safeStore.commandStore(), txnId, new RuntimeException());
            return recover(txnId).computeWait(attempt, units);
        }

        Command command = safeCommand.current();
        if (command == null)
        {
            noSpamLogger.warn("{} invoked slowCoordinatorDelay for {} without knowing the command", safeStore.commandStore(), txnId, new RuntimeException());
            return recover(txnId).computeWait(attempt, units);
        }


        // TODO (expected): make this a configurable calculation on normal request latencies (like ContentionStrategy)
        long nowMicros = MILLISECONDS.toMicros(Clock.Global.currentTimeMillis());
        long mostRecentStart = mostRecentStart(command, nowMicros);
        long waitMicros = recover(txnId).computeWait(attempt, MICROSECONDS);
        long startTime = mostRecentStart + waitMicros;
        if (startTime < nowMicros)
        {
            // TODO (expected): support no waiting here
            if (attempt == 1)
                return 1;

            startTime = nowMicros + waitMicros/2;
        }

        RoutingKey homeKey = command.route().homeKey();
        Shard shard = node.topology().active().forEpochIfKnown(homeKey, command.txnId().epoch());

        startTime = nonClashingStartTime(startTime, shard == null ? null : shard.nodes, node.id(), ONE_SECOND, random);
        long delayMicros = Math.max(1, startTime - nowMicros);
        Invariants.require(delayMicros < TimeUnit.HOURS.toMicros(1L), "unexpectedly long coordination recovery delay proposed: %d (start %d, now %d)", delayMicros, startTime, nowMicros, command.txnId(), command.promised());
        return units.convert(delayMicros, MICROSECONDS);
    }

    private static long mostRecentStart(Command command, long nowMicros)
    {
        // TODO (expected): make this a configurable calculation on normal request latencies (like ContentionStrategy)
        long promisedHlc = command.promised().hlc();
        if (promisedHlc > nowMicros + ONE_MINUTE)
            promisedHlc = 0;
        long result = Math.max(command.txnId().hlc(), promisedHlc);
        if (result > nowMicros + ONE_SECOND)
            noSpamLogger.warn("max({},{})>{}", command.txnId(), command.promised(), nowMicros);
        return result;
    }

    @Override
    public boolean isSlowCoordinator(long elapsed, TimeUnit units, TxnId txnId, int attempt)
    {
        long maxWait = recover(txnId).computeMaxWait(attempt, units);
        return elapsed >= maxWait;
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
        SafeCommand safeCommand = safeStore.unsafeGetNoCleanup(txnId);
        if (safeCommand == null)
        {
            noSpamLogger.warn("{} invoked slowReplicaDelay for {} without having it in cache", safeStore.commandStore(), txnId, new RuntimeException());
            return fetch(txnId).computeWait(attempt, units);
        }

        Command command = safeCommand.current();
        if (command == null)
        {
            noSpamLogger.warn("{} invoked slowReplicaDelay for {} without knowing the command", safeStore.commandStore(), txnId, new RuntimeException());
            return fetch(txnId).computeWait(attempt, units);
        }

        long nowMicros = MILLISECONDS.toMicros(Clock.Global.currentTimeMillis());
        long mostRecentStart = mostRecentStart(command, nowMicros);
        long waitMicros = fetch(txnId).computeWait(attempt, units);
        long startTime = mostRecentStart + waitMicros;
        if (startTime < nowMicros)
        {
            // TODO (expected): support no waiting here
            if (attempt == 1) return 1;
            else return waitMicros/2;
        }
        return waitMicros;
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
    public long retryTopologyDelay(Node node, int attempt, TimeUnit units)
    {
        return retryFetchTopology.computeWait(attempt, units);
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
    public long selfSlowAt(TxnId txnId, MessageType type, TimeUnit unit)
    {
        if (type.getClass() == MessageType.StandardMessage.class)
        {
            switch ((MessageType.StandardMessage)type)
            {
                case PRE_ACCEPT_REQ:
                    return unit.convert(slowTxnPreaccept.computeWaitUntil(1), unit);
                case READ_EPHEMERAL_REQ:
                case READ_REQ:
                case STABLE_THEN_READ_REQ:
                    return unit.convert(slowRead.computeWaitUntil(1), NANOSECONDS);
            }
        }
        return -1;
    }

    @Override
    public long selfExpiresAt(TxnId txnId, MessageType type, TimeUnit unit)
    {
        return unit.convert((txnId.isSyncPoint() ? expireSyncPoint : expireTxn).computeWaitUntil(1), NANOSECONDS);
    }

    @Override
    public AsyncChain<TxnId> awaitStaleId(Node node, TxnId staleId, boolean isRequested)
    {
        long waitMicros = (staleId.hlc() + getAccordScheduleDurabilityTxnIdLag(MICROSECONDS)) - node.now();
        if (waitMicros <= 0)
            return AsyncChains.success(staleId);

        logger.debug("Waiting {} micros for {} to be stale", waitMicros, staleId);
        return new AsyncChains.Head<>()
        {
            @Override
            protected @Nullable Cancellable start(BiConsumer<? super TxnId, Throwable> callback)
            {
                node.scheduler().once(() -> callback.accept(staleId, null), waitMicros, MICROSECONDS);
                return null;
            }
        };
    }

    @Override
    public long minStaleHlc(Node node, boolean requested)
    {
        return node.now() - (100 + getAccordScheduleDurabilityTxnIdLag(MICROSECONDS));
    }

    @Override
    public boolean reportRemoteSuccess(Result success)
    {
        return success instanceof TxnResult && ((TxnResult) success).kind() == txn_data;
    }
}
