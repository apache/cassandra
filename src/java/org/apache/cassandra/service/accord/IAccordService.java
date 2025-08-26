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

package org.apache.cassandra.service.accord;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.cassandra.tcm.ClusterMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.DurableBefore;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.RedundantBefore;
import accord.local.durability.DurabilityService.SyncLocal;
import accord.local.durability.DurabilityService.SyncRemote;
import accord.messages.Reply;
import accord.messages.Request;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.TopologyManager;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import org.agrona.collections.Int2ObjectHashMap;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.journal.Params;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordSyncPropagator.Notification;
import org.apache.cassandra.service.accord.api.AccordScheduler;
import org.apache.cassandra.service.accord.api.AccordTopologySorter;
import org.apache.cassandra.service.accord.txn.TxnResult;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.transport.Dispatcher.RequestTime;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

// Avoid default methods that aren't just providing wrappers around other methods
// so it will be a compile error if DelegatingAccordService doesn't implement them
public interface IAccordService
{
    Logger logger = LoggerFactory.getLogger(IAccordService.class);

    EnumSet<ConsistencyLevel> SUPPORTED_COMMIT_CONSISTENCY_LEVELS = EnumSet.of(ConsistencyLevel.ANY, ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.SERIAL, ConsistencyLevel.ALL);
    EnumSet<ConsistencyLevel> SUPPORTED_READ_CONSISTENCY_LEVELS = EnumSet.of(ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.SERIAL, ConsistencyLevel.ALL);
    long NO_HLC = Long.MIN_VALUE;

    IVerbHandler<? extends Request> requestHandler();
    IVerbHandler<? extends Reply> responseHandler();

    AsyncChain<Void> sync(Object requestedBy, @Nullable Timestamp minBound, Ranges ranges, @Nullable Collection<Id> include, SyncLocal syncLocal, SyncRemote syncRemote, long timeout, TimeUnit timeoutUnits);
    AsyncChain<Void> sync(@Nullable Timestamp minBound, Keys keys, SyncLocal syncLocal, SyncRemote syncRemote);
    AsyncChain<Timestamp> maxConflict(Ranges ranges);

    @Nonnull
    default IAccordResult<TxnResult> coordinateAsync(long minEpoch, @Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, RequestTime requestTime)
    {
        return coordinateAsync(minEpoch, txn, consistencyLevel, requestTime, IAccordService.NO_HLC);
    }
    @Nonnull IAccordResult<TxnResult> coordinateAsync(long minEpoch, @Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, RequestTime requestTime, long minHlc);
    @Nonnull default TxnResult coordinate(long minEpoch, @Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, RequestTime requestTime)
    {
        return coordinate(minEpoch, txn, consistencyLevel, requestTime, NO_HLC);
    }
    @Nonnull TxnResult coordinate(long minEpoch, @Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, RequestTime requestTime, long minHlc) throws RequestExecutionException;

    List<AccordExecutor> executors();

    interface IAccordResult<V>
    {
        V success();
        Throwable fail();
        V awaitAndGet() throws RequestExecutionException;
        IAccordResult<V> addCallback(BiConsumer<? super V, Throwable> callback);
    }

    long currentEpoch();

    void setCacheSize(long kb);
    void setWorkingSetSize(long kb);

    TopologyManager topology();

    void startup();

    Future<Void> flushCaches();
    void markShuttingDown();
    void shutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException;

    AccordScheduler scheduler();

    /**
     * Return a future that will complete once the accord has completed it's local bootstrap process
     * for any ranges gained in the given epoch
     */
    Future<Void> epochReady(Epoch epoch);

    Future<Void> epochReadyFor(ClusterMetadata epoch);

    void receive(Message<AccordSyncPropagator.Notification> message);

    class AccordCompactionInfo
    {
        public final int commandStoreId;
        public final RedundantBefore redundantBefore;
        public final RangesForEpoch ranges;
        public final TableId tableId;

        public AccordCompactionInfo(int commandStoreId, RedundantBefore redundantBefore, RangesForEpoch ranges, TableId tableId)
        {
            this.commandStoreId = commandStoreId;
            this.redundantBefore = Invariants.nonNull(redundantBefore);
            this.ranges = Invariants.nonNull(ranges);
            this.tableId = Invariants.nonNull(tableId);
        }
    }

    class AccordCompactionInfos extends Int2ObjectHashMap<AccordCompactionInfo>
    {
        public final DurableBefore durableBefore;
        public final long minEpoch;

        public AccordCompactionInfos(DurableBefore durableBefore, long minEpoch)
        {
            this.durableBefore = durableBefore;
            this.minEpoch = minEpoch;
        }

        public AccordCompactionInfos(DurableBefore durableBefore, long minEpoch, AccordCompactionInfos copy)
        {
            super(copy);
            this.durableBefore = durableBefore;
            this.minEpoch = minEpoch;
        }
    }

    /**
     * Fetch the redundnant befores for every command store
     */
    AccordCompactionInfos getCompactionInfo();

    Agent agent();

    Id nodeId();

    List<CommandStoreTxnBlockedGraph> debugTxnBlockedGraph(TxnId txnId);

    long minEpoch();

    void awaitDone(TableId id, long epoch);

    AccordConfigurationService configService();

    Params journalConfiguration();

    boolean shouldAcceptMessages();

    Node node();

    /**
     * Ensure Accord's hlc is at least larger than this for anything accepted at this node
     */
    void ensureMinHlc(long minHlc);

    // Implementation for the NO_OP service that also has what used to be the default implementations
    // that had to be overridden by the real AccordService anyways
    class NoOpAccordService implements IAccordService
    {
        private static final Future<Void> BOOTSTRAP_SUCCESS = ImmediateFuture.success(null);

        @Override
        public IVerbHandler<? extends Request> requestHandler()
        {
            return null;
        }

        @Override
        public IVerbHandler<? extends Reply> responseHandler()
        {
            return null;
        }

        @Override
        public AsyncChain<Void> sync(Object requestedBy, @Nullable Timestamp onOrAfter, Ranges ranges, @Nullable Collection<Id> include, SyncLocal syncLocal, SyncRemote syncRemote, long timeout, TimeUnit timeoutUnits)
        {
            throw new UnsupportedOperationException("No accord transaction should be executed when accord.enabled = false in cassandra.yaml");
        }

        @Override
        public AsyncChain<Void> sync(@Nullable Timestamp onOrAfter, Keys keys, SyncLocal syncLocal, SyncRemote syncRemote)
        {
            throw new UnsupportedOperationException("No accord transaction should be executed when accord.enabled = false in cassandra.yaml");
        }

        @Override
        public AsyncChain<Timestamp> maxConflict(Ranges ranges)
        {
            throw new UnsupportedOperationException("No accord transaction should be executed when accord.enabled = false in cassandra.yaml");
        }

        @Override
        public @Nonnull TxnResult coordinate(long minEpoch, @Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, @Nonnull RequestTime requestTime, long minHlc)
        {
            throw new UnsupportedOperationException("No accord transaction should be executed when accord.enabled = false in cassandra.yaml");
        }

        @Override
        public List<AccordExecutor> executors()
        {
            return List.of();
        }

        @Override
        public @Nonnull IAccordResult<TxnResult> coordinateAsync(long minEpoch, @Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, RequestTime requestTime, long minHlc)
        {
            throw new UnsupportedOperationException("No accord transaction should be executed when accord.enabled = false in cassandra.yaml");
        }

        @Override
        public long currentEpoch()
        {
            throw new UnsupportedOperationException("Cannot return epoch when accord.enabled = false in cassandra.yaml");
        }

        @Override
        public void setCacheSize(long kb) { }

        @Override
        public void setWorkingSetSize(long kb) {}

        @Override
        public TopologyManager topology()
        {
            throw new UnsupportedOperationException("Cannot return topology when accord.enabled = false in cassandra.yaml");
        }

        @Override
        public void startup()
        {
            try
            {
                AccordTopologySorter.checkSnitchSupported(DatabaseDescriptor.getNodeProximity());
            }
            catch (Throwable t)
            {
                logger.warn("Current snitch  is not compatable with Accord, make sure to fix the snitch before enabling Accord; {}", t.toString());
            }
        }

        @Override
        public void markShuttingDown()
        {
        }

        @Override
        public Future<Void> flushCaches()
        {
            return ImmediateFuture.success(null);
        }

        @Override
        public void shutdownAndWait(long timeout, TimeUnit unit) { }

        @Override
        public AccordScheduler scheduler()
        {
            return null;
        }

        @Override
        public Future<Void> epochReady(Epoch epoch)
        {
            return BOOTSTRAP_SUCCESS;
        }

        @Override
        public Future<Void> epochReadyFor(ClusterMetadata epoch)
        {
            return BOOTSTRAP_SUCCESS;
        }

        @Override
        public void receive(Message<AccordSyncPropagator.Notification> message) {}

        @Override
        public AccordCompactionInfos getCompactionInfo()
        {
            return new AccordCompactionInfos(DurableBefore.EMPTY, 0);
        }

        @Override
        public Agent agent()
        {
            return null;
        }

        @Override
        public Id nodeId()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<CommandStoreTxnBlockedGraph> debugTxnBlockedGraph(TxnId txnId)
        {
            return Collections.emptyList();
        }

        @Override
        public long minEpoch()
        {
            return -1;
        }

        @Override
        public void awaitDone(TableId id, long epoch)
        {

        }

        @Override
        public AccordConfigurationService configService()
        {
            return null;
        }

        @Override
        public Params journalConfiguration()
        {
            throw new UnsupportedOperationException("Cannot return configuration when accord.enabled = false in cassandra.yaml");
        }

        @Override
        public boolean shouldAcceptMessages()
        {
            return true;
        }

        @Override
        public Node node()
        {
            return null;
        }

        @Override
        public void ensureMinHlc(long minHlc)
        {

        }
    }

    class DelegatingAccordService implements IAccordService
    {
        protected final IAccordService delegate;

        public DelegatingAccordService(IAccordService delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public IVerbHandler<? extends Request> requestHandler()
        {
            return delegate.requestHandler();
        }

        @Override
        public IVerbHandler<? extends Reply> responseHandler()
        {
            return delegate.responseHandler();
        }

        @Override
        public AsyncChain<Void> sync(Object requestedBy, @Nullable Timestamp onOrAfter, Ranges ranges, @Nullable Collection<Id> include, SyncLocal syncLocal, SyncRemote syncRemote, long timeout, TimeUnit timeoutUnits)
        {
            return delegate.sync(requestedBy, onOrAfter, ranges, include, syncLocal, syncRemote, timeout, timeoutUnits);
        }

        @Override
        public AsyncChain<Void> sync(@Nullable Timestamp onOrAfter, Keys keys, SyncLocal syncLocal, SyncRemote syncRemote)
        {
            return delegate.sync(onOrAfter, keys, syncLocal, syncRemote);
        }

        @Override
        public AsyncChain<Timestamp> maxConflict(Ranges ranges)
        {
            return delegate.maxConflict(ranges);
        }

        @Override
        public AccordConfigurationService configService()
        {
            return delegate.configService();
        }

        @Nonnull
        @Override
        public TxnResult coordinate(long minEpoch, @Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, RequestTime requestTime, long minHlc)
        {
            return delegate.coordinate(minEpoch, txn, consistencyLevel, requestTime, minHlc);
        }

        @Override
        public List<AccordExecutor> executors()
        {
            return delegate.executors();
        }

        @Nonnull
        @Override
        public IAccordResult<TxnResult> coordinateAsync(long minEpoch, @Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, RequestTime requestTime, long minHlc)
        {
            return delegate.coordinateAsync(minEpoch, txn, consistencyLevel, requestTime, minHlc);
        }

        @Override
        public long currentEpoch()
        {
            return delegate.currentEpoch();
        }

        @Override
        public void setCacheSize(long kb)
        {
            delegate.setCacheSize(kb);
        }

        @Override
        public void setWorkingSetSize(long kb)
        {
            delegate.setWorkingSetSize(kb);
        }

        @Override
        public TopologyManager topology()
        {
            return delegate.topology();
        }

        @Override
        public void startup()
        {
            delegate.startup();
        }

        @Override
        public Future<Void> flushCaches()
        {
            return delegate.flushCaches();
        }

        @Override
        public void markShuttingDown()
        {
            delegate.markShuttingDown();
        }

        @Override
        public void shutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
        {
            delegate.shutdownAndWait(timeout, unit);
        }

        @Override
        public AccordScheduler scheduler()
        {
            return delegate.scheduler();
        }

        @Override
        public Future<Void> epochReady(Epoch epoch)
        {
            return delegate.epochReady(epoch);
        }

        @Override
        public Future<Void> epochReadyFor(ClusterMetadata epoch)
        {
            return delegate.epochReadyFor(epoch);
        }

        @Override
        public void receive(Message<Notification> message)
        {
            delegate.receive(message);
        }

        @Override
        public AccordCompactionInfos getCompactionInfo()
        {
            return delegate.getCompactionInfo();
        }

        @Override
        public Agent agent()
        {
            return delegate.agent();
        }

        @Override
        public Id nodeId()
        {
            return delegate.nodeId();
        }

        @Override
        public List<CommandStoreTxnBlockedGraph> debugTxnBlockedGraph(TxnId txnId)
        {
            return delegate.debugTxnBlockedGraph(txnId);
        }

        @Override
        public long minEpoch()
        {
            return delegate.minEpoch();
        }

        @Override
        public void awaitDone(TableId id, long epoch)
        {
            delegate.awaitDone(id, epoch);
        }

        @Override
        public Params journalConfiguration()
        {
            return delegate.journalConfiguration();
        }

        @Override
        public boolean shouldAcceptMessages()
        {
            return delegate.shouldAcceptMessages();
        }

        @Override
        public Node node()
        {
            return delegate.node();
        }

        @Override
        public void ensureMinHlc(long minHlc)
        {
            delegate.ensureMinHlc(minHlc);
        }
    }
}
