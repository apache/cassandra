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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.local.durability.DurabilityService.SyncLocal;
import accord.local.durability.DurabilityService.SyncRemote;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.DurableBefore;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.RedundantBefore;
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

    IVerbHandler<? extends Request> requestHandler();
    IVerbHandler<? extends Reply> responseHandler();

    AsyncChain<Void> sync(Object requestedBy, @Nullable Timestamp minBound, Ranges ranges, @Nullable Collection<Id> include, SyncLocal syncLocal, SyncRemote syncRemote);
    AsyncChain<Void> sync(@Nullable Timestamp minBound, Keys keys, SyncLocal syncLocal, SyncRemote syncRemote);
    AsyncChain<Timestamp> maxConflict(Ranges ranges);

    @Nonnull IAccordResult<TxnResult> coordinateAsync(long minEpoch, @Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, RequestTime requestTime);
    @Nonnull TxnResult coordinate(long minEpoch, @Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, RequestTime requestTime) throws RequestExecutionException;

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

    void shutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException;

    AccordScheduler scheduler();

    /**
     * Return a future that will complete once the accord has completed it's local bootstrap process
     * for any ranges gained in the given epoch
     */
    Future<Void> epochReady(Epoch epoch);

    void receive(Message<List<AccordSyncPropagator.Notification>> message);

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

        public AccordCompactionInfos(DurableBefore durableBefore)
        {
            this.durableBefore = durableBefore;
        }

        public AccordCompactionInfos(DurableBefore durableBefore, AccordCompactionInfos copy)
        {
            super(copy);
            this.durableBefore = durableBefore;
        }
    }

    /**
     * Fetch the redundnant befores for every command store
     */
    AccordCompactionInfos getCompactionInfo();

    Agent agent();

    Id nodeId();

    List<CommandStoreTxnBlockedGraph> debugTxnBlockedGraph(TxnId txnId);
    @Nullable
    Long minEpoch();

    void awaitDone(TableId id, long epoch);

    AccordConfigurationService configService();

    Params journalConfiguration();

    boolean shouldAcceptMessages();

    Node node();

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
        public AsyncChain<Void> sync(Object requestedBy, @Nullable Timestamp onOrAfter, Ranges ranges, @Nullable Collection<Id> include, SyncLocal syncLocal, SyncRemote syncRemote)
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
        public @Nonnull TxnResult coordinate(long minEpoch, @Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, @Nonnull RequestTime requestTime)
        {
            throw new UnsupportedOperationException("No accord transaction should be executed when accord.enabled = false in cassandra.yaml");
        }

        @Override
        public @Nonnull IAccordResult<TxnResult> coordinateAsync(long minEpoch, @Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, RequestTime requestTime)
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
        public void receive(Message<List<AccordSyncPropagator.Notification>> message) {}

        @Override
        public AccordCompactionInfos getCompactionInfo()
        {
            return new AccordCompactionInfos(DurableBefore.EMPTY);
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

        @Nullable
        @Override
        public Long minEpoch()
        {
            return null;
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
        public AsyncChain<Void> sync(Object requestedBy, @Nullable Timestamp onOrAfter, Ranges ranges, @Nullable Collection<Id> include, SyncLocal syncLocal, SyncRemote syncRemote)
        {
            return delegate.sync(requestedBy, onOrAfter, ranges, include, syncLocal, syncRemote);
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
        public TxnResult coordinate(long minEpoch, @Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, RequestTime requestTime)
        {
            return delegate.coordinate(minEpoch, txn, consistencyLevel, requestTime);
        }

        @Nonnull
        @Override
        public IAccordResult<TxnResult> coordinateAsync(long minEpoch, @Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, RequestTime requestTime)
        {
            return delegate.coordinateAsync(minEpoch, txn, consistencyLevel, requestTime);
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
        public void receive(Message<List<Notification>> message)
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

        @Nullable
        @Override
        public Long minEpoch()
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
    }
}
