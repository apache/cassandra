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

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.BarrierType;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.DurableBefore;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.RedundantBefore;
import accord.messages.Reply;
import accord.messages.Request;
import accord.primitives.Ranges;
import accord.primitives.Seekables;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Topology;
import accord.topology.TopologyManager;
import org.agrona.collections.Int2ObjectHashMap;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.journal.Params;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordSyncPropagator.Notification;
import org.apache.cassandra.service.accord.api.AccordScheduler;
import org.apache.cassandra.service.accord.api.AccordTopologySorter;
import org.apache.cassandra.service.accord.txn.TxnResult;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.Dispatcher.RequestTime;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

import static com.google.common.base.Preconditions.checkNotNull;


// Avoid default methods that aren't just providing wrappers around other methods
// so it will be a compile error if DelegatingAccordService doesn't implement them
public interface IAccordService
{
    Logger logger = LoggerFactory.getLogger(IAccordService.class);

    EnumSet<ConsistencyLevel> SUPPORTED_COMMIT_CONSISTENCY_LEVELS = EnumSet.of(ConsistencyLevel.ANY, ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.SERIAL, ConsistencyLevel.ALL);
    EnumSet<ConsistencyLevel> SUPPORTED_READ_CONSISTENCY_LEVELS = EnumSet.of(ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.SERIAL, ConsistencyLevel.ALL);

    IVerbHandler<? extends Request> requestHandler();
    IVerbHandler<? extends Reply> responseHandler();

    Seekables<?, ?> barrierWithRetries(Seekables<?, ?> keysOrRanges, long minEpoch, BarrierType barrierType, boolean isForWrite) throws InterruptedException;

    Seekables<?, ?> barrier(@Nonnull Seekables<?, ?> keysOrRanges, long minEpoch, Dispatcher.RequestTime requestTime, long timeoutNanos, BarrierType barrierType, boolean isForWrite);

    Seekables<?, ?> repairWithRetries(Seekables<?, ?> keysOrRanges, long minEpoch, BarrierType barrierType, boolean isForWrite, List<InetAddressAndPort> allEndpoints) throws InterruptedException;

    Seekables<?, ?> repair(@Nonnull Seekables<?, ?> keysOrRanges, long epoch, Dispatcher.RequestTime requestTime, long timeoutNanos, BarrierType barrierType, boolean isForWrite, List<InetAddressAndPort> allEndpoints);

    default void postStreamReceivingBarrier(ColumnFamilyStore cfs, List<Range<Token>> ranges)
    {
        String ks = cfs.keyspace.getName();
        Ranges accordRanges = AccordTopology.toAccordRanges(ks, ranges);
        try
        {
            barrierWithRetries(accordRanges, Epoch.FIRST.getEpoch(), BarrierType.global_async, true);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Nonnull TxnResult coordinate(long minEpoch, @Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime);

    class AsyncTxnResult extends AsyncPromise<TxnResult>
    {
        public final @Nonnull TxnId txnId;
        public final long minEpoch;
        @Nullable
        public final ConsistencyLevel consistencyLevel;
        public final boolean isWrite;
        public final Dispatcher.RequestTime requestTime;

        public AsyncTxnResult(@Nonnull TxnId txnId, long minEpoch, @Nullable ConsistencyLevel consistencyLevel, boolean isWrite, @Nonnull Dispatcher.RequestTime requestTime)
        {
            checkNotNull(txnId);
            checkNotNull(requestTime);
            this.txnId = txnId;
            this.minEpoch = minEpoch;
            this.consistencyLevel = consistencyLevel;
            this.isWrite = isWrite;
            this.requestTime = requestTime;
        }
    }

    @Nonnull
    AsyncTxnResult coordinateAsync(long minEpoch, @Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime);
    TxnResult getTxnResult(AsyncTxnResult asyncTxnResult);

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

    class CompactionInfo
    {
        static final Supplier<CompactionInfo> NO_OP = () ->  new CompactionInfo(new Int2ObjectHashMap<>(), new Int2ObjectHashMap<>(), new Int2ObjectHashMap<>());

        public final Int2ObjectHashMap<RedundantBefore> redundantBefores;
        public final Int2ObjectHashMap<DurableBefore> durableBefores;
        public final Int2ObjectHashMap<RangesForEpoch> ranges;

        public CompactionInfo(Int2ObjectHashMap<RedundantBefore> redundantBefores, Int2ObjectHashMap<RangesForEpoch> ranges, Int2ObjectHashMap<DurableBefore> durableBefores)
        {
            this.redundantBefores = redundantBefores;
            this.ranges = ranges;
            this.durableBefores = durableBefores;
        }
    }

    /**
     * Fetch the redundnant befores for every command store
     */
    CompactionInfo getCompactionInfo();

    Agent agent();

    Id nodeId();

    List<CommandStoreTxnBlockedGraph> debugTxnBlockedGraph(TxnId txnId);
    @Nullable
    Long minEpoch();

    void tryMarkRemoved(Topology topology, Node.Id node);
    void awaitTableDrop(TableId id);

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
        public Seekables<?, ?> barrierWithRetries(Seekables<?, ?> keysOrRanges, long minEpoch, BarrierType barrierType, boolean isForWrite) throws InterruptedException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Seekables<?, ?> barrier(@Nonnull Seekables<?, ?> keysOrRanges, long minEpoch, Dispatcher.RequestTime requestTime, long timeoutNanos, BarrierType barrierType, boolean isForWrite)
        {
            throw new UnsupportedOperationException("No accord barriers should be executed when accord.enabled = false in cassandra.yaml");
        }

        @Override
        public Seekables<?, ?> repairWithRetries(Seekables<?, ?> keysOrRanges, long minEpoch, BarrierType barrierType, boolean isForWrite, List<InetAddressAndPort> allEndpoints) throws InterruptedException
        {
            return null;
        }

        @Override
        public Seekables<?, ?> repair(@Nonnull Seekables<?, ?> keysOrRanges, long epoch, Dispatcher.RequestTime requestTime, long timeoutNanos, BarrierType barrierType, boolean isForWrite, List<InetAddressAndPort> allEndpoints)
        {
            throw new UnsupportedOperationException("No accord repairs should be executed when accord.enabled = false in cassandra.yaml");
        }

        @Override
        public @Nonnull TxnResult coordinate(long minEpoch, @Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, @Nonnull Dispatcher.RequestTime requestTime)
        {
            throw new UnsupportedOperationException("No accord transaction should be executed when accord.enabled = false in cassandra.yaml");
        }

        @Override
        public @Nonnull AsyncTxnResult coordinateAsync(long minEpoch, @Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime)
        {
            throw new UnsupportedOperationException("No accord transaction should be executed when accord.enabled = false in cassandra.yaml");
        }

        @Override
        public TxnResult getTxnResult(AsyncTxnResult asyncTxnResult)
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
                AccordTopologySorter.checkSnitchSupported(DatabaseDescriptor.getEndpointSnitch());
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
        public CompactionInfo getCompactionInfo()
        {
            return new CompactionInfo(new Int2ObjectHashMap<>(), new Int2ObjectHashMap<>(), new Int2ObjectHashMap<>());
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
        public void tryMarkRemoved(Topology topology, Id node)
        {

        }

        @Override
        public void awaitTableDrop(TableId id)
        {

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
        public Seekables<?, ?> barrierWithRetries(Seekables<?, ?> keysOrRanges, long minEpoch, BarrierType barrierType, boolean isForWrite) throws InterruptedException
        {
            return delegate.barrierWithRetries(keysOrRanges, minEpoch, barrierType, isForWrite);
        }

        @Override
        public Seekables<?, ?> barrier(@Nonnull Seekables<?, ?> keysOrRanges, long minEpoch, RequestTime requestTime, long timeoutNanos, BarrierType barrierType, boolean isForWrite)
        {
            return delegate.barrier(keysOrRanges, minEpoch, requestTime, timeoutNanos, barrierType, isForWrite);
        }

        @Override
        public Seekables<?, ?> repairWithRetries(Seekables<?, ?> keysOrRanges, long minEpoch, BarrierType barrierType, boolean isForWrite, List<InetAddressAndPort> allEndpoints) throws InterruptedException
        {
            return delegate.repairWithRetries(keysOrRanges, minEpoch, barrierType, isForWrite, allEndpoints);
        }

        @Override
        public Seekables<?, ?> repair(@Nonnull Seekables<?, ?> keysOrRanges, long epoch, RequestTime requestTime, long timeoutNanos, BarrierType barrierType, boolean isForWrite, List<InetAddressAndPort> allEndpoints)
        {
            return delegate.repair(keysOrRanges, epoch, requestTime, timeoutNanos, barrierType, isForWrite, allEndpoints);
        }

        @Override
        public void postStreamReceivingBarrier(ColumnFamilyStore cfs, List<Range<Token>> ranges)
        {
            IAccordService.super.postStreamReceivingBarrier(cfs, ranges);
        }

        @Nonnull
        @Override
        public TxnResult coordinate(long minEpoch, @Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, RequestTime requestTime)
        {
            return delegate.coordinate(minEpoch, txn, consistencyLevel, requestTime);
        }

        @Nonnull
        @Override
        public AsyncTxnResult coordinateAsync(long minEpoch, @Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, RequestTime requestTime)
        {
            return delegate.coordinateAsync(minEpoch, txn, consistencyLevel, requestTime);
        }

        @Override
        public TxnResult getTxnResult(AsyncTxnResult asyncTxnResult)
        {
            return delegate.getTxnResult(asyncTxnResult);
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
        public CompactionInfo getCompactionInfo()
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
        public void tryMarkRemoved(Topology topology, Id node)
        {
            delegate.tryMarkRemoved(topology, node);
        }

        @Override
        public void awaitTableDrop(TableId id)
        {
            delegate.awaitTableDrop(id);
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
