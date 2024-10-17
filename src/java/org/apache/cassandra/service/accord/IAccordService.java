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
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.journal.Params;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.service.accord.api.AccordScheduler;
import org.apache.cassandra.service.accord.txn.TxnResult;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

import static com.google.common.base.Preconditions.checkNotNull;


public interface IAccordService
{
    EnumSet<ConsistencyLevel> SUPPORTED_COMMIT_CONSISTENCY_LEVELS = EnumSet.of(ConsistencyLevel.ANY, ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.SERIAL, ConsistencyLevel.ALL);
    EnumSet<ConsistencyLevel> SUPPORTED_READ_CONSISTENCY_LEVELS = EnumSet.of(ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.SERIAL, ConsistencyLevel.ALL);

    IVerbHandler<? extends Request> requestHandler();
    IVerbHandler<? extends Reply> responseHandler();

    Seekables<?, ?> barrierWithRetries(Seekables<?, ?> keysOrRanges, long minEpoch, BarrierType barrierType, boolean isForWrite) throws InterruptedException;

    Seekables<?, ?> barrier(@Nonnull Seekables<?, ?> keysOrRanges, long minEpoch, Dispatcher.RequestTime requestTime, long timeoutNanos, BarrierType barrierType, boolean isForWrite);

    default Seekables<?, ?> repairWithRetries(Seekables<?, ?> keysOrRanges, long minEpoch, BarrierType barrierType, boolean isForWrite, List<InetAddressAndPort> allEndpoints) throws InterruptedException
    {
        throw new UnsupportedOperationException();
    }

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

    @Nonnull TxnResult coordinate(@Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime);

    class AsyncTxnResult extends AsyncPromise<TxnResult>
    {
        public final @Nonnull TxnId txnId;

        public AsyncTxnResult(@Nonnull TxnId txnId)
        {
            checkNotNull(txnId);
            this.txnId = txnId;
        }
    }

    @Nonnull
    AsyncTxnResult coordinateAsync(@Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime);
    TxnResult getTxnResult(AsyncTxnResult asyncTxnResult, boolean isWrite, @Nullable ConsistencyLevel consistencyLevel, Dispatcher.RequestTime requestTime);

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

    AccordAgent agent();

    default Id nodeId() { throw new UnsupportedOperationException(); }

    List<CommandStoreTxnBlockedGraph> debugTxnBlockedGraph(TxnId txnId);
    @Nullable
    Long minEpoch(Collection<TokenRange> ranges);

    void tryMarkRemoved(Topology topology, Node.Id node);
    default void awaitTableDrop(TableId id)
    {

    }

    Params journalConfiguration();
}
