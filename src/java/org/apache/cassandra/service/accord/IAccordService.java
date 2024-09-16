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

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import accord.api.BarrierType;
import accord.local.CommandStores;
import accord.local.DurableBefore;
import accord.local.Node.Id;
import accord.local.RedundantBefore;
import accord.messages.Request;
import accord.primitives.Ranges;
import accord.primitives.Seekables;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.TopologyManager;
import org.agrona.collections.Int2ObjectHashMap;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.accord.api.AccordScheduler;
import org.apache.cassandra.service.accord.txn.TxnResult;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

import static com.google.common.base.Preconditions.checkNotNull;


public interface IAccordService
{
    Set<ConsistencyLevel> SUPPORTED_COMMIT_CONSISTENCY_LEVELS = ImmutableSet.of(ConsistencyLevel.ANY, ConsistencyLevel.ONE, ConsistencyLevel.LOCAL_ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.SERIAL, ConsistencyLevel.ALL);
    Set<ConsistencyLevel> SUPPORTED_READ_CONSISTENCY_LEVELS = ImmutableSet.of(ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.SERIAL);

    IVerbHandler<? extends Request> verbHandler();

    Seekables barrierWithRetries(Seekables keysOrRanges, long minEpoch, BarrierType barrierType, boolean isForWrite) throws InterruptedException;

    Seekables barrier(@Nonnull Seekables keysOrRanges, long minEpoch, long queryStartNanos, long timeoutNanos, BarrierType barrierType, boolean isForWrite);

    default Seekables repairWithRetries(Seekables keysOrRanges, long minEpoch, BarrierType barrierType, boolean isForWrite, List<InetAddressAndPort> allEndpoints) throws InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    Seekables repair(@Nonnull Seekables keysOrRanges, long epoch, long queryStartNanos, long timeoutNanos, BarrierType barrierType, boolean isForWrite, List<InetAddressAndPort> allEndpoints);

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

    @Nonnull TxnResult coordinate(@Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, long queryStartNanos);

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
    AsyncTxnResult coordinateAsync(@Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, long queryStartNanos);
    TxnResult getTxnResult(AsyncTxnResult asyncTxnResult, boolean isWrite, @Nullable ConsistencyLevel consistencyLevel, long queryStartNanos);

    long currentEpoch();

    void setCacheSize(long kb);

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
        static final Supplier<CompactionInfo> NO_OP = () ->  new CompactionInfo(new Int2ObjectHashMap<>(), new Int2ObjectHashMap<>(), DurableBefore.EMPTY);

        public final Int2ObjectHashMap<RedundantBefore> redundantBefores;
        public final Int2ObjectHashMap<CommandStores.RangesForEpoch> ranges;
        public final DurableBefore durableBefore;

        public CompactionInfo(Int2ObjectHashMap<RedundantBefore> redundantBefores, Int2ObjectHashMap<CommandStores.RangesForEpoch> ranges, DurableBefore durableBefore)
        {
            this.redundantBefores = redundantBefores;
            this.ranges = ranges;
            this.durableBefore = durableBefore;
        }
    }

    /**
     * Fetch the redundnant befores for every command store
     */
    CompactionInfo getCompactionInfo();

    default Id nodeId() { throw new UnsupportedOperationException(); }

    List<CommandStoreTxnBlockedGraph> debugTxnBlockedGraph(TxnId txnId);
}
