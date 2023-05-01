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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import accord.api.BarrierType;
import accord.local.DurableBefore;
import accord.local.Node.Id;
import accord.local.RedundantBefore;
import accord.messages.Request;
import accord.primitives.Ranges;
import accord.primitives.Seekables;
import accord.primitives.Txn;
import accord.topology.TopologyManager;
import org.agrona.collections.Int2ObjectHashMap;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.service.accord.api.AccordRoutableKey;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.service.accord.api.AccordScheduler;
import org.apache.cassandra.service.accord.txn.TxnResult;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Future;

public interface IAccordService
{
    Set<ConsistencyLevel> SUPPORTED_COMMIT_CONSISTENCY_LEVELS = ImmutableSet.of(ConsistencyLevel.ANY, ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.SERIAL, ConsistencyLevel.ALL);
    Set<ConsistencyLevel> SUPPORTED_READ_CONSISTENCY_LEVELS = ImmutableSet.of(ConsistencyLevel.ONE, ConsistencyLevel.QUORUM, ConsistencyLevel.SERIAL);

    IVerbHandler<? extends Request> verbHandler();

    default long barrierWithRetries(Seekables keysOrRanges, long minEpoch, BarrierType barrierType, boolean isForWrite) throws InterruptedException
    {
        throw new UnsupportedOperationException();
    }

    long barrier(@Nonnull Seekables keysOrRanges, long minEpoch, long queryStartNanos, long timeoutNanos, BarrierType barrierType, boolean isForWrite);

    default void postStreamReceivingBarrier(ColumnFamilyStore cfs, List<Range<Token>> ranges)
    {
        String ks = cfs.keyspace.getName();
        Ranges accordRanges = Ranges.of(ranges
             .stream()
             .map(r -> new TokenRange(new TokenKey(ks, r.left), new TokenKey(ks, r.right)))
             .collect(Collectors.toList())
             .toArray(new accord.primitives.Range[0]));
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

    /**
     * Temporary method to avoid double-streaming keyspaces
     * @param keyspace
     * @return
     */
    boolean isAccordManagedKeyspace(String keyspace);

    /**
     * Fetch the redundnant befores for every command store
     */
    Pair<Int2ObjectHashMap<RedundantBefore>, DurableBefore> getRedundantBeforesAndDurableBefore();

    default Id nodeId() { throw new UnsupportedOperationException(); }

    default void maybeConvertKeyspacesToAccord(Txn txn)
    {
        Set<String> allKeyspaces = new HashSet<>();
        txn.keys().forEach(key -> allKeyspaces.add(((AccordRoutableKey) key).keyspace()));

        for (String keyspace : allKeyspaces)
        {

            ensureKeyspaceIsAccordManaged(keyspace);
        }

        for (String keyspace : allKeyspaces)
        {
            if (!AccordService.instance().isAccordManagedKeyspace(keyspace))
                throw new IllegalStateException(keyspace + " is not an accord managed keyspace");
        }
    }

    void ensureKeyspaceIsAccordManaged(String keyspace);
}
