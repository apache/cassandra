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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import accord.api.BarrierType;
import accord.messages.Request;
import accord.primitives.Ranges;
import accord.primitives.Seekables;
import accord.primitives.Txn;
import accord.topology.TopologyManager;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.service.accord.txn.TxnResult;
import org.apache.cassandra.tcm.Epoch;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public interface IAccordService
{
    IVerbHandler<? extends Request> verbHandler();

    void createEpochFromConfigUnsafe();

    long barrier(@Nonnull Seekables keysOrRanges, long minEpoch, long queryStartNanos, BarrierType barrierType, boolean isForWrite);

    default void barrier(ColumnFamilyStore cfs, List<Range<Token>> ranges)
    {
        String ks = cfs.keyspace.getName();
        Ranges accordRanges = Ranges.of(ranges
             .stream()
             .map(r -> new TokenRange(new TokenKey(ks, r.left), new TokenKey(ks, r.right)))
             .collect(Collectors.toList())
             .toArray(new accord.primitives.Range[0]));
        long start = nanoTime();
        barrier(accordRanges, Epoch.FIRST.getEpoch(), start, BarrierType.global_sync, true);
    }

    @Nonnull TxnResult coordinate(@Nonnull Txn txn, @Nonnull ConsistencyLevel consistencyLevel, long queryStartNanos);

    long currentEpoch();

    void setCacheSize(long kb);

    TopologyManager topology();

    void shutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException;
}
