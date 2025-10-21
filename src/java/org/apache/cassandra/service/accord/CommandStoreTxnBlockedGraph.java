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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import accord.primitives.SaveStatus;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.async.AsyncResults;
import org.apache.cassandra.service.accord.api.TokenKey;

public class CommandStoreTxnBlockedGraph
{
    public final int commandStoreId;
    public final Map<TxnId, TxnState> txns;
    public final Map<TokenKey, TxnId> keys;

    public CommandStoreTxnBlockedGraph(Builder builder)
    {
        commandStoreId = builder.storeId;
        txns = ImmutableMap.copyOf(builder.txns);
        keys = ImmutableMap.copyOf(builder.keys);
    }

    public static class TxnState
    {
        public final TxnId txnId;
        public final Timestamp executeAt;
        public final SaveStatus saveStatus;
        public final List<TxnId> blockedBy;
        public final Set<TokenKey> blockedByKey;

        public TxnState(Builder.TxnBuilder builder)
        {
            txnId = builder.txnId;
            executeAt = builder.executeAt;
            saveStatus = builder.saveStatus;
            blockedBy = ImmutableList.copyOf(builder.blockedBy);
            blockedByKey = ImmutableSet.copyOf(builder.blockedByKey);
        }

        public boolean isBlocked()
        {
            return !notBlocked();
        }

        public boolean notBlocked()
        {
            return blockedBy.isEmpty() && blockedByKey.isEmpty();
        }
    }

    public static class Builder extends AsyncResults.SettableResult<CommandStoreTxnBlockedGraph>
    {
        final AtomicInteger asyncTxns = new AtomicInteger(), asyncKeys = new AtomicInteger();
        final int storeId;
        final Map<TxnId, TxnState> txns = new LinkedHashMap<>();
        final Map<TokenKey, TxnId> keys = new LinkedHashMap<>();

        public Builder(int storeId)
        {
            this.storeId = storeId;
        }

        boolean knows(TxnId id)
        {
            return txns.containsKey(id);
        }

        public void complete()
        {
            trySuccess(build());
        }

        public CommandStoreTxnBlockedGraph build()
        {
            return new CommandStoreTxnBlockedGraph(this);
        }

        public TxnBuilder txn(TxnId txnId, Timestamp executeAt, SaveStatus saveStatus)
        {
            return new TxnBuilder(txnId, executeAt, saveStatus);
        }

        public class TxnBuilder
        {
            final TxnId txnId;
            final Timestamp executeAt;
            final SaveStatus saveStatus;
            List<TxnId> blockedBy = new ArrayList<>();
            Set<TokenKey> blockedByKey = new LinkedHashSet<>();

            public TxnBuilder(TxnId txnId, Timestamp executeAt, SaveStatus saveStatus)
            {
                this.txnId = txnId;
                this.executeAt = executeAt;
                this.saveStatus = saveStatus;
            }

            public TxnState build()
            {
                TxnState state = new TxnState(this);
                txns.put(txnId, state);
                return state;
            }
        }
    }
}
