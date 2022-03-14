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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import accord.api.Agent;
import accord.api.Key;
import accord.api.Store;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandsForKey;
import accord.local.Node;
import accord.topology.KeyRanges;
import accord.topology.Topology;
import accord.txn.Timestamp;
import accord.txn.TxnId;
import org.apache.cassandra.utils.concurrent.Future;

public class AccordCommandStore extends CommandStore
{
    private final ExecutorService executor;

    public AccordCommandStore(int generation, int index, int numShards, Node.Id nodeId, Function<Timestamp, Timestamp> uniqueNow, Agent agent, Store store, KeyRanges ranges, Supplier<Topology> localTopologySupplier)
    {
        super(generation, index, numShards, nodeId, uniqueNow, agent, store, ranges, localTopologySupplier);
        executor = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r);
            thread.setName(CommandStore.class.getSimpleName() + '[' + nodeId + ':' + index + ']');
            return thread;
        });
    }

    @Override
    public Command command(TxnId txnId)
    {
        return null;
    }

    @Override
    public CommandsForKey commandsForKey(Key key)
    {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    protected void onRangeUpdate(KeyRanges previous, KeyRanges current)
    {

    }

    @Override
    public Future<Void> process(Consumer<? super CommandStore> consumer)
    {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public void shutdown()
    {
        throw new UnsupportedOperationException("TODO");
    }
}
