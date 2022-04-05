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
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToLongBiFunction;

import accord.api.Agent;
import accord.api.Store;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.Node;
import accord.topology.KeyRanges;
import accord.txn.Timestamp;
import org.apache.cassandra.service.accord.async.AsyncOperation;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

public class AccordCommandStores extends CommandStores
{
    private final ExecutorService[] executors;

    public AccordCommandStores(int numShards, Node.Id node, Function<Timestamp, Timestamp> uniqueNow, Agent agent, Store store)
    {
        super(numShards, node, uniqueNow, agent, store);
        this.executors = new ExecutorService[numShards];
        for (int i=0; i<numShards; i++)
        {
            int index = i;
            executors[i] = Executors.newSingleThreadExecutor(r -> {
                Thread thread = new Thread(r);
                thread.setName(CommandStore.class.getSimpleName() + '[' + node + ':' + index + ']');
                return thread;
            });
        }
    }

    @Override
    protected CommandStore createCommandStore(int generation, int index, KeyRanges ranges)
    {
        return new AccordCommandStore(generation,
                                      index,
                                      numShards,
                                      node,
                                      uniqueNow,
                                      agent,
                                      store,
                                      ranges,
                                      this::getLocalTopology,
                                      executors[index]);
    }

    private <S, F, T> T mapReduce(ToLongBiFunction<StoreGroup, S> select, S scope, F f, StoreGroups.Fold<F, ?, List<AsyncOperation<T>>> fold, BiFunction<T, T, T> reduce)
    {
        List<AsyncOperation<T>> futures = groups.foldl(select, scope, fold, f, null, ArrayList::new);
        T result = null;
        for (Future<T> future : futures)
        {
            try
            {
                T next = future.get();
                if (result == null) result = next;
                else result = reduce.apply(result, next);
            }
            catch (InterruptedException e)
            {
                throw new UncheckedInterruptedException(e);
            }
            catch (ExecutionException e)
            {
                throw new RuntimeException(e.getCause());
            }
        }
        return result;
    }

    @Override
    protected <S, T> T mapReduce(ToLongBiFunction<StoreGroup, S> select, S scope, Function<? super CommandStore, T> map, BiFunction<T, T, T> reduce)
    {
        return mapReduce(select, scope, map, (store, f, i, t) -> {
            AsyncOperation<T> operation = AsyncOperation.operationFor(store, scope, map);
            t.add(operation);
            ((AccordCommandStore) store).executor().execute(operation);
            return t;
        }, reduce);
    }

    @Override
    protected <S> void forEach(ToLongBiFunction<StoreGroup, S> select, S scope, Consumer<? super CommandStore> forEach)
    {
        mapReduce(select, scope, store -> {
            forEach.accept(store);
            return null;
        }, (Void i1, Void i2) -> null);
    }
}
