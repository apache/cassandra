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

package org.apache.cassandra.service.accord.execution;

import org.junit.Assert;

import accord.local.ExecutionContext;
import accord.local.SafeState;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.ManualExecutor;

import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.LockMode.RELEASE_QUEUE;

public class AccordExecutionTestUtils
{
    public static <K, V, S extends SafeState<V> & SaferState<K, V, S>> AccordCacheEntry<K, V, S> loaded(K key, V value)
    {
        AccordCacheEntry<K, V, S> global = new AccordCacheEntry<>(key, null);
        global.initialize(value);
        return global;
    }

    private static <P1, P2> AccordCacheEntry.LoadExecutor<P1, P2> loadExecutor(ExecutorPlus executor)
    {
        return new AccordCacheEntry.LoadExecutor<>()
        {
            @Override
            public <K, V> IOTask load(P1 p1, P2 p2, AccordCacheEntry<K, V, ?> entry)
            {
                executor.submit(() -> {
                    V v;
                    try { v = entry.owner.parent().adapter().load(entry.owner.commandStore, entry.key()); }
                    catch (Throwable t)
                    {
                        entry.failedToLoad();
                        throw t;
                    }
                    entry.loaded(v);
                });
                return null;
            }
        };
    }

    public static <K, V, S extends SafeState<V> & SaferState<K, V, S>> void testLoad(ManualExecutor executor, S safeState, V val)
    {
        Assert.assertEquals(AccordCacheEntry.Status.WAITING_TO_LOAD, safeState.global().status());
        safeState.global().load(loadExecutor(executor), null, null);
        Assert.assertEquals(AccordCacheEntry.Status.LOADING, safeState.global().status());
        executor.runOne();
        Assert.assertEquals(AccordCacheEntry.Status.LOADED, safeState.global().status());
        safeState.preExecute(new SafeTask<>(null, (ExecutionContext.Empty)() -> "Test", null), RELEASE_QUEUE);
        Assert.assertEquals(val, safeState.current());
    }
}
