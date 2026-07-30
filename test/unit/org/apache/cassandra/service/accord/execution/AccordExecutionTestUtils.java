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

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;

import accord.local.ExecutionContext;
import accord.local.SafeState;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.ManualExecutor;

import static org.apache.cassandra.service.accord.execution.AccordCacheEntry.LockMode.UNQUEUED;


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
        preExecute(safeState);
        Assert.assertEquals(val, safeState.current());
    }

    /**
     * Lock a state for a task with no store behind it. UNQUEUED is the mode for an entry nobody has claimed: a lock is
     * otherwise only granted to a task that already holds a position, which these tests never take.
     */
    public static <K, V, S extends SafeState<V> & SaferState<K, V, S>> SafeTask<?> preExecute(S safeState)
    {
        SafeTask<?> task = new SafeTask<>(null, (ExecutionContext.Empty)() -> "Test", null, new AtomicLong());
        safeState.preExecute(task, UNQUEUED);
        owners.put(safeState, task);
        return task;
    }

    /**
     * The task that locked this state: the entry expects the same task to release it.
     *
     * <p>Throws rather than returning null for a state that was never locked. A null owner is the untracked
     * {@code release(x, null)} form these tests were deliberately moved away from, so silently returning it would let a
     * forgotten {@link #preExecute} revert to it - and the release would then not be checked against the lock holder at
     * all, which is the property the {@code owner()} call sites exist to check.
     */
    public static SafeTask<?> owner(Object safeState)
    {
        SafeTask<?> owner = owners.get(safeState);
        if (owner == null)
            throw new IllegalStateException(safeState + " was never locked by preExecute, so it has no owner to release it");
        return owner;
    }

    /** drop every recorded owner; called from an @After so the registry does not retain a JVM's worth of test state */
    public static void clearOwners()
    {
        owners.clear();
    }

    /**
     * Identity-keyed, because a safe state is identified by reference here (two references to one entry are distinct
     * owners), and synchronised because nothing stops a suite calling {@link #preExecute} from more than one thread.
     */
    private static final Map<Object, SafeTask<?>> owners = Collections.synchronizedMap(new IdentityHashMap<>());
}
